# Architecture

SwiftCache has two kinds of server process plus a client library.

```
            ┌──────────────────────────── clients (SwiftCache library) ────────────────────────────┐
            │  getReadVersion / commit / topology                       read / readRange           │
            ▼                                                                   ▼                   │
   ┌─────────────────┐   ordered batch stream (per server)    ┌────────────┐ ┌────────────┐ ┌────────────┐
   │   Coordinator   │ ─────────────────────────────────────▶ │ storage-0  │ │ storage-2  │ │ storage-4  │
   │ sequencer       │ ◀───────────────── batchAck ────────── │ storage-1  │ │ storage-3  │ │ storage-5  │
   │ resolver        │                                        │  shard 0   │ │  shard 1   │ │  shard 2   │
   │ cluster control │                                        └────────────┘ └────────────┘ └────────────┘
   └─────────────────┘                                           snapshot copies between storage servers
```

## Design principle: one codebase, two runtimes

Every server process is a `ServerNode`: a single-threaded state machine that reacts to
messages and timers and interacts with the world only through `NodeEnvironment`:

```swift
public protocol NodeEnvironment: AnyObject {
    var now: UInt64 { get }                       // monotonic µs
    var versionClock: UInt64 { get }              // wall-clock µs (seeds commit versions)
    func send(_ message: Message, to endpoint: Endpoint)
    func schedule(after micros: UInt64, _ action: @escaping () -> Void)
    func random(_ upperBound: UInt64) -> UInt64
    func log(_ level: LogLevel, _ message: @autoclosure () -> String)
}
```

- **Production** (`SwiftCacheNet.NodeHost`) runs each node on one SwiftNIO event loop: TCP,
  real clocks and system randomness.
- **Simulation** (`SwiftCacheSim.Simulator`) runs every node of a cluster on one thread with a
  virtual clock, a seeded RNG and a simulated network with faults.

The client is written against `ClientTransport` and runs in both worlds too. In simulation its
async tasks run on a custom deterministic `TaskExecutor`. See [simulation.md](simulation.md).

## Data model

- Keys and values are byte strings. The client encodes `String` as UTF-8, `[UInt8]`/`Data` raw,
  and any other `Codable` as JSON.
- **Versions** are `UInt64` commit versions assigned by the coordinator. They advance at about
  1,000,000 per second (they follow the wall clock in microseconds), so a version difference
  reads as elapsed time.
- **Versionstamps** are `(version, order)`: 8 + 2 bytes, totally ordered. `order` distinguishes
  transactions committed in the same batch.
- **MVCC.** Storage servers keep every version of a key written within the MVCC window
  (default 10s), so reads at any version in the window are exact.

## The coordinator

The coordinator combines the roles FoundationDB spreads across master, proxies, resolvers and
the cluster controller:

1. **Read versions.** `getReadVersion` returns the latest *committed* version: the highest
   version that every active replica of every shard has applied.
2. **Commit pipeline.** Every `batchInterval` (1ms) the coordinator takes the pending commit
   requests and:
   - assigns the batch a version `max(prev + 1, wall clock µs)`;
   - resolves each transaction in arrival order against the resolver's write history (see
     [transactions.md](transactions.md)), so later transactions in the batch see earlier ones;
   - gives each committed transaction a versionstamp `(version, i)`;
   - sends **every** storage server a batch for this version containing only the mutations for
     slots it stores. Empty batches still go out; they advance versions and carry the
     committed version. Up to 16 batches can be in flight.
3. **Acknowledgement.** A batch is committed once every *active* replica has acknowledged it.
   Only then are its clients told "committed", in version order.
4. **Cluster control.** Membership, failure detection, re-replication, shard formation and slot
   rebalancing (below).

## Storage servers

A storage server is one replica of one shard. It:

- applies batches strictly in version order, checking that each batch's `prevVersion` equals
  the last version it applied;
- serves `read` and `readRange` at a requested version, waiting until it has applied that
  version **and** knows it is committed. A lagging or partitioned replica therefore never
  returns stale data; at worst the client times out and tries another replica;
- garbage-collects versions older than the window, keeping the newest version at or below the
  horizon;
- copies data to new replicas and new shard owners (snapshot transfer);
- reports itself to the coordinator (`join`) when it starts or loses contact.

The store (`VersionedStore`) holds a hash map from key to an ascending version list, plus a
sorted key index (chunked sorted arrays) for range scans. TTL expiry is version-based: a value
written at `v` with `ttl` is invisible to reads at versions ≥ `v + ttl`.

## Sharding

- 16,384 hash slots. `slot = CRC16(key) mod 16384`, the same function as Redis Cluster,
  including `{hash tag}` support for co-locating related keys.
- Each slot is owned by one shard; each shard has `replication` replicas.
- **Forming shards.** New storage servers join an unassigned pool. When `replication` of them
  are available (after under-replicated shards have been topped up), they form a new empty
  shard.
- **Rebalancing.** The coordinator moves chunks of slots (2,048 at a time) from the largest
  shard to the smallest until slot counts differ by at most one. One data movement runs at a
  time.

### Moving slots (and catching up replicas)

Both use the same mechanism:

1. At batch version `Vb` the coordinator sends `beginReceive(slots, sources)` to the
   destination servers. From the *next* batch on, mutations for those slots go to both the old
   owner and the destination ("dual write"). The destination buffers them.
2. Each destination asks a source replica for a snapshot of those slots at a version ≥ `Vb`.
   The source sends **every** MVCC version it holds for them, tagged with its applied version
   `Va`.
3. The destination installs the snapshot, replays buffered mutations newer than `Va`, skips any
   later batch mutations at or below `Va`, and reports `moveReady`.
4. When every active destination replica is ready, the coordinator sends
   `transfer(slots, from, to)` in the batch stream. The old owner drops the data, the new owner
   starts serving, and clients pick up the new topology on their next `wrong_shard` error.

The destination has the full version history, so historical reads keep working across a
migration. Range reads check that the slot sets returned by every shard cover all 16,384 slots
exactly once, and retry otherwise.

## Failure handling

| Failure | Behaviour |
| --- | --- |
| Storage replica crashes | After `failureTimeout` without acknowledging a batch it owes, it is marked failed and dropped from the commit quorum. When the pod restarts it rejoins empty and catches up from a peer by snapshot. |
| Broken connection / short partition | The coordinator keeps each server's unacknowledged batches. A server that sees a gap asks for a resend and continues without losing data. |
| Last live replica of a shard stops responding | It is **not** dropped on the short timeout: commits stall rather than risk throwing away the shard's only copy. After `shardLossTimeout` (default 60s; 120s in the k8s args) the shard is declared lost, its slots are reassigned empty, and the error is logged. |
| Every replica of a shard really dies | The shard's data is lost, as with Redis without persistence. The cluster stays up. |
| Coordinator crashes | See below. |

### Coordinator recovery

The coordinator keeps no disk state; it rebuilds everything from storage servers.

1. On start it waits `recoveryWindow` for `join` reports from storage servers. Each report
   carries the server's shard, applied version, known-committed version and owned slots.
2. It picks a new epoch (higher than any reported) and computes the truncation point
   `Vr = min over shards (max applied among that shard's active replicas)`. Every version ≤ `Vr`
   is fully present in at least one replica of every shard. Every version acknowledged to a
   client is ≤ `Vr`, because acknowledged means every active replica applied it.
3. It sends `recover(epoch, Vr)`. Replicas at or ahead of `Vr` truncate to exactly `Vr`, rolling
   back unacknowledged partial batches; MVCC makes this a simple trim. Lagging replicas are
   wiped and later re-synced from a peer.
4. It finishes only once every shard has a replica confirmed at `Vr`. If replies are lost it
   resends `recover`, and if a shard never confirms it restarts recovery rather than declare the
   data lost.
5. The resolver's history is gone, so commits whose read version predates recovery fail with
   `transaction_too_old`, and clients retry. Historical *reads* still work, because storage
   keeps its history.

Writes are unavailable for about `recoveryWindow` plus a little; reads at already-known versions
keep working throughout.

Shards whose best replica is behind the highest known-committed version are treated as lost,
not used to roll the cluster back.

## Wire protocol

Length-prefixed (4-byte big-endian) frames over TCP. The first frame on a connection is
`hello(endpoint)`; every later frame is a binary-encoded `Message` (see
`SwiftCacheCore/Message.swift`). Servers always dial each other, one connection per destination,
so message order per link is preserved, matching the simulator's network model. Replies to
clients go back over the client's own connection.
