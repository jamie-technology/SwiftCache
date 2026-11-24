# Architecture

SwiftCache follows FoundationDB's architecture: a small coordinator quorum holds the root
of authority, an elected **cluster controller** manages the cluster, and a **transaction
system** made of separate roles (master, commit proxies, resolver, transaction logs)
commits transactions that **storage servers** then pull and serve. The transaction system
runs in *generations*. When any of its roles fails, the cluster doesn't repair it in place:
it recovers into a new generation, fencing the old one.

```
     ┌──────── coordinators (3 or 5; quorum; durable) ────────┐
     │ leader election (leases)  ·  generation register (Paxos)│
     └───────────────┬──────────────────────────┬──────────────┘
          elects     │                          │ read / write CoreState (fence)
                     ▼                          │
            ┌──────────────────┐   recruits     │
            │ cluster controller│──────────────▶ master (one per generation; sequencer)
            │ workers · failure │                 │ versions
            │ detection · data  │                 ▼
            │ distribution      │◀─ meta commits─ commit proxies ──resolve──▶ resolver
            └──────────────────┘                  │ push every version
                                                  ▼
                                   transaction logs (all of a generation; a commit is on every one)
                                                  │ storage servers pull their tag, pop when applied
                                                  ▼
                          storage servers (shards × replicas; MVCC; serve reads directly to clients)
```

## Roles, authority and fencing

| Role | Count | Authority | Fenced by | State |
| --- | --- | --- | --- | --- |
| **Coordinator** | 3 or 5 (configured) | Elects the controller; stores `CoreState` (current generation and its logs) in a single-decree Paxos register | Ballots: a coordinator never accepts a write below a ballot it has promised | Register persisted to disk (fsync) |
| **Cluster controller** | 1, elected | Worker registry, failure detection, recruiting masters, data distribution, publishing cluster info | Its lease. It steps down before coordinators can elect another. The hard fence is that it can only change things through a generation, and old generations are locked | Rebuilt from worker registrations and the recovered shard map |
| **Master** | 1 per generation | Runs recovery, allocates commit versions (forwarding batches to the resolver), tracks the committed version from the logs' durability reports, serves read versions under log leases | Must write `CoreState` with its ballot before the generation goes live. A newer recovery's promise makes that write fail | In memory |
| **Commit proxy** | N per generation | Batches commits, routes mutations to logs, writes idempotency records | Pushes go to logs that refuse once locked. Read versions require every log to confirm it's unlocked | Transient |
| **Resolver** | 1 per generation | Decides conflicts in version order. Relays metadata changes between proxies | Rejects read versions from before its generation started | Per generation |
| **Transaction log** | L per generation | Durability: a version is committed when *every* log of the generation has it. Grants the master's read-version lease | Locked by the next recovery (whose lock reply waits out the lease) | In memory; releases data every storage tag has popped, keeping only unsuperseded shard-map metadata |
| **Storage server** | shards × replicas | Holds data and serves multi-version reads | Only ever applies versions the logs report as committed | In memory |

Every role is its own state machine with its own lifecycle. A *process* (one
`swiftcache-server`) hosts any number of roles, and which roles share a process is a
deployment decision. Process classes say what the cluster may recruit where: `stateless`
(controller candidate, master, proxies, resolvers), `log` (transaction logs), `storage`
(one storage server). Any process can additionally be a coordinator.

Role endpoints look like `host:port#proxy-3f2a…`. The id is fresh for every incarnation,
so a message to a role that no longer exists gets a definitive "no such role" reply from
its process. Recovery uses this to tell a dead log (safe to give up on) from an
unreachable one (must wait).

## The commit path

1. A client gets a **read version** from the **master**, in one round trip. The master
   answers from memory while it holds a **lease** from every log of its generation (renewed
   every 75 ms, 300 ms long). A log that is asked to lock for a recovery stops accepting
   pushes at once, but holds back its lock reply until the lease it granted has expired. A
   superseded master's lease therefore always runs out before a new generation can go live,
   and it can never hand out a stale read version. (Proxies can also serve read versions,
   confirming with every log per request, if a client has no master address.)
2. The client reads directly from storage replicas at that version, buffering writes locally.
3. The client sends the commit to a commit proxy. Concurrent commits from one client process
   share a `commitBatch` message; each is still resolved and stamped individually.
4. The proxy pipelines batches continuously: a new batch starts as soon as the previous one
   has its resolution, taking everything that arrived meanwhile, so batch size adapts to
   load and no commit waits on a timer. For each batch it sends one `versionAndResolve` to
   the master. The master allocates `(prevVersion, version)` strictly in the proxy's
   request order and forwards the batch's conflict ranges to the resolver, which runs on
   the master's process (no extra network hop). The resolver decides each transaction in
   version order and replies **directly to the proxy** with the version, the verdicts, and
   any metadata other proxies committed in between. The first proxy is recruited onto the
   master's process too, so for commits through it this whole step is local.
5. The proxy tags each mutation with the tags of the storage servers that own its slot (both
   owners during a migration) and pushes the version to **every** log. Logs append strictly
   in `prevVersion` order. Pushes are one-way: a log answers only to refuse (it is locked or
   not ready), and the proxy re-pushes any batch the master hasn't declared committed
   within a request timeout (a repeat of a version a log has is a no-op).
6. Each log reports its durable version **to the master**. The master's committed version is
   the minimum durable version across the generation's logs. The master notifies proxies,
   which reply to their clients, and logs, which make the version visible to storage. A
   client is therefore only told "committed" once every log has the commit **and** the
   master, which hands out read versions, knows it.
7. Storage servers subscribe to ("peek") their tag on a log. The subscription streams: the
   log sends a reply (same request id) whenever the tag has new committed data, a waiting
   read needs a newer version, or the replica would otherwise fall more than 1 ms behind,
   and a heartbeat at least every `peekTimeout`. Each reply carries the version it
   continues from; a storage server applies only a contiguous continuation of what it has,
   and resubscribes after a gap (a lost reply) or silence. A read that arrives before its version raises the
   held peek's needed version with a one-way `tlogNeed`, rather than replacing the peek.
   Logs only expose committed versions, so storage never applies anything a recovery could
   discard.

**Single-key gets** take one round trip. The client asks the master for a read version R
and, in parallel, asks a replica for its latest applied (committed) value and version A.
If A ≥ R, that value is returned: the read is linearized at A, a committed state no older
than any commit acknowledged before the call, observed before the call returns. Otherwise
the client falls back to a read at R. Multi-key transactions always read one snapshot.

A commit has two possible outcomes per transaction: committed with versionstamp
`(version, order)`, or failed (`not_committed`, `transaction_too_old`, `unavailable`). If
the client hears nothing, the outcome is unknown; see [idempotency](transactions.md#idempotency).

## Recovery (starting a generation)

The controller recruits a master whenever it takes office or a role of the current
generation fails (detected through missed worker registrations). It also recruits one when
redundancy can be restored, for example 3 logs again after running on 2. The master then:

1. **Reads `CoreState`** from a coordinator majority with a fresh ballot. This returns the
   current generation and its logs, and promises that coordinators won't accept writes from
   older ballots.
2. **Locks the old generation's logs.** A commit needs every log, so locking any one stops
   the old generation. Every acknowledged commit is on every log. The **recovery version**
   is therefore the minimum durable version among the locked logs: everything at or below it
   is on all of them, and nothing above it was acknowledged.
   - Logs that are unreachable but might be alive: wait for them, or proceed if at least one
     log answered.
   - Every old log definitively gone (all restarted): unapplied commits are lost (in-memory,
     like losing every replica of a shard). The cluster continues from what storage has
     applied, after first waiting out one read-version lease period, since no surviving log
     can delay it for the old master's lease.

   A lock reply is held until the read-version lease that log granted has expired, which
   adds up to 300 ms to recovery but guarantees the old master has stopped serving read
   versions.
3. **Rebuilds the shard map** from the metadata tag of a locked log, and **recruits new
   logs** seeded with the old logs' unreleased data (everything ≤ the recovery version that
   storage hasn't popped yet), plus a resolver.
4. **Writes `CoreState`** (new generation, new logs) with the same ballot. If a newer
   recovery started meanwhile, a coordinator majority promised a higher ballot and the write
   fails: this master is fenced and stops. At most one master can ever complete a given
   generation.
5. **Recruits commit proxies** and tells the controller the generation is live. The
   controller publishes the new roles to every process and halts leftovers.

The first new version is above every version any old log ever saw, so versionstamps are
never reused. Storage servers aren't touched by recovery: they switch to the new logs and
continue from the version they had applied. The resolver's conflict history starts fresh,
so transactions whose read version predates the generation fail with
`transaction_too_old` and retry.

**Liveness backstop.** A master that sees versions handed out but nothing committing for
`recoveryTimeout` ends its generation, and the controller recovers. Whatever wedged the
pipeline, the cluster can't get stuck.

## Leader election

Every stateless process runs a candidate. Coordinators each nominate the best recent
candidate (preferring one that is heartbeating as leader, so coordinators outside the
electing majority converge on it) and keep nominating it while it heartbeats (a lease of
`leaseTimeout`). A
candidate nominated by a majority then confirms with a heartbeat round, and becomes
controller once a majority acknowledges it. (A nomination's lease runs from when the
coordinator granted it, which may be long before the election completes; a heartbeat
refreshes it on receipt.) The leader measures its lease from when it **sent** each
heartbeat, so its view never outlives the coordinators', and it checks the lease before
handling any event, so a leader that wakes from a stall past its lease steps down before
acting. It steps
down (halting the controller role) after `leaderStepDownTimeout` without a majority, which
is well before any coordinator would nominate someone else. A restarted coordinator
abstains for one lease period, since it may have nominated someone before it crashed.

Election is a liveness mechanism. Safety doesn't depend on it: even if two controllers
briefly coexisted, only one could get a master through the generation register, and the
other's generation would be locked.

**Without a coordinator quorum**, no controller can be elected and no generation can form.
The *running* generation keeps committing, because it doesn't need coordinators, until one
of its roles fails.

## Sharding and data distribution

- 16,384 hash slots, `slot = CRC16(key) mod 16384` (Redis Cluster's function, with
  `{hash tag}` support). Each slot is owned by one shard, and each shard has
  `replication` replicas.
- Each storage replica has a **tag**, its identity in the mutation stream. A restarted
  storage process is a new incarnation, with a new endpoint and eventually a new tag.
- The **shard map** (shards, members by tag, slot owners, migration in progress) changes
  only through **metadata operations** that the controller commits through a proxy, like
  any transaction. They are therefore ordered with all data writes, seen by every proxy at
  the same version (relayed by the resolver), delivered to the affected storage servers in
  their tag's stream, and recorded on a metadata tag that recovery replays. A shard-map
  snapshot is written at the start of each generation.

The controller's data distribution loop, one change at a time:

| Situation | Metadata op(s) |
| --- | --- |
| No shards yet and ≥ `replication` idle storage servers | `createShard` with all slots |
| Shard below `replication` and an idle server is available | `addReplica`: the new replica copies the shard from an active member, then `replicaReady` |
| ≥ `replication` idle servers | `createShard` (empty), then rebalancing fills it |
| Slot counts differ by more than 1 | `beginMigration` (dual writes; destination copies a snapshot), then `transfer` once every destination replica is ready |
| Replica gone (dead, restarted or wiped) for `failureTimeout` | `removeReplica`; for the *last* replica, wait `shardLossTimeout` first |
| Last replica of a shard removed | `reassignSlots` (empty) to the smallest shard, then `removeShard` (data loss, logged) |

Copying a snapshot: the destination buffers its tag's mutations for the moving slots,
fetches every MVCC version of those slots from a source replica (at a version ≥ the op's
version), installs them, replays buffered mutations newer than the snapshot, and skips
stream mutations the snapshot already contained. Destinations receive the full version
history, so historical reads keep working across migrations.

## Storage servers

- Apply their tag's stream strictly in version order. Reads at version `v` wait until `v`
  is applied (`future_version` after `readWaitTimeout`).
- Keep every version of each key within the MVCC window, and GC older ones (keeping the
  newest at or below the horizon). TTLs are version-based.
- A storage server that falls behind catches up from the logs, which keep its tag's data
  until it pops it. If the logs no longer have its position, or its tag was retired, it
  wipes itself and returns to the idle pool to be re-added.

## Wire protocol

Length-prefixed (4-byte big-endian) frames over TCP, each carrying `from`, `to` and a
binary-encoded `Message`. Processes always dial each other, one connection per
destination process, so per-link message order is preserved, matching the simulator's
network model. Replies to clients go back over the client's own connection.
