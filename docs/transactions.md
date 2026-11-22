# Transactions, versions and compare-and-swap

## The model

SwiftCache transactions are **strictly serializable**, using optimistic concurrency control in
the style of FoundationDB:

1. **Read version.** The first read fetches the latest committed version `R` from the
   coordinator. Every read in the transaction sees the database exactly as of `R`.
2. **Buffered writes.** `set`, `clear` and `add` are buffered in the client. Later reads in the
   same transaction see them (read-your-writes).
3. **Commit.** The client sends its read version, read-conflict keys and ranges, write keys and
   mutations to the coordinator. The resolver checks whether any transaction that committed
   *after* `R` wrote a key or range this transaction read:
   - if not, the transaction commits at version `C > R` and gets the versionstamp `(C, order)`;
   - if so, it fails with `notCommitted` and nothing is written.
4. **Retry.** `db.transaction { }` reruns the whole closure with a fresh read version on
   retryable errors, with exponential backoff and jitter.

Because the retry reruns your closure, **keep side effects inside the transaction** (or make
them idempotent).

```swift
try await db.transaction { tr in
    let stock = try await tr.get("stock/sku-1", as: Int.self) ?? 0
    guard stock > 0 else { throw OutOfStock() }      // non-SwiftCache errors are not retried
    try tr.set("stock/sku-1", stock - 1)
    try tr.set("orders/\(orderID)", order)
}
```

### Conflicts in detail

| Operation | Read conflict | Write conflict |
| --- | --- | --- |
| `get`, `getVersioned` | the key | — |
| `get(..., snapshot: true)` | none | — |
| `getRange` | the range read; only up to the last key returned when the result is truncated by `limit` | — |
| `set`, `clear` | — | the key |
| `add` | **none**: concurrent adds never conflict | the key |
| `compareAndSwap` | the key (it's a read) | the key |
| `addReadConflict` / `addWriteConflict` | manual | manual |

Read-only transactions always succeed: their snapshot is consistent by construction, so they
skip the resolver.

## Versionstamps

Every transaction that commits writes gets a versionstamp:

```
| 8 bytes: commit version (big-endian) | 2 bytes: order within the commit batch (big-endian) |
```

- Versionstamps are unique and totally ordered. If A commits before B, `A.versionstamp <
  B.versionstamp`.
- Every key a transaction writes records that versionstamp. `getVersioned` returns it.
- The byte encoding sorts the same way as the versionstamp, so it is usable inside keys
  (e.g. building an event log).
- `Versionstamp.description` is 20 hex characters. `Versionstamp(hex:)` parses it, and
  `Codable` uses the hex form.

```swift
let stamp = try await db.set("k", "v")                  // the commit's versionstamp
let entry = try await db.getVersioned("k")!             // entry.versionstamp == stamp
let (_, s) = try await db.transactionWithVersionstamp { tr in ... }
let tr = db.createTransaction(); ...; let s2 = try await tr.commit()
```

## Compare-and-swap on key versions

```swift
let entry = try await database.getVersioned("users/123")

try await database.compareAndSwap(
    key: "users/123",
    expectedVersion: entry.versionstamp,
    newValue: updatedUser
)
```

- Succeeds only if the key's current versionstamp equals `expectedVersion`. It returns the
  key's new versionstamp.
- `expectedVersion: nil` means "the key must not exist" (create-if-absent: locks, leases,
  idempotency markers).
- Otherwise it throws `SwiftCacheError.versionMismatch(key:expected:actual:)`, carrying the
  versionstamp actually found (`nil` if the key is absent or expired).
- `compareAndDelete(key:expectedVersion:)` deletes on match.

**Inside transactions**, `tr.compareAndSwap` / `tr.compareAndDelete` compose with any other
reads and writes, and everything commits atomically or not at all:

```swift
try await db.transaction { tr in
    try await tr.compareAndSwap(key: "doc/42", expectedVersion: seen, newValue: newDoc)
    try tr.set("doc/42/history/\(UUID())", change)     // only written if the CAS holds
    tr.add("stats/doc-edits", 1)
}
```

**How it's implemented.** CAS is a conflict-tracked read of the key at the read version,
followed by a write. If another transaction writes the key after the read version, the
resolver rejects the commit with `notCommitted` and the retry loop re-evaluates the comparison
with fresh data. So:

- a `versionMismatch` always reflects a real, observed change (it is never spurious);
- the CAS is still correct if the expected version came from a read hours ago, because the
  comparison happens against the current state inside the transaction.

`getVersioned` on a key the same transaction has already written throws `invalidRequest`: its
versionstamp isn't known until commit.

## Historical reads

Storage servers keep all versions within the **MVCC window** (`--mvcc-window`, default 10s), so
a read-only transaction can be pinned to any version in that window:

```swift
let snapshotVersion = try await db.readVersion()
// ... later ...
let report = try await db.read(atVersion: snapshotVersion) { tr in
    (try await tr.getRange(prefix: "orders/"), try await tr.get("totals", as: Totals.self))
}

// Or what a key looked like when a particular commit happened:
let s = try await db.set("price/sku-1", 10)
let atThatCommit = try await db.read(atVersion: s.version) { try await $0.get("price/sku-1", as: Int.self) }
```

- Older than the window → `transactionTooOld`, which is **not** retried for pinned reads.
- `tr.setReadVersion(_:)` pins a manually driven transaction. Committing writes from a pinned
  transaction is allowed: it conflicts with anything written since that version.
- A larger window costs memory for every overwritten key. Size it for your longest historical
  read.

### Should there be a "get latest version"?

Yes. SwiftCache exposes `db.readVersion()` and `tr.readVersion()`. Reasons:

- It is the same operation every transaction performs implicitly (FoundationDB's `getReadVersion`),
  so exposing it costs nothing.
- It is a **consistency token**. Record it, and later reads at that version see exactly that
  state (within the window). Pass it between services to get read-after-write consistency
  across processes, or to run several read-only transactions against one snapshot.
- It gives you "time" in versions (≈ µs), useful for computing how far back a pinned read can
  go.

It is not a substitute for a key's versionstamp. Use versionstamps for per-key optimistic
concurrency (CAS) and read versions for whole-database snapshots.

## Atomic add

`tr.add(key, delta)` adds to a little-endian Int64 (missing = 0) without reading, so it never
conflicts with other adds: ideal for counters. Read the value with `entry.int64`, or with `get`
inside the transaction (read-your-writes applies pending deltas).

## TTL

`set(..., ttl: seconds)` makes a value invisible at versions ≥ commit version + ttl. Expiry is
not a write and does not create conflicts, so a transaction that read a value which then expires
can still commit.

## Errors and retries

| Error | Retried by `transaction`? | Meaning |
| --- | --- | --- |
| `notCommitted` | yes | conflict |
| `transactionTooOld` | yes (no, for pinned versions) | read version left the MVCC window, or predates a coordinator recovery |
| `futureVersion` | yes | replicas hadn't caught up in time |
| `unavailable` | yes | recovery, no shards yet, a shard without live replicas, or a stale topology |
| `commitUnknownResult` | yes, except `db.add` | the commit was sent but no reply arrived |
| `versionMismatch` | no | CAS precondition failed |
| `tooLarge` | no | key > 10KB, value > 1MB or transaction > 10MB |

**`commitUnknownResult` and idempotency.** As in FoundationDB, a commit whose reply is lost (for
example, the coordinator crashes mid-commit) may or may not have applied, and the retry loop
runs it again. Transactions that read before writing are naturally safe: the retry sees its own
earlier effect, and CAS-style logic notices. Blind non-idempotent writes (`add`) are not, so
`db.add` surfaces the error instead of retrying. Automatic idempotency IDs are on the
[roadmap](roadmap.md).
