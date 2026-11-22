# Swift client library

```swift
// Package.swift
.package(url: "<your git remote>/SwiftCache.git", branch: "main"),
.target(name: "MyApp", dependencies: [.product(name: "SwiftCache", package: "SwiftCache")]),
```

```swift
import SwiftCache
let db = try await Database.connect(coordinator: "swiftcache-coordinator.swiftcache.svc.cluster.local:7400")
```

The client fetches the cluster topology from the coordinator and talks to storage pods
directly, so it must be able to resolve and reach the pod DNS names. Run it inside the cluster.
`Database` is `Sendable`; create one per process and share it.

## Keys and values

- Keys: `String` (UTF-8), `Substring`, `[UInt8]` or `Data` (anything `CacheKey`).
- Values: `String` (UTF-8), `[UInt8]`/`Data` (raw), any other `Codable` (JSON).
- `VersionedEntry` has `bytes`, `string`, `int64`, `decode(T.self)`, `key`/`keyString` and
  `versionstamp`.

## `Database`

| Method | Description |
| --- | --- |
| `transaction { tr in … }` | Serializable transaction with automatic retry. Returns the closure's result. |
| `transactionWithVersionstamp { tr in … }` | Same, plus the commit's versionstamp. |
| `read(atVersion:) { tr in … }` | Read-only transaction pinned to a historical version. |
| `createTransaction()` | Manually driven transaction (you call `commit()`; no retries). |
| `readVersion()` | Latest committed version (a consistency token). |
| `get(key)`, `get(key, as:)` | Read raw / decoded. |
| `getVersioned(key)` | Value plus the versionstamp of its last write. |
| `set(key, value, ttl:)` | Write; returns the versionstamp. |
| `clear(key)` | Delete; returns the versionstamp. |
| `add(key, delta)` | Atomic Int64 add (doesn't retry unknown results). |
| `compareAndSwap(key:expectedVersion:newValue:ttl:)` | Write iff the key's versionstamp matches (`nil` = absent). |
| `compareAndDelete(key:expectedVersion:)` | Delete iff the versionstamp matches. |
| `getRange(prefix:limit:)`, `getRange(from:to:limit:)` | Ordered range reads across all shards. |
| `status()`, `storageStats(endpoint)` | Admin and introspection. |

## `Transaction`

| Method | Description |
| --- | --- |
| `get(key, snapshot:)`, `get(key, as:, snapshot:)` | Read; `snapshot: true` adds no conflict. |
| `getVersioned(key, snapshot:)` | Read with versionstamp. |
| `getRange(prefix:limit:snapshot:)`, `getRange(from:to:limit:snapshot:)` | Range read merged with local writes. |
| `set(key, value, ttl:)`, `set(key, bytes:, ttl:)` | Buffered write. |
| `clear(key)` | Buffered delete. |
| `add(key, delta)` | Buffered atomic add (no read conflict). |
| `compareAndSwap(key:expectedVersion:newValue:ttl:)` | Conditional write; throws `versionMismatch`. |
| `compareAndDelete(key:expectedVersion:)` | Conditional delete. |
| `readVersion()` / `setReadVersion(_:)` | Get / pin the snapshot version. |
| `addReadConflict(key)` / `addWriteConflict(key)` | Manual conflict ranges. |
| `commit()` | Commit (returns the versionstamp, or nil for read-only). |
| `committedVersionstamp` | After commit. |

Transactions are `Sendable`, so you can issue reads concurrently:

```swift
try await db.transaction { tr in
    async let a = tr.get("a", as: Int.self)
    async let b = tr.get("b", as: Int.self)
    try tr.set("sum", (try await a ?? 0) + (try await b ?? 0))
}
```

## Patterns

**Distributed lock / lease with versionstamps**

```swift
// acquire
let stamp = try await db.compareAndSwap(key: "locks/report", expectedVersion: nil, newValue: me, ttl: 30)
// renew (only if we still hold it)
let renewed = try await db.compareAndSwap(key: "locks/report", expectedVersion: stamp, newValue: me, ttl: 30)
// release
try await db.compareAndDelete(key: "locks/report", expectedVersion: renewed)
```

**Optimistic cache-aside update**

```swift
while true {
    guard let e = try await db.getVersioned(key) else { break }
    var doc = try e.decode(Doc.self)
    doc.apply(change)
    do { try await db.compareAndSwap(key: key, expectedVersion: e.versionstamp, newValue: doc); break }
    catch SwiftCacheError.versionMismatch { continue }   // someone else won; re-read
}
```

**Co-locate related keys** with Redis-style hash tags. `{user:42}:profile` and
`{user:42}:sessions` always land on the same shard.

**Consistent multi-read snapshot across requests**

```swift
let v = try await db.readVersion()               // hand `v` to the next request
let page2 = try await db.read(atVersion: v) { try await $0.getRange(from: cursor, to: end, limit: 100) }
```

## Testing your own code against a simulated cluster

`SwiftCacheSim` exposes the simulator, and `Database(coordinator:transport:)` accepts its
transport, so you can run your own logic against a faulty simulated cluster deterministically
(see `Sources/SwiftCacheSim/Workloads.swift`).
