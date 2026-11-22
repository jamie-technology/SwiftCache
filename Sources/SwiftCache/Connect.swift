import SwiftCacheNet

extension Database {
    /// Connects to a cluster over TCP. `coordinator` is `host:port`, e.g.
    /// `swiftcache-coordinator.swiftcache.svc.cluster.local:7400`.
    ///
    /// The client learns storage server addresses from the coordinator, so it must be
    /// able to reach every storage pod (run it inside the cluster, or on a network that
    /// routes to pod DNS names).
    public static func connect(coordinator: String, options: DatabaseOptions = DatabaseOptions()) async throws -> Database {
        let db = Database(coordinator: coordinator, transport: NIOClientTransport(), options: options)
        // Fail fast if the coordinator is unreachable.
        _ = try await db.client.topology(refresh: true)
        return db
    }
}
