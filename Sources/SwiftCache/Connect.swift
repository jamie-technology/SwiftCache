import SwiftCacheNet

extension Database {
    /// Connects to a cluster over TCP given its coordinators' `host:port` addresses (the
    /// "cluster file"), e.g. `["swiftcache-coordinator-0.swiftcache.swiftcache.svc.cluster.local:4500", ...]`.
    ///
    /// The client learns proxy and storage addresses from the cluster controller, so it
    /// must be able to reach every pod (run it inside the cluster).
    public static func connect(coordinators: [String], options: DatabaseOptions = DatabaseOptions()) async throws -> Database {
        let db = Database(coordinators: coordinators, transport: NIOClientTransport(), options: options)
        // Fail fast if the cluster is unreachable.
        _ = try await db.client.clientInfo(refresh: true)
        return db
    }

    /// Connects using a comma-separated coordinator list (`host:port,host:port,...`).
    public static func connect(cluster: String, options: DatabaseOptions = DatabaseOptions()) async throws -> Database {
        try await connect(coordinators: cluster.split(separator: ",").map { String($0).trimmingCharacters(in: .whitespaces) },
                          options: options)
    }
}
