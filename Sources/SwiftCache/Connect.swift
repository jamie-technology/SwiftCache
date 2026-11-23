import SwiftCacheNet

extension Database {
    /// Connects to a cluster over TCP given its coordinators' `host:port` addresses (the
    /// "cluster file"), e.g. `["swiftcache-coordinator-0.swiftcache.swiftcache.svc.cluster.local:4500", ...]`.
    ///
    /// The client learns proxy and storage addresses from the cluster controller, so it
    /// must be able to reach every pod (run it inside the cluster).
    public static func connect(coordinators: [String], options: DatabaseOptions = DatabaseOptions(), threads: Int = 2) async throws -> Database {
        let db = Database(coordinators: coordinators, transport: NIOClientTransport(threads: threads), options: options)
        // Fail fast if the cluster is unreachable, but wait out a recovery in progress (a
        // controller answering "recovering" is reachable, just briefly unable to route).
        var delay: UInt64 = 50_000
        let deadline = db.client.transport.now() + 15_000_000
        while true {
            do {
                _ = try await db.client.clientInfo(refresh: true)
                return db
            } catch SwiftCacheError.unavailable(let why) where why.contains("recovering") && db.client.transport.now() < deadline {
                try await db.client.transport.sleep(micros: delay)
                delay = min(delay * 2, 1_000_000)
            }
        }
    }

    /// Connects using a comma-separated coordinator list (`host:port,host:port,...`).
    public static func connect(cluster: String, options: DatabaseOptions = DatabaseOptions()) async throws -> Database {
        try await connect(coordinators: cluster.split(separator: ",").map { String($0).trimmingCharacters(in: .whitespaces) },
                          options: options)
    }
}
