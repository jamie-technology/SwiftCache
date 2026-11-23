import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import Synchronization
import SwiftCacheCore

/// The production `ClientTransport`: one multiplexed TCP connection per server process,
/// requests matched to replies by request id.
///
/// Built for many concurrent tasks: each connection has its own lock and pending table
/// (no global lock on the request path), writes from many tasks share one flush, and
/// timeouts are enforced by a periodic sweep rather than a timer per request.
public final class NIOClientTransport: ClientTransport, @unchecked Sendable {
    let group: MultiThreadedEventLoopGroup
    let clientEndpoint: Endpoint
    private let startTime = NIODeadline.now()
    private let nextRid = Atomic<UInt64>(1)
    /// Lock-free splitmix64 state for cheap, non-cryptographic randomness (replica choice,
    /// jitter). `SystemRandomNumberGenerator` is far too slow for per-request use.
    private let randomState = Atomic<UInt64>(DispatchTime.now().uptimeNanoseconds | 1)
    private let connectionsLock = NIOLock()
    private var connections: [Endpoint: Connection] = [:]
    private var sweeper: RepeatedTask?

    /// One connection to one server process.
    final class Connection: @unchecked Sendable {
        let endpoint: Endpoint
        let lock = NIOLock()
        var channel: EventLoopFuture<any Channel>?
        /// The established channel (fast path: no await on the future per request).
        var ready: (any Channel)?
        var pending: [UInt64: (deadline: UInt64, cont: CheckedContinuation<Message, any Error>)] = [:]
        /// Only touched on the channel's event loop.
        var flushScheduled = false
        init(endpoint: Endpoint) { self.endpoint = endpoint }

        func complete(_ rid: UInt64, _ result: Result<Message, any Error>) {
            guard let entry = lock.withLock({ pending.removeValue(forKey: rid) }) else { return }
            entry.cont.resume(with: result)
        }

        func failAll(_ error: any Error) {
            let all = lock.withLock { () -> [CheckedContinuation<Message, any Error>] in
                let c = pending.values.map(\.cont)
                pending = [:]
                channel = nil
                ready = nil
                return c
            }
            for c in all { c.resume(throwing: error) }
        }
    }

    public init(threads: Int = 1) {
        group = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        clientEndpoint = "client:\(UUID().uuidString.lowercased())"
        sweeper = group.next().scheduleRepeatedTask(initialDelay: .milliseconds(20), delay: .milliseconds(20)) { [weak self] _ in
            self?.sweepTimeouts()
        }
    }

    deinit {
        sweeper?.cancel()
        group.shutdownGracefully { _ in }
    }

    /// Closes all connections.
    public func close() {
        let conns = connectionsLock.withLock { Array(connections.values) }
        for c in conns { c.lock.withLock { c.channel }?.whenSuccess { $0.close(promise: nil) } }
    }

    private func connection(for address: Endpoint) -> Connection {
        connectionsLock.withLock {
            if let c = connections[address] { return c }
            let c = Connection(endpoint: address)
            connections[address] = c
            return c
        }
    }

    private func channel(for conn: Connection) -> EventLoopFuture<any Channel> {
        conn.lock.withLock {
            if let f = conn.channel { return f }
            guard let (host, port) = parseEndpoint(conn.endpoint) else {
                return group.next().makeFailedFuture(TransportError.connectionFailed(conn.endpoint, "bad endpoint"))
            }
            let f = ClientBootstrap(group: group)
                .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
                .connectTimeout(.seconds(2))
                .channelInitializer { channel in
                    installFramePipeline(channel, onFrame: { _, frame in
                        guard let rid = frame.message.rid else { return }
                        conn.complete(rid, .success(frame.message))
                    }, onClose: { _ in
                        conn.failAll(TransportError.connectionFailed(conn.endpoint, "connection closed"))
                    })
                }
                .connect(host: host, port: port)
            f.whenFailure { error in conn.failAll(TransportError.connectionFailed(conn.endpoint, "\(error)")) }
            f.whenSuccess { ch in conn.lock.withLock { if conn.channel === f { conn.ready = ch } } }
            conn.channel = f
            return f
        }
    }

    private func sweepTimeouts() {
        let now = self.now()
        let conns = connectionsLock.withLock { Array(connections.values) }
        for c in conns {
            let expired = c.lock.withLock { () -> [CheckedContinuation<Message, any Error>] in
                let rids = c.pending.filter { $0.value.deadline <= now }.map(\.key)
                return rids.compactMap { c.pending.removeValue(forKey: $0)?.cont }
            }
            for cont in expired { cont.resume(throwing: TransportError.timedOut(c.endpoint)) }
        }
    }

    public func request(to endpoint: Endpoint, timeout: UInt64, _ make: (UInt64) -> Message) async throws -> Message {
        let rid = nextRid.add(1, ordering: .relaxed).oldValue
        let message = make(rid)
        let conn = connection(for: endpoint.processAddress)
        let ch: any Channel
        if let ready = conn.lock.withLock({ conn.ready }) {
            ch = ready
        } else {
            do {
                ch = try await channel(for: conn).get()
            } catch {
                throw TransportError.connectionFailed(endpoint, "\(error)")
            }
        }
        let deadline = now() + timeout
        let from = clientEndpoint
        let frame = encodeFrame(Frame(from: from, to: endpoint, message: message), allocator: ch.allocator)
        return try await withCheckedThrowingContinuation { cont in
            conn.lock.withLock { conn.pending[rid] = (deadline, cont) }
            // Write and flush bookkeeping both happen on the channel's loop, so a write is
            // always followed by a flush, and writes from many tasks share one flush.
            ch.eventLoop.execute {
                ch.write(frame).whenFailure { err in
                    conn.complete(rid, .failure(TransportError.connectionFailed(endpoint, "\(err)")))
                }
                if !conn.flushScheduled {
                    conn.flushScheduled = true
                    ch.eventLoop.execute {
                        conn.flushScheduled = false
                        ch.flush()
                    }
                }
            }
        }
    }

    public func sleep(micros: UInt64) async throws {
        try await Task.sleep(nanoseconds: micros * 1000)
    }

    public func now() -> UInt64 {
        UInt64((NIODeadline.now() - startTime).nanoseconds / 1000)
    }

    public func random(_ upperBound: UInt64) -> UInt64 {
        guard upperBound > 1 else { return 0 }
        var z = randomState.wrappingAdd(0x9E37_79B9_7F4A_7C15, ordering: .relaxed).newValue
        z = (z ^ (z >> 30)) &* 0xBF58_476D_1CE4_E5B9
        z = (z ^ (z >> 27)) &* 0x94D0_49BB_1331_11EB
        return (z ^ (z >> 31)) % upperBound
    }
}
