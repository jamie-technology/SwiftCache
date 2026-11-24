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
    /// One endpoint per lane: servers reply to the connection a client endpoint last used,
    /// so each lane's connections must be distinguishable.
    let laneEndpoints: [Endpoint]
    private let startTime = NIODeadline.now()
    private let nextRid = Atomic<UInt64>(1)
    /// Lock-free splitmix64 state for cheap, non-cryptographic randomness (replica choice,
    /// jitter). `SystemRandomNumberGenerator` is far too slow for per-request use.
    private let randomState = Atomic<UInt64>(DispatchTime.now().uptimeNanoseconds | 1)
    /// The event loops (lanes); each has its own connections to every server.
    let loops: [any EventLoop]
    private let executors: [EventLoopTaskExecutor]
    private let connectionsLock = NIOLock()
    private var connections: [[Endpoint: Connection]]
    private var sweeper: RepeatedTask?

    /// One connection to one server process.
    final class Connection: @unchecked Sendable {
        let endpoint: Endpoint
        let lock = NIOLock()
        var channel: EventLoopFuture<any Channel>?
        /// The established channel (fast path: no await on the future per request).
        var ready: (any Channel)?
        var pending: [UInt64: (deadline: UInt64, completion: @Sendable (Result<Message, any Error>) -> Void)] = [:]
        /// Only touched on the channel's event loop.
        var flushScheduled = false
        let loop: any EventLoop
        let lane: Int
        init(endpoint: Endpoint, loop: any EventLoop, lane: Int) { self.endpoint = endpoint; self.loop = loop; self.lane = lane }

        func complete(_ rid: UInt64, _ result: Result<Message, any Error>) {
            guard let entry = lock.withLock({ pending.removeValue(forKey: rid) }) else { return }
            entry.completion(result)
        }

        func failAll(_ error: any Error) {
            let all = lock.withLock { () -> [@Sendable (Result<Message, any Error>) -> Void] in
                let c = pending.values.map(\.completion)
                pending = [:]
                channel = nil
                ready = nil
                return c
            }
            for c in all { c(.failure(error)) }
        }
    }

    public init(threads: Int = 1) {
        group = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        loops = group.makeIterator().map { $0 }
        executors = loops.map(EventLoopTaskExecutor.init)
        connections = Array(repeating: [:], count: loops.count)
        let id = UUID().uuidString.lowercased()
        laneEndpoints = loops.indices.map { "client:\(id)-\($0)" }
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
        let conns = connectionsLock.withLock { connections.flatMap(\.values) }
        for c in conns { c.lock.withLock { c.channel }?.whenSuccess { $0.close(promise: nil) } }
    }

    private func connection(for address: Endpoint, lane: Int) -> Connection {
        connectionsLock.withLock {
            if let c = connections[lane][address] { return c }
            let c = Connection(endpoint: address, loop: loops[lane], lane: lane)
            connections[lane][address] = c
            return c
        }
    }

    public var laneCount: Int { loops.count }

    public func currentLane() -> Int {
        if loops.count == 1 { return 0 }
        if let current = MultiThreadedEventLoopGroup.currentEventLoop,
           let i = loops.firstIndex(where: { $0 === current }) { return i }
        return Int(random(UInt64(loops.count)))
    }

    public var taskExecutors: [any TaskExecutor] { executors }

    private func channel(for conn: Connection) -> EventLoopFuture<any Channel> {
        conn.lock.withLock {
            if let f = conn.channel { return f }
            guard let (host, port) = parseEndpoint(conn.endpoint) else {
                return group.next().makeFailedFuture(TransportError.connectionFailed(conn.endpoint, "bad endpoint"))
            }
            let f = ClientBootstrap(group: conn.loop)
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
        let conns = connectionsLock.withLock { connections.flatMap(\.values) }
        for c in conns {
            let expired = c.lock.withLock { () -> [@Sendable (Result<Message, any Error>) -> Void] in
                let rids = c.pending.filter { $0.value.deadline <= now }.map(\.key)
                return rids.compactMap { c.pending.removeValue(forKey: $0)?.completion }
            }
            for completion in expired { completion(.failure(TransportError.timedOut(c.endpoint))) }
        }
    }

    public func send(to endpoint: Endpoint, timeout: UInt64, _ make: (UInt64) -> Message,
                     completion: @escaping @Sendable (Result<Message, any Error>) -> Void) {
        let rid = nextRid.add(1, ordering: .relaxed).oldValue
        let message = make(rid)
        let conn = connection(for: endpoint.processAddress, lane: currentLane())
        let deadline = now() + timeout
        conn.lock.withLock { conn.pending[rid] = (deadline, completion) }
        if let ch = conn.lock.withLock({ conn.ready }) {
            write(message, rid: rid, endpoint: endpoint, conn: conn, ch: ch)
            return
        }
        // Not connected yet: completes (or fails) on the event loop, never inside `send`.
        let future = channel(for: conn)
        future.whenComplete { result in
            switch result {
            case .success(let ch): self.write(message, rid: rid, endpoint: endpoint, conn: conn, ch: ch)
            case .failure(let error):
                future.eventLoop.execute { conn.complete(rid, .failure(TransportError.connectionFailed(endpoint, "\(error)"))) }
            }
        }
    }

    /// Writes a request frame; write and flush bookkeeping happen on the channel's loop, so
    /// a write is always followed by a flush and writes from many tasks share one flush.
    private func write(_ message: Message, rid: UInt64, endpoint: Endpoint, conn: Connection, ch: any Channel) {
        // Encoded by the channel's FrameEncoder, on its loop, in write order.
        let frame = Frame(from: laneEndpoints[conn.lane], to: endpoint, message: message)
        let loop = ch.eventLoop
        @Sendable func onLoop() {
            ch.write(frame).whenFailure { err in
                // Possibly already failed (closed channel): defer, so completion never runs inside `send`.
                loop.execute { conn.complete(rid, .failure(TransportError.connectionFailed(endpoint, "\(err)"))) }
            }
            if !conn.flushScheduled {
                conn.flushScheduled = true
                loop.execute {
                    conn.flushScheduled = false
                    ch.flush()
                }
            }
        }
        if loop.inEventLoop { onLoop() } else { loop.execute(onLoop) }
    }

    public func spawn(_ body: @escaping @Sendable () async -> Void) {
        Task { await body() }
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

/// Runs tasks on one event loop: a task preferring it is resumed on the loop's thread, so
/// a request it sends on that loop's connections completes without crossing threads.
final class EventLoopTaskExecutor: TaskExecutor, @unchecked Sendable {
    let loop: any EventLoop
    init(_ loop: any EventLoop) { self.loop = loop }

    func enqueue(_ job: consuming ExecutorJob) {
        let job = UnownedJob(job)
        loop.execute { job.runSynchronously(on: self.asUnownedTaskExecutor()) }
    }
}
