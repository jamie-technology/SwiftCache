import Foundation
import NIOConcurrencyHelpers
import NIOCore
import NIOPosix
import SwiftCacheCore

/// The production `ClientTransport`: one multiplexed TCP connection per server endpoint,
/// requests matched to replies by request id.
public final class NIOClientTransport: ClientTransport, @unchecked Sendable {
    let group: MultiThreadedEventLoopGroup
    let clientEndpoint: Endpoint
    private let ownsGroup: Bool
    // Guarded by `lock`.
    private let lock = NIOLock()
    private var connections: [Endpoint: EventLoopFuture<any Channel>] = [:]
    private var pending: [UInt64: (endpoint: Endpoint, cont: CheckedContinuation<Message, any Error>)] = [:]
    private var nextRid: UInt64 = 1
    private let startTime = NIODeadline.now()

    public init(threads: Int = 1) {
        group = MultiThreadedEventLoopGroup(numberOfThreads: threads)
        ownsGroup = true
        clientEndpoint = "client:\(UUID().uuidString.lowercased())"
    }

    deinit {
        if ownsGroup { group.shutdownGracefully { _ in } }
    }

    /// Closes all connections.
    public func close() {
        let conns = lock.withLock { Array(connections.values) }
        for c in conns { c.whenSuccess { $0.close(promise: nil) } }
    }

    private func connection(to ep: Endpoint) -> EventLoopFuture<any Channel> {
        lock.withLock {
            if let f = connections[ep] { return f }
            guard let (host, port) = parseEndpoint(ep) else {
                return group.next().makeFailedFuture(TransportError.connectionFailed(ep, "bad endpoint"))
            }
            let f = ClientBootstrap(group: group)
                .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
                .connectTimeout(.seconds(2))
                .channelInitializer { [weak self] channel in
                    installFramePipeline(channel, onFrame: { _, frame in
                        guard let rid = frame.message.rid else { return }
                        self?.complete(rid, .success(frame.message))
                    }, onClose: { _ in self?.connectionClosed(ep) })
                }
                .connect(host: host, port: port)
            f.whenFailure { [weak self] _ in self?.connectionClosed(ep) }
            connections[ep] = f
            return f
        }
    }

    private func connectionClosed(_ ep: Endpoint) {
        let failed: [CheckedContinuation<Message, any Error>] = lock.withLock {
            connections[ep] = nil
            let rids = pending.filter { $0.value.endpoint.processAddress == ep }.map(\.key)
            return rids.compactMap { pending.removeValue(forKey: $0)?.cont }
        }
        for c in failed { c.resume(throwing: TransportError.connectionFailed(ep, "connection closed")) }
    }

    private func complete(_ rid: UInt64, _ result: Result<Message, any Error>) {
        guard let entry = lock.withLock({ pending.removeValue(forKey: rid) }) else { return }
        entry.cont.resume(with: result)
    }

    public func request(to endpoint: Endpoint, timeout: UInt64, _ make: (UInt64) -> Message) async throws -> Message {
        let rid = lock.withLock { () -> UInt64 in
            defer { nextRid += 1 }
            return nextRid
        }
        let message = make(rid)
        let channel: any Channel
        do {
            channel = try await connection(to: endpoint.processAddress).get()
        } catch {
            throw TransportError.connectionFailed(endpoint, "\(error)")
        }
        return try await withCheckedThrowingContinuation { cont in
            lock.withLock { pending[rid] = (endpoint, cont) }
            channel.writeAndFlush(encodeFrame(Frame(from: clientEndpoint, to: endpoint, message: message), allocator: channel.allocator)).whenFailure { [weak self] err in
                self?.complete(rid, .failure(TransportError.connectionFailed(endpoint, "\(err)")))
            }
            channel.eventLoop.scheduleTask(in: .microseconds(Int64(timeout))) { [weak self] in
                self?.complete(rid, .failure(TransportError.timedOut(endpoint)))
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
        upperBound == 0 ? 0 : UInt64.random(in: 0..<upperBound)
    }
}
