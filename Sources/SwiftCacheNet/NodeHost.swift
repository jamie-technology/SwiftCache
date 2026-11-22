import Foundation
import NIOCore
import NIOPosix
import SwiftCacheCore
import SwiftCacheServer

/// Runs a `ServerNode` state machine in production: one SwiftNIO event loop owns the node,
/// its timers and all of its connections, so the node sees exactly the same
/// single-threaded, message-at-a-time world it sees in simulation.
///
/// Routing: messages to servers (`host:port` endpoints) always go over a connection this
/// node dialed (one per destination, so per-link ordering is preserved, as in the
/// simulator). Messages to clients (`client:` endpoints) go back over the client's
/// inbound connection.
public final class NodeHost: NodeEnvironment, @unchecked Sendable {
    public let endpoint: Endpoint
    let group: MultiThreadedEventLoopGroup
    let loop: any EventLoop
    let startTime = NIODeadline.now()
    let logLevel: LogLevel
    // Everything below is only touched on `loop`.
    var node: (any ServerNode)?

    private enum Outbound {
        case connecting([ByteBuffer])
        case ready(any Channel)
    }
    private var outbound: [Endpoint: Outbound] = [:]
    private var inboundByEndpoint: [Endpoint: any Channel] = [:]
    private var downUntil: [Endpoint: NIODeadline] = [:]

    public init(endpoint: Endpoint, logLevel: LogLevel = .info) {
        self.endpoint = endpoint
        self.logLevel = logLevel
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.loop = group.next()
    }

    /// Binds `host:port`, starts the node and blocks forever.
    public func run(bindHost: String, port: Int, makeNode: @escaping @Sendable (any NodeEnvironment) -> any ServerNode) throws -> Never {
        let bootstrap = ServerBootstrap(group: group)
            .serverChannelOption(ChannelOptions.backlog, value: 256)
            .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
            .childChannelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .childChannelInitializer { [self] channel in
                installFramePipeline(channel,
                                     onFrame: { ch, frame in self.inbound(ch, frame) },
                                     onClose: { ch in self.inboundClosed(ch) })
            }
        let server = try bootstrap.bind(host: bindHost, port: port).wait()
        log(.info, "\(endpoint) listening on \(bindHost):\(port)")
        try loop.submit { [self] in
            let node = makeNode(self)
            self.node = node
            node.start()
        }.wait()
        try server.closeFuture.wait()
        exit(1)
    }

    // MARK: Inbound

    private var channelEndpoint: [ObjectIdentifier: Endpoint] = [:]

    private func inbound(_ channel: any Channel, _ frame: Frame) {
        switch frame {
        case .hello(let ep):
            channelEndpoint[ObjectIdentifier(channel)] = ep
            inboundByEndpoint[ep] = channel
        case .message(let m):
            guard let from = channelEndpoint[ObjectIdentifier(channel)] else { return }
            node?.receive(m, from: from)
        }
    }

    private func inboundClosed(_ channel: any Channel) {
        if let ep = channelEndpoint.removeValue(forKey: ObjectIdentifier(channel)),
           inboundByEndpoint[ep] === channel {
            inboundByEndpoint[ep] = nil
        }
    }

    // MARK: NodeEnvironment

    public var now: UInt64 {
        UInt64((NIODeadline.now() - startTime).nanoseconds / 1000) + 1
    }

    public var versionClock: UInt64 {
        UInt64(Date().timeIntervalSince1970 * 1_000_000)
    }

    public func send(_ message: Message, to ep: Endpoint) {
        loop.preconditionInEventLoop()
        let buf = encodeFrame(.message(message), allocator: ByteBufferAllocator())
        if ep.hasPrefix("client:") {
            inboundByEndpoint[ep]?.writeAndFlush(buf, promise: nil)
            return
        }
        switch outbound[ep] {
        case .ready(let ch)?:
            ch.writeAndFlush(buf, promise: nil)
        case .connecting(var queue)?:
            queue.append(buf)
            outbound[ep] = .connecting(queue)
        case nil:
            if let until = downUntil[ep], until > .now() { return }  // drop, like a lost packet
            dial(ep, first: buf)
        }
    }

    private func dial(_ ep: Endpoint, first: ByteBuffer) {
        guard let (host, port) = parseEndpoint(ep) else {
            log(.warning, "cannot route to \(ep)")
            return
        }
        outbound[ep] = .connecting([first])
        let me = endpoint
        ClientBootstrap(group: loop)
            .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .connectTimeout(.milliseconds(500))
            .channelInitializer { [self] channel in
                // Replies normally come on our own inbound connections; anything a peer
                // sends back on this one is still delivered.
                installFramePipeline(channel, onFrame: { _, frame in
                    if case .message(let m) = frame { self.node?.receive(m, from: ep) }
                }, onClose: { ch in
                    if case .ready(let c)? = self.outbound[ep], c === ch { self.outbound[ep] = nil }
                })
            }
            .connect(host: host, port: port)
            .whenComplete { [self] result in
                switch result {
                case .success(let ch):
                    ch.write(encodeFrame(.hello(me), allocator: ch.allocator), promise: nil)
                    if case .connecting(let queue)? = self.outbound[ep] {
                        for b in queue { ch.write(b, promise: nil) }
                    }
                    ch.flush()
                    self.outbound[ep] = .ready(ch)
                case .failure(let error):
                    self.outbound[ep] = nil
                    self.downUntil[ep] = .now() + .milliseconds(200)
                    self.log(.debug, "dial \(ep) failed: \(error)")
                }
            }
    }

    public func schedule(after micros: UInt64, _ action: @escaping () -> Void) {
        // Timers always fire on `loop`, the node's only thread.
        let box = UncheckedBox(action)
        loop.scheduleTask(in: .microseconds(Int64(micros))) { box.value() }
    }

    public func random(_ upperBound: UInt64) -> UInt64 {
        upperBound == 0 ? 0 : UInt64.random(in: 0..<upperBound)
    }

    public func log(_ level: LogLevel, _ message: @autoclosure () -> String) {
        guard level >= logLevel else { return }
        let ts = ISO8601DateFormatter().string(from: Date())
        FileHandle.standardError.write(Data("\(ts) [\(level)] \(message())\n".utf8))
    }
}

/// Carries a non-Sendable value into a closure that runs on the same event loop.
struct UncheckedBox<T>: @unchecked Sendable {
    let value: T
    init(_ value: T) { self.value = value }
}
