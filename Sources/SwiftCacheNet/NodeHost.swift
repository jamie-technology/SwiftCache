import Foundation
import NIOCore
import NIOPosix
import SwiftCacheCore
import SwiftCacheServer

/// Runs a SwiftCache process in production: one SwiftNIO event loop owns every role of
/// the process, its timers and its connections, so roles see exactly the same
/// single-threaded, message-at-a-time world they see in simulation.
///
/// Routing: messages to other processes go over a connection this process dialed (one
/// per destination process, preserving per-link order as in the simulator). Messages to
/// clients (`client:` endpoints) go back over the client's own connection.
public final class NodeHost: ProcessEnvironment, @unchecked Sendable {
    public let address: Endpoint
    let group: MultiThreadedEventLoopGroup
    let loop: any EventLoop
    let startTime = NIODeadline.now()
    let logLevel: LogLevel
    public let disk: any Disk
    // Everything below is only touched on `loop`.
    var process: (any ProcessNode)?

    private enum Outbound {
        case connecting([Frame])
        case ready(any Channel)
    }
    private var outbound: [Endpoint: Outbound] = [:]
    private var clientChannels: [Endpoint: any Channel] = [:]
    private var downUntil: [Endpoint: NIODeadline] = [:]
    /// Channels written to during this event-loop pass; flushed once at the end, so many
    /// messages to one destination share a syscall and a packet.
    private var dirty: [ObjectIdentifier: any Channel] = [:]
    /// Client connections written to this pass: flushed first. A flush is a syscall that
    /// can cost ~10 µs on a bridged pod network, and replies to clients are what someone
    /// is waiting on (e.g. a commit's reply shouldn't queue behind its log notices).
    private var dirtyClients: [ObjectIdentifier: any Channel] = [:]
    private var flushScheduled = false

    private func write(_ frame: Frame, to ch: any Channel, client: Bool = false) {
        ch.write(frame, promise: nil)
        if client { dirtyClients[ObjectIdentifier(ch)] = ch } else { dirty[ObjectIdentifier(ch)] = ch }
        guard !flushScheduled else { return }
        flushScheduled = true
        loop.execute { [self] in
            self.flushScheduled = false
            let clients = self.dirtyClients, chans = self.dirty
            self.dirtyClients = [:]
            self.dirty = [:]
            for ch in clients.values { ch.flush() }
            for ch in chans.values { ch.flush() }
        }
    }

    public init(address: Endpoint, dataDirectory: String?, logLevel: LogLevel = .info) throws {
        self.address = address
        self.logLevel = logLevel
        self.group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        self.loop = group.next()
        if let dataDirectory {
            self.disk = try FileDisk(directory: dataDirectory)
        } else {
            self.disk = MemoryDisk()
        }
    }

    /// Binds `host:port`, starts the process and blocks forever. If `healthPort` is set,
    /// serves `GET /ready` (200 when `isReady` is true, 503 otherwise) and `GET /live`.
    public func run(bindHost: String, port: Int, healthPort: Int? = nil, isReady: (@Sendable () -> Bool)? = nil,
                    makeProcess: @escaping @Sendable (any ProcessEnvironment) -> any ProcessNode) throws -> Never {
        if let healthPort {
            let loop = self.loop
            _ = try ServerBootstrap(group: group)
                .serverChannelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .childChannelInitializer { channel in
                    channel.eventLoop.makeCompletedFuture {
                        try channel.pipeline.syncOperations.addHandler(HealthHandler(check: {
                            // Role state lives on the process loop (the health server shares it).
                            loop.preconditionInEventLoop()
                            return isReady?() ?? true
                        }))
                    }
                }
                .bind(host: bindHost, port: healthPort).wait()
        }
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
        log(.info, "\(address) listening on \(bindHost):\(port)")
        try loop.submit { [self] in
            let p = makeProcess(self)
            self.process = p
            p.start()
        }.wait()
        try server.closeFuture.wait()
        exit(1)
    }

    // MARK: Inbound

    private func inbound(_ channel: any Channel, _ frame: Frame) {
        // A client endpoint is (re)defined on each connection it uses, so registering on
        // definition tracks the client's current connection without a lookup per frame.
        if frame.fromIsNew, frame.from.isClientEndpoint { clientChannels[frame.from] = channel }
        process?.receive(frame.message, from: frame.from, to: frame.to)
    }

    private func inboundClosed(_ channel: any Channel) {
        for (ep, ch) in clientChannels where ch === channel { clientChannels[ep] = nil }
    }

    // MARK: ProcessEnvironment

    public var now: UInt64 {
        UInt64((NIODeadline.now() - startTime).nanoseconds / 1000) + 1
    }

    public var versionClock: UInt64 {
        UInt64(Date().timeIntervalSince1970 * 1_000_000)
    }

    public func send(_ message: Message, from: Endpoint, toAll: [Endpoint]) {
        guard toAll.count > 1 else { return toAll.forEach { send(message, from: from, to: $0) } }
        // Encode the message once; each connection's frame reuses the bytes.
        var w = WireWriter(capacity: 256)
        message.encode(to: &w)
        let body = w.bytes
        for to in toAll { send(message, from: from, to: to, encoded: body) }
    }

    public func send(_ message: Message, from: Endpoint, to: Endpoint) {
        send(message, from: from, to: to, encoded: nil)
    }

    private func send(_ message: Message, from: Endpoint, to: Endpoint, encoded: Bytes?) {
        loop.preconditionInEventLoop()
        if to.isClientEndpoint {
            if let ch = clientChannels[to.processAddress] {
                var frame = Frame(from: from, to: to, message: message)
                frame.encodedMessage = encoded
                write(frame, to: ch, client: true)
            }
            return
        }
        let dest = to.processAddress
        if dest == address {
            // Local delivery (roles on the same process), asynchronously like the network.
            loop.execute { [self] in self.process?.receive(message, from: from, to: to) }
            return
        }
        var frame = Frame(from: from, to: to, message: message)
        frame.encodedMessage = encoded
        switch outbound[dest] {
        case .ready(let ch)?:
            write(frame, to: ch)
        case .connecting(var queue)?:
            queue.append(frame)
            outbound[dest] = .connecting(queue)
        case nil:
            if let until = downUntil[dest], until > .now() { return }  // drop, like a lost packet
            dial(dest, first: frame)
        }
    }

    private func dial(_ dest: Endpoint, first: Frame) {
        guard let (host, port) = parseEndpoint(dest) else {
            log(.warning, "cannot route to \(dest)")
            return
        }
        outbound[dest] = .connecting([first])
        ClientBootstrap(group: loop)
            .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
            .connectTimeout(.milliseconds(500))
            .channelInitializer { [self] channel in
                // Replies normally arrive on the peer's own connections to us, but deliver
                // anything sent back on this one too.
                installFramePipeline(channel, onFrame: { _, frame in
                    self.process?.receive(frame.message, from: frame.from, to: frame.to)
                }, onClose: { ch in
                    if case .ready(let c)? = self.outbound[dest], c === ch { self.outbound[dest] = nil }
                })
            }
            .connect(host: host, port: port)
            .whenComplete { [self] result in
                switch result {
                case .success(let ch):
                    if case .connecting(let queue)? = self.outbound[dest] {
                        for f in queue { ch.write(f, promise: nil) }
                    }
                    ch.flush()
                    self.outbound[dest] = .ready(ch)
                case .failure(let error):
                    self.outbound[dest] = nil
                    self.downUntil[dest] = .now() + .milliseconds(200)
                    self.log(.debug, "dial \(dest) failed: \(error)")
                }
            }
    }

    public func schedule(after micros: UInt64, _ action: @escaping () -> Void) {
        // Timers always fire on `loop`, the process's only thread.
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

/// Durable small-file storage: each key is a file, written atomically (temp file, fsync,
/// rename, fsync directory).
public final class FileDisk: Disk {
    let directory: String

    public init(directory: String) throws {
        self.directory = directory
        try FileManager.default.createDirectory(atPath: directory, withIntermediateDirectories: true)
    }

    private func path(_ key: String) -> String { "\(directory)/\(key)" }

    public func read(_ key: String) -> Bytes? {
        FileManager.default.contents(atPath: path(key)).map { Bytes($0) }
    }

    public func write(_ key: String, _ value: Bytes) {
        let tmp = path(key) + ".tmp"
        let fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0o644)
        precondition(fd >= 0, "cannot write \(tmp): errno \(errno)")
        value.withUnsafeBytes { buf in
            var off = 0
            while off < buf.count {
                let n = Foundation.write(fd, buf.baseAddress! + off, buf.count - off)
                precondition(n > 0, "write failed: errno \(errno)")
                off += n
            }
        }
        precondition(fsync(fd) == 0, "fsync failed: errno \(errno)")
        close(fd)
        precondition(rename(tmp, path(key)) == 0, "rename failed: errno \(errno)")
        let dfd = open(directory, O_RDONLY)
        if dfd >= 0 { _ = fsync(dfd); close(dfd) }
    }
}

/// Non-durable disk for processes that don't need one (or tests).
public final class MemoryDisk: Disk {
    private var values: [String: Bytes] = [:]
    public init() {}
    public func read(_ key: String) -> Bytes? { values[key] }
    public func write(_ key: String, _ value: Bytes) { values[key] = value }
}

/// A minimal HTTP responder for Kubernetes probes: `/ready` reflects `check`, anything
/// else answers 200 (liveness).
final class HealthHandler: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    typealias OutboundOut = ByteBuffer
    let check: () -> Bool
    private var request = ""

    init(check: @escaping () -> Bool) { self.check = check }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buf = unwrapInboundIn(data)
        request += buf.readString(length: buf.readableBytes) ?? ""
        guard request.contains("\r\n\r\n") || request.contains("\n\n") else { return }
        let ok = request.hasPrefix("GET /ready") ? check() : true
        let body = ok ? "ok\n" : "not ready\n"
        let head = "HTTP/1.1 \(ok ? "200 OK" : "503 Service Unavailable")\r\nContent-Length: \(body.utf8.count)\r\nConnection: close\r\n\r\n"
        var out = context.channel.allocator.buffer(capacity: 128)
        out.writeString(head + body)
        let channel = context.channel
        context.writeAndFlush(wrapOutboundOut(out)).whenComplete { _ in channel.close(promise: nil) }
    }
}
