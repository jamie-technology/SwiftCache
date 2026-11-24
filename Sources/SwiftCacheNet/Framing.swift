import NIOCore
import SwiftCacheCore

/// Wire framing: every frame is a 4-byte big-endian length followed by `from`, `to` and an
/// encoded `Message`. Routing on `to` lets one connection serve every role a process hosts.
///
/// Endpoints are interned per connection (and per direction). An endpoint reference is a
/// little-endian UInt16 `r`:
/// - `r < 0x7FFF`: endpoint number `r`, defined earlier on this connection;
/// - `r == 0x7FFF`: a literal string follows (the table is full; it is not defined);
/// - `r >= 0x8000`: defines endpoint number `r & 0x7FFF` (always the next unused number)
///   as the string that follows.
///
/// Correctness: a connection delivers bytes in order, and the encoder runs in the
/// channel's write order. So every definition reaches the decoder before any use of its
/// number, and both tables start empty with each connection. The decoded endpoints are
/// exactly the encoded ones; messages and routing are unchanged.
public struct Frame: Sendable {
    public var from: Endpoint
    public var to: Endpoint
    public var message: Message
    /// Set by the decoder when `from` was first seen on this connection in this frame.
    public var fromIsNew = false
    public init(from: Endpoint, to: Endpoint, message: Message) { self.from = from; self.to = to; self.message = message }
}

public let maxFrameSize = 64 << 20

enum EndpointRef {
    static let literal: UInt16 = 0x7FFF
    static let define: UInt16 = 0x8000
    /// Endpoints per connection direction before falling back to literals.
    static let capacity = 0x7FFF
}

/// Encodes `Frame`s into length-prefixed bytes, interning endpoints for this connection.
final class FrameEncoder: ChannelOutboundHandler {
    typealias OutboundIn = Frame
    typealias OutboundOut = ByteBuffer

    private var ids: [Endpoint: UInt16] = [:]
    private let capacity: Int
    init(capacity: Int = EndpointRef.capacity) { self.capacity = min(capacity, EndpointRef.capacity) }
    /// The last two endpoints referenced (a connection mostly carries the same pairs):
    /// checked before hashing. String equality is a pointer compare for shared storage.
    private var recent: (Endpoint, UInt16) = ("", 0)
    private var recent2: (Endpoint, UInt16) = ("", 0)

    func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        let frame = unwrapOutboundIn(data)
        var w = WireWriter(capacity: 128)
        w.u32(0)  // length placeholder
        reference(frame.from, &w)
        reference(frame.to, &w)
        frame.message.encode(to: &w)
        var buf = context.channel.allocator.buffer(capacity: w.bytes.count)
        buf.writeBytes(w.bytes)
        buf.setInteger(UInt32(w.bytes.count - 4), at: buf.readerIndex)
        context.write(wrapOutboundOut(buf), promise: promise)
    }

    private func reference(_ ep: Endpoint, _ w: inout WireWriter) {
        if !recent.0.isEmpty, ep == recent.0 { return w.u16(recent.1) }
        if !recent2.0.isEmpty, ep == recent2.0 { swap(&recent, &recent2); return w.u16(recent.1) }
        let id: UInt16
        if let known = ids[ep] {
            id = known
            w.u16(id)
        } else if ids.count < capacity {
            id = UInt16(ids.count)
            ids[ep] = id
            w.u16(EndpointRef.define | id)
            w.string(ep)
        } else {
            w.u16(EndpointRef.literal)
            w.string(ep)
            return
        }
        recent2 = recent
        recent = (ep, id)
    }
}

struct FrameDecoder: ByteToMessageDecoder {
    typealias InboundOut = Frame

    /// Endpoints defined on this connection, by number.
    private var table: [Endpoint] = []

    mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard let length = buffer.getInteger(at: buffer.readerIndex, as: UInt32.self) else { return .needMoreData }
        guard Int(length) <= maxFrameSize else { throw WireError("frame too large: \(length)") }
        guard buffer.readableBytes >= 4 + Int(length) else { return .needMoreData }
        buffer.moveReaderIndex(forwardBy: 4)
        // Decode straight out of the receive buffer (no intermediate copy).
        let frame = try buffer.withUnsafeReadableBytes { raw -> Frame in
            var r = WireReader(UnsafeRawBufferPointer(rebasing: raw[0..<Int(length)]))
            let (from, fromIsNew) = try endpoint(&r)
            let (to, _) = try endpoint(&r)
            let message = try Message.decode(from: &r)
            guard r.isAtEnd else { throw WireError("trailing bytes in \(message.name)") }
            var f = Frame(from: from, to: to, message: message)
            f.fromIsNew = fromIsNew
            return f
        }
        buffer.moveReaderIndex(forwardBy: Int(length))
        context.fireChannelRead(wrapInboundOut(frame))
        return .continue
    }

    private mutating func endpoint(_ r: inout WireReader) throws -> (Endpoint, isNew: Bool) {
        let ref = try r.u16()
        if ref < EndpointRef.literal {
            guard Int(ref) < table.count else { throw WireError("undefined endpoint \(ref)") }
            return (table[Int(ref)], false)
        }
        let ep = try r.string()
        if ref == EndpointRef.literal { return (ep, true) }
        guard Int(ref & ~EndpointRef.define) == table.count else { throw WireError("out-of-order endpoint definition \(ref)") }
        table.append(ep)
        return (ep, true)
    }
}

/// Delivers decoded frames and connection closure to callbacks (on the channel's loop).
/// Handlers are installed with `syncOperations` from inside the channel's event loop, so
/// they never cross threads and needn't be `Sendable`.
final class FrameHandler: ChannelInboundHandler {
    typealias InboundIn = Frame
    let onFrame: (any Channel, Frame) -> Void
    let onClose: (any Channel) -> Void

    init(onFrame: @escaping (any Channel, Frame) -> Void, onClose: @escaping (any Channel) -> Void) {
        self.onFrame = onFrame
        self.onClose = onClose
    }

    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        onFrame(context.channel, unwrapInboundIn(data))
    }

    func channelInactive(context: ChannelHandlerContext) {
        onClose(context.channel)
        context.fireChannelInactive()
    }

    func errorCaught(context: ChannelHandlerContext, error: any Error) {
        context.close(promise: nil)
    }
}

/// Installs the frame codec and handler on a channel (must run on its event loop).
func installFramePipeline(_ channel: any Channel, onFrame: @escaping (any Channel, Frame) -> Void,
                          onClose: @escaping (any Channel) -> Void) -> EventLoopFuture<Void> {
    channel.eventLoop.makeCompletedFuture {
        try channel.pipeline.syncOperations.addHandlers(
            ByteToMessageHandler(FrameDecoder()),
            FrameEncoder(),
            FrameHandler(onFrame: onFrame, onClose: onClose))
    }
}

/// Splits `host:port` (ignoring any `#role` suffix).
public func parseEndpoint(_ ep: Endpoint) -> (host: String, port: Int)? {
    let address = ep.processAddress
    guard let idx = address.lastIndex(of: ":"), let port = Int(address[address.index(after: idx)...]) else { return nil }
    return (String(address[..<idx]), port)
}
