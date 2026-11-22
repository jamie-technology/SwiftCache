import NIOCore
import SwiftCacheCore

/// Wire framing: every frame is a 4-byte big-endian length followed by
/// `from` (string), `to` (string) and an encoded `Message`. Routing on `to` lets one
/// connection serve every role a process hosts.
public struct Frame: Sendable {
    public var from: Endpoint
    public var to: Endpoint
    public var message: Message
    public init(from: Endpoint, to: Endpoint, message: Message) { self.from = from; self.to = to; self.message = message }
}

public let maxFrameSize = 64 << 20

public func encodeFrame(_ frame: Frame, allocator: ByteBufferAllocator) -> ByteBuffer {
    var w = WireWriter(capacity: 128)
    w.string(frame.from)
    w.string(frame.to)
    frame.message.encode(to: &w)
    var buf = allocator.buffer(capacity: w.bytes.count + 4)
    buf.writeInteger(UInt32(w.bytes.count))
    buf.writeBytes(w.bytes)
    return buf
}

struct FrameDecoder: ByteToMessageDecoder {
    typealias InboundOut = Frame

    mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard let length = buffer.getInteger(at: buffer.readerIndex, as: UInt32.self) else { return .needMoreData }
        guard Int(length) <= maxFrameSize else { throw WireError("frame too large: \(length)") }
        guard buffer.readableBytes >= 4 + Int(length) else { return .needMoreData }
        buffer.moveReaderIndex(forwardBy: 4)
        var r = WireReader(buffer.readBytes(length: Int(length))!)
        let from = try r.string()
        let to = try r.string()
        let message = try Message.decode(from: &r)
        context.fireChannelRead(wrapInboundOut(Frame(from: from, to: to, message: message)))
        return .continue
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

/// Installs the frame decoder and handler on a channel (must run on its event loop).
func installFramePipeline(_ channel: any Channel, onFrame: @escaping (any Channel, Frame) -> Void,
                          onClose: @escaping (any Channel) -> Void) -> EventLoopFuture<Void> {
    channel.eventLoop.makeCompletedFuture {
        try channel.pipeline.syncOperations.addHandlers(
            ByteToMessageHandler(FrameDecoder()),
            FrameHandler(onFrame: onFrame, onClose: onClose))
    }
}

/// Splits `host:port` (ignoring any `#role` suffix).
public func parseEndpoint(_ ep: Endpoint) -> (host: String, port: Int)? {
    let address = ep.processAddress
    guard let idx = address.lastIndex(of: ":"), let port = Int(address[address.index(after: idx)...]) else { return nil }
    return (String(address[..<idx]), port)
}
