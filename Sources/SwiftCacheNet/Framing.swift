import NIOCore
import SwiftCacheCore

/// Wire framing: every frame is a 4-byte big-endian length followed by the payload.
/// A payload starting with 0xFF is a `hello` carrying the sender's endpoint (sent once
/// per connection); anything else is an encoded `Message`.
public enum Frame {
    case hello(Endpoint)
    case message(Message)
}

public let maxFrameSize = 64 << 20

public func encodeFrame(_ frame: Frame, allocator: ByteBufferAllocator) -> ByteBuffer {
    let payload: Bytes
    switch frame {
    case .hello(let ep): payload = [0xFF] + Bytes(ep.utf8)
    case .message(let m): payload = m.encode()
    }
    var buf = allocator.buffer(capacity: payload.count + 4)
    buf.writeInteger(UInt32(payload.count))
    buf.writeBytes(payload)
    return buf
}

struct FrameDecoder: ByteToMessageDecoder {
    typealias InboundOut = Frame

    mutating func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard let length = buffer.getInteger(at: buffer.readerIndex, as: UInt32.self) else { return .needMoreData }
        guard Int(length) <= maxFrameSize else { throw WireError("frame too large: \(length)") }
        guard buffer.readableBytes >= 4 + Int(length) else { return .needMoreData }
        buffer.moveReaderIndex(forwardBy: 4)
        let bytes = buffer.readBytes(length: Int(length))!
        let frame: Frame
        if bytes.first == 0xFF {
            frame = .hello(String(decoding: bytes.dropFirst(), as: UTF8.self))
        } else {
            frame = .message(try Message.decode(bytes))
        }
        context.fireChannelRead(wrapInboundOut(frame))
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

/// Splits `host:port`.
public func parseEndpoint(_ ep: Endpoint) -> (host: String, port: Int)? {
    guard let idx = ep.lastIndex(of: ":"), let port = Int(ep[ep.index(after: idx)...]) else { return nil }
    return (String(ep[..<idx]), port)
}
