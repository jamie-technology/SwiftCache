import NIOCore
import NIOEmbedded
import SwiftCacheCore
import Testing
@testable import SwiftCacheNet

@Suite struct FramingTests {
    /// A channel whose encoder output is fed back into its own decoder.
    private func loopback(capacity: Int = EndpointRef.capacity) throws -> EmbeddedChannel {
        let ch = EmbeddedChannel()
        try ch.pipeline.syncOperations.addHandlers(ByteToMessageHandler(FrameDecoder()), FrameEncoder(capacity: capacity))
        return ch
    }

    private func roundTrip(_ ch: EmbeddedChannel, _ frames: [Frame]) throws -> (decoded: [Frame], bytes: [Int]) {
        var sizes: [Int] = []
        for f in frames {
            try ch.writeOutbound(f)
            let buf = try #require(try ch.readOutbound(as: ByteBuffer.self))
            sizes.append(buf.readableBytes)
            try ch.writeInbound(buf)
        }
        var out: [Frame] = []
        while let f = try ch.readInbound(as: Frame.self) { out.append(f) }
        return (out, sizes)
    }

    @Test func endpointsAreInternedPerConnection() throws {
        let a = "storage-3.swiftcache.svc.cluster.local:4500#storage-1a2b3c4d"
        let b = "log-0.swiftcache.svc.cluster.local:4500#tlog-99887766"
        let c = "client:6f1d2c9e-0"
        let frames = [
            Frame(from: a, to: b, message: .tlogPop(tag: 1, version: 10)),
            Frame(from: a, to: b, message: .tlogPop(tag: 1, version: 11)),
            Frame(from: b, to: a, message: .tlogPop(tag: 2, version: 12)),
            Frame(from: c, to: a, message: .tlogPop(tag: 3, version: 13)),
            Frame(from: a, to: c, message: .tlogPop(tag: 4, version: 14)),
        ]
        let ch = try loopback()
        let (decoded, sizes) = try roundTrip(ch, frames)
        #expect(decoded.map(\.from) == frames.map(\.from))
        #expect(decoded.map(\.to) == frames.map(\.to))
        #expect(decoded.map(\.message) == frames.map(\.message))
        #expect(decoded.map(\.fromIsNew) == [true, false, false, true, false])
        // After the first frame, both endpoints are 2-byte references.
        #expect(sizes[1] == 4 + 2 + 2 + (sizes[0] - 4 - 2 - 4 - a.utf8.count - 2 - 4 - b.utf8.count))
        // A new connection starts with empty tables.
        let (fresh, _) = try roundTrip(try loopback(), [frames[1]])
        #expect(fresh.first?.fromIsNew == true && fresh.first?.to == b)
    }

    @Test func preEncodedMessagesAreIdentical() throws {
        let m = Message.tlogPop(tag: 9, version: 99)
        var w = WireWriter()
        m.encode(to: &w)
        var shared = Frame(from: "a:1#x", to: "b:2#y", message: .tlogPop(tag: 0, version: 0))
        shared.encodedMessage = w.bytes  // the bytes, not `message`, go on the wire
        let plain = Frame(from: "a:1#x", to: "b:2#y", message: m)
        let ch1 = try loopback(), ch2 = try loopback()
        try ch1.writeOutbound(shared)
        try ch2.writeOutbound(plain)
        let b1 = try #require(try ch1.readOutbound(as: ByteBuffer.self))
        let b2 = try #require(try ch2.readOutbound(as: ByteBuffer.self))
        #expect(b1 == b2)
        try ch1.writeInbound(b1)
        #expect(try ch1.readInbound(as: Frame.self)?.message == m)
    }

    @Test func fallsBackToLiteralsWhenFull() throws {
        let capacity = 8
        let ch = try loopback(capacity: capacity)
        var frames: [Frame] = []
        for i in 0..<(capacity + 10) {
            frames.append(Frame(from: "client:\(i)", to: "server:1", message: .tlogPop(tag: UInt32(i % 1000), version: UInt64(i))))
        }
        frames.append(Frame(from: "client:5", to: "server:1", message: .tlogPop(tag: 5, version: 5)))
        frames.append(Frame(from: "client:\(capacity + 3)", to: "server:1", message: .tlogPop(tag: 6, version: 6)))
        let (decoded, _) = try roundTrip(ch, frames)
        #expect(decoded.count == frames.count)
        #expect(decoded.map(\.from) == frames.map(\.from))
        #expect(decoded.map(\.message) == frames.map(\.message))
        #expect(decoded[frames.count - 2].fromIsNew == false)
    }

    private func rawFrame(_ body: (inout WireWriter) -> Void) -> ByteBuffer {
        var w = WireWriter()
        body(&w)
        Message.tlogPop(tag: 1, version: 1).encode(to: &w)
        var buf = ByteBufferAllocator().buffer(capacity: w.bytes.count + 4)
        buf.writeInteger(UInt32(w.bytes.count))
        buf.writeBytes(w.bytes)
        return buf
    }

    @Test func rejectsUndefinedReferences() throws {
        let ch = try loopback()
        let buf = rawFrame { w in w.u16(0); w.u16(1) }
        #expect(throws: (any Error).self) { try ch.writeInbound(buf) }
    }

    @Test func rejectsOutOfOrderDefinitions() throws {
        let ch = try loopback()
        let buf = rawFrame { w in w.u16(EndpointRef.define | 1); w.string("x"); w.u16(EndpointRef.literal); w.string("y") }
        #expect(throws: (any Error).self) { try ch.writeInbound(buf) }
    }
}
