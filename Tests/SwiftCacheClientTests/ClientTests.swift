import Foundation
import Testing
@testable import SwiftCache

@Suite struct ValueCodecTests {
    struct User: Codable, Equatable { var name: String }

    @Test func rawAndJSON() throws {
        #expect(try ValueCodec.encode("hi") == Array("hi".utf8))
        #expect(try ValueCodec.encode([UInt8]([1, 2])) == [1, 2])
        let json = try ValueCodec.encode(User(name: "a"))
        #expect(try ValueCodec.decode(User.self, from: json) == User(name: "a"))
        #expect(try ValueCodec.decode(String.self, from: Array("x".utf8)) == "x")
    }

    @Test func errorRetryability() {
        #expect(SwiftCacheError.notCommitted.isRetryable)
        #expect(SwiftCacheError.commitUnknownResult.isRetryable)
        #expect(!SwiftCacheError.versionMismatch(key: [], expected: nil, actual: nil).isRetryable)
    }
}
