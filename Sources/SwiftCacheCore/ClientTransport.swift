/// How the client library talks to the cluster. The production implementation uses
/// SwiftNIO; the simulator provides one backed by its virtual network and clock, so the
/// real client code (retry loops, routing, read-your-writes) runs inside simulations.
public protocol ClientTransport: Sendable {
    /// Sends a request built from a fresh request id to `endpoint` and returns the reply
    /// carrying the same id. Throws `TransportError` on timeout or connection failure.
    func request(to endpoint: Endpoint, timeout: UInt64, _ make: (UInt64) -> Message) async throws -> Message

    /// Suspends for `micros` microseconds of (possibly virtual) time.
    func sleep(micros: UInt64) async throws

    /// Current (possibly virtual) time in microseconds.
    func now() -> UInt64

    /// A random number in `0..<upperBound` (seeded in simulation).
    func random(_ upperBound: UInt64) -> UInt64
}

public enum TransportError: Error, Sendable, Equatable, CustomStringConvertible {
    case timedOut(Endpoint)
    case connectionFailed(Endpoint, String)
    case closed

    public var description: String {
        switch self {
        case .timedOut(let e): "request to \(e) timed out"
        case .connectionFailed(let e, let why): "connection to \(e) failed: \(why)"
        case .closed: "transport closed"
        }
    }
}
