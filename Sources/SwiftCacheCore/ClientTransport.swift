/// How the client library talks to the cluster. The production implementation uses
/// SwiftNIO; the simulator provides one backed by its virtual network and clock, so the
/// real client code (retry loops, routing, read-your-writes) runs inside simulations.
public protocol ClientTransport: Sendable {
    /// Sends a request built from a fresh request id to `endpoint` and calls `completion`
    /// exactly once with the reply carrying the same id, or a `TransportError` on timeout
    /// or connection failure. `completion` is never called before `send` returns, and may
    /// run on any thread (on the transport's I/O thread in production).
    func send(to endpoint: Endpoint, timeout: UInt64, _ make: (UInt64) -> Message,
              completion: @escaping @Sendable (Result<Message, any Error>) -> Void)

    /// Runs `body` as a new task (on the simulator's deterministic executor in simulation).
    func spawn(_ body: @escaping @Sendable () async -> Void)

    /// Independent I/O lanes (one per event-loop thread in production). Each lane has its
    /// own connections; a client keeps per-lane batching state so a request issued on a
    /// lane's thread is sent, answered and resumed on that thread without hand-offs.
    var laneCount: Int { get }

    /// The lane of the calling thread, or some lane if the caller isn't on one.
    func currentLane() -> Int

    /// One task executor per lane: tasks preferring one run on that lane's thread.
    var taskExecutors: [any TaskExecutor] { get }

    /// Suspends for `micros` microseconds of (possibly virtual) time.
    func sleep(micros: UInt64) async throws

    /// Current (possibly virtual) time in microseconds.
    func now() -> UInt64

    /// A random number in `0..<upperBound` (seeded in simulation).
    func random(_ upperBound: UInt64) -> UInt64
}

extension ClientTransport {
    /// Sends a request built from a fresh request id to `endpoint` and returns the reply
    /// carrying the same id. Throws `TransportError` on timeout or connection failure.
    public func request(to endpoint: Endpoint, timeout: UInt64, _ make: (UInt64) -> Message) async throws -> Message {
        try await withCheckedThrowingContinuation { cont in
            send(to: endpoint, timeout: timeout, make) { cont.resume(with: $0) }
        }
    }
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
