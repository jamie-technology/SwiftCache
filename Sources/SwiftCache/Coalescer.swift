import Synchronization

/// Combines concurrent requests into batches. While fewer than `maxInFlight` batches are
/// outstanding, a request is sent at once (with anything already queued); otherwise it
/// queues, and the queue goes out as one batch the moment an outstanding batch completes,
/// sent directly from that batch's completion (no waiting task has to be woken first).
///
/// Every queued item is sent after its caller started waiting, which is what makes this
/// safe for read versions (strict serializability) as well as for reads.
final class Coalescer<Item: Sendable, Output: Sendable>: Sendable {
    typealias Completion = @Sendable (Result<Output, any Error>) -> Void
    typealias Batch = [(item: Item, done: Completion, at: UInt64)]

    /// Where batched requests spent their time (µs), since creation.
    struct Stats: Sendable {
        var items = 0
        var batches = 0
        /// Summed over items: from submit until its batch was sent.
        var queued: UInt64 = 0
        /// Summed over batches: from send until the reply.
        var roundTrip: UInt64 = 0
    }

    private struct State {
        var inFlight = 0
        var queue: Batch = []
        var stats = Stats()
    }
    private let state = Mutex(State())
    private let maxBatch: Int
    private let maxInFlight: Int
    private let now: @Sendable () -> UInt64
    /// Sends a batch and calls back with one result per item, in order. The callback
    /// must not run before `send` returns.
    private let send: @Sendable ([Item], @escaping @Sendable ([Result<Output, any Error>]) -> Void) -> Void

    init(maxBatch: Int = 256, maxInFlight: Int = 1, now: @escaping @Sendable () -> UInt64 = { 0 },
         send: @escaping @Sendable ([Item], @escaping @Sendable ([Result<Output, any Error>]) -> Void) -> Void) {
        self.now = now
        self.maxBatch = max(1, maxBatch)
        self.maxInFlight = max(1, maxInFlight)
        self.send = send
    }

    var stats: Stats { state.withLock { $0.stats } }

    func submit(_ item: Item, _ done: @escaping Completion) {
        let t = now()
        let batch = state.withLock { s -> Batch? in
            guard s.inFlight < maxInFlight else {
                s.queue.append((item, done, t))
                return nil
            }
            s.inFlight += 1
            return [(item, done, t)]
        }
        if let batch { dispatch(batch) }
    }

    func submit(_ item: Item) async throws -> Output {
        try await withCheckedThrowingContinuation { cont in
            submit(item) { cont.resume(with: $0) }
        }
    }

    private func dispatch(_ batch: Batch) {
        let sentAt = now()
        state.withLock { s in
            s.stats.batches += 1
            s.stats.items += batch.count
            for b in batch { s.stats.queued += sentAt &- b.at }
        }
        send(batch.map(\.item)) { [self] results in
            let rtt = now() &- sentAt
            // Send whatever queued meanwhile before delivering results: it's waiting.
            let next = state.withLock { s -> Batch? in
                s.stats.roundTrip += rtt
                if s.queue.isEmpty {
                    s.inFlight -= 1
                    return nil
                }
                let n = min(s.queue.count, maxBatch)
                let b = Array(s.queue.prefix(n))
                s.queue.removeFirst(n)
                return b
            }
            if let next { dispatch(next) }
            for (i, w) in batch.enumerated() {
                w.done(i < results.count ? results[i] : .failure(SwiftCacheError.internalError("missing batch result")))
            }
        }
    }
}

/// Holds the result of an operation started with a callback, for one waiter to await.
final class ResultBox<T: Sendable>: Sendable {
    private enum State {
        case waiting(CheckedContinuation<T, Never>?)
        case done(T)
    }
    private let state = Mutex(State.waiting(nil))

    init() {}

    func set(_ value: T) {
        let cont = state.withLock { s -> CheckedContinuation<T, Never>? in
            guard case .waiting(let c) = s else { return nil }
            s = .done(value)
            return c
        }
        cont?.resume(returning: value)
    }

    var value: T {
        get async {
            if case .done(let v) = state.withLock({ $0 }) { return v }
            return await withCheckedContinuation { c in
                let ready = state.withLock { s -> T? in
                    if case .done(let v) = s { return v }
                    s = .waiting(c)
                    return nil
                }
                if let ready { c.resume(returning: ready) }
            }
        }
    }
}

/// A weak reference that can be captured in `@Sendable` closures.
final class WeakRef<T: AnyObject>: @unchecked Sendable {
    weak var value: T?
    init() {}
}
