import Synchronization

/// Combines concurrent requests into batches: a caller that finds fewer than `maxInFlight`
/// batches outstanding becomes a *driver* and sends one request covering itself and
/// everyone queued so far; callers that arrive meanwhile queue for the next batch, and
/// when a batch completes the driver role passes to the next queued caller (so no
/// caller's latency grows by serving others).
///
/// Every queued item is sent after its caller started waiting, which is what makes this
/// safe for read versions (strict serializability) as well as for reads.
final class Coalescer<Item: Sendable, Output: Sendable>: Sendable {
    enum Signal: Sendable {
        case result(Result<Output, any Error>)
        case drive
    }
    private struct Waiter {
        var item: Item
        var cont: CheckedContinuation<Signal, Never>
    }
    private struct State {
        var inFlight = 0
        var queue: [Waiter] = []
    }
    private let state = Mutex(State())
    private let maxBatch: Int
    private let maxInFlight: Int
    /// Sends a batch; returns one result per item, in order.
    private let send: @Sendable ([Item]) async -> [Result<Output, any Error>]

    init(maxBatch: Int = 256, maxInFlight: Int = 1, send: @escaping @Sendable ([Item]) async -> [Result<Output, any Error>]) {
        self.maxBatch = maxBatch
        self.maxInFlight = max(1, maxInFlight)
        self.send = send
    }

    func submit(_ item: Item) async throws -> Output {
        let drive = state.withLock { s -> Bool in
            if s.inFlight >= maxInFlight { return false }
            s.inFlight += 1
            return true
        }
        if !drive {
            let signal = await withCheckedContinuation { (c: CheckedContinuation<Signal, Never>) in
                let queued = state.withLock { s -> Bool in
                    if s.inFlight >= maxInFlight {
                        s.queue.append(Waiter(item: item, cont: c))
                        return true
                    }
                    s.inFlight += 1  // a driver finished meanwhile: become one
                    return false
                }
                if !queued { c.resume(returning: .drive) }
            }
            if case .result(let r) = signal { return try r.get() }
        }
        // Driver: our item plus everything queued so far.
        let others = state.withLock { s -> [Waiter] in
            let n = min(s.queue.count, maxBatch - 1)
            let b = Array(s.queue.prefix(n))
            s.queue.removeFirst(n)
            return b
        }
        let results = await send([item] + others.map(\.item))
        for (i, w) in others.enumerated() { w.cont.resume(returning: .result(results[i + 1])) }
        let next = state.withLock { s -> CheckedContinuation<Signal, Never>? in
            if s.queue.isEmpty {
                s.inFlight -= 1
                return nil
            }
            return s.queue.removeFirst().cont
        }
        next?.resume(returning: .drive)
        return try results[0].get()
    }
}

/// A weak reference that can be captured in `@Sendable` closures.
final class WeakRef<T: AnyObject>: @unchecked Sendable {
    weak var value: T?
    init() {}
}
