# Priority Message Queue with TTL, Fan-out & Console Dashboard (Java) (OS cource final project)

A small, thread-safe **priority message queue** with **TTL (expiry)**, optional **bounded capacity**, optional **fan‑out** (multiple consumers per topic), a periodic **garbage collector**, and a simple **console dashboard**.

## How it works
- **Message**: holds `id`, `content`, `timestamp`, `ttl` (ms), `priority` (higher = sooner), `estimatedTime` (ms). `ttl=0` means no expiry.
- **Topic**: thread-safe queue using `PriorityQueue<Message>` sorted by **priority descending**. Uses `ReentrantLock` + `Condition` (`notEmpty`, `notFull`) to block producers/consumers. `capacity>0` ⇒ bounded; `0` ⇒ unbounded. `cleanExpired()` removes expired messages.
- **Producer**: generates N messages with random TTL (0 or 5s), priority (0–9), and estimated work time (100–1100ms).
- **Consumer**: pulls the highest-priority message, drops it if expired, then sleeps for `estimatedTime` to simulate work.
- **GarbageCollector**: if `retentionPolicy` is enabled, periodically calls `cleanExpired()` on all topics.
- **Dashboard**: every 2s prints: queue sizes, average `estimatedTime` for messages with `priority>0`, and count of alive consumers.

## Run
Requirements: **Java 8+**  
Save the code as `Main.java`, then:
```bash
javac Main.java
java Main
```
You’ll see producer/consumer logs plus a periodic dashboard.

## Tune (in `main`)
- `retentionPolicy = true|false` (enable expiry cleanup)
- `fanOut = true|false` (multiple consumers per topic; **competing** consumption, no duplication)
- `capacity = 0|>0` (0 = unbounded, >0 = bounded)
- `numConsumersPerTopic` (e.g., 2 when `fanOut=true`)
- `new Producer(topic, 20)` (# messages per producer)
- GC interval: `new GarbageCollector(..., 1000)`
- Dashboard interval: `Thread.sleep(2000)` in dashboard thread
- Demo duration before stopping consumers: `Thread.sleep(10000)`

## Complexity
- `enqueue`/`dequeue`: `O(log n)` (priority queue)
- `cleanExpired`: `O(n)` per run

## Notes & limitations
- **Ordering**: among equal priorities, `PriorityQueue` does **not** guarantee FIFO by time.
- **Starvation**: low-priority messages may wait indefinitely (no aging).
- **Shutdown**: consumers are stopped via `interrupt`.
- **Fan‑out semantics**: messages are processed by **one** consumer only. For true pub/sub (each consumer gets every message), use per-subscriber queues or offsets.

## Ideas to extend
Aging or `(priority DESC, timestamp ASC)` comparator, delayed scheduling, metrics (e.g., Prometheus), backpressure/drop policies, unit tests (JUnit).
