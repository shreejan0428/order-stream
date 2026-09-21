# Design decisions

## Transactional outbox

Writing to PostgreSQL and Kafka independently creates a dual-write problem: one may succeed while the other fails. The API instead writes an order and its pending event together. The relay can retry later. It retains a row lock while waiting for broker acknowledgment, which is simple but limits throughput and holds a database connection. A production relay would need careful batching and backpressure.

## Idempotency and concurrency

Request keys are unique. Advisory transaction locks serialize the check-and-create path, and the database unique constraint remains the final guard. Reusing a key for different input is a conflict.

The consumer uses a separate event identity ledger for each projection. Inserting that identity and changing the projected state happen in one transaction. A retry after commit sees the stored identity and has no second business effect. A reused identity with a different payload is rejected.

## State snapshots

The current producer only emits created version 1 and cancelled version 2. Each event contains the full state and immutable amount. This allows cancellation to arrive before creation without changing the final result. Delta-only events would need gap buffering or strict sequencing; this implementation does not apply that approach to arbitrary event types.

## Recovery boundary

The important test kills the worker after apply_event returns but before consumer.commit. PostgreSQL has committed and Kafka has not. Restarting the same group redelivers the message. The projected amount remains unchanged and duplicate_deliveries increases.

Kafka producer idempotence cannot make a PostgreSQL acknowledgment and a Kafka publication atomic. The consumer's identity ledger is still needed when a relay restarts.

## Summary design

Each order has one projected row. The summary aggregates those rows instead of maintaining a second mutable global counter. This avoids another shared hot row and makes correctness easy to inspect. Aggregating all projected rows grows more expensive with collection size. A rollup table would require additional concurrency tests.

## Operational limits

The single broker does not test node loss, replication, or partitions. The consumer processes messages sequentially, commits each offset synchronously, and retries briefly. Slow database work can cause consumer rebalances; duplicate processing stays safe but throughput may drop. Deduplication rows and failed-event records need a retention policy at larger scale. Database connection pooling, migrations beyond this initial additive schema, and automated failed-event redrive remain future work.

## Interview walkthrough

Explain the three crash windows: before the database transaction commits, after commit but before publication, and after projection commit but before offset commit. Point to the tests and explain what each proves. Be ready to distinguish delivery guarantees from database business effects and to explain why snapshots make out-of-order versions manageable.
