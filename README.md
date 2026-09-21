# Reliable Order Event Processor

A Python service demonstrating PostgreSQL transactions, a transactional outbox, Kafka delivery, idempotent consumers, and replayable order summaries.

The application accepts order creation and cancellation. The consumer builds a separate summary of active order amounts. These are sample order totals, not actual payments or recognized revenue. The project uses integer cents and a deliberately small state machine: created version 1, cancelled version 2.

## Run with Docker

Install Docker Desktop and run:

```sh
docker compose up --build -d
```

Open http://localhost:8020. API documentation is at http://localhost:8020/docs.

The stack starts a dedicated PostgreSQL database, a single Kafka broker, an initialization job, the API, an outbox relay, and a consumer. Ports bind to localhost. Local demonstration credentials are in Compose; there are no cloud credentials. Kafka and PostgreSQL data persist in named volumes. `docker compose down` stops the stack without deleting those volumes.

## Try the behavior

1. Create a 2500-cent order with a request key such as `demo-order-1`.
2. Submit the same key and amount again. The order ID stays the same.
3. Reuse that key with a different amount. The API returns a conflict.
4. Refresh until the consumer summary includes the order.
5. Cancel it. Refresh until active totals decrease. Cancelling it again does not create another version.

To see eventual consistency, stop the worker with `docker compose stop consumer`, create another order, then run `docker compose start consumer`. The summary catches up. It is normal for the source orders and the derived summary to differ briefly.

## Run Python locally

Python 3.11 is recommended; 3.9+ is supported.

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
docker compose up -d postgres kafka
orderstream init
uvicorn orderstream.api:app --port 8020
```

In two additional activated terminals run `orderstream relay` and `orderstream consume`. Do not run a local API on port 8020 while the Compose API is using it.

Defaults are PostgreSQL `localhost:55432`, Kafka `localhost:19092`, and topic `order-events`. Override with `DATABASE_URL`, `KAFKA_BOOTSTRAP`, and `KAFKA_TOPIC`. Each outbox row retains its destination topic so unrelated test/demo topics cannot receive each other's pending events.

## Guarantees and failure handling

- The API writes the order and outbox event in the same PostgreSQL transaction.
- Repeated create requests use a request key. An advisory lock serializes competing requests using that key.
- The relay locks pending rows with `FOR UPDATE SKIP LOCKED` and marks a row published only after Kafka acknowledges it.
- A crash between broker acknowledgment and the database update can publish a duplicate. Producer idempotence does not eliminate this cross-system window.
- The consumer records the event ID and updates the projection in one transaction. Only afterward does it commit the Kafka offset.
- Events contain complete order snapshots. A newer version supersedes an older version; stale snapshots cannot resurrect a cancelled order.
- Malformed events are stored in PostgreSQL's `failed_events` table before acknowledging their offsets. Transient database errors are retried three times. If failure storage is unavailable, processing fails without acknowledging the offset.

This is at-least-once delivery with idempotent database effects per projection. It does not claim a global exactly-once transaction across Kafka and PostgreSQL. Event deduplication assumes producer-stable event IDs and retains processed IDs indefinitely.

## Tests

```sh
pytest -q
RUN_INTEGRATION=1 pytest -q
```

The first command runs unit checks and skips external-service tests. The second requires real PostgreSQL and Kafka and tests concurrent create retries, outbox rollback, duplicate snapshots, stale versions, API validation, actual process death, offset redelivery, failed-event storage, and replay.

Tests use unique Kafka topics, request keys, and projection names; they do not truncate tables. They leave small test records/topics for inspection. Run against a disposable development stack. For a clean container-based check after starting the stack:

```sh
docker compose run --rm -e RUN_INTEGRATION=1 api pytest -q
```

## Replay

Replay into a fresh projection and a fresh consumer group:

```sh
orderstream consume --group review-rebuild-1 --projection review-rebuild-1 --timeout 30
orderstream summary --projection review-rebuild-1
```

Increase the timeout for larger histories. A timed run can stop before catching up, so compare projected counts with expected source counts before calling a rebuild complete. New groups begin at the earliest retained Kafka offset. Replay depends on Kafka retention; the outbox is retained for inspection, but there is no automatic archive restoration command.

To inspect failed records, open `/api/failures`. After fixing a malformed source event, publish a valid event with a new event ID; failed records remain an audit trail. This version does not have an automatic failed-event redrive UI.

Use `orderstream lag` to inspect committed offsets and lag for the live group. This is a read-only snapshot and does not move offsets.

## Benchmark

```sh
python benchmarks/run.py
```

This creates 100 synthetic orders and 25 cancellations, relays 125 events, consumes them, and replays them into the same projection. It checks that the final active amount remains 75000 cents and records duplicate deliveries. It writes `benchmarks/results.json`. Timings include local connection overhead and consumer startup; this is a bounded correctness workload, not a saturation or production-scale benchmark.

## Source map

- `orderstream/storage.py`: SQL transactions, validation, projection updates, and summaries.
- `orderstream/broker.py`: outbox publication, retries, consumption, and offset commits.
- `orderstream/schema.sql`: tables, constraints, and indexes.
- `orderstream/api.py`: order endpoints and browser demonstration.
- `orderstream/cli.py`: initialization, workers, and replay options.
- `tests/test_integration.py`: real broker/database failure scenarios.

See `docs/DESIGN.md` and `docs/VALIDATION.md` for decisions, evidence, and limits.

## Scope and references

The project extends the distributed streaming system topic from the supplied Python project list. The implementation uses [Confluent's Python client documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html) and [PostgreSQL transaction documentation](https://www.postgresql.org/docs/current/tutorial-transactions.html).

This local stack has one broker and no authentication, TLS, payment gateway, inventory reservation, or multi-node failover. It demonstrates worker recovery, not broker high availability. Keep it local unless you add the deployment controls required for your environment.

For the tested Python 3.11 dependency versions, install with `pip install -c requirements-tested.txt -e ".[dev]"`. The snapshot records the environment used during validation.
