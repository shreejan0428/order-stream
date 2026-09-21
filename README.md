# Order Stream

An order event processing system built with Python, PostgreSQL, Kafka, and FastAPI. It handles order creation and cancellation, publishes changes through a transactional outbox, and maintains a separate summary of active order amounts.

The central requirement is that retries and worker crashes must not apply the same order update twice.

## Architecture

```text
Order API → PostgreSQL orders + outbox → Relay → Kafka
                                                  ↓
                                      Python consumer
                                                  ↓
                                  PostgreSQL order summary
```

The API writes an order and its outbox event in one transaction. The relay publishes pending events and marks them sent only after Kafka acknowledges delivery. The consumer records the event ID and updates the order projection in another transaction, then commits the Kafka offset.

Delivery is at least once. Duplicate events are expected and handled through the database event ledger.

## Failure handling

- **Repeated requests:** a request key and database lock prevent concurrent retries from creating multiple orders. Reusing a key with a different amount returns a conflict.
- **Interrupted publication:** pending outbox rows remain retryable. A crash after Kafka acknowledgment can cause redelivery, which the consumer handles idempotently.
- **Consumer crashes:** a committed database update remains valid even if the corresponding Kafka offset was not committed.
- **Out-of-order events:** full snapshots and version checks prevent an older creation event from reversing a cancellation.
- **Invalid events:** malformed messages are recorded before their offsets are acknowledged. Transient database failures receive bounded retries.
- **Replay:** a new consumer group can rebuild a separate projection from retained Kafka events.

Implementation: [database operations](orderstream/storage.py), [relay and consumer](orderstream/broker.py), [schema](orderstream/schema.sql), and [design decisions](docs/DESIGN.md).

## Testing and results

The **14-test suite** covers validation, concurrent requests, transaction rollback, duplicate events, stale versions, failure storage, replay, and consumer lag. Integration tests run against real PostgreSQL and Kafka containers.

The crash-recovery test terminates a worker **after the database commit and before the Kafka offset commit**. Restarting the group redelivers the event without changing the projected total.

A local synthetic workload produced these results:

| Check | Result |
| --- | ---: |
| Orders created | 100 |
| Cancellations | 25 |
| Events processed | 125 |
| Events replayed | 125 |
| Active amount before and after replay | 75,000 cents |
| Create-order median / p95 | 7.83 / 12.05 ms |

The workload verifies behavior on a single broker; it is not a production throughput or availability benchmark. Amounts represent sample orders, not payment processing.

[Benchmark results](benchmarks/results.json) · [Integration tests](tests/test_integration.py) · [Validation details](docs/VALIDATION.md)

## Run locally

With Docker installed:

```sh
docker compose up --build -d
```

Open [localhost:8020](http://localhost:8020), or [the API documentation](http://localhost:8020/docs). Compose starts PostgreSQL, Kafka, the API, the outbox relay, and the consumer.

Submitting the same request key and amount returns the same order. Cancelling an order updates the summary asynchronously. PostgreSQL and Kafka data persist in Docker volumes.

For local Python setup, replay commands, configuration, and benchmarks, see [setup and usage](docs/SETUP.md).

## Tests

With the Docker stack running:

```sh
docker compose run --rm -e RUN_INTEGRATION=1 api pytest -q
```

GitHub Actions builds the application image, checks the Python code, and runs the integration suite with PostgreSQL and Kafka.

## Limitations

The current order lifecycle is creation followed by optional cancellation. Events contain full snapshots, rather than arbitrary state changes. The local stack uses one broker and does not test broker failover. Deduplication records are retained indefinitely, and replay depends on Kafka retention. Public deployment, authentication, and automated failed-event redrive are outside the current scope.

## References

- [Confluent Python client documentation](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [PostgreSQL transactions](https://www.postgresql.org/docs/current/tutorial-transactions.html)
