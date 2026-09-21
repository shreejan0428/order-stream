# Validation results

Recorded September 20, 2026. PostgreSQL 17 and Apache Kafka 3.9.1 ran as real Docker containers on an arm64 Mac. The final suite ran with Python 3.11 inside the application container.

- 14 unit/integration tests passed in 18.83 seconds.
- Two upstream test-client deprecation warnings were emitted; no test failures remained.
- Ruff checks passed.
- Docker build, initialization, API, relay, and consumer startup succeeded.
- The browser created a real 2500-cent demo order. The background relay and consumer updated the live summary to one active order and 2500 cents.
- A test terminated a consumer process with exit code 86 after PostgreSQL commit and before Kafka offset commit. Restarting the group redelivered the event without changing the total.
- Tests verified concurrent request retries, atomic rollback on outbox failure, duplicate and stale snapshots, malformed-event persistence, bounded transient retries, offset lag after consumption, and replay into a fresh projection.
- A separate benchmark created 100 orders and 25 cancellations, processed 125 events, then replayed all 125 into the same projection. The final total remained 75000 cents and 125 duplicate deliveries were recorded.
- Benchmark create-order median was 7.83 ms; p95 was 12.05 ms. Relaying took 1.07 seconds; consumption 1.29 seconds; replay 1.13 seconds. Consumer timings include group startup.

See `benchmarks/results.json` for raw measurements. This is a bounded synthetic workload on one broker, not a high-throughput or multi-node availability claim. The GitHub Actions workflow is provided but has not yet run on GitHub.
