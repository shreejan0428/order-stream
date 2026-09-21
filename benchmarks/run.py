"""Run a bounded local workload through the real broker and database."""

import json
import os
import platform
import statistics
import time
import uuid
from pathlib import Path

from orderstream import broker, storage


def main():
    name = "benchmark-" + uuid.uuid4().hex
    os.environ["KAFKA_TOPIC"] = name
    storage.initialize()
    broker.initialize_topic()
    create_times = []
    orders = []
    start = time.perf_counter()
    for number in range(100):
        before = time.perf_counter()
        orders.append(storage.create_order(name + str(number), 1000))
        create_times.append((time.perf_counter() - before) * 1000)
    for order in orders[:25]:
        storage.cancel_order(order["id"])
    write_seconds = time.perf_counter() - start
    producer = broker.producer()
    start = time.perf_counter()
    while broker.publish_pending(producer):
        pass
    relay_seconds = time.perf_counter() - start
    start = time.perf_counter()
    consumed = broker.consume(name, name, max_messages=125, timeout=60)
    consume_seconds = time.perf_counter() - start
    assert consumed == 125
    totals = storage.summary(name)
    assert totals["orders"] == 100 and totals["cancelled_orders"] == 25
    assert totals["active_amount_cents"] == 75000
    start = time.perf_counter()
    replayed = broker.consume(name + "-replay", name, max_messages=125, timeout=60)
    replay_seconds = time.perf_counter() - start
    assert replayed == 125
    replay_totals = storage.summary(name)
    assert replay_totals["active_amount_cents"] == 75000
    assert replay_totals["duplicate_deliveries"] == 125
    report = {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "note": "100 synthetic orders and 25 cancellations; local single broker, sequential publisher. Consumer timings include group startup. Not a saturation benchmark.",
        "orders": 100,
        "events": 125,
        "create_median_ms": statistics.median(create_times),
        "create_p95_ms": sorted(create_times)[94],
        "write_seconds": write_seconds,
        "relay_seconds": relay_seconds,
        "consume_seconds": consume_seconds,
        "replay_seconds": replay_seconds,
        "totals": {
            key: value for key, value in replay_totals.items() if key != "projection"
        },
    }
    Path("benchmarks/results.json").write_text(
        json.dumps(report, indent=2, default=int)
    )
    print(json.dumps(report, indent=2, default=int))


if __name__ == "__main__":
    main()
