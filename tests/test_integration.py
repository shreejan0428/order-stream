"""Uses isolated event topics and projections; never truncates application tables."""

import json
import os
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from orderstream import broker, storage
from orderstream.api import app

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="Set RUN_INTEGRATION=1 with PostgreSQL/Kafka running",
    ),
]


@pytest.fixture
def run(monkeypatch):
    name = "test-" + uuid.uuid4().hex
    monkeypatch.setenv("KAFKA_TOPIC", name)
    storage.initialize()
    broker.initialize_topic()
    yield name
    # Keep topics available for inspection; they contain test data only.


def fetch_event(order_id):
    with storage.connect() as database:
        return database.execute(
            "SELECT payload FROM outbox WHERE order_id=%s ORDER BY sequence DESC",
            (order_id,),
        ).fetchone()["payload"]


def test_concurrent_requests_and_atomic_outbox(run):
    key = uuid.uuid4().hex
    with ThreadPoolExecutor(max_workers=8) as workers:
        orders = list(workers.map(lambda _: storage.create_order(key, 1500), range(16)))
    assert len({order["id"] for order in orders}) == 1
    with storage.connect() as database:
        assert (
            database.execute(
                "SELECT COUNT(*) AS n FROM outbox WHERE order_id=%s", (orders[0]["id"],)
            ).fetchone()["n"]
            == 1
        )
    with pytest.raises(ValueError):
        storage.create_order(key, 1800)


def test_duplicate_and_out_of_order_snapshots(run):
    order = storage.create_order(uuid.uuid4().hex, 2300)
    created = fetch_event(order["id"])
    storage.cancel_order(order["id"])
    cancelled = fetch_event(order["id"])
    assert storage.apply_event(cancelled, run) == "applied"
    assert storage.apply_event(created, run) == "stale"
    with ThreadPoolExecutor(max_workers=4) as workers:
        assert set(
            workers.map(lambda _: storage.apply_event(cancelled, run), range(8))
        ) == {"duplicate"}
    totals = storage.summary(run)
    assert totals["orders"] == 1 and totals["active_amount_cents"] == 0
    assert totals["duplicate_deliveries"] == 8
    altered = {**cancelled, "amount_cents": 10}
    with pytest.raises(ValueError):
        storage.apply_event(altered, run)


def test_rollback_if_outbox_write_fails(run, monkeypatch):
    key = uuid.uuid4().hex

    def fail(*args):
        raise RuntimeError("test outbox failure")

    monkeypatch.setattr(storage, "add_outbox", fail)
    with pytest.raises(RuntimeError):
        storage.create_order(key, 200)
    with storage.connect() as database:
        assert (
            database.execute(
                "SELECT * FROM orders WHERE request_key=%s", (key,)
            ).fetchone()
            is None
        )


def test_api_retry_and_cancel(run):
    with TestClient(app) as client:
        body = {"request_key": uuid.uuid4().hex, "amount_cents": 3200}
        first = client.post("/api/orders", json=body)
        assert first.status_code == 200
        assert client.post("/api/orders", json=body).json()["id"] == first.json()["id"]
        assert (
            client.post("/api/orders", json={**body, "amount_cents": 10}).status_code
            == 409
        )
        assert (
            client.post("/api/orders", json={**body, "amount_cents": True}).status_code
            == 422
        )
        path = "/api/orders/" + first.json()["id"] + "/cancel"
        assert client.post(path).json()["version"] == 2
        assert client.post(path).json()["version"] == 2
        assert client.get("/").status_code == 200


def test_kafka_crash_recovery_failure_queue_and_replay(run):
    order = {"id": uuid.uuid4()}
    payload = {
        "event_id": str(uuid.uuid4()),
        "order_id": str(order["id"]),
        "schema_version": 1,
        "version": 1,
        "status": "created",
        "amount_cents": 4200,
    }
    client = broker.producer()
    errors = []
    client.produce(
        run,
        key=str(order["id"]),
        value=json.dumps(payload),
        on_delivery=lambda error, message: errors.append(error) if error else None,
    )
    assert client.flush(15) == 0 and not errors
    command = [
        sys.executable,
        "-m",
        "orderstream.cli",
        "consume",
        "--group",
        run,
        "--projection",
        run,
        "--timeout",
        "30",
        "--max-messages",
        "1",
    ]
    crashed = subprocess.run(command + ["--crash-after-apply"], timeout=45)
    assert crashed.returncode == 86
    assert storage.summary(run)["active_amount_cents"] == 4200
    recovered = subprocess.run(command, timeout=45)
    assert recovered.returncode == 0
    assert storage.summary(run)["duplicate_deliveries"] == 1
    assert storage.summary(run)["active_amount_cents"] == 4200
    client.produce(run, key=str(order["id"]), value="{bad json")
    assert client.flush(15) == 0
    assert broker.consume(run, run, max_messages=1, timeout=30) == 1
    assert storage.summary(run)["failed_events"] == 1
    rebuilt = run + "-rebuild"
    assert broker.consume(rebuilt, rebuilt, max_messages=2, timeout=30) == 2
    assert (
        storage.summary(rebuilt)["active_amount_cents"]
        == storage.summary(run)["active_amount_cents"]
    )
    assert storage.summary(rebuilt)["orders"] == storage.summary(run)["orders"]
    assert broker.consumer_lag(rebuilt)["total_lag"] == 0


def test_outbox_relay_publishes_acknowledged_event(run):
    order = storage.create_order(uuid.uuid4().hex, 1700)
    client = broker.producer()
    while broker.publish_pending(client, batch_size=100):
        pass
    with storage.connect() as database:
        assert (
            database.execute(
                "SELECT published_at FROM outbox WHERE order_id=%s", (order["id"],)
            ).fetchone()["published_at"]
            is not None
        )
    broker.consume(run, run, timeout=8)
    with storage.connect() as database:
        assert (
            database.execute(
                "SELECT * FROM order_projection WHERE projection=%s AND order_id=%s",
                (run, order["id"]),
            ).fetchone()["amount_cents"]
            == 1700
        )


def test_transient_retry_exhaustion_is_recorded(run, monkeypatch):
    import psycopg

    class Message:
        def value(self):
            return b"{}"

        def topic(self):
            return run

        def partition(self):
            return 0

        def offset(self):
            return 99

    attempts = []

    def unavailable(*args):
        attempts.append(1)
        raise psycopg.OperationalError("temporary connection failure")

    monkeypatch.setattr(broker, "apply_event", unavailable)
    assert broker.handle_message(Message(), run) == "failed"
    assert len(attempts) == 3
    with storage.connect() as database:
        row = database.execute(
            "SELECT attempts FROM failed_events WHERE projection=%s", (run,)
        ).fetchone()
        assert row["attempts"] == 3
