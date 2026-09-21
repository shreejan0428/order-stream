import hashlib
import json
import os
import uuid
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


def connect():
    return psycopg.connect(
        os.getenv("DATABASE_URL", "postgresql://orders:orders@localhost:55432/orders"),
        row_factory=dict_row,
    )


def initialize():
    with connect() as database:
        database.execute(Path(__file__).with_name("schema.sql").read_text())


def event_payload(order):
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": str(order["id"]),
        "schema_version": 1,
        "version": order["version"],
        "status": order["status"],
        "amount_cents": order["amount_cents"],
    }


def add_outbox(database, order):
    event = event_payload(order)
    database.execute(
        "INSERT INTO outbox(event_id,order_id,payload,topic) VALUES(%s,%s,%s,%s)",
        (
            event["event_id"],
            event["order_id"],
            Jsonb(event),
            os.getenv("KAFKA_TOPIC", "order-events"),
        ),
    )


def create_order(request_key, amount_cents):
    if not isinstance(request_key, str) or not 1 <= len(request_key) <= 100:
        raise ValueError("Request key must contain 1–100 characters")
    if type(amount_cents) is not int or not 1 <= amount_cents <= 100_000_000:
        raise ValueError("Amount must be an integer from 1 to 100000000 cents")
    with connect() as database:
        # Concurrent retries wait on the same key before reading or creating an order.
        database.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (request_key,)
        )
        old = database.execute(
            "SELECT * FROM orders WHERE request_key=%s", (request_key,)
        ).fetchone()
        if old:
            if old["amount_cents"] != amount_cents:
                raise ValueError("Request key was already used with a different amount")
            return old
        order = database.execute(
            """INSERT INTO orders(id,request_key,amount_cents,status,version)
            VALUES(%s,%s,%s,'created',1) RETURNING *""",
            (uuid.uuid4(), request_key, amount_cents),
        ).fetchone()
        add_outbox(database, order)
        return order


def cancel_order(order_id):
    with connect() as database:
        order = database.execute(
            "SELECT * FROM orders WHERE id=%s FOR UPDATE", (order_id,)
        ).fetchone()
        if order is None:
            raise LookupError("Order not found")
        if order["status"] == "cancelled":
            return order
        order = database.execute(
            "UPDATE orders SET status='cancelled',version=version+1 WHERE id=%s RETURNING *",
            (order_id,),
        ).fetchone()
        add_outbox(database, order)
        return order


def validate_event(event):
    required = {
        "event_id",
        "order_id",
        "schema_version",
        "version",
        "status",
        "amount_cents",
    }
    if not isinstance(event, dict) or set(event) != required:
        raise ValueError("Event fields do not match schema version 1")
    for field in ("event_id", "order_id"):
        if not isinstance(event[field], str):
            raise ValueError(f"{field} must be a UUID string")
        try:
            uuid.UUID(event[field])
        except ValueError:
            raise ValueError(f"{field} must be a UUID string")
    if type(event["schema_version"]) is not int or event["schema_version"] != 1:
        raise ValueError("Unsupported schema version")
    if (
        type(event["amount_cents"]) is not int
        or not 1 <= event["amount_cents"] <= 100_000_000
    ):
        raise ValueError("Invalid amount")
    if (
        not isinstance(event["status"], str)
        or type(event["version"]) is not int
        or (event["status"], event["version"]) not in {("created", 1), ("cancelled", 2)}
    ):
        raise ValueError("Invalid order state or version")
    return event


def apply_event(event, projection="live"):
    event = validate_event(event)
    digest = hashlib.sha256(json.dumps(event, sort_keys=True).encode()).hexdigest()
    with connect() as database:
        database.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (projection + event["order_id"],),
        )
        inserted = database.execute(
            """INSERT INTO processed_events(projection,event_id,digest,outcome)
            VALUES(%s,%s,%s,'pending') ON CONFLICT DO NOTHING RETURNING event_id""",
            (projection, event["event_id"], digest),
        ).fetchone()
        if not inserted:
            old = database.execute(
                "SELECT digest FROM processed_events WHERE projection=%s AND event_id=%s",
                (projection, event["event_id"]),
            ).fetchone()
            if old["digest"] != digest:
                raise ValueError("Event ID reused with different content")
            database.execute(
                """INSERT INTO processing_metrics(projection,duplicate_deliveries) VALUES(%s,1)
                ON CONFLICT(projection) DO UPDATE
                SET duplicate_deliveries = processing_metrics.duplicate_deliveries + 1""",
                (projection,),
            )
            return "duplicate"
        old = database.execute(
            "SELECT * FROM order_projection WHERE projection=%s AND order_id=%s",
            (projection, event["order_id"]),
        ).fetchone()
        if old and old["amount_cents"] != event["amount_cents"]:
            raise ValueError("Order amount cannot change")
        outcome = "stale" if old and old["version"] >= event["version"] else "applied"
        if outcome == "applied":
            # Events are full snapshots, so version 2 can safely arrive before version 1.
            database.execute(
                """INSERT INTO order_projection(projection,order_id,amount_cents,status,version)
                VALUES(%s,%s,%s,%s,%s) ON CONFLICT(projection,order_id) DO UPDATE
                SET amount_cents=excluded.amount_cents,status=excluded.status,version=excluded.version""",
                (
                    projection,
                    event["order_id"],
                    event["amount_cents"],
                    event["status"],
                    event["version"],
                ),
            )
        database.execute(
            "UPDATE processed_events SET outcome=%s WHERE projection=%s AND event_id=%s",
            (outcome, projection, event["event_id"]),
        )
        return outcome


def record_failure(projection, message, error, attempts):
    with connect() as database:
        database.execute(
            """INSERT INTO failed_events(projection,topic,partition_id,offset_id,raw_value,error,attempts)
            VALUES(%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
            (
                projection,
                message.topic(),
                message.partition(),
                message.offset(),
                (message.value() or b"").decode("utf-8", errors="replace"),
                str(error)[:1000],
                attempts,
            ),
        )


def summary(projection="live"):
    with connect() as database:
        result = database.execute(
            """SELECT COUNT(*) AS orders,
            COUNT(*) FILTER(WHERE status='created') AS active_orders,
            COUNT(*) FILTER(WHERE status='cancelled') AS cancelled_orders,
            COALESCE(SUM(amount_cents) FILTER(WHERE status='created'),0) AS active_amount_cents
            FROM order_projection WHERE projection=%s""",
            (projection,),
        ).fetchone()
        result["projection"] = projection
        result["processed_events"] = database.execute(
            "SELECT COUNT(*) AS n FROM processed_events WHERE projection=%s",
            (projection,),
        ).fetchone()["n"]
        result["failed_events"] = database.execute(
            "SELECT COUNT(*) AS n FROM failed_events WHERE projection=%s", (projection,)
        ).fetchone()["n"]
        metrics = database.execute(
            "SELECT duplicate_deliveries FROM processing_metrics WHERE projection=%s",
            (projection,),
        ).fetchone()
        result["duplicate_deliveries"] = (
            metrics["duplicate_deliveries"] if metrics else 0
        )
        result["pending_outbox"] = database.execute(
            "SELECT COUNT(*) AS n FROM outbox WHERE published_at IS NULL"
        ).fetchone()["n"]
        return result
