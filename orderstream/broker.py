import json
import logging
import os
import time

import psycopg
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from orderstream.storage import apply_event, connect, record_failure

logger = logging.getLogger(__name__)


def settings():
    return {
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:19092"),
        "broker.address.family": "v4",
    }


def topic():
    return os.getenv("KAFKA_TOPIC", "order-events")


def initialize_topic():
    admin = AdminClient(settings())
    future = admin.create_topics(
        [NewTopic(topic(), num_partitions=3, replication_factor=1)]
    )[topic()]
    try:
        future.result()
    except KafkaException as error:
        from confluent_kafka import KafkaError

        if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def producer():
    return Producer(
        {**settings(), "enable.idempotence": True, "message.timeout.ms": 10000}
    )


def publish_pending(client, batch_size=50):
    published = 0
    for _ in range(batch_size):
        with connect() as database:
            event = database.execute("""SELECT * FROM outbox WHERE published_at IS NULL
                ORDER BY sequence FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if event is None:
                break
            errors = []

            def record_delivery(error, message):
                if error is not None:
                    errors.append(error)

            client.produce(
                event["topic"],
                key=str(event["order_id"]),
                value=json.dumps(event["payload"]),
                on_delivery=record_delivery,
            )
            remaining = client.flush(12)
            if remaining or errors:
                raise RuntimeError(f"Kafka publication was not acknowledged: {errors}")
            # A crash before this update leaves a retryable outbox row.
            database.execute(
                "UPDATE outbox SET published_at=now() WHERE sequence=%s",
                (event["sequence"],),
            )
            published += 1
    return published


def handle_message(message, projection):
    for attempt in range(1, 4):
        try:
            event = json.loads(message.value())
            return apply_event(event, projection)
        except (ValueError, TypeError, UnicodeDecodeError) as error:
            record_failure(projection, message, error, attempt)
            return "failed"
        except (
            psycopg.OperationalError,
            psycopg.errors.DeadlockDetected,
            psycopg.errors.SerializationFailure,
        ) as error:
            if attempt == 3:
                # If PostgreSQL is unavailable, this also fails and no offset is committed.
                record_failure(projection, message, error, attempt)
                return "failed"
            time.sleep(0.2 * attempt)


def consume(
    group="orders-live",
    projection="live",
    max_messages=0,
    timeout=0,
    crash_after_apply=False,
):
    client = Consumer(
        {
            **settings(),
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "session.timeout.ms": 6000,
            "heartbeat.interval.ms": 2000,
        }
    )
    client.subscribe([topic()])
    count = 0
    started = time.monotonic()
    try:
        while not timeout or time.monotonic() - started < timeout:
            message = client.poll(1)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            outcome = handle_message(message, projection)
            logger.info(
                "event topic=%s partition=%s offset=%s outcome=%s",
                message.topic(),
                message.partition(),
                message.offset(),
                outcome,
            )
            if crash_after_apply and outcome == "applied":
                # Test hook: leave the database committed and the Kafka offset uncommitted.
                os._exit(86)
            client.commit(message=message, asynchronous=False)
            count += 1
            if max_messages and count >= max_messages:
                break
    finally:
        client.close()
    return count


def consumer_lag(group="orders-live"):
    """Inspect committed offsets without joining or changing the consumer group."""
    from confluent_kafka import TopicPartition

    client = Consumer({**settings(), "group.id": group, "enable.auto.commit": False})
    try:
        metadata = client.list_topics(topic(), timeout=5)
        info = metadata.topics.get(topic())
        if info is None or info.error:
            raise ValueError("Topic is not ready")
        partitions = [
            TopicPartition(topic(), partition) for partition in info.partitions
        ]
        offsets = client.committed(partitions, timeout=5)
        results = []
        for partition in offsets:
            low, high = client.get_watermark_offsets(partition, timeout=5)
            committed = partition.offset if partition.offset >= 0 else low
            results.append(
                {
                    "partition": partition.partition,
                    "committed": partition.offset,
                    "high_watermark": high,
                    "lag": max(0, high - committed),
                }
            )
        return {
            "group": group,
            "partitions": results,
            "total_lag": sum(row["lag"] for row in results),
        }
    finally:
        client.close()
