"""Smoke test for Kafka on its own: create a topic, write messages, read them back, clean up.

    docker compose up -d kafka
    python tests/test_kafka.py
"""
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

BROKER = "localhost:29092"
TOPIC = f"smoke-test-{uuid.uuid4().hex[:8]}"
MESSAGES = 100
KEYS = ["account-1", "account-2", "account-3"]

admin = AdminClient({"bootstrap.servers": BROKER})
sent = []
received = []


def test_kafka_is_reachable():
    brokers = admin.list_topics(timeout=10).brokers
    return f"{len(brokers)} broker"


def test_topic_can_be_created():
    admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC].result(timeout=10)
    assert TOPIC in admin.list_topics(timeout=10).topics, "topic not listed after creating it"
    return TOPIC


def test_messages_are_delivered():
    producer = Producer({"bootstrap.servers": BROKER})
    delivered = []
    for i in range(MESSAGES):
        key = KEYS[i % len(KEYS)]
        value = f"{key}:{i}"
        producer.produce(TOPIC, key=key, value=value,
                         on_delivery=lambda err, msg: delivered.append(err is None))
        sent.append((key, value))
    producer.flush(10)
    assert sum(delivered) == MESSAGES, f"only {sum(delivered)}/{MESSAGES} confirmed"
    return f"{MESSAGES}/{MESSAGES}"


def test_messages_can_be_read_back():
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": f"smoke-test-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    deadline = time.time() + 30
    while len(received) < MESSAGES and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg and not msg.error():
            received.append((msg.key().decode(), msg.value().decode()))
    consumer.close()
    assert len(received) == MESSAGES, f"only {len(received)}/{MESSAGES} read back"
    return f"{MESSAGES}/{MESSAGES}"


def test_contents_are_unchanged():
    assert sorted(received) == sorted(sent), "messages read back differ from messages sent"


def test_order_is_kept_per_key():
    for key in KEYS:
        got = [v for k, v in received if k == key]
        expected = [v for k, v in sent if k == key]
        assert got == expected, f"{key} arrived out of order"
    return f"{len(KEYS)} keys"


def clean_up():
    try:
        admin.delete_topics([TOPIC])[TOPIC].result(timeout=10)
    except Exception:
        pass  # topic was never created, or Kafka is down


TESTS = [
    test_kafka_is_reachable,
    test_topic_can_be_created,
    test_messages_are_delivered,
    test_messages_can_be_read_back,
    test_contents_are_unchanged,
    test_order_is_kept_per_key,
]


def main():
    print(f"Kafka smoke test against {BROKER}\n")
    failed = False
    try:
        for test in TESTS:
            name = test.__name__.removeprefix("test_").replace("_", " ")
            if failed:
                print(f"SKIP  {name}")
                continue
            try:
                print(f"PASS  {name}  {test() or ''}")
            except Exception as e:
                print(f"FAIL  {name}  {e}")
                failed = True
    finally:
        clean_up()

    print("\nSome checks failed" if failed else "\nAll Kafka checks passed")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()