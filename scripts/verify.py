"""Проверка настоящего кластера, SSL и ACL. Запуск из корня репозитория."""
import json
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.admin import AdminClient
from ssl_config import ssl_config

out = Path("evidence")
out.mkdir(exist_ok=True)
lines = []
def log(text):
    print(text, flush=True)
    lines.append(text)
    (out / "verification.txt").write_text("\n".join(lines) + "\n")

admin = AdminClient(ssl_config("admin"))
metadata = admin.list_topics(timeout=30)
assert len(metadata.brokers) == 3, metadata.brokers
log("PASS: SSL metadata contains 3 brokers")
for topic in ("topic-1", "topic-2"):
    parts = metadata.topics[topic].partitions
    assert len(parts) == 3
    assert all(len(p.replicas) == 3 and len(p.isrs) == 3 for p in parts.values())
    log(f"PASS: {topic}: 3 partitions, replication factor 3, ISR 3")

marker = f"ssl-acl-test-{time.time_ns()}"
errors = []
delivered = []
def report(err, msg):
    if err:
        errors.append(str(err))
    else:
        delivered.append(msg.topic())
        log(f"PRODUCER {msg.topic()} partition={msg.partition()} offset={msg.offset()} value={msg.value().decode()}")
producer = Producer({**ssl_config("producer"), "acks": "all", "enable.idempotence": False})
for topic in ("topic-1", "topic-2"):
    for i in range(3):
        producer.produce(topic, value=f"{marker}-{i}", callback=report)
assert producer.flush(30) == 0
assert not errors, errors
assert sorted(delivered) == ["topic-1"] * 3 + ["topic-2"] * 3

consumer = Consumer({**ssl_config("consumer"), "group.id": "consumer-group", "auto.offset.reset": "earliest", "enable.auto.commit": False})
consumer.subscribe(["topic-1"])
received = set()
try:
    deadline = time.monotonic() + 60
    while len(received) < 3 and time.monotonic() < deadline:
        msg = consumer.poll(1)
        if msg is None:
            continue
        assert not msg.error(), str(msg.error())
        value = msg.value().decode()
        log(f"CONSUMER topic-1 partition={msg.partition()} offset={msg.offset()} value={value}")
        if value.startswith(marker):
            received.add(value)
    assert len(received) == 3, received
finally:
    consumer.close()
log("PASS: consumer reads topic-1")

consumer = Consumer({**ssl_config("consumer"), "group.id": "consumer-group", "auto.offset.reset": "earliest", "enable.auto.commit": False})
consumer.subscribe(["topic-2"])
denied = False
try:
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        msg = consumer.poll(1)
        if msg is None:
            continue
        assert msg.error(), "SECURITY FAILURE: consumer read topic-2"
        if msg.error().code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
            log(f"EXPECTED DENIAL topic-2: {msg.error()}")
            denied = True
            break
        raise AssertionError(str(msg.error()))
finally:
    consumer.close()
assert denied, "No authorization denial received within 45 seconds"
log("PASS: consumer cannot read topic-2")
log("ALL CHECKS PASSED")
