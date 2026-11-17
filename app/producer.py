import sys
from confluent_kafka import Producer
from ssl_config import ssl_config

# Сертификат producer дает право записи в оба топика.
producer = Producer({**ssl_config("producer"), "acks": "all", "enable.idempotence": False})
errors = []


def delivery_report(error, message):
    if error:
        errors.append(str(error))
        print("Ошибка отправки:", error)
    else:
        print("Отправлено в", message.topic(), ":", message.value().decode())


# Отправляем по три сообщения в каждый топик.
for topic in ["topic-1", "topic-2"]:
    for number in range(1, 4):
        text = f"Привет, Kafka! Сообщение {number}"
        producer.produce(topic, value=text.encode("utf-8"), callback=delivery_report)

# Ждем подтверждения от брокеров, а не просто постановки в очередь.
remaining = producer.flush(15)
if remaining or errors:
    print("Не удалось отправить все сообщения. Проверьте кластер, SSL и ACL.")
    sys.exit(1)
