import sys
from confluent_kafka import Consumer, KafkaError
from ssl_config import ssl_config

# Имя топика можно передать после имени файла.
topic = sys.argv[1] if len(sys.argv) > 1 else "topic-1"
consumer = Consumer({
    **ssl_config("consumer"),
    "group.id": "consumer-group",
    "auto.offset.reset": "earliest",
})
consumer.subscribe([topic])
print("Читаю", topic, "(выход: Ctrl+C)")

try:
    while True:
        message = consumer.poll(1)
        if message is None:
            continue
        if message.error():
            if message.error().code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                print("Нет прав на чтение:", topic)
                break
            print("Ошибка Kafka:", message.error())
            continue

        value = message.value()
        print("Сообщение:", value.decode("utf-8", errors="replace") if value is not None else "<пустое>")
except KeyboardInterrupt:
    print("\nОстановлено")
finally:
    consumer.close()
