#!/usr/bin/env bash
# Создание топиков и ACL. Запускается внутри контейнера kafka-1:
#   docker exec kafka-1 bash /scripts/init-topics-acl.sh
set -euo pipefail

BS=kafka-1:19093
ADMIN=/etc/kafka/configs/admin.properties

PRODUCER=User:CN=producer
CONSUMER=User:CN=consumer
GROUP=consumer-group

echo "== Топики"
for t in topic-1 topic-2; do
  kafka-topics --bootstrap-server "$BS" --command-config "$ADMIN" \
    --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 3
done
kafka-topics --bootstrap-server "$BS" --command-config "$ADMIN" --describe --topic topic-1
kafka-topics --bootstrap-server "$BS" --command-config "$ADMIN" --describe --topic topic-2

acl() { kafka-acls --bootstrap-server "$BS" --command-config "$ADMIN" "$@"; }

echo "== topic-1: доступен продюсерам и консьюмерам"
acl --add --allow-principal "$PRODUCER" --operation Write --operation Describe --topic topic-1
acl --add --allow-principal "$CONSUMER" --operation Read  --operation Describe --topic topic-1
acl --add --allow-principal "$CONSUMER" --operation Read  --group "$GROUP"

echo "== topic-2: продюсеры пишут, консьюмеры читать не могут"
acl --add --allow-principal "$PRODUCER" --operation Write --operation Describe --topic topic-2
# Без allow-правила чтение и так запрещено (allow.everyone.if.no.acl.found=false),
# явный deny фиксирует запрет даже при случайной выдаче прав в будущем.
acl --add --deny-principal "$CONSUMER" --operation Read --topic topic-2

echo "== Итоговые ACL"
acl --list

