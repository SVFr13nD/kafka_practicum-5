#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p evidence
docker compose up -d
# Ждем доступности всех трех брокеров через SSL, не просто запуска контейнеров.
for broker in kafka-1 kafka-2 kafka-3; do
  ready=false
  for attempt in $(seq 1 40); do
    if docker compose exec -T kafka-1 kafka-broker-api-versions \
      --bootstrap-server "$broker:19093" \
      --command-config /etc/kafka/configs/admin.properties > "evidence/$broker-readiness.txt" 2>&1; then
      ready=true
      break
    fi
    sleep 3
  done
  if [ "$ready" != true ]; then
    cat "evidence/$broker-readiness.txt"
    exit 1
  fi
done
docker compose exec -T kafka-1 bash /scripts/init-topics-acl.sh | tee evidence/topics-and-acl.txt
