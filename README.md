# Практическая работа 5: Kafka, SSL и ACL

Учебный кластер: ZooKeeper и три Kafka-брокера. Все подключения к Kafka, включая межброкерные, используют SSL с обязательным клиентским сертификатом (mTLS). Проверка имени сервера включена.

## Что настроено

| Сертификат клиента | topic-1 | topic-2 |
|---|---|---|
| producer | Запись | Запись |
| consumer | Чтение | Чтение запрещено явным DENY |
| admin | Администрирование | Администрирование |

Каждый брокер имеет отдельный сертификат, приватный ключ, PKCS12 keystore и truststore. Имена `CN=producer` и `CN=consumer` используются в ACL. Группа потребителя — `consumer-group`. У каждого топика три раздела и три реплики. Без ACL доступ запрещен; брокеры и admin — super users.

## Запуск на другом компьютере

Нужны Docker с Compose v2, Python 3.10+ и свободные порты 9093–9095. Команды выполняются в Bash (Linux, macOS или WSL). Рекомендуется выделить Docker не менее 6 ГБ RAM.

```bash
git clone https://github.com/SVFr13nD/kafka_practicum-5.git
cd kafka_practicum-5
bash scripts/start.sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r app/requirements.txt
python app/producer.py
python app/consumer.py topic-1
```

Consumer работает до Ctrl+C. Отдельно проверить запрет:

```bash
python app/consumer.py topic-2
```

Ожидаемый результат: `Нет прав на чтение: topic-2`.

Полная проверка с ограничением времени ожидания, подтверждением записи в оба топика и проверкой отказа в чтении:

```bash
python scripts/verify.py
```

Скрипт завершится с ненулевым кодом при любой непрошедшей проверке. Реальные результаты CI находятся в [evidence/](evidence/), запуск — в [Actions](../../actions). `evidence/run.txt` указывает проверенный коммит. Полные логи Docker доступны в артефакте запуска.

Остановка: `docker compose down -v`. Данные этого учебного стенда не сохраняются после удаления контейнеров.

## Файлы

- `docker-compose.yml` — ZooKeeper и три брокера.
- `configs/broker-common.env`, `configs/kafka-*.env` — SSL, listeners и авторизация брокеров.
- `configs/*.properties` — Java-клиенты admin, producer, consumer.
- `certs/` — готовые сертификаты, ключи, keystore, truststore и файлы паролей.
- `scripts/generate-certs.sh` — повторный выпуск полного комплекта.
- `scripts/init-topics-acl.sh` — создание топиков и правил ACL.
- `app/` — настроенные Python-продюсер и консьюмер.
- `.github/workflows/verify.yml` — реальный запуск стенда и проверка задания.

## Сертификаты учебного стенда

**Ключи и пароль `kafka123` опубликованы намеренно для запуска задания. Это общедоступные учебные учетные данные, непригодные для production.** Клиент с опубликованным ключом admin может администрировать этот стенд. Порты брокеров привязаны к localhost, ZooKeeper наружу не опубликован.

Сертификаты действуют 365 дней с даты выпуска. SAN брокеров содержит `kafka-1`/`kafka-2`/`kafka-3` соответственно и `localhost`. Стенд запускается на другом компьютере без изменения файлов; клиенты запускаются на том же компьютере, что Docker. Для удаленных клиентов нужны новые SAN и advertised listeners.

Для перевыпуска нужен OpenSSL >= 3.2. Сначала остановить стенд, затем:

```bash
mv certs certs.old
bash scripts/generate-certs.sh
bash scripts/start.sh
```

Скрипт не перезаписывает существующие сертификаты. Параметр `-jdktrust` создает truststore, совместимые с Java.

