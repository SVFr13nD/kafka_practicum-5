#!/usr/bin/env bash
# Генерация CA, сертификатов брокеров и клиентов, keystore/truststore (PKCS12).
# Требуется OpenSSL >= 3.2 (опция -jdktrust для Java-truststore).
# Запуск из корня проекта: bash scripts/generate-certs.sh
set -euo pipefail

PASS=kafka123
DAYS=365
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CERTS="$ROOT/certs"

BROKERS=(kafka-1 kafka-2 kafka-3)
CLIENTS=(admin producer consumer)

if [ -e "$CERTS" ]; then
  echo "Каталог $CERTS уже существует. Необходимо переименовать или удалить его и повторно запустить скрипт." >&2
  exit 1
fi
# Проверяем возможность создать Java truststore до создания файлов.
command -v openssl >/dev/null || { echo "Установите OpenSSL >= 3.2" >&2; exit 1; }
if ! openssl pkcs12 -help 2>&1 | grep -q -- '-jdktrust'; then
  echo "Нужен OpenSSL >= 3.2 с опцией -jdktrust. Проверьте openssl version." >&2
  exit 1
fi
umask 022
mkdir -p "$CERTS/clients"
cd "$CERTS"

# ---------- 1. Корневой CA ----------
cat > ca.cnf <<'EOF'
[ req ]
prompt = no
distinguished_name = dn
default_md = sha256
x509_extensions = v3_ca

[ dn ]
commonName = kafka-root-ca

[ v3_ca ]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer:always
basicConstraints = critical,CA:true
keyUsage = critical,keyCertSign,cRLSign
EOF

openssl req -new -x509 -nodes -newkey rsa:4096 -days "$DAYS" \
  -config ca.cnf -keyout ca.key -out ca.crt

# Общий truststore для Java-клиентов (admin, console producer/consumer)
openssl pkcs12 -export -nokeys -in ca.crt -name ca -jdktrust anyExtendedKeyUsage \
  -out clients/client.truststore.p12 -passout pass:$PASS

# ---------- Функция выпуска сертификата ----------
# $1 - каталог, $2 - CN, $3 - extendedKeyUsage, $4 - subjectAltName (может быть пустым)
issue() {
  local dir=$1 cn=$2 eku=$3 san=${4:-}
  mkdir -p "$dir"
  {
    echo "[ req ]"
    echo "prompt = no"
    echo "distinguished_name = dn"
    echo "default_md = sha256"
    echo "req_extensions = v3_req"
    echo
    echo "[ dn ]"
    echo "commonName = $cn"
    echo
    echo "[ v3_req ]"
    echo "subjectKeyIdentifier = hash"
    echo "basicConstraints = CA:FALSE"
    echo "keyUsage = critical,digitalSignature,keyEncipherment"
    echo "extendedKeyUsage = $eku"
    if [ -n "$san" ]; then
      echo "subjectAltName = $san"
    fi
  } > "$dir/$cn.cnf"

  openssl req -new -nodes -newkey rsa:2048 -config "$dir/$cn.cnf" \
    -keyout "$dir/$cn.key" -out "$dir/$cn.csr"
  openssl x509 -req -in "$dir/$cn.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days "$DAYS" -extfile "$dir/$cn.cnf" -extensions v3_req -out "$dir/$cn.crt"
  rm -f "$dir/$cn.csr"

  # Keystore: ключ + сертификат + цепочка до CA
  openssl pkcs12 -export -in "$dir/$cn.crt" -inkey "$dir/$cn.key" \
    -chain -CAfile ca.crt -name "$cn" \
    -out "$dir/$cn.keystore.p12" -passout pass:$PASS

  openssl verify -CAfile ca.crt "$dir/$cn.crt"
}

# ---------- 2. Брокеры ----------
for b in "${BROKERS[@]}"; do
  issue "$b" "$b" "serverAuth,clientAuth" "DNS:$b,DNS:localhost"

  openssl pkcs12 -export -nokeys -in ca.crt -name ca -jdktrust anyExtendedKeyUsage \
    -out "$b/$b.truststore.p12" -passout pass:$PASS

  # Файлы с паролями для образа confluentinc/cp-kafka (без BOM и перевода строки)
  printf '%s' "$PASS" > "$b/${b}_keystore_creds"
  printf '%s' "$PASS" > "$b/${b}_sslkey_creds"
  printf '%s' "$PASS" > "$b/${b}_truststore_creds"
done

# ---------- 3. Клиенты ----------
for c in "${CLIENTS[@]}"; do
  issue "clients/$c" "$c" "clientAuth"
done

# cp-kafka работает от своего пользователя: учебные PKCS12 должны читаться
# внутри контейнера. PEM-ключи остаются доступны только владельцу.
find "$CERTS" -name '*.p12' -exec chmod 644 {} +
find "$CERTS" -name '*.key' -exec chmod 600 {} +
rm -f ca.srl
echo "Готово: сертификаты в $CERTS"

