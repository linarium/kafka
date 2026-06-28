#!/usr/bin/env bash
# Генерация CA, сертификатов брокеров и клиентов (PKCS12 keystore/truststore).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

PASSWORD="${SSL_PASSWORD:-mypass123}"
VALIDITY_DAYS="${SSL_VALIDITY_DAYS:-3650}"
CA_SUBJECT="${SSL_CA_SUBJECT:-/CN=Kafka-CA/O=Kafka/C=RU}"

echo "=== Генерация SSL-материалов (password=${PASSWORD}) ==="

mkdir -p broker1 broker2 broker3 clients/admin clients/producer clients/consumer

if [[ ! -f ca-key.pem ]]; then
  echo "Создание CA..."
  openssl req -new -x509 \
    -keyout ca-key.pem \
    -out ca-cert.pem \
    -days "${VALIDITY_DAYS}" \
    -nodes \
    -subj "${CA_SUBJECT}"
fi

create_pkcs12_truststore() {
  local out="$1"
  rm -f "${out}"
  if command -v keytool >/dev/null 2>&1; then
    keytool -importcert \
      -alias CARoot \
      -file ca-cert.pem \
      -keystore "${out}" \
      -storetype PKCS12 \
      -storepass "${PASSWORD}" \
      -noprompt
  else
    docker run --rm \
      -v "${SCRIPT_DIR}:/ssl" \
      -w /ssl \
      confluentinc/cp-kafka:7.6.1 \
      keytool -importcert \
      -alias CARoot \
      -file ca-cert.pem \
      -keystore "${out}" \
      -storetype PKCS12 \
      -storepass "${PASSWORD}" \
      -noprompt
  fi
}

create_broker_materials() {
  local name="$1"
  local cn="$2"
  local dir="$3"
  local key="${dir}/${name}-key.pem"
  local csr="${dir}/${name}.csr"
  local cert="${dir}/${name}-cert.pem"
  local ext="${dir}/${name}.ext"
  local keystore="${dir}/${name}.keystore.p12"
  local truststore="${dir}/${name}.truststore.p12"

  echo "Брокер ${name} (${cn})..."

  cat > "${ext}" <<EOF
subjectAltName=DNS:${cn},DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
keyUsage=digitalSignature,keyEncipherment
EOF

  openssl genrsa -out "${key}" 2048
  openssl req -new \
    -key "${key}" \
    -out "${csr}" \
    -subj "/CN=${cn}/OU=Broker/O=Kafka/C=RU"

  openssl x509 -req \
    -in "${csr}" \
    -CA ca-cert.pem \
    -CAkey ca-key.pem \
    -CAcreateserial \
    -out "${cert}" \
    -days "${VALIDITY_DAYS}" \
    -extfile "${ext}"

  openssl pkcs12 -export \
    -inkey "${key}" \
    -in "${cert}" \
    -certfile ca-cert.pem \
    -out "${keystore}" \
    -passout "pass:${PASSWORD}" \
    -name "${name}"

  create_pkcs12_truststore "${truststore}"

  echo "${PASSWORD}" > "${dir}/credentials"

  rm -f "${csr}" "${ext}"
}

create_client_materials() {
  local name="$1"
  local cn="$2"
  local dir="$3"
  local key="${dir}/${name}-key.pem"
  local csr="${dir}/${name}.csr"
  local cert="${dir}/${name}-cert.pem"
  local ext="${dir}/${name}.ext"
  local keystore="${dir}/${name}.keystore.p12"
  local truststore="${dir}/${name}.truststore.p12"

  echo "Клиент ${name} (${cn})..."

  cat > "${ext}" <<EOF
extendedKeyUsage=clientAuth
keyUsage=digitalSignature,keyEncipherment
EOF

  openssl genrsa -out "${key}" 2048
  openssl req -new \
    -key "${key}" \
    -out "${csr}" \
    -subj "/CN=${cn}/OU=Client/O=Kafka/C=RU"

  openssl x509 -req \
    -in "${csr}" \
    -CA ca-cert.pem \
    -CAkey ca-key.pem \
    -CAcreateserial \
    -out "${cert}" \
    -days "${VALIDITY_DAYS}" \
    -extfile "${ext}"

  openssl pkcs12 -export \
    -inkey "${key}" \
    -in "${cert}" \
    -certfile ca-cert.pem \
    -out "${keystore}" \
    -passout "pass:${PASSWORD}" \
    -name "${name}"

  create_pkcs12_truststore "${truststore}"

  echo "${PASSWORD}" > "${dir}/credentials"

  rm -f "${csr}" "${ext}"
}

write_client_properties() {
  local file="$1"
  local keystore="$2"
  local truststore="$3"

  cat > "${file}" <<EOF
security.protocol=SSL
ssl.truststore.location=${truststore}
ssl.truststore.password=${PASSWORD}
ssl.truststore.type=PKCS12
ssl.keystore.location=${keystore}
ssl.keystore.password=${PASSWORD}
ssl.keystore.type=PKCS12
ssl.key.password=${PASSWORD}
ssl.endpoint.identification.algorithm=
EOF
}

create_broker_materials "kafka1" "kafka1" "broker1"
create_broker_materials "kafka2" "kafka2" "broker2"
create_broker_materials "kafka3" "kafka3" "broker3"

create_client_materials "admin" "admin" "clients/admin"
create_client_materials "producer" "kafka-producer" "clients/producer"
create_client_materials "consumer" "kafka-consumer" "clients/consumer"

write_client_properties "admin-client.properties" "clients/admin/admin.keystore.p12" "clients/admin/admin.truststore.p12"
write_client_properties "producer-client.properties" "clients/producer/producer.keystore.p12" "clients/producer/producer.truststore.p12"
write_client_properties "consumer-client.properties" "clients/consumer/consumer.keystore.p12" "clients/consumer/consumer.truststore.p12"

cat > credentials.properties <<EOF
ssl.keystore.password=${PASSWORD}
ssl.key.password=${PASSWORD}
ssl.truststore.password=${PASSWORD}
EOF

if command -v keytool >/dev/null 2>&1; then
  for dir in broker1 broker2 broker3 clients/admin clients/producer clients/consumer; do
    for p12 in "${dir}"/*.p12; do
      [[ -f "${p12}" ]] || continue
      keytool -importkeystore \
        -srckeystore "${p12}" \
        -srcstoretype PKCS12 \
        -srcstorepass "${PASSWORD}" \
        -destkeystore "${p12%.p12}.jks" \
        -deststoretype JKS \
        -deststorepass "${PASSWORD}" \
        -noprompt >/dev/null 2>&1 || true
    done
  done
fi

echo "Готово"
