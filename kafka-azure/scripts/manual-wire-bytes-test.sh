#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-kafka-demo}"
TOPIC="${TOPIC:-manual-wire-$(date +%s)}"
SCHEMA_SUBJECT="${SCHEMA_SUBJECT:-${TOPIC}-value}"
KAFKA_DEPLOYMENT="${KAFKA_DEPLOYMENT:-kafka}"
KAFKA_SERVICE="${KAFKA_SERVICE:-kafka}"
SCHEMA_REGISTRY_SERVICE="${SCHEMA_REGISTRY_SERVICE:-schema-registry}"
PRODUCER_DEPLOYMENT="${PRODUCER_DEPLOYMENT:-producer}"
CONSUMER_DEPLOYMENT="${CONSUMER_DEPLOYMENT:-consumer}"
IMAGE="${IMAGE:-imeshrajdeep/kafka-repo:kafka-go-app-v3}"

TS="$(date +%s)"
NAME="wire-${TS}"
EMAIL="wire+${TS}@example.com"

echo "Using namespace: ${NAMESPACE}"
echo "Using topic: ${TOPIC}"
echo "Using payload: name=${NAME} email=${EMAIL}"

echo "Patching producer and consumer to the same topic..."
kubectl -n "${NAMESPACE}" patch deployment "${PRODUCER_DEPLOYMENT}" --type='json' -p="[
  {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/args\",\"value\":[\"-mode=produce\",\"-brokers=kafka:9092\",\"-topic=${TOPIC}\",\"-schema-url=http://${SCHEMA_REGISTRY_SERVICE}:8081\",\"-name=${NAME}\",\"-email=${EMAIL}\"]}
]"

kubectl -n "${NAMESPACE}" patch deployment "${CONSUMER_DEPLOYMENT}" --type='json' -p="[
  {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/args\",\"value\":[\"-mode=consume\",\"-count=1000000\",\"-brokers=kafka:9092\",\"-topic=${TOPIC}\",\"-group=demo-consumer\",\"-schema-url=http://${SCHEMA_REGISTRY_SERVICE}:8081\",\"-poll-timeout-ms=600000\"]}
]"

kubectl -n "${NAMESPACE}" rollout status deployment/"${CONSUMER_DEPLOYMENT}" --timeout=180s >/dev/null
kubectl -n "${NAMESPACE}" rollout status deployment/"${PRODUCER_DEPLOYMENT}" --timeout=180s >/dev/null

PORT_FORWARD_LOG="$(mktemp /tmp/manual-wire-portforward.XXXXXX.log)"
cleanup() {
  if [[ -n "${PF_PID:-}" ]] && kill -0 "${PF_PID}" 2>/dev/null; then
    kill "${PF_PID}" 2>/dev/null || true
  fi
  rm -f "${PORT_FORWARD_LOG}" "${GO_HELPER:-}" 2>/dev/null || true
}
trap cleanup EXIT

echo "Starting Kafka port-forward on localhost:9092..."
kubectl -n "${NAMESPACE}" port-forward "svc/${KAFKA_SERVICE}" 9092:9092 >"${PORT_FORWARD_LOG}" 2>&1 &
PF_PID=$!
sleep 3

echo "Starting producer..."
kubectl -n "${NAMESPACE}" scale deployment "${PRODUCER_DEPLOYMENT}" --replicas=1 >/dev/null
sleep 8
kubectl -n "${NAMESPACE}" scale deployment "${PRODUCER_DEPLOYMENT}" --replicas=0 >/dev/null

echo
echo "--- PRODUCER LOGS ---"
kubectl -n "${NAMESPACE}" logs deployment/"${PRODUCER_DEPLOYMENT}" --tail=50

echo
echo "--- SCHEMA REGISTRY LATEST ---"
kubectl -n "${NAMESPACE}" run sr-curl --image=curlimages/curl:8.7.1 --restart=Never --rm -i --quiet --command -- sh -lc \
  "curl -s http://${SCHEMA_REGISTRY_SERVICE}:8081/subjects/${SCHEMA_SUBJECT}/versions/latest" | tee /tmp/schema-latest.json

SCHEMA_ID="$(sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p' /tmp/schema-latest.json | head -n1)"
echo "Schema id from registry: ${SCHEMA_ID}"

echo
echo "--- REAL CONFLUENT WIRE BYTES (hex) ---"
GO_HELPER="$(mktemp .manual-wire-reader.XXXXXX.go)"
cat >"${GO_HELPER}" <<EOF
package main

import (
  "encoding/binary"
  "encoding/hex"
  "fmt"
  "log"
  "time"

  "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
  consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id": "manual-wire-reader",
    "auto.offset.reset": "earliest",
    "security.protocol": "plaintext",
  })
  if err != nil {
    log.Fatal(err)
  }
  defer consumer.Close()

  topic := "${TOPIC}"
  if err := consumer.SubscribeTopics([]string{topic}, nil); err != nil {
    log.Fatal(err)
  }

  msg, err := consumer.ReadMessage(30 * time.Second)
  if err != nil {
    log.Fatal(err)
  }

  fmt.Println(hex.EncodeToString(msg.Value))
  if len(msg.Value) >= 5 {
    fmt.Printf("magic=%02x schema_id=%d payload_hex=%s\n", msg.Value[0], binary.BigEndian.Uint32(msg.Value[1:5]), hex.EncodeToString(msg.Value[5:]))
  }
}
EOF

RAW_HEX="$(go run "${GO_HELPER}" | head -n1)"

echo "raw_hex=${RAW_HEX}"
MAGIC_HEX="${RAW_HEX:0:2}"
SCHEMA_HEX="${RAW_HEX:2:8}"
PAYLOAD_HEX="${RAW_HEX:10}"

echo "magic_hex=${MAGIC_HEX}"
echo "schema_hex=${SCHEMA_HEX}"
echo "schema_id_from_wire=$((16#${SCHEMA_HEX}))"
echo "payload_hex=${PAYLOAD_HEX}"

echo
echo "--- CONSUMER LOGS (decoded event) ---"
kubectl -n "${NAMESPACE}" logs deployment/"${CONSUMER_DEPLOYMENT}" --tail=200 | grep "${EMAIL}" || true

echo
echo "Done."
