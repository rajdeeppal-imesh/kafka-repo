#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-kafka-demo}"
KAFKA_DEPLOYMENT="${KAFKA_DEPLOYMENT:-kafka}"
SCHEMA_REGISTRY_SERVICE="${SCHEMA_REGISTRY_SERVICE:-schema-registry}"
TOPIC="${TOPIC:-demo-events}"
IMAGE="${IMAGE:-imeshrajdeep/kafka-repo:kafka-go-app-v3}"
PRODUCER_DEPLOYMENT="${PRODUCER_DEPLOYMENT:-producer}"
CONSUMER_DEPLOYMENT="${CONSUMER_DEPLOYMENT:-consumer}"
SCHEMA_SUBJECT="${SCHEMA_SUBJECT:-${TOPIC}-value}"

TS="$(date +%s)"
NAME="manual-${TS}"
EMAIL="manual+${TS}@example.com"

echo "Using namespace: ${NAMESPACE}"
echo "Using payload: name=${NAME} email=${EMAIL}"

echo "Updating producer args to use the manual test payload..."
kubectl -n "${NAMESPACE}" patch deployment "${PRODUCER_DEPLOYMENT}" --type='json' -p="[
  {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/args\",\"value\":[\"-mode=produce\",\"-brokers=kafka:9092\",\"-topic=${TOPIC}\",\"-schema-url=http://${SCHEMA_REGISTRY_SERVICE}:8081\",\"-name=${NAME}\",\"-email=${EMAIL}\"]}
]"

echo "Updating consumer args to read from the same topic..."
kubectl -n "${NAMESPACE}" patch deployment "${CONSUMER_DEPLOYMENT}" --type='json' -p="[
  {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/args\",\"value\":[\"-mode=consume\",\"-count=1000000\",\"-brokers=kafka:9092\",\"-topic=${TOPIC}\",\"-group=demo-consumer\",\"-schema-url=http://${SCHEMA_REGISTRY_SERVICE}:8081\",\"-poll-timeout-ms=600000\"]}
]"

echo "Waiting for consumer rollout to pick up the topic..."
kubectl -n "${NAMESPACE}" rollout status deployment/"${CONSUMER_DEPLOYMENT}" --timeout=180s >/dev/null

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
echo "--- RAW ENCODED KAFKA VALUE ---"
LATEST_OFFSET="$(kubectl -n "${NAMESPACE}" exec deploy/"${KAFKA_DEPLOYMENT}" -- bash -lc "kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic '${TOPIC}' --time -1 | awk -F: '{print \$3}' | head -n1")"
READ_OFFSET="$((LATEST_OFFSET - 1))"
echo "Latest offset: ${LATEST_OFFSET}"
echo "Reading offset: ${READ_OFFSET}"

RAW_HEX="$(kubectl -n "${NAMESPACE}" exec deploy/"${KAFKA_DEPLOYMENT}" -- bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic '${TOPIC}' --partition 0 --offset ${READ_OFFSET} --max-messages 1 --formatter kafka.tools.DefaultMessageFormatter --property print.value=true --property value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer 2>/dev/null | od -An -tx1 -v | tr -d ' \n'")"

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
kubectl -n "${NAMESPACE}" logs deployment/"${CONSUMER_DEPLOYMENT}" --tail=120 | grep "${EMAIL}" || true

echo
echo "Done."