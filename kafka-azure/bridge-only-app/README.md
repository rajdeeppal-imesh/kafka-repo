# Bridge-Only App

This folder contains a standalone bridge-only binary split from the multi-mode app.
It keeps the existing setup in the parent folder intact.

## What is included

- HTTP Kafka bridge endpoint (`POST /<topic>`)
- Avro JSON -> Confluent wire serialization path
- Confluent wire pass-through modes
- Kong-style envelope mode (`body`, `body_args`, `body_base64`)

## Build and run locally

```bash
cd /Users/rajdeep/kafka-eg/kafka-azure/bridge-only-app
go mod tidy
go run . \
  -brokers=localhost:9092 \
  -schema-url=http://localhost:8081 \
  -http-listen=:8082
```

## Build Docker image

```bash
cd /Users/rajdeep/kafka-eg/kafka-azure/bridge-only-app
docker buildx build --platform linux/amd64 -t <your-repo>/kafka-bridge-only:<tag> --push .
```

## Key flags

- `-bridge-avro=true|false`
- `-bridge-require-schema=true|false`
- `-bridge-auto-register=true|false`
- `-subject-template=%s-value`
- `-max-body-bytes=1048576`
- `-delivery-timeout-ms=15000`
