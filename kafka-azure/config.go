package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func loadConfig() appConfig {
	mode := flag.String("mode", envOrDefault("APP_MODE", "produce"), "Mode: produce|consume|bridge")
	brokers := flag.String("brokers", envOrDefault("KAFKA_BROKERS", "localhost:9092"), "Comma separated Kafka brokers")
	topic := flag.String("topic", envOrDefault("KAFKA_TOPIC", "demo-events"), "Kafka topic")
	groupID := flag.String("group", envOrDefault("KAFKA_GROUP_ID", "demo-consumer"), "Kafka consumer group")
	schemaURL := flag.String("schema-url", envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"), "Confluent Schema Registry URL")
	subject := flag.String("subject", envOrDefault("SCHEMA_SUBJECT", ""), "Schema subject (default: <topic>-value)")
	subjectTemplate := flag.String("subject-template", envOrDefault("SCHEMA_SUBJECT_TEMPLATE", "%s-value"), "Schema subject template used in bridge mode")
	name := flag.String("name", envOrDefault("EVENT_NAME", "raj"), "Event user name for producer")
	email := flag.String("email", envOrDefault("EVENT_EMAIL", "raj@example.com"), "Event email for producer")
	consumeCount := flag.Int("count", envIntOrDefault("CONSUME_COUNT", 1), "How many messages to consume before exit")
	pollTimeout := flag.Int("poll-timeout-ms", envIntOrDefault("POLL_TIMEOUT_MS", 60000), "Read timeout per message in milliseconds")
	httpListen := flag.String("http-listen", envOrDefault("HTTP_LISTEN_ADDR", ":8082"), "HTTP listen address for bridge mode")
	kafkaKeyHeader := flag.String("kafka-key-header", envOrDefault("KAFKA_KEY_HEADER", "x-kafka-key"), "Header used for Kafka message key in bridge mode")
	bridgeAvro := flag.Bool("bridge-avro", envBoolOrDefault("BRIDGE_AVRO", true), "Enable Avro serialization in bridge mode")
	requireSchema := flag.Bool("bridge-require-schema", envBoolOrDefault("BRIDGE_REQUIRE_SCHEMA", false), "Fail when schema lookup or Avro serialization fails")
	autoRegister := flag.Bool("bridge-auto-register", envBoolOrDefault("BRIDGE_AUTO_REGISTER_SCHEMA", true), "Auto-register default schema if subject missing")
	maxBodyBytes := flag.Int("max-body-bytes", envIntOrDefault("MAX_BODY_BYTES", 1048576), "Max bridge request body bytes")
	deliveryTimeoutMs := flag.Int("delivery-timeout-ms", envIntOrDefault("DELIVERY_TIMEOUT_MS", 15000), "Kafka delivery timeout in milliseconds")
	flag.Parse()

	if *maxBodyBytes <= 0 {
		*maxBodyBytes = 1024 * 1024
	}
	if *deliveryTimeoutMs <= 0 {
		*deliveryTimeoutMs = 15000
	}
	if *pollTimeout <= 0 {
		*pollTimeout = 60000
	}
	if *consumeCount <= 0 {
		*consumeCount = 1
	}

	return appConfig{
		mode:          strings.ToLower(strings.TrimSpace(*mode)),
		brokers:       splitAndTrim(*brokers),
		topic:         strings.TrimSpace(*topic),
		groupID:       strings.TrimSpace(*groupID),
		schemaURL:     strings.TrimSpace(*schemaURL),
		subject:       strings.TrimSpace(*subject),
		subjectTmpl:   strings.TrimSpace(*subjectTemplate),
		name:          strings.TrimSpace(*name),
		email:         strings.TrimSpace(*email),
		consumeCount:  *consumeCount,
		pollTimeoutMs: *pollTimeout,
		httpListen:    strings.TrimSpace(*httpListen),
		kafkaKeyHdr:   strings.TrimSpace(*kafkaKeyHeader),
		bridgeAvro:    *bridgeAvro,
		requireSchema: *requireSchema,
		autoRegister:  *autoRegister,
		maxBodyBytes:  *maxBodyBytes,
		deliveryMs:    *deliveryTimeoutMs,
	}
}

func splitAndTrim(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		t := strings.TrimSpace(p)
		if t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func envBoolOrDefault(key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}
