package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
)

func loadConfig() appConfig {
	brokers := flag.String("brokers", envOrDefault("KAFKA_BROKERS", "localhost:9092"), "Comma separated Kafka brokers")
	schemaURL := flag.String("schema-url", envOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"), "Confluent Schema Registry URL")
	subject := flag.String("subject", envOrDefault("SCHEMA_SUBJECT", ""), "Schema subject (overrides subject template)")
	subjectTemplate := flag.String("subject-template", envOrDefault("SCHEMA_SUBJECT_TEMPLATE", "%s-value"), "Schema subject template")
	httpListen := flag.String("http-listen", envOrDefault("HTTP_LISTEN_ADDR", ":8082"), "HTTP listen address")
	kafkaKeyHeader := flag.String("kafka-key-header", envOrDefault("KAFKA_KEY_HEADER", "x-kafka-key"), "Header used for Kafka key")
	bridgeAvro := flag.Bool("bridge-avro", envBoolOrDefault("BRIDGE_AVRO", true), "Enable Avro serialization")
	requireSchema := flag.Bool("bridge-require-schema", envBoolOrDefault("BRIDGE_REQUIRE_SCHEMA", false), "Require schema resolution/serialization")
	autoRegister := flag.Bool("bridge-auto-register", envBoolOrDefault("BRIDGE_AUTO_REGISTER_SCHEMA", true), "Auto-register default schema if missing")
	maxBodyBytes := flag.Int("max-body-bytes", envIntOrDefault("MAX_BODY_BYTES", 1048576), "Max request body bytes")
	deliveryTimeoutMs := flag.Int("delivery-timeout-ms", envIntOrDefault("DELIVERY_TIMEOUT_MS", 15000), "Kafka delivery timeout in milliseconds")
	flag.Parse()

	if *maxBodyBytes <= 0 {
		*maxBodyBytes = 1024 * 1024
	}
	if *deliveryTimeoutMs <= 0 {
		*deliveryTimeoutMs = 15000
	}

	return appConfig{
		brokers:       splitAndTrim(*brokers),
		schemaURL:     strings.TrimSpace(*schemaURL),
		subject:       strings.TrimSpace(*subject),
		subjectTmpl:   strings.TrimSpace(*subjectTemplate),
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
