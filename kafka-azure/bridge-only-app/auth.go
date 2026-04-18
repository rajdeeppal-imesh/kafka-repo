package main

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func kafkaConfig(cfg appConfig) kafka.ConfigMap {
	config := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.brokers, ","),
		"client.id":         "kafka-azure-bridge-only",
		"security.protocol": "plaintext",
	}

	return config
}
