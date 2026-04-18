package main

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func kafkaConfig(cfg appConfig) kafka.ConfigMap {
	config := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.brokers, ","),
		"client.id":         "kafka-azure",
		"security.protocol": "plaintext",
	}

	return config
}

func consumerConfig(cfg appConfig) *kafka.ConfigMap {
	config := kafkaConfig(cfg)
	config["group.id"] = cfg.groupID
	config["auto.offset.reset"] = "earliest"
	config["enable.auto.commit"] = true
	return &config
}
