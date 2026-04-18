package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func newProducer(cfg appConfig) (*kafka.Producer, error) {
	producerConfig := kafkaConfig(cfg)
	return kafka.NewProducer(&producerConfig)
}

func produceBinary(producer *kafka.Producer, cfg appConfig, topic string, key, value []byte) (kafka.TopicPartition, error) {
	deliveryChan := make(chan kafka.Event, 1)
	defer close(deliveryChan)

	if err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          value,
		Timestamp:      time.Now(),
	}, deliveryChan); err != nil {
		return kafka.TopicPartition{}, err
	}

	deliveryTimeout := 15 * time.Second
	if cfg.deliveryMs > 0 {
		deliveryTimeout = time.Duration(cfg.deliveryMs) * time.Millisecond
	}

	select {
	case ev := <-deliveryChan:
		msg, ok := ev.(*kafka.Message)
		if !ok {
			return kafka.TopicPartition{}, fmt.Errorf("unexpected delivery event type %T", ev)
		}
		if msg.TopicPartition.Error != nil {
			return kafka.TopicPartition{}, msg.TopicPartition.Error
		}
		return msg.TopicPartition, nil
	case <-time.After(deliveryTimeout):
		return kafka.TopicPartition{}, fmt.Errorf("timed out waiting for delivery confirmation to topic %s on brokers %s", topic, strings.Join(cfg.brokers, ","))
	}
}
