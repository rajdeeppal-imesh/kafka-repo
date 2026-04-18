package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/riferrei/srclient"
)

func consume(cfg appConfig, client *srclient.SchemaRegistryClient, count int, timeout time.Duration) error {
	if count <= 0 {
		count = 1
	}

	schemaCache := map[int]*srclient.Schema{}

	consumer, err := kafka.NewConsumer(consumerConfig(cfg))
	if err != nil {
		return err
	}
	defer consumer.Close()

	if err := consumer.SubscribeTopics([]string{cfg.topic}, nil); err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		msg, err := consumer.ReadMessage(timeout)
		if err != nil {
			return err
		}

		topic := ""
		if msg.TopicPartition.Topic != nil {
			topic = *msg.TopicPartition.Topic
		}

		if envelope, ok := decodeBridgeEnvelope(msg.Value); ok {
			log.Printf("Consumed bridge envelope topic=%s partition=%d offset=%d key=%s method=%s path=%s body_base64=%t body_args_present=%t", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), envelope.Method, envelope.Path, envelope.BodyBase64, envelope.BodyArgs != nil)
			continue
		}

		schemaID, payload, err := decodeKafkaValue(msg.Value)
		if err != nil {
			log.Printf("skipping undecodable message topic=%s partition=%d offset=%d err=%v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, err)
			continue
		}

		parsedSchema, ok := schemaCache[schemaID]
		if !ok {
			registered, err := client.GetSchema(schemaID)
			if err != nil {
				log.Printf("skipping message topic=%s partition=%d offset=%d schema_id=%d err=fetch schema failed: %v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, schemaID, err)
				continue
			}
			if registered.Codec() == nil {
				log.Printf("skipping message topic=%s partition=%d offset=%d schema_id=%d err=missing schema codec", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, schemaID)
				continue
			}
			parsedSchema = registered
			schemaCache[schemaID] = parsedSchema
		}

		codec := parsedSchema.Codec()
		if codec == nil {
			log.Printf("skipping message topic=%s partition=%d offset=%d schema_id=%d err=schema codec is nil", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, schemaID)
			continue
		}

		native, _, err := codec.NativeFromBinary(payload)
		if err != nil {
			log.Printf("skipping message topic=%s partition=%d offset=%d schema_id=%d err=decode binary avro failed: %v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, schemaID, err)
			continue
		}

		value, err := codec.TextualFromNative(nil, native)
		if err != nil {
			log.Printf("skipping message topic=%s partition=%d offset=%d schema_id=%d err=encode avro textual failed: %v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, schemaID, err)
			continue
		}

		var evt userEvent
		if err := json.Unmarshal(value, &evt); err != nil {
			log.Printf("skipping message topic=%s partition=%d offset=%d schema_id=%d err=decode event json failed: %v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, schemaID, err)
			continue
		}

		log.Printf("Consumed topic=%s partition=%d offset=%d key=%s schema_id=%d event=%+v", topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), schemaID, evt)
	}

	return nil
}

func decodeKafkaValue(raw []byte) (int, []byte, error) {
	if schemaID, payload, err := fromConfluentWire(raw); err == nil {
		return schemaID, payload, nil
	}

	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, nil, fmt.Errorf("message is neither wire-format nor base64: %w", err)
	}

	schemaID, payload, err := fromConfluentWire(decoded)
	if err != nil {
		return 0, nil, fmt.Errorf("base64-decoded message is not confluent wire format: %w", err)
	}

	return schemaID, payload, nil
}

type bridgeEnvelope struct {
	Method      string         `json:"method"`
	Path        string         `json:"path"`
	QueryString string         `json:"querystring"`
	Headers     map[string]any `json:"headers"`
	Body        string         `json:"body"`
	BodyArgs    any            `json:"body_args"`
	BodyBase64  bool           `json:"body_base64"`
}

func decodeBridgeEnvelope(raw []byte) (*bridgeEnvelope, bool) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || !strings.HasPrefix(trimmed, "{") {
		return nil, false
	}

	var env bridgeEnvelope
	if err := json.Unmarshal([]byte(trimmed), &env); err != nil {
		return nil, false
	}

	if env.Method == "" && env.Path == "" && env.Body == "" && env.BodyArgs == nil {
		return nil, false
	}

	if env.BodyBase64 {
		if decoded, err := base64.StdEncoding.DecodeString(env.Body); err == nil {
			env.Body = string(decoded)
		}
	}

	return &env, true
}
