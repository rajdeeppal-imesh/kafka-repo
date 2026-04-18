package main

import (
	"log"
	"time"

	"github.com/riferrei/srclient"
)

func main() {
	cfg := loadConfig()

	if cfg.mode == "bridge" {
		if err := runBridge(cfg); err != nil {
			log.Fatalf("bridge failed: %v", err)
		}
		return
	}

	schemaClient := srclient.CreateSchemaRegistryClient(cfg.schemaURL)

	subject := cfg.subject
	if subject == "" {
		subject = cfg.topic + "-value"
	}

	schema, err := ensureSchema(schemaClient, subject, defaultSchema)
	if err != nil {
		log.Fatalf("schema setup failed: %v", err)
	}

	switch cfg.mode {
	case "produce":
		evt := userEvent{
			Name:      cfg.name,
			Email:     cfg.email,
			CreatedAt: time.Now().UnixMilli(),
		}
		if err := produceOne(cfg, schema, evt); err != nil {
			log.Fatalf("produce failed: %v", err)
		}
		log.Printf("produced event to topic=%s subject=%s schema_id=%d", cfg.topic, subject, schema.ID())
	case "consume":
		if err := consume(cfg, schemaClient, cfg.consumeCount, time.Duration(cfg.pollTimeoutMs)*time.Millisecond); err != nil {
			log.Fatalf("consume failed: %v", err)
		}
	default:
		log.Fatalf("unsupported mode %q (use produce|consume|bridge)", cfg.mode)
	}
}
