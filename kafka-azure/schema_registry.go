package main

import (
	"fmt"
	"strings"

	"github.com/riferrei/srclient"
)

func ensureSchema(client *srclient.SchemaRegistryClient, subject, schemaStr string) (*srclient.Schema, error) {
	registered, err := client.GetLatestSchema(subject)
	if err != nil {
		if !isSchemaNotFound(err) {
			return nil, err
		}
		registered, err = client.CreateSchema(subject, schemaStr, srclient.Avro)
		if err != nil {
			return nil, err
		}
	}

	if registered.Codec() == nil {
		return nil, fmt.Errorf("failed to initialize codec for subject %s", subject)
	}

	return registered, nil
}

func isSchemaNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "subject not found") || strings.Contains(msg, "404")
}
