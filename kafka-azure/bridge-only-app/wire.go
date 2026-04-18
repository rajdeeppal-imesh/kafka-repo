package main

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func toConfluentWire(schemaID int, payload []byte) []byte {
	out := make([]byte, 1+4+len(payload))
	out[0] = confluentMagicByte
	binary.BigEndian.PutUint32(out[1:5], uint32(schemaID))
	copy(out[5:], payload)
	return out
}

func fromConfluentWire(raw []byte) (int, []byte, error) {
	if len(raw) < 5 {
		return 0, nil, errors.New("message too short for confluent framing")
	}
	if raw[0] != confluentMagicByte {
		return 0, nil, fmt.Errorf("unexpected magic byte: %d", raw[0])
	}
	schemaID := int(binary.BigEndian.Uint32(raw[1:5]))
	return schemaID, raw[5:], nil
}
