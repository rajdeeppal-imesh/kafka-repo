package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/riferrei/srclient"
)

type bridgeServer struct {
	cfg          appConfig
	producer     *kafka.Producer
	schemaClient *srclient.SchemaRegistryClient

	schemaMu    sync.RWMutex
	schemaCache map[string]*srclient.Schema
}

func runBridge(cfg appConfig) error {
	p, err := newProducer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	defer p.Close()

	s := &bridgeServer{
		cfg:         cfg,
		producer:    p,
		schemaCache: map[string]*srclient.Schema{},
	}

	if strings.TrimSpace(cfg.schemaURL) != "" {
		s.schemaClient = srclient.CreateSchemaRegistryClient(cfg.schemaURL)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/", s.handleProduce)

	listenAddr := cfg.httpListen
	if listenAddr == "" {
		listenAddr = ":8082"
	}

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("kafka bridge started on %s", listenAddr)
	return server.ListenAndServe()
}

func (s *bridgeServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *bridgeServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	topic := topicFromPath(r.URL.Path)
	if topic == "" {
		http.Error(w, "path must include topic: /<topic>", http.StatusBadRequest)
		return
	}
	if !validTopicName(topic) {
		http.Error(w, "invalid topic name", http.StatusBadRequest)
		return
	}

	maxBody := int64(s.cfg.maxBodyBytes)
	if maxBody <= 0 {
		maxBody = 1024 * 1024
	}
	limitedBody := http.MaxBytesReader(w, r.Body, maxBody)
	defer limitedBody.Close()

	raw, err := io.ReadAll(limitedBody)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed reading request body: %v", err), http.StatusRequestEntityTooLarge)
		return
	}

	keyHeader := s.cfg.kafkaKeyHdr
	if keyHeader == "" {
		keyHeader = "x-kafka-key"
	}
	key := []byte(r.Header.Get(keyHeader))

	payload, schemaID, ingestMode, err := s.buildPayload(topic, r, raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	topicPartition, err := produceBinary(s.producer, s.cfg, topic, key, payload)
	if err != nil {
		http.Error(w, fmt.Sprintf("kafka produce failed: %v", err), http.StatusBadGateway)
		return
	}

	ack := map[string]any{
		"status":      "accepted",
		"topic":       topic,
		"partition":   int(topicPartition.Partition),
		"offset":      int64(topicPartition.Offset),
		"schema_id":   schemaID,
		"bridge_avro": s.cfg.bridgeAvro,
		"ingest_mode": ingestMode,
	}
	encoded, _ := json.Marshal(ack)
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(encoded)
}

func (s *bridgeServer) buildPayload(topic string, r *http.Request, raw []byte) ([]byte, int, string, error) {
	if !s.cfg.bridgeAvro {
		envelope, err := buildKongEnvelope(r, raw)
		if err != nil {
			return nil, 0, "", err
		}
		payload, err := json.Marshal(envelope)
		if err != nil {
			return nil, 0, "", fmt.Errorf("failed to encode kong-style envelope: %w", err)
		}
		return payload, 0, "envelope_json", nil
	}

	if schemaID, payload, ok, err := parseConfluentWirePayload(r, raw); ok {
		if err != nil {
			return nil, 0, "", err
		}
		return payload, schemaID, "passthrough_wire", nil
	}

	if !isLikelyJSONBody(raw) {
		envelope, err := buildKongEnvelope(r, raw)
		if err != nil {
			return nil, 0, "", err
		}
		payload, err := json.Marshal(envelope)
		if err != nil {
			return nil, 0, "", fmt.Errorf("failed to encode kong-style envelope: %w", err)
		}
		return payload, 0, "envelope_json", nil
	}

	schema, err := s.resolveSchema(topic)
	if err != nil {
		return nil, 0, "", fmt.Errorf("schema lookup failed: %w", err)
	}
	codec := schema.Codec()
	if codec == nil {
		return nil, 0, "", fmt.Errorf("schema codec is nil for schema id %d", schema.ID())
	}

	valueJSON := extractValueIfWrapped(raw)
	native, _, err := codec.NativeFromTextual(valueJSON)
	if err != nil {
		return nil, 0, "", fmt.Errorf("request does not match avro schema: %w", err)
	}
	avroPayload, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, 0, "", fmt.Errorf("failed to serialize avro payload: %w", err)
	}
	wire := toConfluentWire(schema.ID(), avroPayload)
	encoded := base64.StdEncoding.EncodeToString(wire)
	return []byte(encoded), schema.ID(), "json_to_avro", nil
}

func parseConfluentWirePayload(r *http.Request, raw []byte) (int, []byte, bool, error) {
	mode, ok := confluentWireMode(r)
	if !ok {
		return 0, nil, false, nil
	}

	var wire []byte
	switch mode {
	case "binary":
		wire = raw
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(raw)))
		if err != nil {
			return 0, nil, true, fmt.Errorf("invalid base64 avro wire payload: %w", err)
		}
		wire = decoded
	default:
		return 0, nil, true, fmt.Errorf("unsupported avro payload mode: %s", mode)
	}

	schemaID, _, err := fromConfluentWire(wire)
	if err != nil {
		return 0, nil, true, fmt.Errorf("invalid confluent wire payload: %w", err)
	}

	encoded := base64.StdEncoding.EncodeToString(wire)
	return schemaID, []byte(encoded), true, nil
}

func confluentWireMode(r *http.Request) (string, bool) {
	mode := strings.ToLower(strings.TrimSpace(r.Header.Get("x-avro-payload-format")))
	switch mode {
	case "confluent-wire", "wire", "binary":
		return "binary", true
	case "confluent-wire-base64", "wire-base64", "base64":
		return "base64", true
	}

	contentType := r.Header.Get("content-type")
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = strings.TrimSpace(strings.Split(contentType, ";")[0])
	}

	mt := strings.ToLower(strings.TrimSpace(mediaType))
	switch mt {
	case "application/vnd.kafka.confluent-wire", "application/x-confluent-wire":
		return "binary", true
	case "application/vnd.kafka.confluent-wire+base64", "application/x-confluent-wire+base64":
		return "base64", true
	default:
		return "", false
	}
}

func buildKongEnvelope(r *http.Request, raw []byte) (map[string]any, error) {
	contentType := r.Header.Get("content-type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		mediaType = strings.TrimSpace(strings.Split(contentType, ";")[0])
		params = map[string]string{}
	}

	bodyBase64 := shouldEncodeBodyAsBase64(mediaType)
	bodyValue := string(raw)
	if bodyBase64 {
		bodyValue = base64.StdEncoding.EncodeToString(raw)
	}

	envelope := map[string]any{
		"method":      r.Method,
		"path":        r.URL.Path,
		"querystring": r.URL.RawQuery,
		"headers":     r.Header,
		"body":        bodyValue,
		"body_base64": bodyBase64,
	}

	if shouldParseBodyArgs(mediaType) {
		bodyArgs, parseErr := parseBodyArgs(mediaType, params, raw)
		if parseErr != nil {
			return nil, fmt.Errorf("failed parsing body_args for content-type %q: %w", contentType, parseErr)
		}
		envelope["body_args"] = bodyArgs
	}

	return envelope, nil
}

func shouldEncodeBodyAsBase64(mediaType string) bool {
	mt := strings.ToLower(strings.TrimSpace(mediaType))
	switch mt {
	case "text/plain", "text/html", "application/xml", "text/xml", "application/soap+xml":
		return false
	default:
		return true
	}
}

func shouldParseBodyArgs(mediaType string) bool {
	mt := strings.ToLower(strings.TrimSpace(mediaType))
	switch mt {
	case "application/x-www-form-urlencoded", "multipart/form-data", "application/json":
		return true
	default:
		return false
	}
}

func parseBodyArgs(mediaType string, params map[string]string, raw []byte) (any, error) {
	mt := strings.ToLower(strings.TrimSpace(mediaType))
	switch mt {
	case "application/json":
		if len(bytes.TrimSpace(raw)) == 0 {
			return map[string]any{}, nil
		}
		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	case "application/x-www-form-urlencoded":
		form, err := url.ParseQuery(string(raw))
		if err != nil {
			return nil, err
		}
		return flattenValues(form), nil
	case "multipart/form-data":
		boundary := params["boundary"]
		if boundary == "" {
			return nil, fmt.Errorf("multipart boundary missing")
		}
		reader := multipart.NewReader(bytes.NewReader(raw), boundary)
		out := map[string]any{}
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			name := part.FormName()
			if name == "" {
				continue
			}
			chunk, err := io.ReadAll(part)
			if err != nil {
				return nil, err
			}
			out[name] = string(chunk)
		}
		return out, nil
	default:
		return map[string]any{}, nil
	}
}

func flattenValues(values url.Values) map[string]any {
	out := map[string]any{}
	for k, v := range values {
		if len(v) == 1 {
			out[k] = v[0]
			continue
		}
		items := make([]string, len(v))
		copy(items, v)
		out[k] = items
	}
	return out
}

func extractValueIfWrapped(raw []byte) []byte {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return raw
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &m); err != nil {
		return raw
	}
	if v, ok := m["value"]; ok && len(bytes.TrimSpace(v)) > 0 {
		return v
	}
	return raw
}

func (s *bridgeServer) resolveSchema(topic string) (*srclient.Schema, error) {
	if s.schemaClient == nil {
		return nil, fmt.Errorf("schema registry not configured")
	}

	subject := schemaSubjectForTopic(s.cfg, topic)
	s.schemaMu.RLock()
	cached := s.schemaCache[subject]
	s.schemaMu.RUnlock()
	if cached != nil && cached.Codec() != nil {
		return cached, nil
	}

	schema, err := s.schemaClient.GetLatestSchema(subject)
	if err != nil {
		if s.cfg.autoRegister {
			schema, err = s.schemaClient.CreateSchema(subject, defaultSchema, srclient.Avro)
		}
		if err != nil {
			return nil, err
		}
	}
	if schema.Codec() == nil {
		return nil, fmt.Errorf("schema codec is nil for subject %s", subject)
	}

	s.schemaMu.Lock()
	s.schemaCache[subject] = schema
	s.schemaMu.Unlock()
	return schema, nil
}

func schemaSubjectForTopic(cfg appConfig, topic string) string {
	if strings.TrimSpace(cfg.subject) != "" {
		return strings.TrimSpace(cfg.subject)
	}
	tmpl := strings.TrimSpace(cfg.subjectTmpl)
	if tmpl == "" {
		tmpl = "%s-value"
	}
	if strings.Contains(tmpl, "%s") {
		return fmt.Sprintf(tmpl, topic)
	}
	return tmpl
}

func topicFromPath(path string) string {
	clean := strings.Trim(path, "/")
	if clean == "" {
		return ""
	}
	first := strings.SplitN(clean, "/", 2)[0]
	decoded, err := url.PathUnescape(first)
	if err != nil {
		return strings.TrimSpace(first)
	}
	return strings.TrimSpace(decoded)
}

func validTopicName(topic string) bool {
	if topic == "" || len(topic) > 249 {
		return false
	}
	for _, r := range topic {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= 'A' && r <= 'Z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		switch r {
		case '.', '_', '-':
			continue
		default:
			return false
		}
	}
	return true
}

func isLikelyJSONBody(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[')
}
