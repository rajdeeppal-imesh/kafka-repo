package main

const (
	confluentMagicByte = byte(0)
	defaultSchema      = `{"type":"record","name":"UserEvent","namespace":"demo","fields":[{"name":"name","type":"string"},{"name":"email","type":"string"},{"name":"created_at","type":"long"}]}`
)

type appConfig struct {
	brokers       []string
	topic         string
	groupID       string
	schemaURL     string
	subject       string
	subjectTmpl   string
	mode          string
	name          string
	email         string
	consumeCount  int
	pollTimeoutMs int
	httpListen    string
	kafkaKeyHdr   string
	bridgeAvro    bool
	requireSchema bool
	autoRegister  bool
	maxBodyBytes  int
	deliveryMs    int
}

type userEvent struct {
	Name      string `avro:"name" json:"name"`
	Email     string `avro:"email" json:"email"`
	CreatedAt int64  `avro:"created_at" json:"created_at"`
}
