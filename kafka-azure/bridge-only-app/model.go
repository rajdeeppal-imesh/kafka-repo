package main

const (
	confluentMagicByte = byte(0)
	defaultSchema      = `{"type":"record","name":"UserEvent","namespace":"demo","fields":[{"name":"name","type":"string"},{"name":"email","type":"string"},{"name":"created_at","type":"long"}]}`
)

type appConfig struct {
	brokers       []string
	schemaURL     string
	subject       string
	subjectTmpl   string
	httpListen    string
	kafkaKeyHdr   string
	bridgeAvro    bool
	requireSchema bool
	autoRegister  bool
	maxBodyBytes  int
	deliveryMs    int
}
