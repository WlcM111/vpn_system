package config

type KafkaConfig struct {
	Brokers []string
}

type HTTPConfig struct {
	Addr string
}
