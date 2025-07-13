package config

import (
	"os"
	"strconv"
)

const (
	KeyKafkaClusterName = "KAFKA_CLUSTER_NAME"
	KeyKafkaPort        = "KAFKA_PORT"
	KeyKafkaAuth        = "KAFKA_AUTH"
	KafkaAuthUser       = "KAFKA_AUTH_USER"
)

type Config struct {
	KafkaClusterName string
	KafkaPort        string
	KafkaAuth        bool
	KafkaAuthUser    string
}

func NewConfig() *Config {
	clusterName := os.Getenv(KeyKafkaClusterName)
	port := os.Getenv(KeyKafkaPort)
	kafkaAuth, err := strconv.ParseBool(os.Getenv(KeyKafkaAuth))
	kafkaAuthUser := os.Getenv(KafkaAuthUser)
	if err != nil {
		kafkaAuth = false
	}

	return &Config{
		KafkaClusterName: clusterName,
		KafkaPort:        port,
		KafkaAuth:        kafkaAuth,
		KafkaAuthUser:    kafkaAuthUser,
	}
}
