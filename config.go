package consumer

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type flagKeyType string

const HandlerFlagKey flagKeyType = "_handler_flag_key_"

type KafkaConfig = kafka.ConfigMap

type Config struct {
	Topic       string // 指定 topic
	IsTestEnv   bool
	KafkaConfig KafkaConfig
}

func (cfg *Config) buildConfig() KafkaConfig {
	cfg.KafkaConfig.SetKey("max.in.flight.requests.per.connection", 1)
	cfg.KafkaConfig.SetKey("go.events.channel.enable", false)
	cfg.KafkaConfig.SetKey("go.application.rebalance.enable", false)
	cfg.KafkaConfig.SetKey("auto.offset.reset", "earliest")
	cfg.KafkaConfig.SetKey("enable.auto.commit", true)

	return cfg.KafkaConfig
}
