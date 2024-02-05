package main

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/glog"
	consumer "github.com/liburdi/go-kafka-consumer"
	"github.com/liburdi/go-kafka-consumer/config"
	"github.com/liburdi/go-kafka-consumer/event"
	"github.com/spf13/viper"
)

func main() {
	err := config.Init()
	if err != nil {
		glog.Infof("get config error:%s", err.Error())
		return
	}
	topic := viper.GetString("kafka.config.topic")
	rc := &consumer.Config{
		Topic:     topic,
		IsTestEnv: false,
		KafkaConfig: map[string]kafka.ConfigValue{
			"api.version.request":                   viper.GetBool("kafka.config.api_version_request"),
			"auto.offset.reset":                     viper.GetString("kafka.config.auto_offset_reset"),
			"bootstrap.servers":                     viper.GetString("kafka.config.bootstrap_servers"),
			"enable.auto.commit":                    viper.GetBool("kafka.config.enable_auto_commit"),
			"fetch.max.bytes":                       viper.GetInt("kafka.config.fetch_max_bytes"),
			"go.application.rebalance.enable":       viper.GetBool("kafka.config.go_application_rebalance_enable"),
			"go.events.channel.enable":              viper.GetBool("kafka.config.go_events_channel_enable"),
			"heartbeat.interval.ms":                 viper.GetInt("kafka.config.heartbeat_interval_ms"),
			"max.in.flight.requests.per.connection": viper.GetInt("kafka.config.max_in_flight_requests_per_connection"),
			"max.partition.fetch.bytes":             viper.GetInt("kafka.config.max_partition_fetch_bytes"),
			"max.poll.interval.ms":                  viper.GetInt("kafka.config.max_poll_interval_ms"),
			"session.timeout.ms":                    viper.GetInt("kafka.config.session_timeout_ms"),
		},
	}
	c, err := consumer.New(rc)
	if err != nil {
		glog.Fatalf("consumer new err %s", err.Error())
	}
	c.Register("example_test_1", func(ctx context.Context, ev event.Event) error {
		glog.Info("consumer ...")
		glog.Infof("%s %s %s", ev.Name, string(ev.Body), ev.ID)
		return nil
	})

	c.Register("example_test_2", func(ctx context.Context, ev event.Event) error {
		glog.Info("consumer ...")
		glog.Infof("%s %s %s", ev.Name, string(ev.Body), ev.ID)
		return nil
	})

	c.Register("example_test_3", func(ctx context.Context, ev event.Event) error {
		glog.Info("consumer ...")
		glog.Infof("%s %s %s", ev.Name, string(ev.Body), ev.ID)
		return nil
	})
	err = c.Start()
	if err != nil {
		glog.Error(err.Error())
		return
	}
	time.Sleep(30 * time.Second)

	defer func(c *consumer.Consumer) {
		err := c.Stop()
		if err != nil {

		}
	}(c)

}
