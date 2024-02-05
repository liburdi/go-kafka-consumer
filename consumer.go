package consumer

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	log "github.com/golang/glog"
	"github.com/liburdi/go-kafka-consumer/event"
)

type Consumer struct {
	counter     sync.WaitGroup
	Topic       string
	handler     *handler
	consumers   sync.Map // name => *kafka.Consumer
	kafkaConfig kafka.ConfigMap
	exitCh      chan interface{}
}

type KafkaConsumerHandler = func(m *kafka.Message) error

type TypeVersion int

func New(cfg *Config) (*Consumer, error) {
	if len(cfg.KafkaConfig) == 0 {
		return nil, fmt.Errorf("consumer: config error")
	}

	c := &Consumer{
		handler: newHandler(cfg.IsTestEnv),
		exitCh:  make(chan interface{}, 1),
	}

	if cfg.Topic != "" {
		c.Topic = cfg.Topic
	}

	c.kafkaConfig = cfg.buildConfig()
	return c, nil
}

func (c *Consumer) Register(topic string, handler event.EventHandler) {
	c.handler.register(topic, handler)
}

func (c *Consumer) Start() error {
	go c.keepLive()

	return c.newConsumer(c.Topic, func(msg *kafka.Message) error {
		c.counter.Add(1)
		defer c.counter.Done()

		return c.handler.handler(c.Topic, msg)
	})
}

func (c *Consumer) newConsumer(flag string, handler KafkaConsumerHandler) error {
	pkc := kafka.ConfigMap{}
	for k, v := range c.kafkaConfig {
		pkc[k] = v
	}

	pkc.SetKey("group.id", kafka.ConfigValue(fmt.Sprintf("runner-c-%s", flag)))

	cs, err := kafka.NewConsumer(&pkc)
	if err != nil {
		return err
	}

	err = cs.Subscribe(flag, nil)
	if err != nil {
		return err
	}

	c.consumers.Store(flag, cs)

	log.Info("consumer: start")
	for {
		select {
		case <-c.exitCh:
			return nil
		default:
		}

		msg, err := cs.ReadMessage(time.Second)
		if err != nil {
			if !errors.Is(err, kafka.NewError(kafka.ErrTimedOut, "", false)) &&
				!errors.Is(err, kafka.NewError(kafka.ErrMsgTimedOut, "", false)) {
				return fmt.Errorf("read message error: %v, %v", err, msg)
			}

			continue
		}

		go func() {
			if err := handler(msg); err != nil {
				log.Error("consumer: consumer.Do() error")
			}
		}()
	}
}

// Stop  consumer stop
func (c *Consumer) Stop() error {
	defer close(c.exitCh)
	defer c.counter.Wait()

	c.consumers.Range(func(key, value any) bool {
		if err := value.(*kafka.Consumer).Close(); err != nil {
			log.Error("consumer: stop")
		}

		return true
	})

	return nil
}

// keepLive kafka client keep live
func (c *Consumer) keepLive() {
	ticker := time.NewTicker(3 * time.Second)

	for {
		select {
		case <-c.exitCh:
			return
		case <-ticker.C:
			c.consumers.Range(func(key, value any) bool {
				topic := key.(string)

				if _, err := value.(*kafka.Consumer).GetMetadata(&topic, false, 1000); err != nil {
					log.Error("flush consumer metadata failure")
				}

				return true
			})
		}
	}
}
