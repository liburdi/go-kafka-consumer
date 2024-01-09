package consumer

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"gitlab.weipaitang.com/go/zhenjin-consumer/event"
	"gitlab.weipaitang.com/golang/log"
)

type Consumer struct {
	version     TypeVersion
	counter     sync.WaitGroup
	Topic       string
	handler     *handler
	consumers   sync.Map // name => *kafka.Consumer
	kafkaConfig kafka.ConfigMap
	exitCh      chan interface{}
}

type KafkaConsumerHandler = func(m *kafka.Message) error

type TypeVersion int

const (
	Version_1 TypeVersion = 1
	Version_2 TypeVersion = 2
)

func New(cfg *Config) (*Consumer, error) {
	if len(cfg.KafkaConfig) == 0 || (cfg.Version != 1 && cfg.Version != 2) {
		return nil, fmt.Errorf("zhenjin consumer: config error")
	}

	if cfg.Version == Version_2 && cfg.Topic == "" {
		return nil, fmt.Errorf("zhenjin consumer: config error")
	}

	c := &Consumer{
		version: cfg.Version,
		handler: newHandler(cfg.IsTestEnv),
		exitCh:  make(chan interface{}, 1),
	}

	if cfg.Topic != "" {
		c.Topic = cfg.Topic
	}

	c.kafkaConfig = cfg.buildConfig()
	return c, nil
}

func (c *Consumer) Register(flag string, handler event.EventHandler) {
	c.handler.register(flag, handler)
}

func (c *Consumer) Start() error {
	go c.keepLive()

	if c.version == Version_1 {
		return c.startV1()
	}

	return c.startV2()
}

func (c *Consumer) startV1() error {
	var counter int32

	errChan := make(chan error, 1000)

	c.handler.handlers.Range(func(key, value any) bool {
		atomic.AddInt32(&counter, 1)

		go func(topic string, eventHandler event.EventHandler) {
			defer func() {
				atomic.AddInt32(&counter, -1)
			}()

			err := c.newConsumer(topic, func(msg *kafka.Message) error {
				c.counter.Add(1)
				defer c.counter.Done()

				return c.handler.handler(topic, msg, eventHandler)
			})

			errChan <- err
		}(key.(string), value.(event.EventHandler))

		return true
	})

	for {
		select {
		case err := <-errChan:
			return err
		default:
			if atomic.LoadInt32(&counter) == 0 {
				return nil
			}

			time.Sleep(time.Second)
		}
	}
}

func (c *Consumer) startV2() error {
	return c.newConsumer(c.Topic, func(msg *kafka.Message) error {
		c.counter.Add(1)
		defer c.counter.Done()

		return c.handler.handler(c.Topic, msg)
	})
}

func (c *Consumer) newConsumer(topic string, handler KafkaConsumerHandler) error {
	pkc := kafka.ConfigMap{}
	for k, v := range c.kafkaConfig {
		pkc[k] = v
	}

	pkc.SetKey("group.id", kafka.ConfigValue(fmt.Sprintf("runner-c-%s", topic)))

	cs, err := kafka.NewConsumer(&pkc)
	if err != nil {
		return err
	}

	err = cs.Subscribe(topic, nil)
	if err != nil {
		return err
	}

	c.consumers.Store(topic, cs)

	log.Infoc("zhenjin consumer: start", log.String("topic", topic))
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
				log.Errorz("zhenjin consumer: consumer.Do() error", log.String("topic", topic), log.Error(err))
			}
		}()
	}
}

func (c *Consumer) Stop() error {
	defer close(c.exitCh)
	defer c.counter.Wait()

	c.consumers.Range(func(key, value any) bool {
		if err := value.(*kafka.Consumer).Close(); err != nil {
			log.Errorz("zhenjin consumer: stop", log.String("topic", key.(string)), log.Error(err))
		}

		return true
	})

	return nil
}

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
					log.Errorz("flush consumer metadata failure", log.String("topic", topic), log.Error(err))
				}

				return true
			})
		}
	}
}
