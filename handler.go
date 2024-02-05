package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/glog"
	"github.com/liburdi/go-kafka-consumer/event"
	"github.com/pkg/errors"
)

type handler struct {
	isTestEnv bool
	handlers  sync.Map
}

func newHandler(isTestEnv bool) *handler {
	return &handler{
		isTestEnv: isTestEnv,
	}
}

func (h *handler) register(flag string, handler event.EventHandler) {
	if _, ok := h.handlers.Load(flag); ok {
		glog.Error("handler already exists")
		return
	}

	h.handlers.Store(flag, handler)
}

func (h *handler) handler(topic string, msg *kafka.Message, handler ...event.EventHandler) error {
	task := &Task{}
	if err := json.Unmarshal(msg.Value, task); err != nil {
		return errors.Wrap(err, "parse task")
	}

	if task.Content == "" {
		return fmt.Errorf("nil content data")
	}

	content := &TaskContent{}
	if err := json.Unmarshal([]byte(task.Content), content); err != nil {
		return errors.Wrap(err, "parse task content")
	}

	if content.Topic != topic {
		if h.isTestEnv {
			if !strings.HasSuffix(topic, content.Topic) {
				return fmt.Errorf("error topic %s, consumer topic is %s", content.Topic, topic)
			}
		} else {
			return fmt.Errorf("error topic %s, consumer topic is %s", content.Topic, topic)
		}
	}

	glog.Infof("received: %v", task)

	ev := event.NewEvent(context.Background(), task.TaskName)
	ev.ID = task.JobId
	ev.Body = msg.Value

	ctx := context.Background()

	if len(handler) > 0 {
		return handler[0](ctx, *ev)
	}

	if len(content.Flag) == 0 {
		return fmt.Errorf("content flag is empty")
	}

	eventHandler, ok := h.handlers.Load(content.Flag)
	if !ok || eventHandler == nil {
		return errors.New("not found func: " + content.Flag)
	}

	return eventHandler.(event.EventHandler)(context.WithValue(ctx, HandlerFlagKey, content.Flag), *ev)
}
