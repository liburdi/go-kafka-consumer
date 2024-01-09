package event

import (
	"context"
	"strconv"
	"time"
)

type EventHandler func(ctx context.Context, ev Event) error

type PriorityLevel string

const (
	Low  PriorityLevel = "Low"
	Mid  PriorityLevel = "Mid"
	High PriorityLevel = "High"
)

type Event struct {
	context.Context `json:"-"`

	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Error     string            `json:"error"`
	Timestamp time.Time         `json:"timestamp"`
	Headers   map[string]string `json:"Headers"`
	Body      []byte            `json:"body"`

	Priority      PriorityLevel `json:"priority,omitempty"`
	Timeout       time.Duration `json:"timeout,omitempty"`
	Retry         uint8         `json:"retry,omitempty"`
	ReplayChannel string        `json:"replay_channel,omitempty"`
}

func NewEvent(ctx context.Context, name string) *Event {
	now := time.Now()
	id := name + "." + strconv.Itoa(int(now.UnixNano()/1000))

	return &Event{
		ID:            id,
		Name:          name,
		Timestamp:     now,
		Priority:      Mid,
		Timeout:       0,
		Retry:         0,
		ReplayChannel: "",
		Headers:       map[string]string{},
		Body:          nil,
		Context:       ctx,
	}
}
