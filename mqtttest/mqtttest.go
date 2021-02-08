// Package mqtttest provides utilities for MQTT testing.
package mqtttest

import (
	"bytes"
	"testing"

	"github.com/pascaldekloe/mqtt"
)

// Transfer defines a message exchange.
type Transfer struct {
	Message []byte // payload
	Topic   string // destination
	Err     error  // result
}

// PublishMock returns a new mock for mqtt.Client Publish, which compares the
// invocation with want in order of appearance.
func PublishMock(t *testing.T, want ...Transfer) func(message []byte, topic string) error {
	var i int

	t.Cleanup(func() {
		if n := len(want) - i; n > 0 {
			t.Errorf("want %d more MQTT publishes", n)
		}
	})

	return func(message []byte, topic string) error {
		if i >= len(want) {
			t.Errorf("unwanted MQTT publish of %#x to %q", message, topic)
			return nil
		}
		transfer := want[i]
		i++

		if !bytes.Equal(message, transfer.Message) && topic != transfer.Topic {
			t.Errorf("got MQTT publish of %#x to %q, want %#x to %q", message, topic, transfer.Message, transfer.Topic)
		}
		return transfer.Err
	}
}

// Filter defines a (un)subscription exchange.
type Filter struct {
	Topics []string // order is ignored
	Err    error    // result
}

// SubscribeMock returns a new mock for mqtt.Client Subscribe,
// SubscribeLimitAtMostOnce and SubscribeLimitAtLeastOnce, which compares the
// invocation with want in order of appearece.
func SubscribeMock(t *testing.T, want ...Filter) func(quit <-chan struct{}, topicFilters ...string) error {
	var i int

	t.Cleanup(func() {
		if i < len(want) {
			t.Errorf("want %d more MQTT subscribes", len(want)-i)
		}
	})

	return func(quit <-chan struct{}, topicFilters ...string) error {
		if len(topicFilters) == 0 {
			t.Fatal("MQTT subscribe without topic filters")
		}
		select {
		case <-quit:
			return mqtt.ErrCanceled
		default:
			break
		}

		if i >= len(want) {
			t.Errorf("unwanted MQTT subscribe of %q", topicFilters)
		}
		filter := want[i]
		i++

		todo := make(map[string]struct{}, len(filter.Topics))
		for _, topic := range filter.Topics {
			todo[topic] = struct{}{}
		}
		var wrong []string
		for _, filter := range topicFilters {
			if _, ok := todo[filter]; ok {
				delete(todo, filter)
			} else {
				wrong = append(wrong, filter)
			}
		}
		if len(wrong) != 0 {
			t.Errorf("unwanted MQTT subscribe of %q (out of %q)", wrong, filter.Topics)
		}
		if len(todo) != 0 {
			var miss []string
			for filter := range todo {
				miss = append(miss, filter)
			}
			t.Errorf("no MQTT subscribe of %q (out of %q)", miss, filter.Topics)
		}

		return filter.Err
	}
}
