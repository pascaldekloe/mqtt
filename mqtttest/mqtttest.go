// Package mqtttest provides utilities for MQTT testing.
package mqtttest

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pascaldekloe/mqtt"
)

// NewPublishStub returns a new stub for mqtt.Client Publish with a fixed return
// value.
func NewPublishStub(returnFix error) func(message []byte, topic string) error {
	return func(message []byte, topic string) error {
		return returnFix
	}
}

// Transfer defines a message exchange.
type Transfer struct {
	Message []byte // payload
	Topic   string // destination
	Err     error  // result
}

// NewPublishMock returns a new mock for mqtt.Client Publish, which compares the
// invocation with want in order of appearance.
func NewPublishMock(t testing.TB, want ...Transfer) func(message []byte, topic string) error {
	t.Helper()

	var wantIndex uint64

	t.Cleanup(func() {
		if n := uint64(len(want)) - atomic.LoadUint64(&wantIndex); n > 0 {
			t.Errorf("want %d more MQTT publishes", n)
		}
	})

	return func(message []byte, topic string) error {
		i := atomic.AddUint64(&wantIndex, 1) - 1
		if i >= uint64(len(want)) {
			t.Errorf("unwanted MQTT publish of %#x to %q", message, topic)
			return nil
		}
		transfer := want[i]

		if !bytes.Equal(message, transfer.Message) && topic != transfer.Topic {
			t.Errorf("got MQTT publish of %#x to %q, want %#x to %q", message, topic, transfer.Message, transfer.Topic)
		}
		return transfer.Err
	}
}

// AckBlock prevents ack <-chan error submission.
type AckBlock struct {
	Delay time.Duration // zero defaults to indefinite
}

// Error implements the standard error interface.
func (b AckBlock) Error() string {
	return "mqtttest: AckBlock used as an error"
}

// NewPublishAckStub returns a stub for mqtt.Client PublishAtLeastOnce or
// PublishExactlyOnce with a fixed return value.
//
// The ackFix errors are applied to the ack return, with an option for AckBlock
// entries. An mqtt.ErrClosed in the ackFix keeps the ack channel open (without
// an extra AckBlock entry.
func NewPublishAckStub(errFix error, ackFix ...error) func(message []byte, topic string) (ack <-chan error, err error) {
	if errFix != nil && len(ackFix) != 0 {
		panic("ackFix entries with non-nil errFix")
	}
	var block AckBlock
	for i, err := range ackFix {
		switch {
		case err == nil:
			panic("nil entry in ackFix")
		case errors.Is(err, mqtt.ErrClosed):
			if i+1 < len(ackFix) {
				panic("followup of mqtt.ErrClosed ackFix entry")
			}
		case errors.As(err, &block):
			if block.Delay == 0 && i+1 < len(ackFix) {
				panic("followup of indefinite AckBlock ackFix entry")
			}
		}
	}

	return func(message []byte, topic string) (ack <-chan error, err error) {
		if errFix != nil {
			return nil, errFix
		}

		ch := make(chan error, len(ackFix))
		go func() {
			var block AckBlock
			for _, err := range ackFix {
				switch {
				default:
					ch <- err
				case errors.Is(err, mqtt.ErrClosed):
					ch <- err
					return // without close
				case errors.As(err, &block):
					if block.Delay == 0 {
						return // without close
					}
					time.Sleep(block.Delay)
				}
			}
			close(ch)
		}()
		return ch, nil
	}
}

// NewSubscribeStub returns a stub for mqtt.Client Subscribe with a fixed return
// value.
func NewSubscribeStub(returnFix error) func(quit <-chan struct{}, topicFilters ...string) error {
	return newSubscribeStub("subscribe", returnFix)
}

// NewUnsubscribeStub returns a stub for mqtt.Client Unsubscribe with a fixed
// return value.
func NewUnsubscribeStub(returnFix error) func(quit <-chan struct{}, topicFilters ...string) error {
	return newSubscribeStub("unsubscribe", returnFix)
}

func newSubscribeStub(name string, returnFix error) func(quit <-chan struct{}, topicFilters ...string) error {
	return func(quit <-chan struct{}, topicFilters ...string) error {
		if len(topicFilters) == 0 {
			// TODO(pascaldekloe): move validation to internal
			// package and then return appropriate errors here.
			panic("MQTT " + name + " without topic filters")
		}
		select {
		case <-quit:
			return mqtt.ErrCanceled
		default:
			break
		}
		return returnFix
	}
}

// Filter defines a subscription exchange.
type Filter struct {
	Topics []string // order is ignored
	Err    error    // result
}

// NewSubscribeMock returns a new mock for mqtt.Client Subscribe, which compares
// the invocation with want in order of appearece.
func SubscribeMock(t testing.TB, want ...Filter) func(quit <-chan struct{}, topicFilters ...string) error {
	t.Helper()
	return newSubscribeMock("subscribe", t, want...)
}

// NewUnsubscribeMock returns a new mock for mqtt.Client Unsubscribe, which
// compares the invocation with want in order of appearece.
func NewUnsubscribeMock(t testing.TB, want ...Filter) func(quit <-chan struct{}, topicFilters ...string) error {
	t.Helper()
	return newSubscribeMock("unsubscribe", t, want...)
}

func newSubscribeMock(name string, t testing.TB, want ...Filter) func(quit <-chan struct{}, topicFilters ...string) error {
	t.Helper()

	var wantIndex uint64

	t.Cleanup(func() {
		if n := uint64(len(want)) - atomic.LoadUint64(&wantIndex); n > 0 {
			t.Errorf("want %d more MQTT %ss", n, name)
		}
	})

	return func(quit <-chan struct{}, topicFilters ...string) error {
		if len(topicFilters) == 0 {
			t.Fatalf("MQTT %s without topic filters", name)
		}
		select {
		case <-quit:
			return mqtt.ErrCanceled
		default:
			break
		}

		i := atomic.AddUint64(&wantIndex, 1) - 1
		if i >= uint64(len(want)) {
			t.Errorf("unwanted MQTT %s of %q", name, topicFilters)
		}
		filter := want[i]

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
			t.Errorf("unwanted MQTT %s of %q (out of %q)", name, wrong, filter.Topics)
		}
		if len(todo) != 0 {
			var miss []string
			for filter := range todo {
				miss = append(miss, filter)
			}
			t.Errorf("no MQTT %s of %q (out of %q)", name, miss, filter.Topics)
		}

		return filter.Err
	}
}
