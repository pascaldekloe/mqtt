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

// Transfer defines a message exchange.
type Transfer struct {
	Message []byte // payload
	Topic   string // destination
	Err     error  // result
}

// NewReadSlicesMock returns a new mock for mqtt.Client ReadSlices, which
// returns the Transfers in order of appearance.
func NewReadSlicesMock(t testing.TB, want ...Transfer) func() (message, topic []byte, err error) {
	t.Helper()

	var wantIndex uint64

	t.Cleanup(func() {
		if n := uint64(len(want)) - atomic.LoadUint64(&wantIndex); n > 0 {
			t.Errorf("want %d more MQTT ReadSlices", n)
		}
	})

	return func() (message, topic []byte, err error) {
		i := atomic.AddUint64(&wantIndex, 1) - 1
		if i >= uint64(len(want)) {
			err = errors.New("unwanted MQTT ReadSlices")
			t.Error(err)
			return
		}

		// use copies to prevent some hard to trace issues
		message = make([]byte, len(want[i].Message))
		copy(message, want[i].Message)
		topic = []byte(want[i].Topic)
		return message, topic, want[i].Err
	}
}

// NewPublishMock returns a new mock for mqtt.Client Publish, which compares the
// invocation with want in order of appearance.
func NewPublishMock(t testing.TB, want ...Transfer) func(quit <-chan struct{}, message []byte, topic string) error {
	t.Helper()

	var wantIndex uint64

	t.Cleanup(func() {
		if n := uint64(len(want)) - atomic.LoadUint64(&wantIndex); n > 0 {
			t.Errorf("want %d more MQTT publishes", n)
		}
	})

	return func(quit <-chan struct{}, message []byte, topic string) error {
		select {
		case <-quit:
			return mqtt.ErrCanceled
		default:
			break
		}

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

// NewPublishStub returns a new stub for mqtt.Client Publish with a fixed return
// value.
func NewPublishStub(returnFix error) func(quit <-chan struct{}, message []byte, topic string) error {
	return func(quit <-chan struct{}, message []byte, topic string) error {
		select {
		case <-quit:
			return mqtt.ErrCanceled
		default:
			return returnFix
		}
	}
}

// ExchangeBlock prevents exchange <-chan error submission.
type ExchangeBlock struct {
	Delay time.Duration // zero defaults to indefinite
}

// Error implements the standard error interface.
func (b ExchangeBlock) Error() string {
	return "mqtttest: ExchangeBlock used as an error"
}

// NewPublishEnqueuedStub returns a stub for mqtt.Client PublishAtLeastOnce or
// PublishExactlyOnce with a fixed return value.
//
// The exchangeFix errors are applied to the exchange return, with an option for
// ExchangeBlock entries. An mqtt.ErrClosed in the exchangeFix keeps the
// exchange channel open (without an extra ExchangeBlock entry).
func NewPublishEnqueuedStub(errFix error, exchangeFix ...error) func(message []byte, topic string) (exchange <-chan error, err error) {
	if errFix != nil && len(exchangeFix) != 0 {
		panic("exchangeFix entries with non-nil errFix")
	}
	var block ExchangeBlock
	for i, err := range exchangeFix {
		switch {
		case err == nil:
			panic("nil entry in exchangeFix")
		case errors.Is(err, mqtt.ErrClosed):
			if i+1 < len(exchangeFix) {
				panic("followup on mqtt.ErrClosed exchangeFix entry")
			}
		case errors.As(err, &block):
			if block.Delay == 0 && i+1 < len(exchangeFix) {
				panic("followup on indefinite ExchangeBlock exchangeFix entry")
			}
		}
	}

	return func(message []byte, topic string) (exchange <-chan error, err error) {
		if errFix != nil {
			return nil, errFix
		}

		ch := make(chan error, len(exchangeFix))
		go func() {
			var block ExchangeBlock
			for _, err := range exchangeFix {
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
func NewSubscribeMock(t testing.TB, want ...Filter) func(quit <-chan struct{}, topicFilters ...string) error {
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
