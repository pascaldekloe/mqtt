package mqtttest_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pascaldekloe/mqtt"
	"github.com/pascaldekloe/mqtt/mqtttest"
)

// Signatures
var (
	client          mqtt.Client
	subscribe       = client.Subscribe
	unsubscribe     = client.Unsubscribe
	publish         = client.Publish
	publishEnqueued = client.PublishAtLeastOnce
	readSlices      = client.ReadSlices
)

// Won't compile on failure.
func TestSignatureMatch(t *testing.T) {
	var c mqtt.Client
	// check dupe assumptions
	subscribe = c.SubscribeLimitAtMostOnce
	subscribe = c.SubscribeLimitAtLeastOnce
	publishEnqueued = c.PublishExactlyOnce

	// check fits
	readSlices = mqtttest.NewReadSlicesStub(mqtttest.Transfer{})
	readSlices = mqtttest.NewReadSlicesMock(t)
	publish = mqtttest.NewPublishMock(t)
	publish = mqtttest.NewPublishStub(nil)
	publishEnqueued = mqtttest.NewPublishExchangeStub(nil)
	subscribe = mqtttest.NewSubscribeMock(t)
	subscribe = mqtttest.NewSubscribeStub(nil)
	unsubscribe = mqtttest.NewUnsubscribeMock(t)
	unsubscribe = mqtttest.NewUnsubscribeStub(nil)
}

func ExampleNewPublishExchangeStub() {
	PublishExchange := mqtttest.NewPublishExchangeStub(nil,
		mqtttest.ExchangeBlock{Delay: time.Millisecond},
		errors.New("test storage failure"),
	)

	exchange, err := PublishExchange([]byte("Hi!"), "announce")
	if err != nil {
		fmt.Println("publish error:", err)
		return
	}
	for err := range exchange {
		fmt.Println("exchange error:", err)
	}
	// Output:
	// exchange error: test storage failure
}
