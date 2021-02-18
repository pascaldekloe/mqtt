package mqtttest_test

import (
	"testing"

	"github.com/pascaldekloe/mqtt"
	"github.com/pascaldekloe/mqtt/mqtttest"
)

// Signatures
var (
	client      mqtt.Client
	subscribe   = client.Subscribe
	unsubscribe = client.Unsubscribe
	publish     = client.Publish
	publishAck  = client.PublishAtLeastOnce
	readSlices  = client.ReadSlices
)

// Won't compile on failure.
func TestSignatureMatch(t *testing.T) {
	var c mqtt.Client
	// check dupe assumptions
	subscribe = c.SubscribeLimitAtMostOnce
	subscribe = c.SubscribeLimitAtLeastOnce
	publishAck = c.PublishExactlyOnce

	// check fits
	readSlices = mqtttest.NewReadSlicesMock(t)
	publish = mqtttest.NewPublishMock(t)
	publish = mqtttest.NewPublishStub(nil)
	publishAck = mqtttest.NewPublishAckStub(nil)
	subscribe = mqtttest.NewSubscribeMock(t)
	subscribe = mqtttest.NewSubscribeStub(nil)
	unsubscribe = mqtttest.NewUnsubscribeMock(t)
	unsubscribe = mqtttest.NewUnsubscribeStub(nil)
}
