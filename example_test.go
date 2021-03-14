package mqtt_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/pascaldekloe/mqtt"
	"github.com/pascaldekloe/mqtt/mqtttest"
)

// Publish is a method from mqtt.Client.
var Publish func(quit <-chan struct{}, message []byte, topic string) error

// PublishAtLeastOnce is a method from mqtt.Client.
var PublishAtLeastOnce func(message []byte, topic string) (ack <-chan error, err error)

// Subscribe is a method from mqtt.Client.
var Subscribe func(quit <-chan struct{}, topicFilters ...string) error

// Online is a method from mqtt.Client.
var Online func() <-chan struct{}

func init() {
	PublishAtLeastOnce = mqtttest.NewPublishExchangeStub(nil)
	Subscribe = mqtttest.NewSubscribeStub(nil)
	Online = func() <-chan struct{} { return nil }
}

// It is good practice to install the client from main.
func ExampleClient_setup() {
	client, err := mqtt.VolatileSession("demo-client", &mqtt.Config{
		Dialer:       mqtt.NewDialer("tcp", "localhost:1883"),
		PauseTimeout: 4 * time.Second,
	})
	if err != nil {
		log.Fatal("exit on broken setup: ", err)
	}

	// launch read-routine
	go func() {
		var big *mqtt.BigMessage
		for {
			message, topic, err := client.ReadSlices()
			switch {
			case err == nil:
				// do something with inbound message
				log.Printf("📥 %q: %q", topic, message)

			case errors.As(err, &big):
				log.Printf("📥 %q: %d byte message omitted", big.Topic, big.Size)

			case errors.Is(err, mqtt.ErrClosed):
				log.Print(err)
				return // terminated

			case mqtt.IsConnectionRefused(err):
				log.Print("queue unavailable: ", err)
				// ErrDown for a while
				time.Sleep(15 * time.Minute)

			default:
				log.Print("queue unavailable: ", err)
				// ErrDown during backoff
				time.Sleep(2 * time.Second)
			}
		}
	}()

	// Install each method in use as a package variable. Such setup is
	// compatible with the tools proveded from the mqtttest subpackage.
	Publish = client.Publish
	// Output:
}

// Demonstrates all error scenario and the respective recovery options.
func ExampleClient_PublishAtLeastOnce_critical() {
	for {
		exchange, err := PublishAtLeastOnce([]byte("🍸🆘"), "demo/alert")
		switch {
		case err == nil:
			fmt.Println("alert submitted…")
			break

		case mqtt.IsDeny(err), errors.Is(err, mqtt.ErrClosed):
			fmt.Println("🚨 alert not send:", err)
			return

		case errors.Is(err, mqtt.ErrMax):
			fmt.Println("⚠️ alert submission hold-up:", err)
			time.Sleep(time.Second / 4)
			continue

		default:
			fmt.Println("⚠️ alert submission blocked on persistence malfunction:", err)
			time.Sleep(4 * time.Second)
			continue
		}

		for err := range exchange {
			if errors.Is(err, mqtt.ErrClosed) {
				fmt.Println("🚨 alert exchange suspended:", err)
				// An AdoptSession may continue the transaction.
				return
			}

			fmt.Println("⚠️ alert request transfer interrupted:", err)
		}
		fmt.Println("alert acknowledged ✓")
		break
	}

	// Output:
	// alert submitted…
	// alert acknowledged ✓
}

// Demonstrates all error scenario and the respective recovery options.
func ExampleClient_Subscribe_sticky() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for {
		err := Subscribe(ctx.Done(), "demo/+")
		switch {
		case err == nil:
			fmt.Println("subscribe confirmed by broker")
			return

		case errors.As(err, new(mqtt.SubscribeError)):
			fmt.Println("subscribe failed by broker")
			return

		case mqtt.IsDeny(err): // illegal topic filter
			fmt.Println(err)
			return

		case errors.Is(err, mqtt.ErrClosed):
			fmt.Println("no subscribe due client close")
			return

		case errors.Is(err, mqtt.ErrCanceled):
			fmt.Println("no subscribe due timeout")
			return

		case errors.Is(err, mqtt.ErrAbandoned):
			fmt.Println("subscribe state unknown due timeout")
			return

		case errors.Is(err, mqtt.ErrBreak):
			fmt.Println("subscribe state unknown due connection loss")
			select {
			case <-Online():
				fmt.Println("subscribe retry with new connection")
			case <-ctx.Done():
				fmt.Println("subscribe timeout")
				return
			}

		case errors.Is(err, mqtt.ErrDown):
			fmt.Println("subscribe delay while service is down")
			select {
			case <-Online():
				fmt.Println("subscribe retry with new connection")
			case <-ctx.Done():
				fmt.Println("subscribe timeout")
				return
			}

		case errors.Is(err, mqtt.ErrMax):
			fmt.Println("subscribe hold-up due excessive number of pending requests")
			time.Sleep(2 * time.Second) // backoff

		default:
			fmt.Println("subscribe request transfer interrupted:", err)
			time.Sleep(time.Second / 2) // backoff
		}
	}
	// Output:
	// subscribe confirmed by broker
}
