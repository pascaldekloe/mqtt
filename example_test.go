package mqtt_test

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pascaldekloe/mqtt"
)

// Publish is a method from mqtt.Client.
var Publish func(message []byte, topic string) error

// PublishAtLeastOnce is a method from mqtt.Client.
var PublishAtLeastOnce func(message []byte, topic string) (ack <-chan error, err error)

// Subscribe is a method from mqtt.Client.
var Subscribe func(quit <-chan struct{}, topicFilters ...string) error

func init() {
	// The log lines serve as example explanation only.
	log.SetOutput(ioutil.Discard)

	c := mqtt.NewClient(&mqtt.ClientConfig{
		SessionConfig: mqtt.NewVolatileSessionConfig("demo"),
		Connecter: func(context.Context) (net.Conn, error) {
			return nil, errors.New("won't connect demo client")
		},
	})
	c.Close()

	PublishAtLeastOnce = c.PublishAtLeastOnce
	Subscribe = c.Subscribe
}

// It is good practice to setup the client in main.
func ExampleNewClient() {
	client := mqtt.NewClient(&mqtt.ClientConfig{
		Connecter:     mqtt.UnsecuredConnecter("tcp", "localhost:1883"),
		SessionConfig: mqtt.NewVolatileSessionConfig("demo"),
		WireTimeout:   time.Second,
		BufSize:       8192,
	})

	// launch read-routine
	go func() {
		for {
			message, channel, err := client.ReadSlices()
			switch {
			case err == nil:
				// do something with inbound message
				log.Printf("📥 %q: %q", channel, message)

			case errors.Is(err, mqtt.ErrClosed):
				return // terminated
			case mqtt.IsDeny(err):
				log.Fatal(err) // faulty configuration

			default:
				log.Print("MQTT unavailable: ", err)
				// backoff prevents resource hog
				time.Sleep(time.Second / 2)
			}
		}
	}()

	// Install the methods used by the respective packages as variables.
	// Such setup allows for unit tests with stubs.
	Publish = client.Publish

	// apply signals
	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		for sig := range signals {
			switch sig {
			case syscall.SIGINT:
				log.Print("MQTT close on SIGINT…")
				err := client.Close()
				if err != nil {
					log.Print(err)
				}

			case syscall.SIGTERM:
				log.Print("MQTT disconnect on SIGTERM…")
				err := client.Disconnect(nil)
				if err != nil {
					log.Print(err)
				}
			}
		}
	}()
	// Output:
}

func ExampleClient_PublishAtLeastOnce() {
	for {
		ack, err := PublishAtLeastOnce([]byte("🍸🆘"), "demo/alert")
		switch {
		case err == nil:
			log.Print("alert submitted")

		case mqtt.IsDeny(err), errors.Is(err, mqtt.ErrClosed):
			log.Print("🚨 alert not send: ", err)
			return

		default:
			backoff := time.Second
			log.Printf("⚠️ alert retry in %s on: %s", backoff, err)
			time.Sleep(backoff)
			continue
		}

		for err := range ack {
			if errors.Is(err, mqtt.ErrClosed) {
				log.Print("🚨 alert suspended: ", err)
				// Submission will continue when the Client
				// is restarted with the same Store again.
				return
			}
			log.Print("⚠️ alert delay: ", err)
		}
		log.Print("alert confirmed")
		return
	}
	// Output:
}

// Demo various error scenario and how to act uppon them.
func ExampleClient_Subscribe_context() {
	const topicFilter = "demo/+"
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for {
		err := Subscribe(ctx.Done(), topicFilter)
		switch {
		case err == nil:
			log.Printf("subscribed to %q", topicFilter)
			return
		case mqtt.IsDeny(err), errors.Is(err, mqtt.ErrClosed):
			log.Print("no subscribe: ", err)
			return
		case errors.Is(err, mqtt.ErrAbandon):
			log.Print("subscribe state unknown: ", ctx.Err())
			return
		default:
			log.Printf("subscribe retry on: %s", err)
			backoff := time.NewTimer(time.Second)
			select {
			case <-backoff.C:
				continue
			case <-ctx.Done():
				backoff.Stop()
				log.Print("subscribe abort: ", ctx.Err())
				return
			}
		}
	}
	// Output:
}
