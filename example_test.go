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

// DemoClient returns a closed client.
func demoClient() *mqtt.Client {
	// The log lines serve as example explaination only.
	log.SetOutput(ioutil.Discard)

	c := mqtt.NewClient(&mqtt.ClientConfig{
		Connecter: func(context.Context) (net.Conn, error) {
			return nil, errors.New("won't connect demo client")
		},
	})
	go c.Close()
	for {
		time.Sleep(10 * time.Millisecond)
		_, _, err := c.ReadSlices()
		if errors.Is(err, mqtt.ErrClosed) {
			return c
		}
	}
}

func ExampleClient_PublishAtLeastOnce() {
	MQTT := demoClient()

	ack := make(chan error, 1) // must buffer
	for {
		err := MQTT.PublishAtLeastOnce([]byte("🍸🆘"), "demo/alert", ack)
		switch {
		case err == nil:
			log.Print("alert submitted")
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

		case mqtt.IsDeny(err), errors.Is(err, mqtt.ErrClosed):
			log.Print("🚨 alert not send: ", err)
			return

		default:
			backoff := time.Second
			log.Printf("⚠️ alert retry in %s on: %s", backoff, err)
			time.Sleep(backoff)
		}
	}
	// Output:
}

// Demo various error scenario and how to act uppon them.
func ExampleClient_Subscribe_context() {
	MQTT := demoClient()
	const topicFilter = "demo/+"
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	for {
		err := MQTT.Subscribe(ctx.Done(), topicFilter)
		switch {
		case err == nil:
			log.Printf("subscribed to %q", topicFilter)
			return
		case mqtt.IsDeny(err), errors.Is(err, mqtt.ErrClosed):
			log.Print("no subscribe: ", err)
			return
		case errors.Is(err, mqtt.ErrAbort):
			log.Print("subscribe state unknown: ", ctx.Err())
			return
		default:
			backoff := time.Second
			log.Printf("subscribe retry in %s: %s", backoff, err)
			select {
			case <-time.After(backoff):
				continue
			case <-ctx.Done():
				log.Print("subscribe abort: ", ctx.Err())
				return
			}
		}
	}
	// Output:
}
