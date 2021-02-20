package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pascaldekloe/mqtt"
)

var hosts = strings.Fields(os.Getenv("MQTT_HOSTS"))

func TestMain(m *testing.M) {
	if len(hosts) == 0 {
		fmt.Println("integration tests skipped without MQTT_HOSTS environment variable")
	} else {
		os.Exit(m.Run())
	}
}

func TestRace(t *testing.T) {
	for i := range hosts {
		t.Run(hosts[i], func(t *testing.T) {
			host := hosts[i]
			t.Run("at-most-once", func(t *testing.T) {
				t.Parallel()
				race(t, host, 0)
			})
			t.Run("at-least-once", func(t *testing.T) {
				t.Parallel()
				race(t, host, 1)
			})
			t.Run("exactly-once", func(t *testing.T) {
				t.Parallel()
				race(t, host, 2)
			})
		})
	}
}

func race(t *testing.T, host string, deliveryLevel int) {
	const testN = 100
	done := make(chan struct{}) // closed once testN messages received
	testMessage := []byte("Hello World!")
	testTopic := fmt.Sprintf("test/race-%d", deliveryLevel)

	client, err := mqtt.VolatileSession(t.Name(), &mqtt.Config{
		Dialer:         mqtt.NewDialer("tcp", net.JoinHostPort(host, "1883")),
		WireTimeout:    time.Second,
		CleanSession:   true,
		AtLeastOnceMax: testN,
		ExactlyOnceMax: testN,
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := client.Disconnect(ctx.Done()); err != nil {
			t.Error(err)
		}
		wg.Wait()
	})

	wg.Add(1)
	go func() {
		defer wg.Done()

		// collect until closed
		var gotN int
		for {
			message, topic, err := client.ReadSlices()
			switch {
			case err == nil:
				if !bytes.Equal(message, testMessage) || string(topic) != testTopic {
					t.Errorf("got message %q @ %q, want %q @ %q", message, topic, testMessage, testTopic)
				}
				gotN++
				if gotN == testN {
					close(done)
				}

			case errors.Is(err, mqtt.ErrClosed):
				if gotN != testN {
					t.Errorf("got %d messages, want %d", gotN, testN)
				}
				return

			default:
				t.Log("read error:", err)
				time.Sleep(time.Second / 8)
				continue
			}
		}
	}()

	err = client.Subscribe(nil, testTopic)
	if err != nil {
		t.Fatal("subscribe error: ", err)
	}

	// install contenders
	launch := make(chan struct{})
	wg.Add(testN)
	for i := 0; i < testN; i++ {
		go func() {
			defer wg.Done()
			var exchange <-chan error
			<-launch
			var err error
			switch deliveryLevel {
			case 0:
				err = client.Publish(nil, testMessage, testTopic)
			case 1:
				exchange, err = client.PublishAtLeastOnce(testMessage, testTopic)
			case 2:
				exchange, err = client.PublishExactlyOnce(testMessage, testTopic)
			}
			if err != nil {
				t.Error("publish error:", err)
				return
			}
			if deliveryLevel != 0 {
				for err := range exchange {
					t.Error("publish exchange error:", err)
					if errors.Is(err, mqtt.ErrClosed) {
						break
					}
				}
			}
		}()
	}

	time.Sleep(time.Second / 4) // await subscription & contenders
	close(launch)
	select {
	case <-done:
		break
	case <-time.After(4 * time.Second):
		t.Fatal("timeout awaiting message reception")
	}
}
