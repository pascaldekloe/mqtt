package integration

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pascaldekloe/mqtt"
)

func hosts(tb testing.TB) []string {
	s, ok := os.LookupEnv("MQTT_HOSTS")
	if !ok {
		tb.Skip("no test targets without MQTT_HOSTS environment variable")
	}
	return strings.Fields(s)
}

// NewTestClient returns an instance for testing.
func newTestClient(t *testing.T, host string, config *mqtt.Config) (client *mqtt.Client, messages <-chan uint64) {
	config.Dialer = mqtt.NewDialer("tcp", net.JoinHostPort(host, "1883"))
	config.WireTimeout = 2 * time.Second
	config.CleanSession = true
	client, err := mqtt.VolatileSession(t.Name(), config)
	if err != nil {
		t.Fatal("volatile session error:", err)
	}

	// messages contain their respective sequence number
	ch := make(chan uint64, 16)
	t.Cleanup(func() {
		err := client.Close()
		if err != nil {
			t.Error("client close error:", err)
		}
		// await read-routine exit
		seqNo, ok := <-ch
		if ok {
			t.Errorf("got message # %d after close", seqNo)
		}
	})
	// launch read routine
	go func() {
		defer close(ch)
		for {
			message, topic, err := client.ReadSlices()
			switch {
			case err == nil:
				if len(message) != 8 {
					t.Errorf("unexpected message %#x on topic %q", message, topic)
				} else {
					ch <- binary.LittleEndian.Uint64(message)
				}

			case errors.Is(err, mqtt.ErrClosed):
				return

			default:
				t.Log("client error:", err)
				time.Sleep(time.Second / 2)
			}
		}
	}()

	for {
		err := client.Subscribe(nil, t.Name())
		switch {
		case err == nil:
			return client, ch
		case errors.Is(err, mqtt.ErrDown):
			time.Sleep(10 * time.Millisecond)
			continue
		default:
			t.Fatal("subscribe error: ", err)
		}
	}
}

func TestRace(t *testing.T) {
	for _, host := range hosts(t) {
		t.Run(host, func(t *testing.T) {
			t.Run("at-most-once", func(t *testing.T) {
				client, messages := newTestClient(t, host, new(mqtt.Config))
				raceAtLevel(t, client, messages, 0)
			})
			t.Run("at-least-once", func(t *testing.T) {
				client, messages := newTestClient(t, host, &mqtt.Config{
					AtLeastOnceMax: 9,
				})
				raceAtLevel(t, client, messages, 1)
			})
			t.Run("exactly-once", func(t *testing.T) {
				client, messages := newTestClient(t, host, &mqtt.Config{
					ExactlyOnceMax: 9,
				})
				raceAtLevel(t, client, messages, 2)
			})
		})
	}
}

func raceAtLevel(t *testing.T, client *mqtt.Client, messages <-chan uint64, deliveryLevel int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	testTopic := t.Name()

	const testN = 100 // message amount
	launch := make(chan struct{})

	// install contenders
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(testN)
	for n := uint64(1); n <= testN; n++ {
		go func(seqNo uint64) {
			defer wg.Done()

			var message [8]byte
			binary.LittleEndian.PutUint64(message[:], seqNo)

			<-launch

			var exchange <-chan error
		Publish:
			for {
				var err error
				switch deliveryLevel {
				case 0:
					err = client.Publish(ctx.Done(), message[:], testTopic)
				case 1:
					exchange, err = client.PublishAtLeastOnce(message[:], testTopic)
				case 2:
					exchange, err = client.PublishExactlyOnce(message[:], testTopic)
				}
				switch {
				case err == nil:
					break Publish
				case errors.Is(err, mqtt.ErrMax):
					time.Sleep(200 * time.Microsecond)
				case errors.Is(err, mqtt.ErrClosed):
					return
				default:
					t.Errorf("publish #%d error: %s", seqNo, err)
					return
				}
			}

			if deliveryLevel != 0 {
				err, ok := <-exchange
				if ok {
					t.Errorf("publish # %d exchange error: %s", seqNo, err)
				}
			}
		}(n)
	}

	time.Sleep(50 * time.Millisecond)
	close(launch)

	timeout := time.After(time.Minute)
	for i := 0; i < testN; i++ {
		select {
		case _, ok := <-messages:
			if !ok {
				t.Fatalf("want %d more messages", testN-i)
			}
		case <-timeout:
			t.Fatalf("timeout; want %d more messages", testN-i)
		}
	}
}

func TestRoundtrip(t *testing.T) {
	for _, host := range hosts(t) {
		t.Run(host, func(t *testing.T) {
			t.Run("at-least-once", func(t *testing.T) {
				client, messages := newTestClient(t, host, &mqtt.Config{
					AtLeastOnceMax: 9,
				})
				testRoundtrip(t, client, client.PublishAtLeastOnce, messages)
			})

			t.Run("exactly-once", func(t *testing.T) {
				client, messages := newTestClient(t, host, &mqtt.Config{
					ExactlyOnceMax: 9,
				})
				testRoundtrip(t, client, client.PublishExactlyOnce, messages)
			})
		})
	}
}

func testRoundtrip(t *testing.T, client *mqtt.Client,
	publish func(message []byte, topic string) (exchange <-chan error, err error),
	messages <-chan uint64) {
	testTopic := t.Name()

	var wg sync.WaitGroup
	defer wg.Wait()
	const testN = 17_000 // causes mqtt.publishIDMask overflow

	wg.Add(1)
	go func() {
		defer wg.Done()

		message := make([]byte, 8)
		for n := uint64(1); n <= testN; {
			binary.LittleEndian.PutUint64(message, n)
			exchange, err := publish(message, testTopic)
			switch {
			case err == nil:
				n++

				wg.Add(1)
				go func(seqNo uint64, exchange <-chan error) {
					defer wg.Done()
					err, ok := <-exchange
					if ok {
						t.Errorf("publish # %d exchange error: %s", seqNo, err)
					}
				}(n, exchange)
			case errors.Is(err, mqtt.ErrMax):
				time.Sleep(time.Millisecond)
			case errors.Is(err, mqtt.ErrClosed):
				return
			default:
				t.Errorf("publish # %d error: %s", n, err)
				return
			}
		}
	}()

	timeout := time.After(10 * time.Minute)
	for n := 1; n <= testN; n++ {
		select {
		case seqNo, ok := <-messages:
			if !ok {
				t.Fatalf("did not receive message # %d–%d", n, testN)
			}
			if seqNo != uint64(n) {
				t.Errorf("want message # %d, got # %d", n, seqNo)
			}
		case <-timeout:
			t.Fatalf("timeout before message # %d-%d", n, testN)
		}
	}
}
