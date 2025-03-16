package integration

import (
	"encoding/binary"
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

func hosts(tb testing.TB) []string {
	s, ok := os.LookupEnv("MQTT_HOSTS")
	if !ok {
		tb.Skip("no test targets without MQTT_HOSTS environment variable")
	}
	return strings.Fields(s)
}

// BatchSize is a reasonable number of messages which should not cause any of
// them to be dropped (by the broker) when send sequentially.
const batchSize = 128

// SendBatch publishes a total of batchSize messages, with 8-byte payloads,
// containing msgOffset until msgOffset + batchSize − 1. It pushes for the
// maximum number of in-flight messages allowed by the mqtt.Client.
func sendBatch(t testing.TB, publish func([]byte, string) (<-chan error, error), msgOffset uint64) {
	retry := time.NewTicker(time.Microsecond)
	defer retry.Stop()

	// confirmation channels
	var exchanges [batchSize]<-chan error
	topic := t.Name()

	// publish each
	for i := uint64(0); i < batchSize; {
		var msg [8]byte
		binary.BigEndian.PutUint64(msg[:], msgOffset+i)
		ch, err := publish(msg[:], topic)
		switch {
		case err == nil:
			exchanges[i] = ch
			i++

		case errors.Is(err, mqtt.ErrMax):
			<-retry.C

		default:
			t.Error("publish batch abort on:", err)
			return
		}
	}

	// read confirmations
	for i := range exchanges {
		for err := range exchanges[i] {
			t.Error("publish exchange abort on:", err)
			return
		}
	}
}

// ReceiveBatch reads a sendBatch sequence.
func receiveBatch(t testing.TB, messages <-chan uint64, offset uint64) {
	for i := uint64(0); i < batchSize; i++ {
		got, ok := <-messages
		want := offset + i
		if !ok {
			t.Errorf("receive stopped, want message # %d", want)
			return
		}
		if got != want {
			t.Errorf("got message # %d, want # %d", got, want)
		}
	}
}

func exchangeN(t testing.TB, n uint64, publish func([]byte, string) (<-chan error, error), messages <-chan uint64) {
	if n%batchSize != 0 {
		t.Fatalf("exchange count %d must be a multiple of %d", n, batchSize)
	}

	for i := uint64(0); i < n; i += batchSize {
		done := make(chan struct{})
		go func() {
			defer close(done)
			sendBatch(t, publish, i)
		}()
		receiveBatch(t, messages, i)
		<-done
		if t.Failed() {
			return
		}
	}
}

func TestRoundtrip(t *testing.T) {
	for _, host := range hosts(t) {
		t.Run(host, func(t *testing.T) {
			testRoundtripHost(t, host)
		})
	}
}

func testRoundtripHost(t *testing.T, host string) {
	// client instantiation
	clientID := t.Name()
	config := mqtt.Config{
		Dialer:         mqtt.NewDialer("tcp", net.JoinHostPort(host, "1883")),
		PauseTimeout:   time.Second,
		CleanSession:   true,
		AtLeastOnceMax: 9,
		ExactlyOnceMax: 9,
	}

	// target specifics
	switch host {
	case "activemq":
		config.UserName = "artemis"
		config.Password = []byte("artemis")

	case "rumqttd":
		config.KeepAlive = 20
		clientID = strings.Replace(clientID, "/", "-", 1)

	case "volantmq":
		config.UserName = "testuser"
		config.Password = []byte("testpassword")
	}

	client, err := mqtt.VolatileSession(clientID, &config)
	if err != nil {
		t.Fatal("client instantiation:", err)
	}

	testRoundtripClient(t, client)
}

func testRoundtripClient(t *testing.T, client *mqtt.Client) {
	const messageN = 16384 + batchSize // overflows mqtt.publishIDMask

	// receive streams
	atMostOnceMessages := make(chan uint64)
	atLeastOnceMessages := make(chan uint64)
	exactlyOnceMessages := make(chan uint64)

	// The read routine continues until Client Close.
	// Errors are discarded once the channel buffer is full.
	readDone := make(chan error, 60)
	go func() {
		defer close(readDone)

		defer close(atMostOnceMessages)
		defer close(atLeastOnceMessages)
		defer close(exactlyOnceMessages)

		for {
			message, topic, err := client.ReadSlices()
			if err != nil {
				wait := client.ReadBackoff(err)
				if wait == nil {
					return // terminated
				}

				select {
				case readDone <- err:
				default: // discard
				}

				<-wait
				continue
			}

			if len(message) != 8 {
				select {
				case readDone <- fmt.Errorf("unexpected message %#x on topic %q", message, topic):
				default: // discard
				}
				continue
			}
			seqNo := binary.BigEndian.Uint64(message)

			switch s := string(topic); {
			case strings.HasSuffix(s, "/at-most-once"):
				atMostOnceMessages <- seqNo
			case strings.HasSuffix(s, "/at-least-once"):
				atLeastOnceMessages <- seqNo
			case strings.HasSuffix(s, "/exactly-once"):
				exactlyOnceMessages <- seqNo
			default:
				select {
				case readDone <- fmt.Errorf("message # %d on unexpected topic %q", seqNo, topic):
				default: // discard
				}
			}
		}
	}()

	// test each QoS in parallel
	var testGroup sync.WaitGroup
	testGroup.Add(3)

	t.Run("at-most-once", func(t *testing.T) {
		defer testGroup.Done()
		t.Parallel()

		<-client.Online()
		t.Log("client online")

		err := client.Subscribe(nil, t.Name())
		if err != nil {
			t.Fatal(err)
		}
		exchange := make(chan error)
		close(exchange)
		exchangeN(t, messageN, func(message []byte, topic string) (<-chan error, error) {
			err := client.Publish(nil, message, topic)
			return exchange, err
		}, atMostOnceMessages)
	})

	t.Run("at-least-once", func(t *testing.T) {
		defer testGroup.Done()
		t.Parallel()

		<-client.Online()
		t.Log("client online")

		err := client.Subscribe(nil, t.Name())
		if err != nil {
			t.Fatal(err)
		}
		exchangeN(t, messageN, client.PublishAtLeastOnce, atLeastOnceMessages)
	})

	t.Run("exactly-once", func(t *testing.T) {
		defer testGroup.Done()
		t.Parallel()

		<-client.Online()
		t.Log("client online")

		err := client.Subscribe(nil, t.Name())
		if err != nil {
			t.Fatal(err)
		}
		exchangeN(t, messageN, client.PublishExactlyOnce, exactlyOnceMessages)
	})

	t.Run("clean-exit", func(t *testing.T) {
		t.Parallel()

		<-client.Online()
		t.Log("client online")

		testGroup.Wait()

		t.Log("disconnect request")
		err := client.Disconnect(nil)
		if err != nil {
			t.Error(err)
		}

		<-client.Offline()
		t.Log("client offline")

		for err := range readDone {
			t.Error(err)
		}
	})
}
