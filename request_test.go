package mqtt

import (
	"context"
	"encoding/hex"
	"errors"
	"net"
	"testing"
	"time"
)

func testRoutine(t *testing.T, f func()) {
	t.Helper()
	done := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-done:
			break // OK
		case <-time.After(time.Second / 8):
			t.Error("test routine leak")
		}
	})
	go func() {
		defer close(done)
		f()
	}()
}

// Reception defines an inbound message.
type reception struct {
	Message, Topic string
}

// NewClientPipe returns a client which is connected to a pipe.
// The setup comes with cleanup logic plus timeout protection.
func newClientPipe(t *testing.T, want ...reception) (*Client, net.Conn) {
	clientEnd, brokerEnd := net.Pipe()

	timeoutDone := make(chan struct{})
	timeout := time.AfterFunc(time.Second, func() {
		defer close(timeoutDone)
		t.Error("test timeout; closing pipe now…")
		brokerEnd.Close()
	})

	var connectN int
	client := NewClient(&ClientConfig{
		Connecter: func(context.Context) (net.Conn, error) {
			if connectN != 0 {
				return nil, errors.New("reconnect (with test pipe) denied")
			}
			connectN++
			return clientEnd, nil
		},
		WireTimeout:    time.Second / 2,
		AtLeastOnceMax: 2,
		ExactlyOnceMax: 2,
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if !timeout.Stop() {
				// await all routines
				<-timeoutDone
			}
		}()

		for {
			message, topic, err := client.ReadSlices()
			switch {
			case err == nil:
				switch {
				case len(want) == 0:
					t.Errorf("got unexpected message %q @ %q", message, topic)
				case string(message) != want[0].Message, string(topic) != want[0].Topic:
					t.Errorf("got message %q @ %q, want %q @ %q", message, topic, want[0].Message, want[0].Topic)
				default:
					want = want[1:] // move to next in line
				}

			case errors.Is(err, ErrClosed):
				if len(want) != 0 {
					t.Errorf("client closed—want %d more messages", len(want))
				}
				return

			default:
				t.Error("read routine error:", err)
				// Prevent a log flood on persistent
				// errors with a small retry delay.
				time.Sleep(time.Second / 3)
			}
		}
	}()

	// read CONNECT
	wantPacketHex(t, brokerEnd, "100c00044d515454040000000000")
	// write CONNACK
	sendPacketHex(t, brokerEnd, "20020000")

	t.Cleanup(func() {
		err := client.Close()
		if err != nil {
			t.Error("client close error:", err)
		}
		select {
		case <-done:
			break
		case <-time.After(2 * time.Second):
			t.Error("timeout on read routine exit")
		}
	})

	return client, brokerEnd
}

func TestPing(t *testing.T) {
	client, conn := newClientPipe(t)

	mockDone := make(chan struct{})
	go func() {
		defer close(mockDone)

		t.Log("read PINGREQ…")
		wantPacketHex(t, conn, "c000")
		t.Log("write PINGRESP…")
		sendPacketHex(t, conn, "d000")
		t.Log("mock finished")
	}()

	err := client.Ping(nil)
	if err != nil {
		t.Error("ping error:", err)
	}

	select {
	case <-mockDone:
		break // OK
	case <-time.After(100 * time.Millisecond):
		t.Error("mock timeout")
	}
}

func TestSubscribe(t *testing.T) {
	client, conn := newClientPipe(t)

	testRoutine(t, func() {
		err := client.Subscribe(nil, "u/noi", "u/shin")
		if err != nil {
			t.Fatal("subscribe error:", err)
		}
	})

	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x82, 19,
		0x40, 0x00, // packet identifier
		0, 5, 'u', '/', 'n', 'o', 'i',
		2, // max QOS
		0, 6, 'u', '/', 's', 'h', 'i', 'n',
		2, // max QOS
	}))

	sendPacketHex(t, conn, "900440000102") // SUBACK
}

func TestPublish(t *testing.T) {
	client, conn := newClientPipe(t)

	testRoutine(t, func() {
		err := client.Publish([]byte("hello"), "greet")
		if err != nil {
			t.Error("publish error:", err)
		}
	})

	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x30, 12,
		0, 5, 'g', 'r', 'e', 'e', 't',
		'h', 'e', 'l', 'l', 'o'}))
}

func TestPublishAtLeastOnce(t *testing.T) {
	client, conn := newClientPipe(t)

	ack := make(chan error)
	testRoutine(t, func() {
		err := client.PublishAtLeastOnce([]byte("hello"), "greet", ack)
		if err != nil {
			t.Fatal("publish error:", err)
		}
	})

	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x32, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0x80, 0x00, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	select {
	case err, ok := <-ack:
		if ok {
			t.Error("acknowledge error:", err)
		} else {
			t.Error("acknowledge before PUBACK")
		}
	case <-time.After(time.Millisecond):
		break
	}

	sendPacketHex(t, conn, "40028000") // SUBACK
	select {
	case err, ok := <-ack:
		if ok {
			t.Error("acknowledge error:", err)
		}
	case <-time.After(time.Second / 4):
		t.Error("acknowledge timeout (after PUBACK)")
	}
}

func TestPublishExactlyOnce(t *testing.T) {
	client, conn := newClientPipe(t)

	ack := make(chan error)
	testRoutine(t, func() {
		err := client.PublishExactlyOnce([]byte("hello"), "greet", ack)
		if err != nil {
			t.Fatal("publish error:", err)
		}
	})

	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x34, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0xc0, 0x00, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	sendPacketHex(t, conn, "5002c000") // PUBREC
	wantPacketHex(t, conn, "6202c000") // PUBREL
	select {
	case err, ok := <-ack:
		if ok {
			t.Error("acknowledge error:", err)
		} else {
			t.Error("acknowledge before PUBCOMP")
		}
	case <-time.After(time.Millisecond):
		break
	}

	sendPacketHex(t, conn, "7002c000") // PUBCOMP
	select {
	case err, ok := <-ack:
		if ok {
			t.Error("acknowledge error:", err)
		}
	case <-time.After(time.Second / 4):
		t.Error("acknowledge timeout (after PUBCOMP)")
	}
}

func TestReceivePublish(t *testing.T) {
	_, conn := newClientPipe(t, reception{Message: "hello", Topic: "greet"})

	sendPacketHex(t, conn, hex.EncodeToString([]byte{
		0x30, 12,
		0, 5, 'g', 'r', 'e', 'e', 't',
		'h', 'e', 'l', 'l', 'o'}))
	// await message reception with timeout
	time.Sleep(time.Second / 4)
}

func TestReceivePublishAtLeastOnce(t *testing.T) {
	_, conn := newClientPipe(t, reception{Message: "hello", Topic: "greet"})

	sendPacketHex(t, conn, hex.EncodeToString([]byte{
		0x32, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0xab, 0xcd, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	wantPacketHex(t, conn, "4002abcd") // PUBACK
}

func TestReceivePublishExactlyOnce(t *testing.T) {
	_, conn := newClientPipe(t, reception{Message: "hello", Topic: "greet"})

	// write PUBLISH
	sendPacketHex(t, conn, hex.EncodeToString([]byte{
		0x34, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0xab, 0xcd, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	wantPacketHex(t, conn, "5002abcd") // PUBREC
	sendPacketHex(t, conn, "6002abcd") // PUBREL
	wantPacketHex(t, conn, "7002abcd") // PUBCOMP
}
