package mqtt_test

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/pascaldekloe/mqtt"
	"github.com/pascaldekloe/mqtt/mqtttest"
)

// NewClientPipe returns a client which is connected to a pipe.
func newClientPipe(t *testing.T, want ...mqtttest.Transfer) (*mqtt.Client, net.Conn) {
	clientEnd, brokerEnd := net.Pipe()
	client := newClient(t, []net.Conn{clientEnd}, want...)

	wantPacketHex(t, brokerEnd, newClientCONNECTHex)
	sendPacketHex(t, brokerEnd, "20020000") // CONNACK

	return client, brokerEnd
}

func TestPing(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		wantPacketHex(t, conn, "c000") // PINGREQ
		sendPacketHex(t, conn, "d000") // PINGRESP
	})

	err := client.Ping(nil)
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
}

func TestPingReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conn, buf[:]); {
		case err != nil:
			t.Fatal("broker read error:", err)
		case buf[0] != 0xC0:
			t.Fatalf("want PINGREQ head 0xC0, got %#x", buf[0])
		}
		// leave partial read
	})

	err := client.Ping(nil)
	var e net.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
	}
}

func TestPingRespNone(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		wantPacketHex(t, conn, "c000") // PINGREQ
		// leave without response
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*client.WireTimeout)
	defer cancel()
	err := client.Ping(ctx.Done())
	if !errors.Is(err, mqtt.ErrAbandoned) {
		t.Errorf("got error %q [%T], want an mqtt.ErrAbandoned", err, err)
	}
}

func TestSubscribeMultiple(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x82, 19,
			0x60, 0x00, // packet identifier
			0, 5, 'u', '/', 'n', 'o', 'i',
			2, // max QOS
			0, 6, 'u', '/', 's', 'h', 'i', 'n',
			2, // max QOS
		}))
		sendPacketHex(t, conn, "900460000102") // SUBACK
	})

	err := client.Subscribe(nil, "u/noi", "u/shin")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
}

func TestSubscribeReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conn, buf[:]); {
		case err != nil:
			t.Fatal("broker read error:", err)
		case buf[0] != 0x82:
			t.Fatalf("want SUBSCRIBE head 0x82, got %#x", buf[0])
		}
		// leave partial read
	})

	err := client.Subscribe(nil, "x")
	var e net.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
	}
}

func TestSubscribeRespNone(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		wantPacketHex(t, conn, "8206600000017802")
		// leave without response
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*client.WireTimeout)
	defer cancel()
	err := client.Subscribe(ctx.Done(), "x")
	if !errors.Is(err, mqtt.ErrAbandoned) {
		t.Errorf("got error %q [%T], want an mqtt.ErrAbandoned", err, err)
	}
}

func TestPublish(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x30, 12,
			0, 5, 'g', 'r', 'e', 'e', 't',
			'h', 'e', 'l', 'l', 'o'}))
	})

	err := client.Publish(nil, []byte("hello"), "greet")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
}

func TestPublishReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conn, buf[:]); {
		case err != nil:
			t.Fatal("broker read error:", err)
		case buf[0] != 0x30:
			t.Fatalf("want PUBLISH head 0x30, got %#x", buf[0])
		}
		// leave partial read
	})

	err := client.Publish(nil, []byte{'x'}, "y")
	var e net.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
	}
}

func TestPublishAtLeastOnce(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x32, 14,
			0, 5, 'g', 'r', 'e', 'e', 't',
			0x80, 0x00, // packet identifier
			'h', 'e', 'l', 'l', 'o'}))
		sendPacketHex(t, conn, "40028000") // SUBACK
	})

	ack, err := client.PublishAtLeastOnce([]byte("hello"), "greet")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	testAck(t, ack)
}

func TestPublishAtLeastOnceReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conn, buf[:]); {
		case err != nil:
			t.Fatal("broker read error:", err)
		case buf[0] != 0x32:
			t.Fatalf("want PUBLISH head 0x32, got %#x", buf[0])
		}
		// leave partial read
	})

	ack, err := client.PublishAtLeastOnce([]byte{'x'}, "y")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	select {
	case <-time.After(client.WireTimeout):
		t.Error("ack timeout")
	case err, ok := <-ack:
		var e net.Error
		switch {
		case !ok:
			t.Error("ack closed, want a Timeout net.Error")
		case !errors.As(err, &e) || !e.Timeout():
			t.Errorf("got ack error %q [%T], want a Timeout net.Error", err, err)
		}
	}
}

func TestPublishExactlyOnce(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x34, 14,
			0, 5, 'g', 'r', 'e', 'e', 't',
			0xc0, 0x00, // packet identifier
			'h', 'e', 'l', 'l', 'o'}))
		sendPacketHex(t, conn, "5002c000") // PUBREC
		wantPacketHex(t, conn, "6202c000") // PUBREL
		sendPacketHex(t, conn, "7002c000") // PUBCOMP
	})

	ack, err := client.PublishExactlyOnce([]byte("hello"), "greet")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	testAck(t, ack)
}

func TestPublishExactlyOnceReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conn, buf[:]); {
		case err != nil:
			t.Fatal("broker read error:", err)
		case buf[0] != 0x34:
			t.Fatalf("want PUBLISH head 0x34, got %#x", buf[0])
		}
		// leave partial read
	})

	ack, err := client.PublishExactlyOnce([]byte{'x'}, "y")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	select {
	case <-time.After(client.WireTimeout):
		t.Error("ack timeout")
	case err, ok := <-ack:
		var e net.Error
		switch {
		case !ok:
			t.Error("ack closed, want a Timeout net.Error")
		case !errors.As(err, &e) || !e.Timeout():
			t.Errorf("got ack error %q [%T], want a Timeout net.Error", err, err)
		}
	}
}

func testAck(t *testing.T, ack <-chan error) {
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			t.Fatal("ack read timeout")

		case err, ok := <-ack:
			if !ok {
				return
			}
			t.Errorf("ack got error %q [%T]", err, err)
		}
	}
}
