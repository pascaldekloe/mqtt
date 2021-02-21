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

func newClientPipeN(t *testing.T, n int, want ...mqtttest.Transfer) (*mqtt.Client, []net.Conn) {
	clientConns := make([]net.Conn, n)
	brokerConns := make([]net.Conn, n)
	for i := range clientConns {
		clientConns[i], brokerConns[i] = net.Pipe()
	}
	client := newClient(t, clientConns, want...)

	wantPacketHex(t, brokerConns[0], newClientCONNECTHex)
	sendPacketHex(t, brokerConns[0], "20020000") // CONNACK

	return client, brokerConns
}

func TestPing(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conn, "c000") // PINGREQ
		sendPacketHex(t, conn, "d000") // PINGRESP
	})

	err := client.Ping(nil)
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
}

func TestPingReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
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
	<-brokerMockDone
}

func TestSubscribeMultiple(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
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
	<-brokerMockDone
}

func TestSubscribeReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
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
	<-brokerMockDone
}

func TestUnsubscribeMultiple(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0xa2, 17,
			0x40, 0x00, // packet identifier
			0, 5, 'u', '/', 'n', 'o', 'i',
			0, 6, 'u', '/', 's', 'h', 'i', 'n',
		}))
		sendPacketHex(t, conn, "b0024000") // UNSUBACK
	})

	err := client.Unsubscribe(nil, "u/noi", "u/shin")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
}

func TestUnsubscribeReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conn, buf[:]); {
		case err != nil:
			t.Fatal("broker read error:", err)
		case buf[0] != 0xa2:
			t.Fatalf("want UNSUBSCRIBE head 0xa2, got %#x", buf[0])
		}
		// leave partial read
	})

	err := client.Unsubscribe(nil, "x")
	var e net.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
	}
	<-brokerMockDone
}

func TestPublish(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x30, 12,
			0, 5, 'g', 'r', 'e', 'e', 't',
			'h', 'e', 'l', 'l', 'o'}))
	})

	err := client.Publish(nil, []byte("hello"), "greet")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
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
	brokerMockDone := testRoutine(t, func() {
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
	<-brokerMockDone
}

func TestPublishAtLeastOnceReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
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
	<-brokerMockDone
}

// A Client must resend each PUBLISH which is pending PUBACK when the connection
// is reset (for whater reasons).
func TestPublishAtLeastOnceResend(t *testing.T) {
	client, conns := newClientPipeN(t, 2, mqtttest.Transfer{Err: io.EOF})
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conns[0], hex.EncodeToString([]byte{
			0x32, 6,
			0, 1, 'y',
			0x80, 0x00, // packet identifier
			'x'}))
		if err := conns[0].Close(); err != nil {
			t.Fatal("broker got error on first connection close:", err)
		}

		wantPacketHex(t, conns[1], newClientCONNECTHex)
		sendPacketHex(t, conns[1], "20020000") // CONNACK
		wantPacketHex(t, conns[1], hex.EncodeToString([]byte{
			0x3a, 6, // with duplicate [DUP] flag
			0, 1, 'y',
			0x80, 0x00, // packet identifier
			'x'}))
		sendPacketHex(t, conns[1], "40028000") // SUBACK after all
	})

	ack, err := client.PublishAtLeastOnce([]byte{'x'}, "y")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	testAck(t, ack)
	<-brokerMockDone
}

func TestPublishExactlyOnce(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
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
	<-brokerMockDone
}

func TestPublishExactlyOnceReqTimeout(t *testing.T) {
	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
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
	<-brokerMockDone
}

func TestAbandon(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client, conn := newClientPipe(t)

	pingDone := testRoutine(t, func() {
		err := client.Ping(ctx.Done())
		if !errors.Is(err, mqtt.ErrAbandoned) {
			t.Errorf("ping got error %q [%T], want an mqtt.ErrAbandoned", err, err)
		}
	})
	wantPacketHex(t, conn, "c000") // PINGREQ

	subscribeDone := testRoutine(t, func() {
		err := client.Subscribe(ctx.Done(), "x")
		if !errors.Is(err, mqtt.ErrAbandoned) {
			t.Errorf("subscribe got error %q [%T], want an mqtt.ErrAbandoned", err, err)
		}
	})
	wantPacketHex(t, conn, "8206600000017802") // SUBSCRIBE

	unsubscribeDone := testRoutine(t, func() {
		err := client.Unsubscribe(ctx.Done(), "x")
		if !errors.Is(err, mqtt.ErrAbandoned) {
			t.Errorf("unsubscribe got error %q [%T], want an mqtt.ErrAbandoned", err, err)
		}
	})
	wantPacketHex(t, conn, "a2054001000178") // UNSUBSCRIBE

	cancel()
	<-pingDone
	<-subscribeDone
	<-unsubscribeDone
}

func TestBreak(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, conns := newClientPipeN(t, 2, mqtttest.Transfer{Err: io.EOF})

	pingDone := testRoutine(t, func() {
		err := client.Ping(ctx.Done())
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("ping got error %q [%T], want an mqtt.ErrBreak", err, err)
		}
	})
	wantPacketHex(t, conns[0], "c000") // PINGREQ

	subscribeDone := testRoutine(t, func() {
		err := client.Subscribe(ctx.Done(), "x")
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("subscribe got error %q [%T], want an mqtt.ErrBreak", err, err)
		}
	})
	wantPacketHex(t, conns[0], "8206600000017802") // SUBSCRIBE

	unsubscribeDone := testRoutine(t, func() {
		err := client.Unsubscribe(ctx.Done(), "x")
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("unsubscribe got error %q [%T], want an mqtt.ErrBreak", err, err)
		}
	})
	wantPacketHex(t, conns[0], "a2054001000178") // UNSUBSCRIBE

	if err := conns[0].Close(); err != nil {
		t.Error("broker mock got error on pipe close:", err)
	}
	<-pingDone
	<-subscribeDone
	<-unsubscribeDone
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
