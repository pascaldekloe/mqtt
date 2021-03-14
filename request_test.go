package mqtt_test

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/pascaldekloe/mqtt"
	"github.com/pascaldekloe/mqtt/mqtttest"
)

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
	case <-time.After(client.PauseTimeout):
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

		wantPacketHex(t, conns[1], pipeCONNECTHex)
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

func TestPublishAtLeastOnceRestart(t *testing.T) {
	t.Parallel()

	p := mqtt.FileSystem(t.TempDir())

	clientConn, brokerConn := net.Pipe()
	client, err := mqtt.InitSession("test-client", p, &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 3,
		Dialer:         newTestDialer(t, clientConn),
	})
	if err != nil {
		t.Fatal("InitSession error:", err)
	}
	testClient(t, client)
	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK

	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, brokerConn, hex.EncodeToString([]byte{
			0x32, 6,
			0, 1, 'x',
			0x80, 0x00, // 1st packet identifier
			'1'}))
		wantPacketHex(t, brokerConn, hex.EncodeToString([]byte{
			0x32, 6,
			0, 1, 'x',
			0x80, 0x01, // 2nd packet identifier
			'2'}))

		sendPacketHex(t, brokerConn, "40028000") // SUBACK 1st

		var buf [1]byte
		switch _, err := io.ReadFull(brokerConn, buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0x32:
			t.Errorf("want PUBLISH head 0x32, got %#x", buf[0])
		}
		// leave 3rd partial read

		err := client.Close()
		if err != nil {
			t.Error("Close error:", err)
		}
	})

	ack1, err := client.PublishAtLeastOnce([]byte{'1'}, "x")
	if err != nil {
		t.Errorf("publish #1 got error %q [%T]", err, err)
	}
	ack2, err := client.PublishAtLeastOnce([]byte{'2'}, "x")
	if err != nil {
		t.Errorf("publish #2 got error %q [%T]", err, err)
	}
	ack3, err := client.PublishAtLeastOnce([]byte{'3'}, "x")
	if err != nil {
		t.Errorf("publish #3 got error %q [%T]", err, err)
	}
	<-brokerMockDone
	testAck(t, ack1)
	testAckClosed(t, ack2)
	testAckClosed(t, ack3)

	// continue with another Client
	clientConn, brokerConn = net.Pipe()
	client, warn, err := mqtt.AdoptSession(p, &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 3,
		Dialer:         newTestDialer(t, clientConn),
	})
	if err != nil {
		t.Fatal("AdoptSession error:", err)
	}
	for _, err := range warn {
		t.Error("AdoptSession warning:", err)
	}
	testClient(t, client)
	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK
	wantPacketHex(t, brokerConn, hex.EncodeToString([]byte{
		0x3a, 6, // with duplicate [DUP] flag
		0, 1, 'x',
		0x80, 0x01, // 2nd packet identifier
		'2'}))
	wantPacketHex(t, brokerConn, hex.EncodeToString([]byte{
		0x3a, 6, // with duplicate [DUP] flag
		0, 1, 'x',
		0x80, 0x02, // 3rd packet identifier
		'3'}))
	sendPacketHex(t, brokerConn, "40028001") // SUBACK 2nd
	sendPacketHex(t, brokerConn, "40028002") // SUBACK 3rd
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
	case <-time.After(client.PauseTimeout):
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

// Brokers may resend a PUBREL even after receiving PUBCOMP (in case the serice
// crashed for example).
func TestPUBRELRetry(t *testing.T) {
	_, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
		sendPacketHex(t, conn, "62021234") // PUBREL
		wantPacketHex(t, conn, "70021234") // PUBCOMP
	})
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

func TestDeny(t *testing.T) {
	// no invocation to the client allowed
	client, _ := newClientPipe(t)

	// UTF-8 validation
	err := client.PublishRetained(nil, nil, "topic with \xED\xA0\x80 not allowed")
	if !mqtt.IsDeny(err) {
		t.Errorf("publish with U+D800 in topic got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	_, err = client.PublishAtLeastOnceRetained(nil, "topic with \xED\xA0\x81 not allowed")
	if !mqtt.IsDeny(err) {
		t.Errorf("publish with U+D801 in topic got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	_, err = client.PublishExactlyOnceRetained(nil, "topic with \xED\xBF\xBF not allowed")
	if !mqtt.IsDeny(err) {
		t.Errorf("publish with U+DFFF in topic got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	err = client.SubscribeLimitAtMostOnce(nil, "null char \x00 not allowed")
	if !mqtt.IsDeny(err) {
		t.Errorf("subscribe with null character got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	err = client.SubscribeLimitAtLeastOnce(nil, "char \x80 breaks UTF-8")
	if !mqtt.IsDeny(err) {
		t.Errorf("subscribe with broken UTF-8 got error %q [%T], want an mqtt.IsDeny", err, err)
	}

	err = client.Subscribe(nil)
	if !mqtt.IsDeny(err) {
		t.Errorf("subscribe with nothing got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	err = client.Unsubscribe(nil)
	if !mqtt.IsDeny(err) {
		t.Errorf("unsubscribe with nothing got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	err = client.Subscribe(nil, "")
	if !mqtt.IsDeny(err) {
		t.Errorf("subscribe with zero topic got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	err = client.Unsubscribe(nil, "")
	if !mqtt.IsDeny(err) {
		t.Errorf("unsubscribe with zero topic got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	err = client.Publish(nil, nil, "")
	if !mqtt.IsDeny(err) {
		t.Errorf("publish with zero topic got error %q [%T], want an mqtt.IsDeny", err, err)
	}

	// size limits
	tooBig := strings.Repeat("A", 1<<16)
	err = client.Unsubscribe(nil, tooBig)
	if !mqtt.IsDeny(err) {
		t.Errorf("unsubscribe with 64 KiB filter got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	err = client.Publish(nil, make([]byte, 256*1024*1024), "")
	if !mqtt.IsDeny(err) {
		t.Errorf("publish with 256 MiB got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	filtersTooBig := make([]string, 256*1024)
	KiB := strings.Repeat("A", 1024)
	for i := range filtersTooBig {
		filtersTooBig[i] = KiB
	}
	err = client.Subscribe(nil, filtersTooBig...)
	if !mqtt.IsDeny(err) {
		t.Errorf("subscribe with 256 MiB topic filters got error %q [%T], want an mqtt.IsDeny", err, err)
	}
	err = client.Unsubscribe(nil, filtersTooBig...)
	if !mqtt.IsDeny(err) {
		t.Errorf("unsubscribe with 256 MiB topic filters got error %q [%T], want an mqtt.IsDeny", err, err)
	}
}

func testAck(t *testing.T, ack <-chan error) {
	t.Helper()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()

	select {
	case <-timeout.C:
		t.Fatal("ack read timeout")

	case err, ok := <-ack:
		if ok {
			t.Errorf("ack got error %q [%T]", err, err)
		}
	}
}

func testAckClosed(t *testing.T, ack <-chan error) {
	t.Helper()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()

	select {
	case <-timeout.C:
		t.Fatal("ack read timeout")

	case err := <-ack:
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("ack got error %q [%T]", err, err)
		}
	}
}
