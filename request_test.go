package mqtt_test

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pascaldekloe/mqtt"
	"github.com/pascaldekloe/mqtt/mqtttest"
)

func TestBackoff_ErrMax(t *testing.T) {
	client, _, testTimeout := newTestClientOnline(t)

	const parallelism = 5
	backoffs := make(chan (<-chan struct{}), parallelism)

	launch := make(chan struct{})
	for i := 0; i < parallelism; i++ {
		go func() {
			<-launch // race start
			backoffs <- client.Backoff(mqtt.ErrMax)
		}()
	}
	close(launch)

	first := <-backoffs
	if first == nil {
		t.Fatal("got no backoff for retriable error")
	}
	select {
	case <-first:
		t.Fatal("backoff expired on arrival")
	default:
		break // OK
	}

	// match first against all others
	for i := 1; i < parallelism; i++ {
		switch <-backoffs {
		case nil:
			t.Fatal("got no backoff for retriable error")
		case first:
			break // OK
		default:
			t.Errorf("got another wait channel; want all the same")
		}
	}

	t.Log("await backoff channel")
	select {
	case <-first:
		break // good
	case <-testTimeout:
		t.Error("test timeout before backoff expiry")
	}
}

func TestPing(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t)
	pingDone := testRoutine(t, func() {
		err := client.Ping(testTimeout)
		if err != nil {
			t.Error("ping got error:", err)
		}
	})
	wantPacketHex(t, conn, "c000") // PINGREQ
	sendPacketHex(t, conn, "d000") // PINGRESP
	<-pingDone
}

// Ping should await the first connect attempt.
func TestPing_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	pingDone := testRoutine(t, func() {
		err := client.Ping(testTimeout)
		if err != nil {
			t.Error("ping got error:", err)
		}
	})
	// CONNECT slowdown causes wait scenario
	time.Sleep(100 * time.Millisecond)
	wantConnectExchange(t, conn)
	wantPacketHex(t, conn, "c000") // PINGREQ
	sendPacketHex(t, conn, "d000") // PINGRESP
	<-pingDone
}

// Ping should signal ErrDown when the first connect attempt fails.
func TestPing_failedConnect(t *testing.T) {
	client, conns, testTimeout := newTestClientRedial(t,
		mqtttest.Transfer{Err: mqtt.ErrUnavailable})

	pingDone := testRoutine(t, func() {
		err := client.Ping(testTimeout)
		if !errors.Is(err, mqtt.ErrDown) {
			t.Errorf("ping got error %v, want ErrDown", err)
		}

		// second ping when Online again
		wantOnline(t, client, testTimeout)
		err = client.Ping(testTimeout)
		if err != nil {
			t.Error("ping got error after Online signal:", err)
		}
	})

	// fail first connect
	wantPacketHex(t, conns[0], "100c00044d515454040000000000") // CONNECT
	sendPacketHex(t, conns[0], "20020003")                     // CONNACK
	time.Sleep(200 * time.Millisecond)                         // stay ErrDown
	// accept second connect + ping exchange
	wantConnectExchange(t, conns[1])
	wantPacketHex(t, conns[1], "c000") // PINGREQ
	sendPacketHex(t, conns[1], "d000") // PINGRESP
	<-pingDone
}

// Ping should get a timeout error on stale request submission.
func TestPing_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)

	pingDone := testRoutine(t, func() {
		err := client.Ping(testTimeout)
		var e net.Error
		if !errors.As(err, &e) || !e.Timeout() {
			t.Errorf("got error %v, want a Timeout net.Error", err)
			return
		}
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for ping error")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for ping error does not equal Online channel")
		}
	})

	// read first byte
	var buf [1]byte
	_, err := io.ReadFull(conns[0], buf[:])
	if err != nil {
		t.Fatal("broker read error:", err)
	}
	if buf[0] != 0xC0 {
		t.Errorf("want PINGREQ head 0xC0, got %#x", buf[0])
	}
	t.Log("first connection abandoned after partial read")
	// check reconnect
	wantPacketHex(t, conns[1], "100c00044d515454040000000000") // CONNECT
	t.Log("second connection abandoned after connect request")
	<-pingDone
}

// Subscribe should await the first connect attempt.
func TestSubscribe_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	subscribeDone := testRoutine(t, func() {
		err := client.Subscribe(testTimeout, "u/noi", "u/shin")
		if err != nil {
			t.Error("subscribe got error:", err)
		}
	})
	// CONNECT slowdown causes wait scenario
	time.Sleep(100 * time.Millisecond)
	wantConnectExchange(t, conn)
	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x82, 19,
		0x60, 0x00, // packet identifier
		0, 5, 'u', '/', 'n', 'o', 'i',
		2, // max QOS
		0, 6, 'u', '/', 's', 'h', 'i', 'n',
		2, // max QOS
	}))
	sendPacketHex(t, conn, "900460000102") // SUBACK
	<-subscribeDone
}

// Subscribe should get a timeout error on stale request submission.
func TestSubscribe_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)

	subscribeDone := testRoutine(t, func() {
		err := client.Subscribe(testTimeout, "x")
		var e net.Error
		if !errors.As(err, &e) || !e.Timeout() {
			t.Errorf("got error %v, want a Timeout net.Error", err)
			return
		}
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for subscribe error")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for subscribe error does not equal Online channel")
		}
	})

	// read first byte
	var buf [1]byte
	_, err := io.ReadFull(conns[0], buf[:])
	if err != nil {
		t.Fatal("broker read error:", err)
	}
	if buf[0] != 0x82 {
		t.Errorf("want SUBSCRIBE head 0x82, got %#x", buf[0])
	}
	t.Log("first connection abandoned after partial read")
	// check reconnect
	wantPacketHex(t, conns[1], "100c00044d515454040000000000") // CONNECT
	t.Log("second connection abandoned after connect request")
	<-subscribeDone
}

// Unsubscribe should await the first connect attempt.
func TestUnsubscribe_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	unsubscribeDone := testRoutine(t, func() {
		err := client.Unsubscribe(testTimeout, "u/noi", "u/shin")
		if err != nil {
			t.Errorf("got error %q [%T]", err, err)
		}
	})
	// CONNECT slowdown causes wait scenario
	time.Sleep(100 * time.Millisecond)
	wantConnectExchange(t, conn)
	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0xa2, 17,
		0x40, 0x00, // packet identifier
		0, 5, 'u', '/', 'n', 'o', 'i',
		0, 6, 'u', '/', 's', 'h', 'i', 'n',
	}))
	sendPacketHex(t, conn, "b0024000") // UNSUBACK
	<-unsubscribeDone
}

// Unsubscribe should get a timeout error on stale request submission.
func TestUnsubscribe_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)

	unsubscribeDone := testRoutine(t, func() {
		err := client.Unsubscribe(testTimeout, "x")
		var e net.Error
		if !errors.As(err, &e) || !e.Timeout() {
			t.Errorf("unsubscribe got error %v, want a Timeout net.Error", err)
		}
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for unsubscribe error")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for unsubscribe error does not equal Online channel")
		}
	})

	// read first byte
	var buf [1]byte
	_, err := io.ReadFull(conns[0], buf[:])
	if err != nil {
		t.Fatal("broker read error:", err)
	}
	if buf[0] != 0xa2 {
		t.Errorf("want UNSUBSCRIBE head 0xa2, got %#x", buf[0])
	}
	t.Log("first connection abandoned after partial read")
	// check reconnect
	wantPacketHex(t, conns[1], "100c00044d515454040000000000") // CONNECT
	t.Log("second connection abandoned after connect request")
	<-unsubscribeDone
}

// Publish should await the first connect attempt.
func TestPublish_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	publishDone := testRoutine(t, func() {
		err := client.Publish(testTimeout, []byte("hello"), "greet")
		if err != nil {
			t.Error("publish got error:", err)
		}
	})
	// CONNECT slowdown causes wait scenario
	time.Sleep(100 * time.Millisecond)
	wantConnectExchange(t, conn)
	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x30, 12,
		0, 5, 'g', 'r', 'e', 'e', 't',
		'h', 'e', 'l', 'l', 'o'}))
	<-publishDone
}

// Publish should get a timeout error on stale request submission.
func TestPublish_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)

	publishDone := testRoutine(t, func() {
		err := client.Publish(testTimeout, []byte{'x'}, "y")
		var e net.Error
		if !errors.As(err, &e) || !e.Timeout() {
			t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
			return
		}
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for publish error")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for publish error does not equal Online channel")
		}
	})

	// read first byte
	var buf [1]byte
	_, err := io.ReadFull(conns[0], buf[:])
	if err != nil {
		t.Fatal("broker read error:", err)
	}
	if buf[0] != 0x30 {
		t.Errorf("want PUBLISH head 0x30, got %#x", buf[0])
	}
	t.Log("first connection abandoned after partial read")
	// check reconnect
	wantPacketHex(t, conns[1], "100c00044d515454040000000000")
	t.Log("second connection abandoned after connect request")
	<-publishDone
}

func TestPublishAtLeastOnce(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t)
	publishDone := testRoutine(t, func() {
		exchange, err := client.PublishAtLeastOnce([]byte("hello"), "greet")
		if err != nil {
			t.Error("publish got error:", err)
		}
		verifyExchange(t, testTimeout, exchange, nil)
	})
	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x32, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0x80, 0x00, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	sendPacketHex(t, conn, "40028000") // PUBACK
	<-publishDone
}

// Publish should enqueue and continue once online.
func TestPublishAtLeastOnce_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	publishDone := testRoutine(t, func() {
		exchange, err := client.PublishAtLeastOnce([]byte("hello"), "greet")
		if err != nil {
			t.Error("publish got error:", err)
		}
		verifyExchange(t, testTimeout, exchange,
			"mqtt: not connected; PUBLISH enqueued", nil)
	})
	// CONNECT slowdown causes wait scenario
	time.Sleep(100 * time.Millisecond)
	wantConnectExchange(t, conn)
	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x32, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0x80, 0x00, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	sendPacketHex(t, conn, "40028000") // PUBACK
	<-publishDone
}

// Publish should get a timeout error on stale request submission and resubmit
// after reconnect.
func TestPublishAtLeastOnce_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)
	publishDone := testRoutine(t, func() {
		exchange, err := client.PublishAtLeastOnce([]byte{'x'}, "y")
		if err != nil {
			t.Error("publish got error:", err)
		}
		verifyExchangeTimeout(t, testTimeout, exchange)
	})

	// read first byte
	var buf [1]byte
	_, err := io.ReadFull(conns[0], buf[:])
	if err != nil {
		t.Fatal("broker read error:", err)
	}
	if buf[0] != 0x32 {
		t.Errorf("want PUBLISH head 0x32, got %#x", buf[0])
	}
	t.Log("first connection abandoned after partial read")
	// check reconnect
	wantConnectExchange(t, conns[1])
	wantPacketHex(t, conns[1], hex.EncodeToString([]byte{
		0x32, 6,
		00, 01, 'y',
		0x80, 0x00, // packet identifier
		'x'}))
	sendPacketHex(t, conns[1], "40028000") // PUBACK
	<-publishDone
}

// Publish should get ErrDown notification and resubmit after reconnect.
func TestPublishAtLeastOnce_whileDown(t *testing.T) {
	client, conns, testTimeout := newTestClientRedial(t,
		mqtttest.Transfer{Err: mqtt.ErrUnavailable})

	publishDone := testRoutine(t, func() {
		// await connection refusal
		time.Sleep(100 * time.Millisecond)
		// publish should enqueue with ErrDown notification
		exchange1, err := client.PublishAtLeastOnce([]byte("x"), "y")
		if err != nil {
			t.Error("first publish got error:", err)
			return
		}
		verifyExchange(t, testTimeout, exchange1, mqtt.ErrDown, nil)

		// check recovery
		exchange2, err := client.PublishAtLeastOnce([]byte("a"), "b")
		if err != nil {
			t.Fatal("second publish got error:", err)
		}
		verifyExchange(t, testTimeout, exchange2, nil)
	})

	// fail first connect
	wantPacketHex(t, conns[0], "100c00044d515454040000000000") // CONNECT
	sendPacketHex(t, conns[0], "20020003")                     // CONNACK
	time.Sleep(200 * time.Millisecond)                         // give ErrDown some time
	// accept second connect + two publish exchanges
	wantConnectExchange(t, conns[1])
	wantPacketHex(t, conns[1], "3206000179800078") // PUBLISH #1
	sendPacketHex(t, conns[1], "40028000")         // PUBACK #1
	wantPacketHex(t, conns[1], "3206000162800161") // PUBLISH #2
	sendPacketHex(t, conns[1], "40028001")         // PUBACK #2
	<-publishDone
}

// TestPublishAtLeastOnce_restart sends three messages with QOS 1. The broker
// simulation will do all of the following:
//
//	A. Receive message #1
//	B. Receive message #2
//	C. Acknowledge mesage #1
//	D. Partially receive message #3
//
// Then the session is continued with a new client. It must automatically send
// message #2 and #3 again.
func TestPublishAtLeastOnce_restart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir() // persistence location

	// start timers after Parallel branche
	ctx, cancel := context.WithTimeout(context.Background(), 2*testTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()

	// request packets
	const (
		publish1Hex     = "3206000178800031" // '1' (0x31) @ 'x' (0x78)
		publish2Hex     = "3206000178800132" // '2' (0x32) @ 'x' (0x78)
		publish3Hex     = "3206000178800233" // '3' (0x33) @ 'x' (0x78)
		publish2DupeHex = "3a06000178800132" // with duplicate [DUP] flag
		publish3DupeHex = "3a06000178800233" // with duplicate [DUP] flag
	)

	clientConn, brokerConn := net.Pipe()
	// expire I/O mock before tests timeout
	brokerConn.SetDeadline(deadline.Add(-200 * time.Millisecond))
	client, err := mqtt.InitSession("test-client", mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 3,
		Dialer:         newDialerMock(t, clientConn),
	})
	if err != nil {
		t.Fatal("init session got error:", err)
	}
	verifyClient(t, client)

	publishDone := testRoutine(t, func() {
		select {
		case <-client.Online():
			break // OK
		case <-ctx.Done():
			t.Error("test timeout before Online")
			return
		}
		exchange1, err := client.PublishAtLeastOnce([]byte{'1'}, "x")
		if err != nil {
			t.Errorf("publish #1 got error %q [%T]", err, err)
		}
		exchange2, err := client.PublishAtLeastOnce([]byte{'2'}, "x")
		if err != nil {
			t.Errorf("publish #2 got error %q [%T]", err, err)
		}
		exchange3, err := client.PublishAtLeastOnce([]byte{'3'}, "x")
		if err != nil {
			t.Errorf("publish #3 got error %q [%T]", err, err)
		}
		verifyExchange(t, ctx.Done(), exchange1, nil)
		verifyExchange(t, ctx.Done(), exchange2, mqtt.ErrClosed)
		verifyExchange(t, ctx.Done(), exchange3, mqtt.ErrSubmit, mqtt.ErrClosed)
	})

	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK accept
	wantPacketHex(t, brokerConn, publish1Hex)
	wantPacketHex(t, brokerConn, publish2Hex)
	sendPacketHex(t, brokerConn, "40028000") // PUBACK #1
	// read first byte of publish3
	var buf [1]byte
	switch _, err := io.ReadFull(brokerConn, buf[:]); {
	case err != nil:
		t.Fatal("broker read error:", err)
	case buf[0] != 0x32:
		t.Errorf("want PUBLISH head 0x32, got %#x", buf[0])
	}
	err = client.Close()
	if err != nil {
		t.Error("close got error:", err)
	}
	<-publishDone

	// verify persistence; seals compatibility
	publish2File := filepath.Join(dir, "08001") // named after its packet ID
	publish3File := filepath.Join(dir, "08002")
	if bytes, err := os.ReadFile(publish2File); err != nil {
		t.Error("publish #2 file:", err)
	} else {
		gotHex := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const wantHex = publish2Hex + "0300000000000000" + "c0dcafa6"
		if gotHex != wantHex {
			t.Errorf("publish #2 file contains 0x%s, want 0x%s",
				gotHex, wantHex)
		}
	}
	if bytes, err := os.ReadFile(publish3File); err != nil {
		t.Error("publish #3 file:", err)
	} else {
		gotHex := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const wantHex = publish3Hex + "04000000000000000" + "5a75959"
		if gotHex != wantHex {
			t.Errorf("publish #3 file contains 0x%s, want 0x%s",
				gotHex, wantHex)
		}
	}

	if t.Failed() {
		return
	}
	t.Log("session continue with another Client")

	clientConn, brokerConn = net.Pipe()
	// expire I/O mock before tests timeout
	brokerConn.SetDeadline(deadline.Add(-200 * time.Millisecond))
	client, warn, err := mqtt.AdoptSession(mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 3,
		Dialer:         newDialerMock(t, clientConn),
	})
	if err != nil {
		t.Fatal("adopt session got error:", err)
	}
	for _, err := range warn {
		t.Error("adopt session got warning:", err)
	}
	verifyClient(t, client)

	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK
	wantPacketHex(t, brokerConn, publish2DupeHex)
	wantPacketHex(t, brokerConn, publish3DupeHex)
	sendPacketHex(t, brokerConn, "40028001") // PUBACK #2
	sendPacketHex(t, brokerConn, "40028002") // PUBACK #3

	// await PUBACK appliance
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(publish2File); err == nil {
		t.Error("publish #2 file still exits after PUBACK", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish #2 file error:", err)
	}
	if _, err := os.Stat(publish3File); err == nil {
		t.Error("publish #3 file still exits after PUBACK", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish #3 file error:", err)
	}
}

func TestPublishExactlyOnce(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t)
	publishDone := testRoutine(t, func() {
		exchange, err := client.PublishExactlyOnce([]byte("hello"), "greet")
		if err != nil {
			t.Error("publish got error:", err)
			return
		}
		verifyExchange(t, testTimeout, exchange, nil)
	})
	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x34, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0xc0, 0x00, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	sendPacketHex(t, conn, "5002c000") // PUBREC
	wantPacketHex(t, conn, "6202c000") // PUBREL
	sendPacketHex(t, conn, "7002c000") // PUBCOMP
	<-publishDone
}

// Publish should enqueue and continue once online.
func TestPublishExactlyOnce_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)

	publishDone := testRoutine(t, func() {
		exchange, err := client.PublishExactlyOnce([]byte("hello"), "greet")
		if err != nil {
			t.Error("publish got error:", err)
			return
		}
		verifyExchange(t, testTimeout, exchange,
			"mqtt: not connected; PUBLISH enqueued", nil)
	})

	// CONNECT slowdown causes wait scenario
	time.Sleep(100 * time.Millisecond)
	wantConnectExchange(t, conn)
	wantPacketHex(t, conn, hex.EncodeToString([]byte{
		0x34, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0xC0, 0x00, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	sendPacketHex(t, conn, "5002c000") // PUBREC
	wantPacketHex(t, conn, "6202c000") // PUBREL
	sendPacketHex(t, conn, "7002c000") // PUBCOMP
	<-publishDone
}

// Publish should get a timeout error on stale request submission and resubmit
// after reconnect.
func TestPublishExactlyOnce_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)

	publishDone := testRoutine(t, func() {
		exchange, err := client.PublishExactlyOnce([]byte{'x'}, "y")
		if err != nil {
			t.Error("publish error:", err)
			return
		}
		verifyExchangeTimeout(t, testTimeout, exchange)
	})

	// read first byte
	var buf [1]byte
	_, err := io.ReadFull(conns[0], buf[:])
	if err != nil {
		t.Fatal("broker read error:", err)
	}
	if buf[0] != 0x34 {
		t.Errorf("want PUBLISH head 0x34, got %#x", buf[0])
	}
	t.Log("first connection abandoned after partial read")
	// check reconnect
	wantConnectExchange(t, conns[1])
	wantPacketHex(t, conns[1], hex.EncodeToString([]byte{
		0x34, 6,
		00, 01, 'y',
		0xC0, 0x00, // packet identifier
		'x'}))
	sendPacketHex(t, conns[1], "5002c000") // PUBREC
	wantPacketHex(t, conns[1], "6202c000") // PUBREL
	sendPacketHex(t, conns[1], "7002c000") // PUBCOMP
	<-publishDone
}

// TestPublishExactlyOnce_restart sends five messages with QOS 1. The broker
// simulation will do all of the following:
//
//	A. Complete publish #1              (4/4)
//	B. Leave publish #2 without PUBCOMP (3/4)
//	C. Leave publish #3 without PUBREL  (2/4)
//	D. Leave publish #4 without PUBREC  (1/4)
//	E. Leave publish #5 without PUBLISH (0/4)
//
// Specifically, after the client sends publish #1 and #2, the broker simulation
// will do all of the following to accomplish A and B:
//
//  1. Receive message #1
//  2. Receive message #2
//  3. Recognise #1
//  4. Recognise #2
//  5. Receive release #1
//  6. Receive release #2;
//  7. Complete #1
//
// Then, after a little pause, the client sends publish #2 and #3, and the
// broker simulation will do all of the following to accomplish C, D and E:
//
//  1. Receive message #3
//  2. Receive message #4
//  3. Regognise #3
//
// Then the session is continued with a new client. It must automatically send
// message #4 and #5 again, and it must release message #2 and #3.
func TestPublishExactlyOnce_restart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir() // persistence location

	// start timers after Parallel branche
	ctx, cancel := context.WithTimeout(context.Background(), 2*testTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()

	// request packets
	const (
		publish1Hex     = "3406000178c00031" // '1' (0x31) @ 'x' (0x78)
		publish2Hex     = "3406000178c00132" // '2' (0x32) @ 'x' (0x78)
		publish3Hex     = "3406000178c00233" // '3' (0x33) @ 'x' (0x78)
		publish4Hex     = "3406000178c00334" // '4' (0x34) @ 'x' (0x78)
		publish5Hex     = "3406000178c00435" // '5' (0x35) @ 'x' (0x78)
		publish4DupeHex = "3c06000178c00334" // with duplicate [DUP] flag
		publish5DupeHex = "3c06000178c00435" // with duplicate [DUP] flag
	)

	clientConn, brokerConn := net.Pipe()
	// expire I/O mock before tests timeout
	brokerConn.SetDeadline(deadline.Add(-200 * time.Millisecond))
	client, err := mqtt.InitSession("test-client", mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		ExactlyOnceMax: 5,
		Dialer:         newDialerMock(t, clientConn),
	})
	if err != nil {
		t.Fatal("init session got error:", err)
	}
	verifyClient(t, client, mqtttest.Transfer{Err: io.ErrClosedPipe})

	publishDone := testRoutine(t, func() {
		select {
		case <-client.Online():
			break
		case <-ctx.Done():
			t.Fatal("test timeout before Online")
		}

		exchange1, err := client.PublishExactlyOnce([]byte{'1'}, "x")
		if err != nil {
			t.Errorf("publish #1 got error %q [%T]", err, err)
		}
		exchange2, err := client.PublishExactlyOnce([]byte{'2'}, "x")
		if err != nil {
			t.Errorf("publish #2 got error %q [%T]", err, err)
		}
		// await processing of message #1 and #2
		time.Sleep(200 * time.Millisecond)

		exchange3, err := client.PublishExactlyOnce([]byte{'3'}, "x")
		if err != nil {
			t.Errorf("publish #3 got error %q [%T]", err, err)
		}
		time.Sleep(50 * time.Millisecond)
		exchange4, err := client.PublishExactlyOnce([]byte{'4'}, "x")
		if err != nil {
			t.Errorf("publish #4 got error %q [%T]", err, err)
		}
		time.Sleep(50 * time.Millisecond)
		exchange5, err := client.PublishExactlyOnce([]byte{'5'}, "x")
		if err != nil {
			t.Errorf("publish #5 got error %q [%T]", err, err)
		}

		verifyExchange(t, ctx.Done(), exchange1, nil)
		verifyExchange(t, ctx.Done(), exchange2, mqtt.ErrClosed)
		verifyExchange(t, ctx.Done(), exchange3, mqtt.ErrClosed)
		verifyExchange(t, ctx.Done(), exchange4, mqtt.ErrClosed)
		verifyExchange(t, ctx.Done(), exchange5,
			"mqtt: not connected; PUBLISH enqueued", mqtt.ErrClosed)
	})

	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK
	wantPacketHex(t, brokerConn, publish1Hex)
	wantPacketHex(t, brokerConn, publish2Hex)
	sendPacketHex(t, brokerConn, "5002c000") // PUBREC #1
	wantPacketHex(t, brokerConn, "6202c000") // PUBREL #1
	sendPacketHex(t, brokerConn, "5002c001") // PUBREC #2
	wantPacketHex(t, brokerConn, "6202c001") // PUBREL #2
	sendPacketHex(t, brokerConn, "7002c000") // PUBCOMP #1
	wantPacketHex(t, brokerConn, publish3Hex)
	wantPacketHex(t, brokerConn, publish4Hex)
	sendPacketHex(t, brokerConn, "5002c002") // PUBREC #3
	time.Sleep(100 * time.Millisecond)
	err = client.Close()
	if err != nil {
		t.Error("Close error:", err)
	}
	<-publishDone

	// verify persistence; seals compatibility
	publish1File := filepath.Join(dir, "0c000") // named after it's packet ID
	publish2File := filepath.Join(dir, "0c001")
	publish3File := filepath.Join(dir, "0c002")
	publish4File := filepath.Join(dir, "0c003")
	publish5File := filepath.Join(dir, "0c004")
	if _, err := os.Stat(publish1File); err == nil {
		t.Error("publish #1 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish #1 file error:", err)
	}
	if bytes, err := os.ReadFile(publish2File); err != nil {
		t.Error("publish #2 file:", err)
	} else {
		gotHex := hex.EncodeToString(bytes)
		// PUBREL #2 packet + sequence number + checksum:
		const wantHex = "6202c001" + "0500000000000000" + "74d798bf"
		if gotHex != wantHex {
			t.Errorf("publish #2 file contains 0x%s, want 0x%s",
				gotHex, wantHex)
		}
	}
	if bytes, err := os.ReadFile(publish3File); err != nil {
		t.Error("publish #3 file:", err)
	} else {
		gotHex := hex.EncodeToString(bytes)
		// PUBREL #3 packet + sequence number + checksum:
		const wantHex = "6202c002" + "0800000000000000" + "6bb2f52f"
		if gotHex != wantHex {
			t.Errorf("publish #3 file contains 0x%s, want 0x%s",
				gotHex, wantHex)
		}
	}
	if bytes, err := os.ReadFile(publish4File); err != nil {
		t.Error("publish #4 file:", err)
	} else {
		gotHex := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const wantHex = publish4Hex + "0700000000000000" + "2d4cbd7c"
		if gotHex != wantHex {
			t.Errorf("publish #4 file contains 0x%s, want 0x%s",
				gotHex, wantHex)
		}
	}
	if bytes, err := os.ReadFile(publish5File); err != nil {
		t.Error("publish #5 file:", err)
	} else {
		gotHex := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const wantHex = publish5Hex + "0900000000000000" + "1d6d8b0e"
		if gotHex != wantHex {
			t.Errorf("publish #5 file contains 0x%s, want 0x%s",
				gotHex, wantHex)
		}
	}

	if t.Failed() {
		return
	}
	t.Log("session continue with another Client")

	clientConn, brokerConn = net.Pipe()
	// expire I/O mock before tests timeout
	brokerConn.SetDeadline(deadline.Add(-200 * time.Millisecond))
	client, warn, err := mqtt.AdoptSession(mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		ExactlyOnceMax: 5,
		Dialer:         newDialerMock(t, clientConn),
	})
	if err != nil {
		t.Fatal("adopt session got error:", err)
	}
	for _, err := range warn {
		t.Error("adopt session got warning:", err)
	}
	verifyClient(t, client)

	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK accept
	wantPacketHex(t, brokerConn, "6202c001") // PUBREL #2
	wantPacketHex(t, brokerConn, "6202c002") // PUBREL #3
	wantPacketHex(t, brokerConn, publish4DupeHex)
	wantPacketHex(t, brokerConn, publish5DupeHex)
	sendPacketHex(t, brokerConn, "5002c003") // PUBREC #4
	wantPacketHex(t, brokerConn, "6202c003") // PUBREL #4
	sendPacketHex(t, brokerConn, "5002c004") // PUBREC #5
	wantPacketHex(t, brokerConn, "6202c004") // PUBREL #5
	sendPacketHex(t, brokerConn, "7002c001") // PUBCOMP #2
	sendPacketHex(t, brokerConn, "7002c002") // PUBCOMP #3
	sendPacketHex(t, brokerConn, "7002c003") // PUBCOMP #4
	sendPacketHex(t, brokerConn, "7002c004") // PUBCOMP #5

	// await PUBCOMP appliance
	time.Sleep(200 * time.Millisecond)
	if _, err := os.Stat(publish2File); err == nil {
		t.Error("publish #2 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish #2 file error:", err)
	}
	if _, err := os.Stat(publish3File); err == nil {
		t.Error("publish #3 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish #3 file error:", err)
	}
	if _, err := os.Stat(publish4File); err == nil {
		t.Error("publish #4 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish #4 file error:", err)
	}
	if _, err := os.Stat(publish5File); err == nil {
		t.Error("publish #5 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish #5 file error:", err)
	}
}

// Brokers may resend a PUBREL even after receiving PUBCOMP (in case the serice
// crashed for example).
func TestPUBRELRetry(t *testing.T) {
	_, conn, _ := newTestClientOnline(t)
	sendPacketHex(t, conn, "62021234") // PUBREL
	wantPacketHex(t, conn, "70021234") // PUBCOMP
}

func TestAbandon(t *testing.T) {
	client, conn, _ := newTestClientOnline(t)
	quit := make(chan struct{})

	pingDone := testRoutine(t, func() {
		err := client.Ping(quit)
		if !errors.Is(err, mqtt.ErrAbandoned) {
			t.Errorf("ping got error %q [%T], want an mqtt.ErrAbandoned", err, err)
		}
	})
	wantPacketHex(t, conn, "c000") // PINGREQ

	subscribeDone := testRoutine(t, func() {
		err := client.Subscribe(quit, "x")
		if !errors.Is(err, mqtt.ErrAbandoned) {
			t.Errorf("subscribe got error %q [%T], want an mqtt.ErrAbandoned", err, err)
		}
	})
	wantPacketHex(t, conn, "8206600000017802") // SUBSCRIBE

	unsubscribeDone := testRoutine(t, func() {
		err := client.Unsubscribe(quit, "x")
		if !errors.Is(err, mqtt.ErrAbandoned) {
			t.Errorf("unsubscribe got error %q [%T], want an mqtt.ErrAbandoned", err, err)
		}
	})
	wantPacketHex(t, conn, "a2054001000178") // UNSUBSCRIBE

	time.Sleep(10 * time.Millisecond)
	close(quit)
	<-pingDone
	<-subscribeDone
	<-unsubscribeDone
}

func TestBreak(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t, mqtttest.Transfer{Err: io.EOF})

	pingDone := testRoutine(t, func() {
		err := client.Ping(testTimeout)
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("ping got error %q [%T], want an mqtt.ErrBreak", err, err)
			return
		}
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for ping error")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for ping error is not equal to Online channel")
		}
	})
	wantPacketHex(t, conn, "c000") // PINGREQ

	subscribeDone := testRoutine(t, func() {
		err := client.Subscribe(testTimeout, "x")
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("subscribe got error %q [%T], want an mqtt.ErrBreak", err, err)
			return
		}
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for subscribe error")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for subscribe error is not equal to Online channel")
		}
	})
	wantPacketHex(t, conn, "8206600000017802") // SUBSCRIBE

	unsubscribeDone := testRoutine(t, func() {
		err := client.Unsubscribe(testTimeout, "x")
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("unsubscribe got error %q [%T], want an mqtt.ErrBreak", err, err)
			return
		}
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for unsubscribe error")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for unsubscribe error is not equal to Online channel")
		}
	})
	wantPacketHex(t, conn, "a2054001000178") // UNSUBSCRIBE

	if err := conn.Close(); err != nil {
		t.Error("broker mock got error on pipe close:", err)
	}
	<-pingDone
	<-subscribeDone
	<-unsubscribeDone
}

func TestDeny(t *testing.T) {
	// no invocation to the client allowed
	client, _, testTimeout := newTestClient(t)

	errCheck := func(err error, desc string) {
		if !mqtt.IsDeny(err) {
			t.Errorf("%s got error %q [%T], want an mqtt.IsDeny",
				desc, err, err)
		} else if client.Backoff(err) != nil {
			t.Errorf("%s got backoff for deny error", desc)
		}
	}

	// UTF-8 validation
	errCheck(client.PublishRetained(testTimeout, nil, "topic with \xED\xA0\x80 not allowed"),
		"publish QoS 0 with U+D800 in topic")
	_, err := client.PublishAtLeastOnceRetained(nil, "topic with \xED\xA0\x81 not allowed")
	errCheck(err, "publish QoS 1 with U+D801 in topic")
	_, err = client.PublishExactlyOnceRetained(nil, "topic with \xED\xBF\xBF not allowed")
	errCheck(err, "publish QoS 2 with U+DFFF in topic")

	errCheck(client.SubscribeLimitAtMostOnce(nil, "null char \x00 not allowed"),
		"subscribe max QoS 0 with null character")
	errCheck(client.SubscribeLimitAtLeastOnce(nil, "char \x80 breaks UTF-8"),
		"subscribe max QoS 1 with broken UTF-8")

	// empty vararg
	errCheck(client.Subscribe(testTimeout),
		"subscribe with nothing")
	errCheck(client.Unsubscribe(testTimeout),
		"unsubscribe with nothing")

	// empty topic
	errCheck(client.Subscribe(testTimeout, ""),
		"subscribe with zero topic")
	errCheck(client.Unsubscribe(testTimeout, ""),
		"unsubscribe with zero topic")
	errCheck(client.Publish(testTimeout, nil, ""),
		"publish with zero topic")

	// size limits
	tooBig := strings.Repeat("A", 1<<16)
	errCheck(client.Unsubscribe(testTimeout, tooBig),
		"unsubscribe with 64 KiB filter")
	errCheck(client.Publish(testTimeout, make([]byte, 256*1024*1024), ""),
		"publish with 256 MiB")

	filtersTooBig := make([]string, 256*1024)
	KiB := strings.Repeat("A", 1024)
	for i := range filtersTooBig {
		filtersTooBig[i] = KiB
	}
	errCheck(client.Subscribe(testTimeout, filtersTooBig...),
		"subscribe with 256 MiB topic filters")
	errCheck(client.Unsubscribe(testTimeout, filtersTooBig...),
		"unsubscribe with 256 MiB topic filters")
}

// VerifyExchange compares exchange reception against the wanted list in order
// of appearence. Errors want a errors.Is, strings want a strings.Contain, and
// nil wants a closed channel.
func verifyExchange(t *testing.T, testTimeout <-chan struct{}, exchange <-chan error, wanted ...any) {
	t.Helper()
	defer t.Log("exchange verification done")

	if len(wanted) == 0 {
		panic("can't verify without wanted listing")
	}

	for i := range wanted {
		var got error
		select {
		case <-testTimeout:
			t.Error("test timeout while awaiting exchange error")
			return
		case err, ok := <-exchange:
			if !ok {
				if wanted[i] != nil {
					t.Errorf("exchange closed after %d errors, want error %q",
						i, wanted[i])
				}
				return
			}
			got = err
		}

		switch want := wanted[i].(type) {
		case nil:
			t.Errorf("got exchange error %q [%T], want channel close",
				got, got)
		case string:
			if !strings.Contains(got.Error(), want) {
				t.Errorf("got exchange error %q [%T], want %q mentioned",
					got, got, want)
			}
		case error:
			if !errors.Is(got, want) {
				t.Errorf("got exchange error %q [%T], want a %q [%T]",
					got, got, want, want)
			}
		default:
			panic("want is non-nil, non-string and non-error")
		}
	}
}

func verifyExchangeTimeout(t *testing.T, testTimeout <-chan struct{}, exchange <-chan error) {
	t.Helper()
	defer t.Log("exchange verification done")

	select {
	case <-testTimeout:
		t.Error("test timeout while awaiting timeout error")
		return
	case err, ok := <-exchange:
		if !ok {
			t.Errorf("exchange complete, want timeout error")
			return
		}
		var e net.Error
		if !errors.As(err, &e) || !e.Timeout() {
			t.Errorf("got exchange error %v, want a Timeout net.Error", err)
		}
	}

	select {
	case <-testTimeout:
		t.Error("test timeout while awaiting exchange complete")
		return
	case err, ok := <-exchange:
		if ok {
			t.Errorf("got exchange error %v, want exchange complete", err)
		}
	}
}
