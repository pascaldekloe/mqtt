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
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conn, "c000") // PINGREQ
		sendPacketHex(t, conn, "d000") // PINGRESP
	})

	err := client.Ping(testTimeout)
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
}

// Ping should await the first connect attempt.
func TestPing_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	brokerMockDone := testRoutine(t, func() {
		// CONNECT slowdown causes wait scenario
		time.Sleep(100 * time.Millisecond)
		wantConnectExchange(t, conn)
		wantPacketHex(t, conn, "c000") // PINGREQ
		sendPacketHex(t, conn, "d000") // PINGRESP
	})

	err := client.Ping(testTimeout)
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
}

// Ping should signal ErrDown when the first connect attempt fails.
func TestPing_failedConnect(t *testing.T) {
	// fail first CONNECT with refuse
	client, conns, testTimeout := newTestClientRedial(t,
		mqtttest.Transfer{Err: mqtt.ErrUnavailable})
	wantPacketHex(t, conns[0], "100c00044d515454040000000000") // CONNECT
	sendPacketHex(t, conns[0], "20020003")                     // CONNACK

	err := client.Ping(testTimeout)
	if !errors.Is(err, mqtt.ErrDown) {
		t.Errorf("ping got error %v, want ErrDown", err)
	}

	// accept second CONNECT + PING exchange
	brokerMockDone := testRoutine(t, func() {
		wantConnectExchange(t, conns[1])
		wantPacketHex(t, conns[1], "c000") // PINGREQ
		sendPacketHex(t, conns[1], "d000") // PINGRESP
	})

	// second ping when Online again
	wantOnline(t, client, testTimeout)
	err = client.Ping(testTimeout)
	if err != nil {
		t.Error("ping got error once Online again:", err)
	}
	<-brokerMockDone
}

func TestPing_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conns[0], buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0xC0:
			t.Errorf("want PINGREQ head 0xC0, got %#x", buf[0])
		}
		t.Log("first connection abandoned after partial read")

		wantPacketHex(t, conns[1], "100c00044d515454040000000000")
		t.Log("second connection abandoned after connect request")
	})

	err := client.Ping(testTimeout)
	var e net.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
	} else {
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for timeout")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for timeout not online await")
		}
	}
	<-brokerMockDone
}

func TestSubscribe_multiple(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t)
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

	err := client.Subscribe(testTimeout, "u/noi", "u/shin")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
}

func TestSubscribe_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conns[0], buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0x82:
			t.Errorf("want SUBSCRIBE head 0x82, got %#x", buf[0])
		}
		t.Log("first connection abandoned after partial read")

		wantPacketHex(t, conns[1], "100c00044d515454040000000000")
		t.Log("second connection abandoned after connect request")
	})

	err := client.Subscribe(testTimeout, "x")
	var e net.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
	} else {
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for timeout")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for timeout not online await")
		}
	}
	<-brokerMockDone
}

func TestUnsubscribe_multiple(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t)
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0xa2, 17,
			0x40, 0x00, // packet identifier
			0, 5, 'u', '/', 'n', 'o', 'i',
			0, 6, 'u', '/', 's', 'h', 'i', 'n',
		}))
		sendPacketHex(t, conn, "b0024000") // UNSUBACK
	})

	err := client.Unsubscribe(testTimeout, "u/noi", "u/shin")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
}

func TestUnsubscribe_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conns[0], buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0xa2:
			t.Errorf("want UNSUBSCRIBE head 0xa2, got %#x", buf[0])
		}
		t.Log("first connection abandoned after partial read")

		wantPacketHex(t, conns[1], "100c00044d515454040000000000")
		t.Log("second connection abandoned after connect request")
	})

	err := client.Unsubscribe(testTimeout, "x")
	var e net.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
	}
	<-brokerMockDone
}

func TestPublish(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t)
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x30, 12,
			0, 5, 'g', 'r', 'e', 'e', 't',
			'h', 'e', 'l', 'l', 'o'}))
	})

	err := client.Publish(testTimeout, []byte("hello"), "greet")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
}

func TestPublish_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	brokerMockDone := testRoutine(t, func() {
		time.Sleep(100 * time.Millisecond)
		wantConnectExchange(t, conn)
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x30, 12,
			0, 5, 'g', 'r', 'e', 'e', 't',
			'h', 'e', 'l', 'l', 'o'}))
	})

	err := client.Publish(testTimeout, []byte("hello"), "greet")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	<-brokerMockDone
}

func TestPublish_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conns[0], buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0x30:
			t.Errorf("want PUBLISH head 0x30, got %#x", buf[0])
		}
		t.Log("first connection abandoned after partial read")

		wantPacketHex(t, conns[1], "100c00044d515454040000000000")
		t.Log("second connection abandoned after connect request")
	})

	err := client.Publish(testTimeout, []byte{'x'}, "y")
	var e net.Error
	if !errors.As(err, &e) || !e.Timeout() {
		t.Errorf("got error %q [%T], want a Timeout net.Error", err, err)
	} else {
		switch client.Backoff(err) {
		case nil:
			t.Error("no backoff for timeout")
		case client.Online():
			break // OK
		default:
			t.Error("backoff for timeout not online await")
		}
	}
	<-brokerMockDone
}

func TestPublishAtLeastOnce(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t)
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x32, 14,
			0, 5, 'g', 'r', 'e', 'e', 't',
			0x80, 0x00, // packet identifier
			'h', 'e', 'l', 'l', 'o'}))
		sendPacketHex(t, conn, "40028000") // PUBACK
	})

	exchange, err := client.PublishAtLeastOnce([]byte("hello"), "greet")
	if err != nil {
		t.Errorf("publish got error %q [%T]", err, err)
	}
	verifyExchange(t, testTimeout, exchange, nil)
	<-brokerMockDone
}

func TestPublishAtLeastOnce_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	brokerMockDone := testRoutine(t, func() {
		time.Sleep(100 * time.Millisecond)
		wantConnectExchange(t, conn)
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x32, 14,
			0, 5, 'g', 'r', 'e', 'e', 't',
			0x80, 0x00, // packet identifier
			'h', 'e', 'l', 'l', 'o'}))
		sendPacketHex(t, conn, "40028000") // PUBACK
	})

	exchange, err := client.PublishAtLeastOnce([]byte("hello"), "greet")
	if err != nil {
		t.Errorf("publish got error %q [%T]", err, err)
	}
	verifyExchange(t, testTimeout, exchange,
		"mqtt: not connected; PUBLISH enqueued")
	<-brokerMockDone
}

func TestPublishAtLeastOnce_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conns[0], buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0x32:
			t.Errorf("want PUBLISH head 0x32, got %#x", buf[0])
		}
		t.Log("first connection abandoned after partial read")

		wantPacketHex(t, conns[1], "100c00044d515454040000000000")
		t.Log("second connection abandoned after connect request")
	})

	exchange, err := client.PublishAtLeastOnce([]byte{'x'}, "y")
	if err != nil {
		t.Fatalf("got error %q [%T]", err, err)
	}
	verifyExchangeTimeout(t, testTimeout, exchange)
	<-brokerMockDone
}

// A Client must resend each PUBLISH which is pending PUBACK when the connection
// is reset (for whater reasons).
func TestPublishAtLeastOnce_resend(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t, mqtttest.Transfer{Err: io.EOF})
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, conns[0], hex.EncodeToString([]byte{
			0x32, 6,
			0, 1, 'y',
			0x80, 0x00, // packet identifier
			'x'}))
		if err := conns[0].Close(); err != nil {
			t.Error("broker got error on first connection close:", err)
		}

		wantConnectExchange(t, conns[1])
		wantPacketHex(t, conns[1], hex.EncodeToString([]byte{
			0x3a, 6, // with duplicate [DUP] flag
			0, 1, 'y',
			0x80, 0x00, // packet identifier
			'x'}))
		sendPacketHex(t, conns[1], "40028000") // PUBACK after all
	})

	exchange, err := client.PublishAtLeastOnce([]byte{'x'}, "y")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	verifyExchange(t, testTimeout, exchange, nil)
	<-brokerMockDone
}

// A Client must send pending PUBLISH once reconnected and continue.
func TestPublishAtLeastOnce_whileDown(t *testing.T) {
	// fail first CONNECT with refuse
	client, conns, testTimeout := newTestClientRedial(t,
		mqtttest.Transfer{Err: mqtt.ErrUnavailable})
	wantPacketHex(t, conns[0], "100c00044d515454040000000000") // CONNECT
	sendPacketHex(t, conns[0], "20020003")                     // CONNACK
	// let client process refuse
	time.Sleep(200 * time.Millisecond)

	// publish should enqueue with ErrDown notification
	exchange1, err := client.PublishAtLeastOnce([]byte("x"), "y")
	if err != nil {
		t.Fatal("PublishAtLeastOnce got error:", err)
	}
	verifyExchange(t, testTimeout, exchange1, mqtt.ErrDown)

	// accept second connect + two publish exchanges
	brokerMockDone := testRoutine(t, func() {
		wantConnectExchange(t, conns[1])
		wantPacketHex(t, conns[1], "3206000179800078") // PUBLISH #1
		wantPacketHex(t, conns[1], "3206000162800161") // PUBLISH #2
		sendPacketHex(t, conns[1], "40028000")         // PUBACK #1
		sendPacketHex(t, conns[1], "40028001")         // PUBACK #2
	})

	// second publish when Online again
	wantOnline(t, client, testTimeout)
	exchange2, err := client.PublishAtLeastOnce([]byte("a"), "b")
	if err != nil {
		t.Fatal("PublishAtLeastOnce got error:", err)
	}

	verifyExchange(t, testTimeout, exchange1, nil)
	verifyExchange(t, testTimeout, exchange2, nil)
	<-brokerMockDone
}

// TestPublishAtLeastOnceRestart sends three messages as QOS 1 PUBLISH. The
// broker simulation will do all of the following:
//
//  1. Receive the first message.
//  2. Receive the second message.
//  3. Acknowledge the first mesage.
//  4. Partially receive the third message.
//
// Then the session is continued with a new client. It must automatically send
// the second and the third message again.
func TestPublishAtLeastOnce_restart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir() // persistence location

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	testTimeout := ctx.Done()

	const publish1 = "3206000178800031"     // '1' (0x31) @ 'x' (0x78)
	const publish2 = "3206000178800132"     // '2' (0x32) @ 'x' (0x78)
	const publish3 = "3206000178800233"     // '3' (0x33) @ 'x' (0x78)
	const publish2Dupe = "3a06000178800132" // with duplicate [DUP] flag
	const publish3Dupe = "3a06000178800233" // with duplicate [DUP] flag

	clientConn, brokerConn := net.Pipe()
	client, err := mqtt.InitSession("test-client", mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 3,
		Dialer:         newDialerMock(t, 0, clientConn),
	})
	if err != nil {
		t.Fatal("InitSession error:", err)
	}
	verifyClient(t, client)
	brokerConn.SetDeadline(time.Now().Add(800 * time.Millisecond))
	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK

	select {
	case <-client.Online():
		break // OK
	case <-testTimeout:
		t.Fatal("test timeout before Online")
	}
	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, brokerConn, publish1)
		wantPacketHex(t, brokerConn, publish2)
		sendPacketHex(t, brokerConn, "40028000") // PUBACK № 1

		var buf [1]byte
		switch _, err := io.ReadFull(brokerConn, buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0x32:
			t.Errorf("want PUBLISH head 0x32, got %#x", buf[0])
		}
		// leave № 3 partial read
		err := client.Close()
		if err != nil {
			t.Error("Close error:", err)
		}
	})

	exchange1, err := client.PublishAtLeastOnce([]byte{'1'}, "x")
	if err != nil {
		t.Errorf("publish № 1 got error %q [%T]", err, err)
	}
	exchange2, err := client.PublishAtLeastOnce([]byte{'2'}, "x")
	if err != nil {
		t.Errorf("publish № 2 got error %q [%T]", err, err)
	}
	exchange3, err := client.PublishAtLeastOnce([]byte{'3'}, "x")
	if err != nil {
		t.Errorf("publish № 3 got error %q [%T]", err, err)
	}
	verifyExchange(t, testTimeout, exchange1, nil)
	verifyExchange(t, testTimeout, exchange2, mqtt.ErrClosed)
	verifyExchange(t, testTimeout, exchange3, io.ErrClosedPipe)

	<-brokerMockDone

	// verify persistence; seals compatibility
	publish2File := filepath.Join(dir, "08001") // named after it's packet ID
	publish3File := filepath.Join(dir, "08002")
	if bytes, err := os.ReadFile(publish2File); err != nil {
		t.Error("publish № 2 file:", err)
	} else {
		got := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const want = publish2 + "0300000000000000" + "c0dcafa6"
		if got != want {
			t.Errorf("publish № 2 file contains 0x%s, want 0x%s", got, want)
		}
	}
	if bytes, err := os.ReadFile(publish3File); err != nil {
		t.Error("publish № 3 file:", err)
	} else {
		got := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const want = publish3 + "04000000000000000" + "5a75959"
		if got != want {
			t.Errorf("publish № 3 file contains 0x%s, want 0x%s", got, want)
		}
	}

	if t.Failed() {
		return
	}

	t.Log("continue with another Client")
	clientConn, brokerConn = net.Pipe()
	client, warn, err := mqtt.AdoptSession(mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 3,
		Dialer:         newDialerMock(t, 0, clientConn),
	})
	if err != nil {
		t.Fatal("AdoptSession error:", err)
	}
	for _, err := range warn {
		t.Error("AdoptSession warning:", err)
	}
	verifyClient(t, client)
	brokerConn.SetDeadline(time.Now().Add(800 * time.Millisecond))
	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK
	wantPacketHex(t, brokerConn, publish2Dupe)
	wantPacketHex(t, brokerConn, publish3Dupe)
	sendPacketHex(t, brokerConn, "40028001") // PUBACK № 2
	sendPacketHex(t, brokerConn, "40028002") // PUBACK № 3

	// await PUBACK appliance
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(publish2File); err == nil {
		t.Error("publish № 2 file exits after PUBACK", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 2 file error:", err)
	}
	if _, err := os.Stat(publish3File); err == nil {
		t.Error("publish № 3 file exits after PUBACK", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 3 file error:", err)
	}
}

func TestPublishExactlyOnce(t *testing.T) {
	client, conn, testTimeout := newTestClientOnline(t)
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

	exchange, err := client.PublishExactlyOnce([]byte("hello"), "greet")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	verifyExchange(t, testTimeout, exchange, nil)
	<-brokerMockDone
}

func TestPublishExactlyOnce_beforeConnect(t *testing.T) {
	client, conn, testTimeout := newTestClient(t)
	brokerMockDone := testRoutine(t, func() {
		time.Sleep(100 * time.Millisecond)
		wantConnectExchange(t, conn)
		wantPacketHex(t, conn, hex.EncodeToString([]byte{
			0x34, 14,
			0, 5, 'g', 'r', 'e', 'e', 't',
			0xc0, 0x00, // packet identifier
			'h', 'e', 'l', 'l', 'o'}))
		sendPacketHex(t, conn, "5002c000") // PUBREC
		wantPacketHex(t, conn, "6202c000") // PUBREL
		sendPacketHex(t, conn, "7002c000") // PUBCOMP
	})

	exchange, err := client.PublishExactlyOnce([]byte("hello"), "greet")
	if err != nil {
		t.Errorf("got error %q [%T]", err, err)
	}
	verifyExchange(t, testTimeout, exchange,
		"mqtt: not connected; PUBLISH enqueued")
	<-brokerMockDone
}

func TestPublishExactlyOnce_reqTimeout(t *testing.T) {
	client, conns, testTimeout := newTestClientOnlineRedial(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conns[0], buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0x34:
			t.Errorf("want PUBLISH head 0x34, got %#x", buf[0])
		}
		t.Log("first connection abandoned after partial read")

		wantPacketHex(t, conns[1], "100c00044d515454040000000000")
		t.Log("second connection abandoned after connect request")
	})

	exchange, err := client.PublishExactlyOnce([]byte{'x'}, "y")
	if err != nil {
		t.Fatalf("Publish error %q [%T]", err, err)
	}
	verifyExchangeTimeout(t, testTimeout, exchange)
	<-brokerMockDone
}

// TestPublishExactlyOnceRestart sends five messages as QOS 2 PUBLISH. The
// broker simulation will do all of the following:
//
//   - Complete PUBLISH № 1.
//   - Halt at PUBCOMP № 2 (not send).
//   - Halt at PUBREL № 3 (not receive).
//   - Halt at PUBREC № 4 (not send)
//   - Halt at PUBLISH № 5 (not receive).
//
// … it does so with the following steps.
//
//   - Receive PUBLISH № 1.
//   - Receive PUBLISH № 2.
//   - Send PUBREC № 1.
//   - Send PUBREC № 2.
//   - Receive PUBREL № 1.
//   - Receive PUBREL № 2.
//   - Send PUBCOMP № 1.
//   - <little sleep>
//   - Receive PUBLISH № 3.
//   - Receive PUBLISH № 4.
//   - Send PUBREC № 3.
//
// Then the session is continued with a new client. It must automatically
// PUBLISH message № 4 and 5 again, and it must PUBREL message № 2 and 3.
func TestPublishExactlyOnce_restart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir() // persistence location

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	testTimeout := ctx.Done()

	const publish1 = "3406000178c00031"     // '1' (0x31) @ 'x' (0x78)
	const publish2 = "3406000178c00132"     // '2' (0x32) @ 'x' (0x78)
	const publish3 = "3406000178c00233"     // '3' (0x33) @ 'x' (0x78)
	const publish4 = "3406000178c00334"     // '4' (0x34) @ 'x' (0x78)
	const publish5 = "3406000178c00435"     // '5' (0x35) @ 'x' (0x78)
	const publish4Dupe = "3c06000178c00334" // with duplicate [DUP] flag
	const publish5Dupe = "3c06000178c00435" // with duplicate [DUP] flag

	clientConn, brokerConn := net.Pipe()
	client, err := mqtt.InitSession("test-client", mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		ExactlyOnceMax: 5,
		Dialer:         newDialerMock(t, 0, clientConn),
	})
	if err != nil {
		t.Fatal("InitSession error:", err)
	}
	verifyClient(t, client, mqtttest.Transfer{Err: io.ErrClosedPipe})
	brokerConn.SetDeadline(time.Now().Add(time.Second))
	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK

	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, brokerConn, publish1)
		wantPacketHex(t, brokerConn, publish2)
		sendPacketHex(t, brokerConn, "5002c000") // PUBREC № 1
		wantPacketHex(t, brokerConn, "6202c000") // PUBREL № 1
		sendPacketHex(t, brokerConn, "5002c001") // PUBREC № 2
		wantPacketHex(t, brokerConn, "6202c001") // PUBREL № 2
		sendPacketHex(t, brokerConn, "7002c000") // PUBCOMP № 1
		wantPacketHex(t, brokerConn, publish3)
		wantPacketHex(t, brokerConn, publish4)
		sendPacketHex(t, brokerConn, "5002c002") // PUBREC № 3

		// PUBLISH № 5 before client close
		time.Sleep(200 * time.Millisecond)
		err := client.Close()
		if err != nil {
			t.Error("Close error:", err)
		}
	})

	select {
	case <-client.Online():
		break
	case <-testTimeout:
		t.Fatal("test timeout before Online")
	}
	exchange1, err := client.PublishExactlyOnce([]byte{'1'}, "x")
	if err != nil {
		t.Errorf("publish № 1 got error %q [%T]", err, err)
	}
	exchange2, err := client.PublishExactlyOnce([]byte{'2'}, "x")
	if err != nil {
		t.Errorf("publish № 2 got error %q [%T]", err, err)
	}
	time.Sleep(50 * time.Millisecond)
	exchange3, err := client.PublishExactlyOnce([]byte{'3'}, "x")
	if err != nil {
		t.Errorf("publish № 3 got error %q [%T]", err, err)
	}
	time.Sleep(50 * time.Millisecond)
	exchange4, err := client.PublishExactlyOnce([]byte{'4'}, "x")
	if err != nil {
		t.Errorf("publish № 4 got error %q [%T]", err, err)
	}
	time.Sleep(50 * time.Millisecond)
	exchange5, err := client.PublishExactlyOnce([]byte{'5'}, "x")
	if err != nil {
		t.Errorf("publish № 5 got error %q [%T]", err, err)
	}
	<-brokerMockDone
	verifyExchange(t, testTimeout, exchange1, nil)
	verifyExchange(t, testTimeout, exchange2, mqtt.ErrClosed)
	verifyExchange(t, testTimeout, exchange3, mqtt.ErrClosed)
	verifyExchange(t, testTimeout, exchange4, mqtt.ErrClosed)
	verifyExchange(t, testTimeout, exchange5, "mqtt: not connected")

	// verify persistence; seals compatibility
	publish1File := filepath.Join(dir, "0c000") // named after it's packet ID
	publish2File := filepath.Join(dir, "0c001")
	publish3File := filepath.Join(dir, "0c002")
	publish4File := filepath.Join(dir, "0c003")
	publish5File := filepath.Join(dir, "0c004")
	if _, err := os.Stat(publish1File); err == nil {
		t.Error("publish № 1 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 1 file error:", err)
	}
	if bytes, err := os.ReadFile(publish2File); err != nil {
		t.Error("publish № 2 file:", err)
	} else {
		got := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const want = "6202c001" + "0500000000000000" + "74d798bf" // PUBREL № 2
		if got != want {
			t.Errorf("publish № 2 file contains 0x%s, want 0x%s", got, want)
		}
	}
	if bytes, err := os.ReadFile(publish3File); err != nil {
		t.Error("publish № 3 file:", err)
	} else {
		got := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const want = "6202c002" + "0800000000000000" + "6bb2f52f" // PUBREL № 3
		if got != want {
			t.Errorf("publish № 3 file contains 0x%s, want 0x%s", got, want)
		}
	}
	if bytes, err := os.ReadFile(publish4File); err != nil {
		t.Error("publish № 4 file:", err)
	} else {
		got := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const want = publish4 + "0700000000000000" + "2d4cbd7c"
		if got != want {
			t.Errorf("publish № 4 file contains 0x%s, want 0x%s", got, want)
		}
	}
	if bytes, err := os.ReadFile(publish5File); err != nil {
		t.Error("publish № 5 file:", err)
	} else {
		got := hex.EncodeToString(bytes)
		// packet + sequence number + checksum:
		const want = publish5 + "0900000000000000" + "1d6d8b0e"
		if got != want {
			t.Errorf("publish № 5 file contains 0x%s, want 0x%s", got, want)
		}
	}

	if t.Failed() {
		return
	}

	t.Log("continue with another Client")
	clientConn, brokerConn = net.Pipe()
	client, warn, err := mqtt.AdoptSession(mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		ExactlyOnceMax: 4,
		Dialer:         newDialerMock(t, 0, clientConn),
	})
	for _, err := range warn {
		t.Error("AdoptSession warning:", err)
	}
	if err != nil {
		t.Fatal("AdoptSession error:", err)
	}
	verifyClient(t, client)
	brokerConn.SetDeadline(time.Now().Add(time.Second))
	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK

	wantPacketHex(t, brokerConn, "6202c001") // PUBREL № 2
	wantPacketHex(t, brokerConn, "6202c002") // PUBREL № 3
	wantPacketHex(t, brokerConn, publish4Dupe)
	wantPacketHex(t, brokerConn, publish5Dupe)
	go func() {
		sendPacketHex(t, brokerConn, "5002c003") // PUBREC № 4
		sendPacketHex(t, brokerConn, "5002c004") // PUBREC № 5
	}()
	wantPacketHex(t, brokerConn, "6202c003") // PUBREL № 4
	wantPacketHex(t, brokerConn, "6202c004") // PUBREL № 5
	sendPacketHex(t, brokerConn, "7002c001") // PUBCOMP № 2
	sendPacketHex(t, brokerConn, "7002c002") // PUBCOMP № 3
	sendPacketHex(t, brokerConn, "7002c003") // PUBCOMP № 4
	sendPacketHex(t, brokerConn, "7002c004") // PUBCOMP № 5

	// await PUBCOMP appliance
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(publish2File); err == nil {
		t.Error("publish № 2 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 2 file error:", err)
	}
	if _, err := os.Stat(publish3File); err == nil {
		t.Error("publish № 3 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 3 file error:", err)
	}
	if _, err := os.Stat(publish4File); err == nil {
		t.Error("publish № 4 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 4 file error:", err)
	}
	if _, err := os.Stat(publish5File); err == nil {
		t.Error("publish № 5 file still exits after PUBCOMP", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 5 file error:", err)
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
		} else {
			switch client.Backoff(err) {
			case nil:
				t.Error("no backoff for ErrBreak")
			case client.Online():
				break // OK
			default:
				t.Error("backoff for ErrBreak not online await")
			}
		}
	})
	wantPacketHex(t, conn, "c000") // PINGREQ

	subscribeDone := testRoutine(t, func() {
		err := client.Subscribe(testTimeout, "x")
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("subscribe got error %q [%T], want an mqtt.ErrBreak", err, err)
		} else {
			switch client.Backoff(err) {
			case nil:
				t.Error("no backoff for ErrBreak")
			case client.Online():
				break // OK
			default:
				t.Error("backoff for ErrBreak not online await")
			}
		}
	})
	wantPacketHex(t, conn, "8206600000017802") // SUBSCRIBE

	unsubscribeDone := testRoutine(t, func() {
		err := client.Unsubscribe(testTimeout, "x")
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("unsubscribe got error %q [%T], want an mqtt.ErrBreak", err, err)
		} else {
			switch client.Backoff(err) {
			case nil:
				t.Error("no backoff for ErrBreak")
			case client.Online():
				break // OK
			default:
				t.Error("backoff for ErrBreak not online await")
			}
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
		t.Fatal("can't verify without wanted listing")
	}

	for i := range wanted {
		var got error
		select {
		case <-testTimeout:
			t.Fatal("test timeout while awaiting exchange error")
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

	for i := 0; ; i++ {
		select {
		case <-testTimeout:
			t.Fatal("test timeout while awaiting exchange")

		case err, ok := <-exchange:
			if !ok {
				t.Errorf("exchange complete, want timeout error")
				return
			}
			var e net.Error
			if errors.As(err, &e) && e.Timeout() {
				return // OK
			}

			if i != 0 || err.Error() != "mqtt: pending connect; PUBLISH enqueued" {
				t.Errorf("got exchange error %q [%T], want a Timeout net.Error",
					err, err)
				return
			}
		}
	}
}
