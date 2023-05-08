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
			t.Error("broker read error:", err)
		case buf[0] != 0xC0:
			t.Errorf("want PINGREQ head 0xC0, got %#x", buf[0])
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
			t.Error("broker read error:", err)
		case buf[0] != 0x82:
			t.Errorf("want SUBSCRIBE head 0x82, got %#x", buf[0])
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
			t.Error("broker read error:", err)
		case buf[0] != 0xa2:
			t.Errorf("want UNSUBSCRIBE head 0xa2, got %#x", buf[0])
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
			t.Error("broker read error:", err)
		case buf[0] != 0x30:
			t.Errorf("want PUBLISH head 0x30, got %#x", buf[0])
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
	verifyAck(t, ack)
	<-brokerMockDone
}

func TestPublishAtLeastOnceReqTimeout(t *testing.T) {
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conn, buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0x32:
			t.Errorf("want PUBLISH head 0x32, got %#x", buf[0])
		}
		// leave partial read
	})

	select {
	case <-client.Online():
		break
	case <-timeout.C:
		t.Fatal("client Online timeout")
	}

	ack, err := client.PublishAtLeastOnce([]byte{'x'}, "y")
	if err != nil {
		t.Fatalf("got error %q [%T]", err, err)
	}
	select {
	case err, ok := <-ack:
		var e net.Error
		switch {
		case !ok:
			t.Error("ack closed, want a Timeout net.Error")
		case !errors.As(err, &e) || !e.Timeout():
			t.Errorf("got ack error %q [%T], want a Timeout net.Error", err, err)
		}
	case <-timeout.C:
		t.Error("ack timeout")
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
			t.Error("broker got error on first connection close:", err)
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
	verifyAck(t, ack)
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
func TestPublishAtLeastOnceRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir() // persistence location

	const publish1 = "3206000178800031"     // '1' (0x31) @ 'x' (0x78)
	const publish2 = "3206000178800132"     // '2' (0x32) @ 'x' (0x78)
	const publish3 = "3206000178800233"     // '3' (0x33) @ 'x' (0x78)
	const publish2Dupe = "3a06000178800132" // with duplicate [DUP] flag
	const publish3Dupe = "3a06000178800233" // with duplicate [DUP] flag

	clientConn, brokerConn := net.Pipe()
	client, err := mqtt.InitSession("test-client", mqtt.FileSystem(dir), &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 3,
		Dialer:         newTestDialer(t, clientConn),
	})
	if err != nil {
		t.Fatal("InitSession error:", err)
	}
	testClient(t, client)
	brokerConn.SetDeadline(time.Now().Add(800 * time.Millisecond))
	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK

	<-client.Online()

	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, brokerConn, publish1)
		wantPacketHex(t, brokerConn, publish2)
		sendPacketHex(t, brokerConn, "40028000") // SUBACK № 1

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

	ack1, err := client.PublishAtLeastOnce([]byte{'1'}, "x")
	if err != nil {
		t.Errorf("publish № 1 got error %q [%T]", err, err)
	}
	ack2, err := client.PublishAtLeastOnce([]byte{'2'}, "x")
	if err != nil {
		t.Errorf("publish № 2 got error %q [%T]", err, err)
	}
	ack3, err := client.PublishAtLeastOnce([]byte{'3'}, "x")
	if err != nil {
		t.Errorf("publish № 3 got error %q [%T]", err, err)
	}
	verifyAck(t, ack1)
	verifyAckError(t, ack2, mqtt.ErrClosed)
	verifyAckError(t, ack3, io.ErrClosedPipe)

	<-brokerMockDone
	if t.Failed() {
		return
	}

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
		Dialer:         newTestDialer(t, clientConn),
	})
	if err != nil {
		t.Fatal("AdoptSession error:", err)
	}
	for _, err := range warn {
		t.Error("AdoptSession warning:", err)
	}
	testClient(t, client)
	brokerConn.SetDeadline(time.Now().Add(800 * time.Millisecond))
	wantPacketHex(t, brokerConn, "101700044d51545404000000000b746573742d636c69656e74")
	sendPacketHex(t, brokerConn, "20020000") // CONNACK
	wantPacketHex(t, brokerConn, publish2Dupe)
	wantPacketHex(t, brokerConn, publish3Dupe)
	sendPacketHex(t, brokerConn, "40028001") // SUBACK № 2
	sendPacketHex(t, brokerConn, "40028002") // SUBACK № 3

	// await SUBACK appliance
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(publish2File); err == nil {
		t.Error("publish № 2 file exits after SUBACK", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 2 file error:", err)
	}
	if _, err := os.Stat(publish3File); err == nil {
		t.Error("publish № 3 file exits after SUBACK", err)
	} else if !os.IsNotExist(err) {
		t.Error("publish № 3 file error:", err)
	}
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
	verifyAck(t, ack)
	<-brokerMockDone
}

func TestPublishExactlyOnceReqTimeout(t *testing.T) {
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	client, conn := newClientPipe(t)
	brokerMockDone := testRoutine(t, func() {
		var buf [1]byte
		switch _, err := io.ReadFull(conn, buf[:]); {
		case err != nil:
			t.Error("broker read error:", err)
		case buf[0] != 0x34:
			t.Errorf("want PUBLISH head 0x34, got %#x", buf[0])
		}
		// leave partial read
	})

	select {
	case <-client.Online():
		break
	case <-timeout.C:
		t.Fatal("client Online timeout")
	}

	ack, err := client.PublishExactlyOnce([]byte{'x'}, "y")
	if err != nil {
		t.Fatalf("Publish error %q [%T]", err, err)
	}
	select {
	case err, ok := <-ack:
		var e net.Error
		switch {
		case !ok:
			t.Error("ack closed, want a Timeout net.Error")
		case !errors.As(err, &e) || !e.Timeout():
			t.Errorf("got ack error %q [%T], want a Timeout net.Error", err, err)
		}
	case <-timeout.C:
		t.Error("ack timeout")
	}
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
func TestPublishExactlyOnceRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir() // persistence location

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
		Dialer:         newTestDialer(t, clientConn),
	})
	if err != nil {
		t.Fatal("InitSession error:", err)
	}
	testClient(t, client, mqtttest.Transfer{Err: io.ErrClosedPipe})
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

	<-client.Online()

	ack1, err := client.PublishExactlyOnce([]byte{'1'}, "x")
	if err != nil {
		t.Errorf("publish № 1 got error %q [%T]", err, err)
	}
	ack2, err := client.PublishExactlyOnce([]byte{'2'}, "x")
	if err != nil {
		t.Errorf("publish № 2 got error %q [%T]", err, err)
	}
	time.Sleep(50 * time.Millisecond)
	ack3, err := client.PublishExactlyOnce([]byte{'3'}, "x")
	if err != nil {
		t.Errorf("publish № 3 got error %q [%T]", err, err)
	}
	time.Sleep(50 * time.Millisecond)
	ack4, err := client.PublishExactlyOnce([]byte{'4'}, "x")
	if err != nil {
		t.Errorf("publish № 4 got error %q [%T]", err, err)
	}
	time.Sleep(50 * time.Millisecond)
	ack5, err := client.PublishExactlyOnce([]byte{'5'}, "x")
	if err != nil {
		t.Errorf("publish № 5 got error %q [%T]", err, err)
	}
	<-brokerMockDone
	verifyAck(t, ack1)
	verifyAckError(t, ack2, mqtt.ErrClosed)
	verifyAckError(t, ack3, mqtt.ErrClosed)
	verifyAckError(t, ack4, mqtt.ErrClosed)
	verifyAckError(t, ack5, mqtt.ErrClosed)

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
		Dialer:         newTestDialer(t, clientConn),
	})
	for _, err := range warn {
		t.Error("AdoptSession warning:", err)
	}
	if err != nil {
		t.Fatal("AdoptSession error:", err)
	}
	testClient(t, client)
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
	client, conn := newClientPipe(t, mqtttest.Transfer{Err: io.EOF})

	pingDone := testRoutine(t, func() {
		err := client.Ping(ctx.Done())
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("ping got error %q [%T], want an mqtt.ErrBreak", err, err)
		}
	})
	wantPacketHex(t, conn, "c000") // PINGREQ

	subscribeDone := testRoutine(t, func() {
		err := client.Subscribe(ctx.Done(), "x")
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("subscribe got error %q [%T], want an mqtt.ErrBreak", err, err)
		}
	})
	wantPacketHex(t, conn, "8206600000017802") // SUBSCRIBE

	unsubscribeDone := testRoutine(t, func() {
		err := client.Unsubscribe(ctx.Done(), "x")
		if !errors.Is(err, mqtt.ErrBreak) {
			t.Errorf("unsubscribe got error %q [%T], want an mqtt.ErrBreak", err, err)
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

func verifyAck(t *testing.T, ack <-chan error) {
	t.Helper()
	timeout := time.NewTimer(200 * time.Millisecond)
	defer timeout.Stop()

	var errDownN int

	for {
		select {
		case <-timeout.C:
			t.Fatal("ack timeout")

		case err, ok := <-ack:
			if !ok {
				return
			}

			if !errors.Is(err, mqtt.ErrDown) {
				t.Errorf("ack error %q [%T]", err, err)
				return
			}

			errDownN++
			if errDownN > 1 {
				t.Error("duplicate ack ErrDown")
				return
			}
			t.Log("ack ErrDown permitted")
		}
	}
}

func verifyAckError(t *testing.T, ack <-chan error, want error) {
	t.Helper()
	timeout := time.NewTimer(200 * time.Millisecond)
	defer timeout.Stop()

	var errDownN int

	for {
		select {
		case <-timeout.C:
			t.Fatal("ack read timeout")

		case err, ok := <-ack:
			if !ok {
				t.Errorf("ack release, want error %q", want)
				return
			}

			if !errors.Is(err, mqtt.ErrDown) {
				if !errors.Is(err, want) {
					t.Errorf("ack error %q [%T], want error %q", err, err, want)
				}
				return
			}

			errDownN++
			if errDownN > 1 {
				t.Error("duplicate ack ErrDown")
				return
			}
			t.Log("ack ErrDown permitted")
		}
	}
}
