package mqtt_test

import (
	"encoding/hex"
	"errors"
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
		t.Error("ping error:", err)
	}
}

func TestSubscribe(t *testing.T) {
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
		t.Fatal("subscribe error:", err)
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
		t.Error("publish error:", err)
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
		t.Fatal("publish error:", err)
	}
	testAckErrors(t, ack)
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
		t.Fatal("publish error:", err)
	}
	testAckErrors(t, ack)
}

func testAckErrors(t *testing.T, ack <-chan error, want ...error) {
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-timeout.C:
			t.Fatal("ack read timeout")

		case err, ok := <-ack:
			switch {
			case !ok:
				return // done
			case len(want) == 0:
				t.Errorf("ack error %q, want close", err)
			case !errors.Is(err, want[0]):
				t.Errorf("ack error %q, want %q", err, want[0])
			default:
				want = want[1:]
			}
		}
	}
}
