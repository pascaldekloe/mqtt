package mqtt_test

import (
	"bytes"
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

// NewClientCONNECTHex is the initial packet send to conns of a newClient.
const newClientCONNECTHex = "100c00044d515454040000000000"

// NewClient returns a new client with a Dialer which returns conns in order of
// appearence.
func newClient(t *testing.T, conns []net.Conn, want ...mqtttest.Transfer) *mqtt.Client {
	timeoutDone := make(chan struct{})
	timeout := time.AfterFunc(time.Second, func() {
		defer close(timeoutDone)
		t.Error("test timeout; closing connections…")
		for _, conn := range conns {
			conn.Close()
		}
	})

	var dialN int
	client := mqtt.NewClient(&mqtt.Config{
		WireTimeout:    time.Second / 4,
		AtLeastOnceMax: 2,
		ExactlyOnceMax: 2,
	}, func(context.Context) (net.Conn, error) {
		dialN++
		t.Log("Dial #", dialN)
		if dialN > len(conns) {
			return nil, errors.New("no more connections for test")
		}
		return conns[dialN-1], nil
	})

	// launch read-routine
	testRoutine(t, func() {
		defer func() {
			if !timeout.Stop() {
				// await all routines
				<-timeoutDone
			}
		}()

		for {
			message, topic, err := client.ReadSlices()
			if big := (*mqtt.BigMessage)(nil); errors.As(err, &big) {
				t.Log("ReadSlices got BigMessage")
				topic = []byte(big.Topic)
				message, err = big.ReadAll()
			}
			switch {
			case err != nil:
				switch {
				case errors.Is(err, mqtt.ErrClosed):
					if len(want) != 0 {
						t.Errorf("client closed, want %d more ReadSlices", len(want))
					}
					return

				case len(want) == 0:
					t.Errorf("ReadSlices got error %q, want close", err)
				case want[0].Err == nil:
					t.Errorf("ReadSlices got error %q, want message %#x @ %q", err, want[0].Message, want[0].Topic)
				case !errors.Is(err, want[0].Err) && err.Error() != want[0].Err.Error():
					t.Errorf("ReadSlices got error %q, want errors.Is %q", err, want[0].Err)
				}
				// small backoff may prevent error flood
				time.Sleep(time.Second / 8)

			case len(want) == 0:
				t.Errorf("ReadSlices got message %q @ %q, want close", message, topic)
			case want[0].Err != nil:
				t.Errorf("ReadSlices got message %#x @ %q, want error %q", message, topic, want[0].Err)
			case !bytes.Equal(message, want[0].Message), string(topic) != want[0].Topic:
				t.Errorf("got message %#x @ %q, want %#x @ %q", message, topic, want[0].Message, want[0].Topic)
			}

			if len(want) != 0 {
				want = want[1:] // move to next in line
			}
		}
	})

	t.Cleanup(func() {
		err := client.Close()
		if err != nil {
			t.Error("client close error:", err)
		}
	})

	return client
}

func TestCONNACK(t *testing.T) {
	golden := []struct {
		send string
		want error
	}{
		{send: "20020001", want: mqtt.ErrProtocolLevel},
		{send: "20020102", want: mqtt.ErrClientID},
		{send: "20020003", want: mqtt.ErrUnavailable},
		{send: "20020104", want: mqtt.ErrAuthBad},
		{send: "20020005", want: mqtt.ErrAuth},
		// The first packet must be a CONNACK.
		{send: "d000", want: errors.New("mqtt: connection reset on protocol violation by the broker: want fixed CONNACK header 0x2002, got 0xd000")},
	}

	for _, gold := range golden {
		t.Run("0x"+gold.send, func(t *testing.T) {
			t.Parallel()

			clientEnd, brokerEnd := net.Pipe()
			blocked, _ := net.Pipe()
			client := newClient(t, []net.Conn{clientEnd, blocked}, mqtttest.Transfer{Err: gold.want})

			wantPacketHex(t, brokerEnd, newClientCONNECTHex)
			sendPacketHex(t, brokerEnd, gold.send)

			err := client.Ping(nil)
			if !errors.Is(err, mqtt.ErrDown) {
				t.Errorf("ping got error %q, want an ErrDown", err)
			}
			err = client.Close()
			if err != nil {
				t.Error("close error:", err)
			}

			// give ReadSlices time to ErrClosed
			time.Sleep(time.Second / 8)
		})
	}
}

func TestClose(t *testing.T) {
	t.Parallel()

	client := mqtt.NewClient(&mqtt.Config{WireTimeout: time.Second / 2}, func(context.Context) (net.Conn, error) {
		return nil, errors.New("dialer invoked")
	})
	err := client.VolatileSession("test-client")
	if err != nil {
		t.Fatal("volatile session error:", err)
	}

	// Invoke Close before ReadSlices (connects).
	// Race because we can. ™️
	for n := 0; n < 3; n++ {
		go func() {
			err := client.Close()
			if err != nil {
				t.Error("got close error:", err)
			}
		}()
	}
	time.Sleep(time.Second / 4)
	_, _, err = client.ReadSlices()
	if !errors.Is(err, mqtt.ErrClosed) {
		t.Fatalf("ReadSlices got error %q, want ErrClosed", err)
	}
	// Read (routine) stopped now

	// Run twice to ensure the semaphores ain't leaking.
	for n := 0; n < 2; n++ {
		err = client.Subscribe(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Subscribe %d got error %q, want ErrClosed", n, err)
		}
		err = client.Unsubscribe(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Unsubscribe %d got error %q, want ErrClosed", n, err)
		}
		err = client.Publish(nil, nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Publish %d got error %q, want ErrClosed", n, err)
		}
		err = client.PublishRetained(nil, nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishRetained %d got error %q, want ErrClosed", n, err)
		}
		_, err = client.PublishAtLeastOnce(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishAtLeastOnce %d got error %q, want ErrClosed", n, err)
		}
		_, err = client.PublishAtLeastOnceRetained(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishAtLeastOnceRetained %d got error %q, want ErrClosed", n, err)
		}
		_, err = client.PublishExactlyOnce(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishExactlyOnce %d got error %q, want ErrClosed", n, err)
		}
		_, err = client.PublishExactlyOnceRetained(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishExactlyOnceRetained %d got error %q, want ErrClosed", n, err)
		}
		err = client.Ping(nil)
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Ping %d got error %q, want ErrClosed", n, err)
		}
		err = client.Disconnect(nil)
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Disconnect %d got error %q, want ErrClosed", n, err)
		}
	}
	_, _, err = client.ReadSlices()
	if !errors.Is(err, mqtt.ErrClosed) {
		t.Errorf("another ReadSlices got error %q, want ErrClosed", err)
	}
}

func TestReceivePublishAtLeastOnce(t *testing.T) {
	_, conn := newClientPipe(t, mqtttest.Transfer{Message: []byte("hello"), Topic: "greet"})

	sendPacketHex(t, conn, hex.EncodeToString([]byte{
		0x32, 14,
		0, 5, 'g', 'r', 'e', 'e', 't',
		0xab, 0xcd, // packet identifier
		'h', 'e', 'l', 'l', 'o'}))
	wantPacketHex(t, conn, "4002abcd") // PUBACK
}

func TestReceivePublishExactlyOnce(t *testing.T) {
	_, conn := newClientPipe(t, mqtttest.Transfer{Message: []byte("hello"), Topic: "greet"})

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

func TestReceivePublishAtLeastOnceBig(t *testing.T) {
	const bigN = 256 * 1024

	_, conn := newClientPipe(t, mqtttest.Transfer{Message: bytes.Repeat([]byte{'A'}, bigN), Topic: "bam"})

	sendPacketHex(t, conn, hex.EncodeToString([]byte{
		0x32, 0x87, 0x80, 0x10,
		0, 3, 'b', 'a', 'm',
		0xab, 0xcd})+strings.Repeat("41", bigN))
	wantPacketHex(t, conn, "4002abcd") // PUBACK
}

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

func sendPacketHex(t *testing.T, conn net.Conn, send string) {
	t.Helper()
	t.Logf("send %s…", typeLabelFromHex(send[0]))
	packet, err := hex.DecodeString(send)
	if err != nil {
		t.Fatalf("test has malformed packet data 0x%s: %s", send, err)
	}
	_, err = conn.Write(packet)
	if err != nil {
		t.Fatalf("broker write 0x%s error: %s", send, err)
	}
}

func wantPacketHex(t *testing.T, conn net.Conn, want string) {
	t.Helper()
	t.Logf("want %s…", typeLabelFromHex(want[0]))
	var buf [128]byte
	_, err := io.ReadFull(conn, buf[:2])
	if err != nil {
		t.Fatalf("broker read error %q, want 0x%s", err, want)
	}
	if buf[1] > 126 {
		t.Fatalf("packet %#x… too big for test, want 0x%s", buf[:2], want)
	}
	n, err := io.ReadFull(conn, buf[2:2+buf[1]])
	if err != nil {
		t.Fatalf("broker read error %q after %#x, want 0x%s", err, buf[:2+n], want)
	}
	got := hex.EncodeToString(buf[:2+n])
	if want != got {
		t.Errorf("broker got packet 0x%s, want 0x%s", got, want)
	}
}

func typeLabelFromHex(char byte) string {
	switch char {
	case '0':
		return "RESERVED0"
	case '1':
		return "CONNECT"
	case '2':
		return "CONNACK"
	case '3':
		return "PUBLISH"
	case '4':
		return "PUBACK"
	case '5':
		return "PUBREC"
	case '6':
		return "PUBREL"
	case '7':
		return "PUBCOMP"
	case '8':
		return "SUBSCRIBE"
	case '9':
		return "SUBACK"
	case 'a', 'A':
		return "UNSUBSCRIBE"
	case 'b', 'B':
		return "UNSUBACK"
	case 'c', 'C':
		return "PINGREQ"
	case 'd', 'D':
		return "PINGRESP"
	case 'e', 'E':
		return "DISCONNECT"
	case 'f', 'F':
		return "RESERVED15"
	default:
		panic("not a hex character")
	}
}
