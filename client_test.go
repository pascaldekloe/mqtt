package mqtt_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pascaldekloe/mqtt"
	"github.com/pascaldekloe/mqtt/mqtttest"
)

// TestClient reads the client with assertions and timeouts.
func testClient(t *testing.T, client *mqtt.Client, want ...mqtttest.Transfer) {
	// extra offline state check
	select {
	case <-client.Online():
		t.Fatal("online signal receive before ReadSlices")
	default:
		break
	}
	select {
	case <-client.Offline():
		break
	default:
		t.Fatal("offline signal blocked before ReadSlices")
	}

	timeoutOver := make(chan struct{})
	timeout := time.AfterFunc(30*time.Second, func() {
		defer close(timeoutOver)

		t.Error("test timeout; closing Client now…")
		err := client.Close()
		if err != nil {
			t.Error("Close error:", err)
		}
	})

	readRoutineDone := testRoutine(t, func() {
		defer func() {
			if timeout.Stop() {
				close(timeoutOver)
			}
		}()

		const readSlicesMax = 10
		for n := 0; n < readSlicesMax; n++ {
			message, topic, err := client.ReadSlices()

			if errors.Is(err, errLastTestConn) {
				t.Log("backoff on:", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if errors.Is(err, mqtt.ErrClosed) {
				for i := range want {
					if want[i].Err != nil {
						t.Errorf("client closed, want ReadSlices error: %v", want[i].Err)
					} else {
						t.Errorf("client closed, want ReadSlices message %#.1000x @ %q", want[i].Message, want[i].Topic)
					}
				}

				return
			}

			if len(want) == 0 {
				t.Errorf("ReadSlices got message %q, topic %q, and error %q, want ErrClosed", message, topic, err)
				continue
			}

			var big *mqtt.BigMessage
			if errors.As(err, &big) {
				t.Log("got BigMessage")
				topic = []byte(big.Topic)
				message, err = big.ReadAll()
			}

			if err != nil {
				if want[0].Err == nil {
					t.Errorf("ReadSlices got error %q, want message %#.200x @ %q", err, want[0].Message, want[0].Topic)
				} else if !errors.Is(err, want[0].Err) && err.Error() != want[0].Err.Error() {
					t.Errorf("ReadSlices got error %q, want errors.Is %q", err, want[0].Err)
				}
			} else {
				if want[0].Err != nil {
					t.Errorf("ReadSlices got message %#.200x @ %q, want error %q", message, topic, want[0].Err)
				} else if !bytes.Equal(message, want[0].Message) || string(topic) != want[0].Topic {
					t.Errorf("ReadSlices got message %#.200x @ %q, want %#.200x @ %q", message, topic, want[0].Message, want[0].Topic)
				}
			}

			want = want[1:] // move to next in line
		}

		t.Errorf("test abort after %d ReadSlices", readSlicesMax)
	})

	t.Cleanup(func() {
		err := client.Close()
		if err != nil {
			t.Error("client close error:", err)
		}

		// no routine leaks
		<-readRoutineDone
		<-timeoutOver

		// extra offline state check
		select {
		case <-client.Online():
			t.Error("online signal receive after client close")
		default:
			break
		}
		select {
		case <-client.Offline():
			break
		default:
			t.Error("offline signal blocked after client close")
		}
	})
}

// PipeCONNECTHex is the initial packet send to conns from a ClientPipe.
const pipeCONNECTHex = "100c00044d515454040000000000"

// NewClientPipe returns a new Client which is connected to a pipe.
func newClientPipe(t *testing.T, want ...mqtttest.Transfer) (*mqtt.Client, net.Conn) {
	client, conns := newClientPipeN(t, 1, want...)
	return client, conns[0]
}

// NewClientPipeN returns a new Client with n piped connections. The client is
// connected to the first pipe. Reconnects get the remaining pipes in order of
// appearance. The test fails on fewer connects than n.
func newClientPipeN(t *testing.T, n int, want ...mqtttest.Transfer) (*mqtt.Client, []net.Conn) {
	// This type of test is slow in general.
	t.Parallel()

	// expire I/O mocks before tests time out
	brokerDeadline := time.Now().Add(800 * time.Millisecond)

	clientConns := make([]net.Conn, n)
	brokerConns := make([]net.Conn, n)
	for i := range clientConns {
		clientConns[i], brokerConns[i] = net.Pipe()
		brokerConns[i].SetDeadline(brokerDeadline)
	}

	client, err := mqtt.VolatileSession("", &mqtt.Config{
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 2,
		ExactlyOnceMax: 2,
		Dialer:         newTestDialer(t, clientConns...),
	})
	if err != nil {
		t.Fatal("volatile session error:", err)
	}

	testClient(t, client, want...)

	wantPacketHex(t, brokerConns[0], pipeCONNECTHex)
	sendPacketHex(t, brokerConns[0], "20020000") // CONNACK

	return client, brokerConns
}

var errLastTestConn = errors.New("Dialer mock exhausted: all connections served")

// NewTestDialer returns a new dialer which returns the conns in order of
// appearance. The test fails on fewer dials.
func newTestDialer(t *testing.T, conns ...net.Conn) mqtt.Dialer {
	t.Helper()

	var dialN atomic.Uint64

	t.Cleanup(func() {
		n := dialN.Load()
		if n < uint64(len(conns)) && !t.Failed() {
			t.Errorf("got %d Dialer invocations, want %d", n, len(conns))
		}
	})

	return func(context.Context) (net.Conn, error) {
		n := dialN.Add(1)
		t.Log("Dial #", n)
		if n > uint64(len(conns)) {
			return nil, errLastTestConn
		}
		return conns[n-1], nil
	}
}

func TestClose(t *testing.T) {
	client, err := mqtt.VolatileSession("test-client", &mqtt.Config{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		PauseTimeout: time.Second / 2,
	})
	if err != nil {
		t.Fatal("volatile session error:", err)
	}

	online := client.Online()
	select {
	case <-online:
		t.Error("online signal receive on initial state")
	default:
		break
	}
	offline := client.Offline()
	select {
	case <-offline:
		break
	default:
		t.Error("offline signal blocked on initial state")
	}

	// Race because we can. ™️
	for n := 0; n < 3; n++ {
		go func() {
			err := client.Close()
			if err != nil {
				t.Error("got close error:", err)
			}
		}()
	}

	_, _, err = client.ReadSlices()
	if !errors.Is(err, mqtt.ErrClosed) {
		t.Fatalf("ReadSlices got error %q, want an ErrClosed", err)
	}

	// Run twice to ensure the semaphores ain't leaking.
	for roundN := 1; roundN <= 2; roundN++ {
		err = client.Subscribe(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Subscribe round %d got error %q, want an ErrClosed", roundN, err)
		}
		err = client.Unsubscribe(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Unsubscribe round %d got error %q, want an ErrClosed", roundN, err)
		}
		err = client.Publish(nil, nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Publish round %d got error %q, want an ErrClosed", roundN, err)
		}
		err = client.PublishRetained(nil, nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishRetained round %d got error %q, want an ErrClosed", roundN, err)
		}
		_, err = client.PublishAtLeastOnce(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishAtLeastOnce round %d got error %q, want an ErrClosed", roundN, err)
		}
		_, err = client.PublishAtLeastOnceRetained(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishAtLeastOnceRetained round %d got error %q, want an ErrClosed", roundN, err)
		}
		_, err = client.PublishExactlyOnce(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishExactlyOnce round %d got error %q, want an ErrClosed", roundN, err)
		}
		_, err = client.PublishExactlyOnceRetained(nil, "x")
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("PublishExactlyOnceRetained round %d got error %q, want an ErrClosed", roundN, err)
		}
		err = client.Ping(nil)
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Ping round %d got error %q, want an ErrClosed", roundN, err)
		}
		err = client.Disconnect(nil)
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Errorf("Disconnect round %d got error %q, want an ErrClosed", roundN, err)
		}
		_, _, err = client.ReadSlices()
		if !errors.Is(err, mqtt.ErrClosed) {
			t.Fatalf("ReadSlices round %d got error %q, want an ErrClosed", roundN, err)
		}
	}

	select {
	case <-online:
		t.Error("online signal receive")
	default:
		break
	}
	select {
	case <-offline:
		break
	default:
		t.Error("offline signal blocked")
	}
}

func TestDown(t *testing.T) {
	brokerEnd, clientEnd := net.Pipe()

	var dialN int
	client, err := mqtt.VolatileSession("", &mqtt.Config{
		Dialer: func(context.Context) (net.Conn, error) {
			dialN++
			if dialN > 1 {
				return nil, errors.New("no more connections for test")
			}
			return clientEnd, nil
		},
		PauseTimeout:   time.Second / 4,
		AtLeastOnceMax: 2,
		ExactlyOnceMax: 2,
	})
	if err != nil {
		t.Fatal("volatile session error:", err)
	}

	brokerMockDone := testRoutine(t, func() {
		wantPacketHex(t, brokerEnd, pipeCONNECTHex)
		sendPacketHex(t, brokerEnd, "20020003")
	})

	message, topic, err := client.ReadSlices()
	if !errors.Is(err, mqtt.ErrUnavailable) {
		t.Fatalf("ReadSlices got (%q, %q, %q), want an ErrUnavailable", message, topic, err)
	}
	if !mqtt.IsConnectionRefused(err) {
		t.Errorf("ReadSlices error %q is not an IsConnectionRefused", err)
	}
	<-brokerMockDone

	// Run twice to ensure the semaphores ain't leaking.
	for roundN := 1; roundN <= 2; roundN++ {
		err := client.Subscribe(nil, "x")
		if !errors.Is(err, mqtt.ErrDown) {
			t.Errorf("Subscribe round %d got error %q, want an ErrDown", roundN, err)
		}
		err = client.Unsubscribe(nil, "x")
		if !errors.Is(err, mqtt.ErrDown) {
			t.Errorf("Unsubscribe round %d got error %q, want an ErrDown", roundN, err)
		}
		err = client.Publish(nil, nil, "x")
		if !errors.Is(err, mqtt.ErrDown) {
			t.Errorf("Publish round %d got error %q, want an ErrDown", roundN, err)
		}
		err = client.PublishRetained(nil, nil, "x")
		if !errors.Is(err, mqtt.ErrDown) {
			t.Errorf("PublishRetained round %d got error %q, want an ErrDown", roundN, err)
		}
		_, err = client.PublishAtLeastOnce(nil, "x")
		if roundN > 1 {
			if !errors.Is(err, mqtt.ErrMax) {
				t.Errorf("PublishAtLeastOnce round %d got error %q, want an ErrMax", roundN, err)
			}
		} else if err != nil {
			t.Errorf("PublishAtLeastOnce round %d got error %q", roundN, err)
		}
		_, err = client.PublishAtLeastOnceRetained(nil, "x")
		if roundN > 1 {
			if !errors.Is(err, mqtt.ErrMax) {
				t.Errorf("PublishAtLeastOnceRetained round %d got error %q, want an ErrMax", roundN, err)
			}
		} else if err != nil {
			t.Errorf("PublishAtLeastOnceRetained round %d got error %q", roundN, err)
		}
		_, err = client.PublishExactlyOnce(nil, "x")
		if roundN > 1 {
			if !errors.Is(err, mqtt.ErrMax) {
				t.Errorf("PublishExactlyOnce round %d got error %q, want an ErrMax", roundN, err)
			}
		} else if err != nil {
			t.Errorf("PublishExactlyOnce round %d got error %q", roundN, err)
		}
		_, err = client.PublishExactlyOnceRetained(nil, "x")
		if roundN > 1 {
			if !errors.Is(err, mqtt.ErrMax) {
				t.Errorf("PublishExactlyOnceRetained round %d got error %q, want an ErrMax", roundN, err)
			}
		} else if err != nil {
			t.Errorf("PublishExactlyOnceRetained round %d got error %q", roundN, err)
		}
		err = client.Ping(nil)
		if !errors.Is(err, mqtt.ErrDown) {
			t.Errorf("Ping round %d got error %q, want an ErrDown", roundN, err)
		}
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

	sendPacketHex(t, conn, "32"+ // publish at least once
		"878010"+ // size varint 7 + bigN
		"000362616d"+ // topic
		"abcd") // packet identifier
	_, err := conn.Write(bytes.Repeat([]byte{'A'}, bigN))
	if err != nil {
		t.Fatal("payload submission error:", err)
	}
	wantPacketHex(t, conn, "4002abcd") // PUBACK
}

func testRoutine(t *testing.T, f func()) (done <-chan struct{}) {
	t.Helper()
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		f()
	}()
	t.Cleanup(func() {
		t.Helper()
		select {
		case <-ch:
			break // OK
		default:
			t.Error("test routine leak")
		}
	})
	return ch
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
