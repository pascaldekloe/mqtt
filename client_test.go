package mqtt

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func sendPacketHex(t *testing.T, conn net.Conn, send string) {
	t.Helper()
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

func TestCONNACK(t *testing.T) {
	golden := []struct {
		send string
		want error
	}{
		{send: "20020000", want: ErrClosed},
		{send: "20020100", want: ErrClosed},
		{send: "20020001", want: ErrProtocolLevel},
		{send: "20020102", want: ErrClientID},
		{send: "20020003", want: ErrUnavailable},
		{send: "20020104", want: ErrAuthBad},
		{send: "20020005", want: ErrAuth},
		{send: "20020106", want: connectReturn(6)},
		{send: "200200ff", want: connectReturn(255)},
		// The first packet must be a CONNACK.
		{send: "d000", want: errProtoReset}, // PINGRESP
	}

	for _, gold := range golden {
		t.Run("0x"+gold.send, func(t *testing.T) {
			clientEnd, brokerEnd := net.Pipe()
			timeout := time.AfterFunc(time.Second, func() {
				t.Error("test timeout: closing pipe")
				brokerEnd.Close()
			})

			var dialN int
			client := NewClient(&Config{WireTimeout: time.Second / 2}, func(context.Context) (net.Conn, error) {
				if dialN != 0 {
					return nil, errors.New("redial (with test pipe) denied")
				}
				dialN++
				return clientEnd, nil
			})

			done := make(chan error)
			go func() {
				defer timeout.Stop()

				_, _, err := client.ReadSlices()
				done <- err
			}()

			// read CONNECT
			wantPacketHex(t, brokerEnd, "100c00044d515454040000000000")
			// write CONNACK
			sendPacketHex(t, brokerEnd, gold.send)

			if gold.want == ErrClosed {
				err := client.Close()
				if err != nil {
					t.Error("client close error:", err)
				}
			}

			select {
			case err := <-done:
				if !errors.Is(err, gold.want) {
					t.Errorf("got error %q, want %q", err, gold.want)
				}
			case <-time.After(2 * time.Second):
				t.Error("read routine timeout")
			}
		})
	}
}

func TestClose(t *testing.T) {
	t.Parallel()

	client := NewClient(&Config{WireTimeout: time.Second / 2}, func(context.Context) (net.Conn, error) {
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
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("ReadSlices got error %q, want ErrClosed", err)
	}
	// Read (routine) stopped now

	// Run twice to ensure the semaphores ain't leaking.
	for n := 0; n < 2; n++ {
		err = client.Subscribe(nil, "x")
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Subscribe %d got error %q, want ErrClosed", n, err)
		}
		err = client.Unsubscribe(nil, "x")
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Unsubscribe %d got error %q, want ErrClosed", n, err)
		}
		err = client.Publish(nil, nil, "x")
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Publish %d got error %q, want ErrClosed", n, err)
		}
		err = client.PublishRetained(nil, nil, "x")
		if !errors.Is(err, ErrClosed) {
			t.Errorf("PublishRetained %d got error %q, want ErrClosed", n, err)
		}
		_, err = client.PublishAtLeastOnce(nil, "x")
		if !errors.Is(err, ErrClosed) {
			t.Errorf("PublishAtLeastOnce %d got error %q, want ErrClosed", n, err)
		}
		_, err = client.PublishAtLeastOnceRetained(nil, "x")
		if !errors.Is(err, ErrClosed) {
			t.Errorf("PublishAtLeastOnceRetained %d got error %q, want ErrClosed", n, err)
		}
		_, err = client.PublishExactlyOnce(nil, "x")
		if !errors.Is(err, ErrClosed) {
			t.Errorf("PublishExactlyOnce %d got error %q, want ErrClosed", n, err)
		}
		_, err = client.PublishExactlyOnceRetained(nil, "x")
		if !errors.Is(err, ErrClosed) {
			t.Errorf("PublishExactlyOnceRetained %d got error %q, want ErrClosed", n, err)
		}
		err = client.Ping(nil)
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Ping %d got error %q, want ErrClosed", n, err)
		}
		err = client.Disconnect(nil)
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Disconnect %d got error %q, want ErrClosed", n, err)
		}
	}
	_, _, err = client.ReadSlices()
	if !errors.Is(err, ErrClosed) {
		t.Errorf("another ReadSlices got error %q, want ErrClosed", err)
	}
}
