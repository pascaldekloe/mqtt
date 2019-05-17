package mqtt

import (
	"context"
	"fmt"
	"net"
)

// Receive is invoked for inbound messages with AtMostOnce or AtLeastOnce QoS.
// AtLeastOnce causes retries on error returns.
type Receive func(ctx context.Context, topic string, message []byte) error

// Client is the initiator of the protocol.
type Client struct {
	ctx         context.Context
	conn        net.Conn
	listener    Receive
	writePacket packet
}

// Connect initiates the protocol over a transport layer such as *net.TCP or
// *tls.Conn. The session context is applied to all the client operations too.
func Connect(ctx context.Context, conn net.Conn, attrs *Attributes, listener Receive) (*Client, error) {
	var writePacket packet
	writePacket.connReq(attrs)

	c := make(chan error)

	// launch handshake
	go func() {
		defer close(c)

		if _, err := conn.Write(writePacket.buf); err != nil {
			c <- err
			return
		}

		var buf [4]byte
		n, err := conn.Read(buf[:])
		if err != nil {
			c <- err
			return
		}
		for {
			if n > 0 && buf[0] != connAck<<4 {
				conn.Close()
				c <- fmt.Errorf("mqtt: received packet type %#x on connect—connection closed", buf[0]>>4)
				return
			}
			if n > 1 && buf[1] != 2 {
				conn.Close()
				c <- fmt.Errorf("mqtt: connect acknowledge remaining length is %d instead of 2—connection closed", buf[1])
				return
			}
			if n > 2 && buf[2] > 1 {
				conn.Close()
				c <- fmt.Errorf("mqtt: received reserved connect acknowledge flags %#x—connection closed", buf[2])
				return
			}
			if n > 3 {
				break
			}

			more, err := conn.Read(buf[n:])
			if err != nil {
				c <- err
				return
			}
			n += more
		}

		if code := connectReturn(buf[3]); code != accepted {
			c <- code
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-c:
		if err != nil {
			return nil, err
		}
	}

	return &Client{ctx, conn, listener, writePacket}, nil
}

// Publish submits a message on a topic with a specific quality of service.
// AtMostOnce returns after network submission, AtLeastOnce awaits receival
// acknowledgement and AtMostOnce undergoes a two-stage acknowledegement.
func (c *Client) Publish(ctx context.Context, topic string, message []byte, deliver QoS) error {
	var id uint
	c.writePacket.pub(id, topic, message, deliver)
	if _, err := c.conn.Write(c.writePacket.buf); err != nil {
		return err
	}

	if deliver == AtMostOnce {
		return nil
	}

	return fmt.Errorf("TODO: QoS %d", deliver)
}

// PublishRetained acts like Publish, but causes the message to be stored on the
// server, so that it can be delivered to future subscribers.
func (c *Client) PublishRetained(ctx context.Context, topic string, message []byte, deliver QoS) error {
	var id uint
	c.writePacket.pub(id, topic, message, deliver)
	c.writePacket.buf[0] |= retainFlag
	if _, err := c.conn.Write(c.writePacket.buf); err != nil {
		return err
	}

	if deliver == AtMostOnce {
		return nil
	}

	return fmt.Errorf("TODO: QoS %d", deliver)
}

// Subscribe requests a subscription for all topics that match the filter.
// The requested quality of service is a maximum for the server.
func (c *Client) Subscribe(ctx context.Context, topicFilter string, max QoS) error {
	var id uint
	c.writePacket.subReq(id, topicFilter, max)
	if _, err := c.conn.Write(c.writePacket.buf); err != nil {
		return err
	}

	panic("TODO: await ack")
}

// Unsubscribe requests a Subscribe cancelation.
func (c *Client) Unsubscribe(ctx context.Context, topicFilter string) error {
	var id uint
	c.writePacket.unsubReq(id, topicFilter)
	if _, err := c.conn.Write(c.writePacket.buf); err != nil {
		return err
	}

	panic("TODO: await ack")
}

// Ping makes a roundtrip to validate the connection.
func (c *Client) Ping() error {
	if _, err := c.conn.Write(pingPacket); err != nil {
		return err
	}

	panic("TODO: await pong")
}

// Disconnect is a graceful termination, which also discards the Will.
// The connection is closed uppon succes.
func (c *Client) Disconnect() error {
	_, err := c.conn.Write(disconnPacket)
	closeErr := c.conn.Close()
	if err == nil {
		err = closeErr
	}
	return err
}
