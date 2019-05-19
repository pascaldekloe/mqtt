package mqtt

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/bits"
	"net"
	"time"
)

// Receive is invoked for inbound messages with AtMostOnce or AtLeastOnce QoS.
// AtLeastOnce causes retries on error returns.
type Receive func(ctx context.Context, topic string, message []byte) error

// Client manages a connection to one server.
type Client struct {
	ctx         context.Context
	conn        net.Conn
	listener    Receive
	writePacket packet
	retryDelay  time.Duration
	pong        chan struct{}
	closed      chan struct{}
	acceptMax   int
}

func (c *Client) write(p []byte) error {
	n, err := c.conn.Write(p)
	for err != nil {
		select {
		case <-c.closed:
			return ErrClosed
		default:
			break
		}

		if e, ok := err.(net.Error); !ok || !e.Temporary() {
			c.conn.Close()
			return err
		}

		delay := c.retryDelay
		log.Print("mqtt: send retry on temporary network error in ", delay, ": ", err)
		time.Sleep(delay)

		var more int
		more, err = c.conn.Write(p[n:])
		// handle error in current loop
		n += more
	}

	return nil
}

func (c *Client) readLoop() {
	// determine only here whether closed
	defer close(c.closed)

	buf := make([]byte, 128)
	var bufN, flushN int
	for {
		read, err := c.conn.Read(buf[bufN:])
		bufN += read
		// error handling delayed!
		// consume available first

		var offset int
		if flushN > 0 {
			if flushN >= bufN {
				flushN -= bufN
				bufN = 0
			} else {
				offset = flushN
			}
		}

		for offset+1 < bufN {
			const sizeErr = "mqtt: protocol violation: remaining length declaration exceeds 4 B—connection closed"
			sizeVal, sizeN := binary.Uvarint(buf[offset+1 : bufN])
			if sizeN == 0 {
				// not enough data
				if bufN-offset > 4 {
					c.conn.Close()
					log.Print(sizeErr)
					return
				}
				break
			}
			if sizeN < 0 || sizeN > 4 {
				c.conn.Close()
				log.Print(sizeErr)
				return
			}

			// won't overflow due to 4 byte varint limit
			packetSize := 1 + sizeN + int(sizeVal)
			if packetSize > c.acceptMax {
				log.Printf("mqtt: skipping %d B inbound packet; limit is %d B", packetSize, c.acceptMax)
				flushN = packetSize
				break
			}

			if packetSize < bufN-offset {
				// not enough data
				if packetSize > len(buf) {
					// buff too small
					grow := make([]byte, 1<<uint(bits.Len(uint(packetSize))))
					copy(grow, buf[:bufN])
					buf = grow
				}
				break
			}

			ok := c.inbound(buf[offset], buf[offset+1+sizeN:offset+packetSize])
			if !ok {
				c.conn.Close()
				return
			}

			offset += packetSize
		}

		if offset > 0 {
			// move to beginning of buffer
			copy(buf, buf[offset:bufN])
			bufN -= offset
		}

		switch err {
		case nil:
			break

		case io.EOF:
			return

		default:
			if e, ok := err.(net.Error); !ok || !e.Temporary() {
				log.Print("mqtt: closing connection on read error: ", err)
				c.conn.Close()
				return
			}

			delay := c.retryDelay
			log.Print("mqtt: read retry on temporary network error in ", delay, ": ", err)
			time.Sleep(delay)
		}
	}
}

func (c *Client) inbound(a byte, p []byte) (ok bool) {
	switch packetType := a >> 4; packetType {
	case connReq, subReq, unsubReq, ping, disconn:
		log.Print("mqtt: close on protocol violation: client received packet type ", packetType)

	case connAck:
		log.Print("mqtt: close on protocol violation: redunant connection acknowledge")

	case pubReq:
		// TODO

	case pubAck, pubReceived, pubRelease, pubComplete, unsubAck:
		if len(p) != 2 {
			log.Print("mqtt: close on protocol violation: remaining length not 2")
			return
		}
		// TODO: acknowledge uint16(p[0])<<8 | uint16(p[1])

	case subAck:
		if len(p) != 3 {
			log.Print("mqtt: close on protocol violation: remaining length not 3")
			return
		}

	case pong:
		if len(p) != 0 {
			log.Print("mqtt: ping response packet remaining length not 0")
		}
		c.pong <- struct{}{}
		ok = true

	default:
		log.Print("mqtt: close on protocol violation: received reserved packet type ", packetType)
	}

	return
}

// Connect initiates the protocol over a transport layer such as *net.TCP or
// *tls.Conn. The session context is applied to all the client operations too.
func Connect(ctx context.Context, conn net.Conn, attrs *Attributes, listener Receive) (*Client, error) {
	c := &Client{
		ctx:      ctx,
		conn:     conn,
		listener: listener,
		closed:   make(chan struct{}),
	}

	c.writePacket.connReq(attrs)

	done := make(chan error)

	// launch handshake
	go func() {
		defer close(done)

		if err := c.write(c.writePacket.buf); err != nil {
			done <- err
			return
		}

		var buf [4]byte
		n, err := conn.Read(buf[:])
		if err != nil {
			done <- err
			return
		}
		for {
			if n > 0 && buf[0] != connAck<<4 {
				conn.Close()
				done <- fmt.Errorf("mqtt: received packet type %#x on connect—connection closed", buf[0]>>4)
				return
			}
			if n > 1 && buf[1] != 2 {
				conn.Close()
				done <- fmt.Errorf("mqtt: connect acknowledge remaining length is %d instead of 2—connection closed", buf[1])
				return
			}
			if n > 2 && buf[2] > 1 {
				conn.Close()
				done <- fmt.Errorf("mqtt: received reserved connect acknowledge flags %#x—connection closed", buf[2])
				return
			}
			if n > 3 {
				break
			}

			more, err := conn.Read(buf[n:])
			if err != nil {
				done <- err
				return
			}
			n += more
		}

		if code := connectReturn(buf[3]); code != accepted {
			done <- code
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-done:
		if err != nil {
			return nil, err
		}
	}

	go c.readLoop()

	return c, nil
}

// Publish submits a message on a topic with a specific quality of service.
// AtMostOnce returns after network submission, AtLeastOnce awaits receival
// acknowledgement and AtMostOnce undergoes a two-stage acknowledegement.
func (c *Client) Publish(ctx context.Context, topic string, message []byte, deliver QoS) error {
	var id uint
	c.writePacket.pub(id, topic, message, deliver)
	if err := c.write(c.writePacket.buf); err != nil {
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
	if err := c.write(c.writePacket.buf); err != nil {
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
	if err := c.write(c.writePacket.buf); err != nil {
		return err
	}

	panic("TODO: await ack")
}

// Unsubscribe requests a Subscribe cancelation.
func (c *Client) Unsubscribe(ctx context.Context, topicFilter string) error {
	var id uint
	c.writePacket.unsubReq(id, topicFilter)
	if err := c.write(c.writePacket.buf); err != nil {
		return err
	}

	panic("TODO: await ack")
}

// Ping makes a roundtrip to validate the connection.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.write(pingPacket); err != nil {
		return err
	}

	select {
	case <-c.pong:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Disconnect is a graceful termination, which also discards the Will.
// The underlying connection is closed.
func (c *Client) Disconnect() error {
	_, err := c.conn.Write(disconnPacket)
	closeErr := c.conn.Close()
	if err == nil {
		err = closeErr
	}
	return err
}
