package mqtt

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/bits"
	"net"
	"time"
)

// Receive gets invoked for inbound messages. AtMostOnce ignores the return.
// ExactlyOnce repeates Receive until the return is true and AtLeastOnce may
// repeat Receive even after the return is true.
type Receive func(topic string, message []byte) bool

// Connecter abstracts the transport layer establishment.
type Connecter func() (net.Conn, error)

// UnsecuredConnecter creates plain network connections.
// See net.Dial for details on the network & address syntax.
// With or without a timeout, the operating system may still
// impose its own earlier timeout. For example, TCP timeouts
// are often around 3 minutes.
func UnsecuredConnecter(network, address string, timeout time.Duration) Connecter {
	dialer := &net.Dialer{Timeout: timeout}
	return func() (net.Conn, error) {
		return dialer.Dial(network, address)
	}
}

// SecuredConnecter creates TLS network connections.
// See net.Dial for details on the network & address syntax.
// With or without a timeout, the operating system may still
// impose its own earlier timeout. For example, TCP timeouts
// are often around 3 minutes.
func SecuredConnecter(network, address string, conf *tls.Config, timeout time.Duration) Connecter {
	dialer := &tls.Dialer{&net.Dialer{Timeout: timeout}, conf}
	return func() (net.Conn, error) {
		return dialer.Dial(network, address)
	}
}

// ClientConfig defines Client settings.
type ClientConfig struct {
	Connecter // remote target

	SessionConfig

	// Enable AtLeastOnce and ExactlyOnce when set.
	Storage

	// Boundary for ErrRequestLimit.
	// Negative values default to the protocol limit of 64 ki.
	RequestLimit int

	// Maximum number of bytes for inbound payloads.
	// Negative values default to the protocol limit of 256 MiB.
	InSizeLimit int

	// Backoff on transport errors.
	RetryDelay time.Duration

	// Limit for transport unit exchange.
	WireTimeout time.Duration
}

// Client manages a single network connection.
type Client struct {
	ClientConfig // read-only

	packetIDs

	conn net.Conn

	listener Receive

	pong   chan struct{}
	closed chan struct{}
}

func NewClient(config *ClientConfig) *Client {
	c := &Client{
		ClientConfig: *config, // copy
		packetIDs: packetIDs{
			inUse: make(map[uint]struct{}),
			limit: config.RequestLimit,
		},
		pong:   make(chan struct{}, 1),
		closed: make(chan struct{}),
	}

	if c.Will != nil {
		willCopy := *c.Will
		c.Will = &willCopy
	}

	const requestMax = 0x10000 // 16-bit address space
	if c.packetIDs.limit < 1 || c.packetIDs.limit > requestMax {
		c.packetIDs.limit = requestMax
	}

	return c
}

func (c *Client) write(p []byte) error {
	c.conn.SetWriteDeadline(time.Now().Add(c.WireTimeout))
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

		delay := c.RetryDelay
		log.Print("mqtt: send retry in ", delay, " on temporary network error: ", err)
		time.Sleep(delay)
		c.conn.SetWriteDeadline(time.Now().Add(c.WireTimeout))

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
			if packetSize > c.InSizeLimit {
				log.Printf("mqtt: skipping %d B inbound packet; limit is %d B", packetSize, c.InSizeLimit)
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

			delay := c.RetryDelay
			log.Print("mqtt: read retry on temporary network error in ", delay, ": ", err)
			time.Sleep(delay)
		}
	}
}

func (c *Client) inbound(a byte, p []byte) (ok bool) {
	switch packetType := a >> 4; packetType {
	case pubReq:
		// parse packet
		i := uint(p[0])<<8 | uint(p[1])
		topic := string(p[2:i])
		id := uint(p[i])<<8 | uint(p[i+1])
		message := p[i+2:]

		var resp *packet
		switch (a >> 1) & 3 {
		case AtMostOnce:
			c.listener(topic, message)
			return

		case AtLeastOnce:
			resp = newPubAck(id)

		case ExactlyOnce:
			resp = newPubReceived(id)

		case reservedQoS3:
			log.Print("mqtt: close on protocol violation: publish request with reserved QoS 3")
			c.conn.Close()
			return
		}

		err := c.Storage.Persist(id, message)
		if err != nil {
			log.Print("mqtt: reception persistence malfuncion: ", err)
			return
		}
		if err := c.write(resp.buf); err != nil {
			log.Print("mqtt: submission publish reception failed on fatal network error: ", err)
			return
		}
		return

	case pubReceived, pubRelease, pubComplete, pubAck, unsubAck:
		if len(p) != 2 {
			log.Print("mqtt: close on protocol violation: received packet type ", packetType, " with remaining length ", len(p))
			c.conn.Close()
			return
		}
		id := uint(binary.BigEndian.Uint16(p))

		if packetType == pubReceived {
			if err := c.Storage.Persist(id, nil); err != nil {
				log.Print("mqtt: reception persistence malfuncion: ", err)
				return
			}

			if err := c.write(newPubComplete(id).buf); err != nil {
				log.Print("mqtt: submission publish complete failed on fatal network error: ", err)
			}
		} else {
			c.Storage.Delete(id)
		}

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

	case connReq, subReq, unsubReq, ping, disconn:
		log.Print("mqtt: close on protocol violation: client received packet type ", packetType)

	case connAck:
		log.Print("mqtt: close on protocol violation: redunant connection acknowledge")

	default:
		log.Print("mqtt: close on protocol violation: received reserved packet type ", packetType)
	}

	return
}

// Connect initiates the transport layer (c.Conn).
func (c *Client) connect() error {
	conn, err := c.Connecter()
	if err != nil {
		return err
	}
	c.conn = conn

	c.conn.SetDeadline(time.Now().Add(c.WireTimeout))

	// launch handshake
	if err := c.write(newConnReq(&c.SessionConfig).buf); err != nil {
		c.conn.Close()
		return err
	}

	var buf [4]byte
	n, err := c.conn.Read(buf[:])
	if err != nil {
		c.conn.Close()
		return err
	}

	for {
		if n > 0 && buf[0] != connAck<<4 {
			c.conn.Close()
			return fmt.Errorf("mqtt: received packet type %#x on connect—connection closed", buf[0]>>4)
		}
		if n > 1 && buf[1] != 2 {
			c.conn.Close()
			return fmt.Errorf("mqtt: connect acknowledge remaining length is %d instead of 2—connection closed", buf[1])
		}
		if n > 2 && buf[2] > 1 {
			c.conn.Close()
			return fmt.Errorf("mqtt: received reserved connect acknowledge flags %#x—connection closed", buf[2])
		}
		if n > 3 {
			break
		}

		more, err := c.conn.Read(buf[n:])
		if err != nil {
			c.conn.Close()
			return err
		}
		n += more
	}

	if code := connectReturn(buf[3]); code != accepted {
		c.conn.Close()
		return code
	}

	c.conn.SetDeadline(time.Time{}) // clear

	go c.readLoop()

	return nil
}

// Publish persists the message (for network submission). Error returns other
// than ErrTopicName, ErrMessageSize and ErrRequestLimit signal fatal Storage
// malfunction. Thus the actual publication is decoupled from the invokation.
//
// Deliver AtMostOnce causes message to be send the server, and that'll be the
// end of operation. Subscribers may or may not receive the message when subject
// to error. Use AtLeastOnce or ExactlyOne for more protection, at the cost of
// higher (performance) overhead.
//
// Multiple goroutines may invoke Publish simultaneously.
func (c *Client) Publish(topic string, message []byte, deliver QoS) error {
	id, err := c.packetIDs.reserve()
	if err != nil {
		return err
	}

	packet := newPub(id, topic, message, deliver)

	return c.Storage.Persist(id|localPacketIDFlag, packet.buf)
}

// PublishRetained acts like Publish, but causes the message to be stored on the
// server, so that they can be delivered to future subscribers.
func (c *Client) PublishRetained(topic string, message []byte, deliver QoS) error {
	id, err := c.packetIDs.reserve()
	if err != nil {
		return err
	}

	packet := newPub(id, topic, message, deliver)
	packet.buf[0] |= retainFlag

	return c.Storage.Persist(id|localPacketIDFlag, packet.buf)
}

// Subscribe requests a subscription for all topics that match the filter.
// The requested quality of service is a maximum for the server.
func (c *Client) Subscribe(topicFilter string, max QoS) error {
	id, err := c.packetIDs.reserve()
	if err != nil {
		return err
	}

	if err := c.write(newSubReq(id, topicFilter, max).buf); err != nil {
		return err
	}

	panic("TODO: await ack")
}

// Unsubscribe requests a Subscribe cancelation.
func (c *Client) Unsubscribe(topicFilter string) error {
	id, err := c.packetIDs.reserve()
	if err != nil {
		return err
	}

	if err := c.write(newUnsubReq(id, topicFilter).buf); err != nil {
		return err
	}

	panic("TODO: await ack")
}

// Ping makes a roundtrip to validate the connection.
func (c *Client) Ping() error {
	return c.write(pingPacket)
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
