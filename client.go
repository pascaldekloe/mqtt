package mqtt

import (
	"bufio"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
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

	// Messages larger than InSizeLimit are not presented to Receive.
	// The protocol limit is 256 MiB.
	InSizeLimit int

	// Backoff on transport errors.
	RetryDelay time.Duration
}

// Client manages a single network connection.
type Client struct {
	ClientConfig // read-only

	packetIDs

	conn net.Conn

	outQ chan *packet // high-prority send queue
	keyQ chan uint    // storage send queue

	listener Receive

	closed chan struct{}
}

func NewClient(config *ClientConfig) *Client {
	c := &Client{
		ClientConfig: *config, // copy
		packetIDs: packetIDs{
			inUse: make(map[uint]struct{}),
			limit: config.RequestLimit,
		},
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

	go c.readRoutine()

	return c
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

		var ne net.Error
		if errors.As(err, &ne) && ne.Temporary() {
			c.conn.Close()
			return err
		}

		delay := c.RetryDelay
		log.Print("mqtt: write retry in ", delay, " on ", err)
		time.Sleep(delay)

		var more int
		more, err = c.conn.Write(p[n:])
		// handle error in current loop
		n += more
	}

	return nil
}

func (c *Client) readRoutine() {
	// determine only here whether closed
	defer close(c.closed)

	for {
		c.reconnect()
		r := bufio.NewReaderSize(c.conn, 2048)
		for {
			err := c.nextPacket(r)
			if err == nil {
				continue
			}
			if errors.Is(err, ErrClosed) {
				return
			}
			log.Print(err)
			break
		}
	}
}

// ErrProtoReset signals illegal reception from the server.
var errProtoReset = errors.New("mqtt: connection reset on protocol violation")

func (c *Client) nextPacket(r *bufio.Reader) error {
	firstByte, err := c.readByte(r)
	if err != nil {
		return err
	}
	l, err := c.readRemainingLength(r)
	if err != nil {
		return err
	}

	if l > c.InSizeLimit {
		// BUG(pascaldekloe):
		// “The Client MUST acknowledge any Publish Packet it receives
		// according to the applicable QoS rules regardless of whether
		// it elects to process the Application Message that it
		// contains [MQTT-4.5.0-2].”
		log.Printf("mqtt: skipping %d B inbound packet content; limit is %d B", l, c.InSizeLimit)
		_, err := r.Discard(l)
		return unexpectEOF(err)
	}

	p, didPeek, err := c.peekOrReadN(r, l)

	err = c.inbound(firstByte, p)
	if err != nil {
		return err
	}

	if didPeek {
		r.Discard(len(p)) // no errors guaranteed
	}
	return nil
}

func (c *Client) readRemainingLength(r *bufio.Reader) (int, error) {
	var l, shift uint
	for i := 0; i < 4; i++ {
		b, err := c.readByte(r)
		if err != nil {
			return 0, unexpectEOF(err)
		}
		l |= (b & 127) << shift
		if b < 0x80 {
			return int(l), nil
		}
		shift += 7
	}
	return 0, fmt.Errorf("%w: remaining length declaration exceeds 4 B", errProtoReset)
}

func (c *Client) readByte(r *bufio.Reader) (uint, error) {
	for {
		b, err := r.ReadByte()

		var ne net.Error
		switch {
		case err == nil:
			return uint(b), nil
		case errors.As(err, &ne) && ne.Temporary():
			delay := c.RetryDelay
			log.Print("mqtt: read retry in ", delay, " on ", err)
			time.Sleep(delay)
		default:
			return 0, err
		}
	}
}

func (c *Client) readN(r *bufio.Reader, n int) ([]byte, error) {
	p := make([]byte, n)
	i, err := r.Read(p)
	for i < n {
		var ne net.Error
		switch {
		case err == nil:
			break // incomplete read
		case errors.As(err, &ne) && ne.Temporary():
			delay := c.RetryDelay
			log.Print("mqtt: read retry in ", delay, " on ", err)
			time.Sleep(delay)
		default:
			return nil, err
		}

		var more int
		more, err = r.Read(p[i:])
		if err != nil && i != 0 {
			err = unexpectEOF(err)
		}
		i += more
	}
	return p, err
}

func (c *Client) peekOrReadN(r *bufio.Reader, n int) (p []byte, didPeek bool, err error) {
	for {
		p, err = r.Peek(n)

		var ne net.Error
		switch {
		case err == nil:
			didPeek = true
			return

		case errors.Is(err, bufio.ErrBufferFull):
			// n does not fit in read buffer
			p, err = c.readN(r, n)
			return

		case errors.As(err, &ne) && ne.Temporary():
			delay := c.RetryDelay
			log.Print("mqtt: read retry in ", delay, " on ", err)
			time.Sleep(delay)

		default:
			return
		}
	}
}

func unexpectEOF(err error) error {
	if errors.Is(err, io.EOF) {
		err = io.ErrUnexpectedEOF
	}
	return err
}

func (c *Client) inbound(firstByte uint, p []byte) error {
	switch packetType := firstByte >> 4; packetType {
	case pubMsg:
		// parse packet
		i := uint(p[0])<<8 | uint(p[1])
		topic := string(p[2:i])
		packetID := uint(p[i])<<8 | uint(p[i+1])
		message := p[i+2:]

		switch firstByte & 0b110 {
		case AtMostOnce << 1:
			c.listener(topic, message)

		case AtLeastOnce << 1:
			if c.listener(topic, message) {
				c.outQ <- newPubAck(packetID)
			}

		case ExactlyOnce << 1:
			bytes := make([]byte, len(topic)+1+len(message))
			copy(bytes, topic)
			copy(bytes[len(topic)+1:], message)
			err := c.Storage.Persist(packetID, bytes)
			if err != nil {
				log.Print("mqtt: persistence malfuncion: ", err)
				return nil // don't confirm
			}
			c.outQ <- newPubReceived(packetID)

		default:
			return fmt.Errorf("%w: received publish with reserved QoS", errProtoReset)
		}

	case pubRelease: // second round trip for ExactlyOnce reception
		if len(p) != 2 {
			return fmt.Errorf("%w: received publish release with remaining length %d", errProtoReset, len(p))
		}
		packetID := uint(binary.BigEndian.Uint16(p))
		bytes, err := c.Storage.Retreive(packetID)
		if err != nil {
			log.Print("mqtt: persistence malfuncion: ", err)
			return nil
		}
		if bytes != nil {
			for i, b := range bytes {
				if b == 0 {
					topic := string(bytes[:i])
					message := bytes[i+1:]
					if !c.listener(topic, message) {
						return nil // don't confirm; keep in storage
					}
					break
				}
			}
			c.Storage.Delete(packetID)
		}
		c.outQ <- newPubComplete(packetID)

	case pubReceived, pubComplete, pubAck, unsubAck:
		if len(p) != 2 {
			return fmt.Errorf("%w: received packet type %d with remaining length %d", errProtoReset, packetType, len(p))
		}
		id := uint(binary.BigEndian.Uint16(p))

		if packetType == pubReceived {
			if err := c.Storage.Persist(id, nil); err != nil {
				log.Print("mqtt: persistence malfuncion: ", err)
				return nil
			}

			c.outQ <- newPubComplete(id)
		} else {
			c.Storage.Delete(id)
		}

	case subAck:
		if len(p) != 3 {
			return fmt.Errorf("%w: received subscribe acknowledge with remaining length %d", errProtoReset, len(p))
		}

	case pong:
		if len(p) != 0 {
			return fmt.Errorf("%w: received ping request with remaining length %d", errProtoReset, len(p))
		}
		c.outQ <- pongPacket

	case connReq, subReq, unsubReq, ping, disconn:
		return fmt.Errorf("%w: received packet type %d", errProtoReset, packetType)

	case connAck:
		return fmt.Errorf("%w: received redundant connection acknowledge", errProtoReset)

	default:
		return fmt.Errorf("%w: received reserved packet type %d", errProtoReset, packetType)
	}

	return nil
}

// Reconnect initiates the transport layer (c.conn).
// The current connection (if any) is terminated.
func (c *Client) reconnect() error {
	if c.conn != nil {
		c.conn.Close()
	}

	conn, err := c.Connecter()
	if err != nil {
		return err
	}
	c.conn = conn

	// launch handshake
	if err := c.write(newConnReq(&c.SessionConfig).buf); err != nil {
		return err
	}

	var buf [4]byte
	n, err := c.conn.Read(buf[:])
	if err != nil {
		return err
	}

	for {
		if n > 0 && buf[0] != connAck<<4 {
			return fmt.Errorf("mqtt: received packet type %#x on connect—connection closed", buf[0]>>4)
		}
		if n > 1 && buf[1] != 2 {
			return fmt.Errorf("mqtt: connect acknowledge remaining length is %d instead of 2—connection closed", buf[1])
		}
		if n > 2 && buf[2] > 1 {
			return fmt.Errorf("mqtt: received reserved connect acknowledge flags %#x—connection closed", buf[2])
		}
		if n > 3 {
			break
		}

		more, err := c.conn.Read(buf[n:])
		if err != nil {
			return err
		}
		n += more
	}

	if code := connectReturn(buf[3]); code != accepted {
		return code
	}

	c.conn.SetDeadline(time.Time{}) // clear

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
	return c.publish(topic, message, deliver, false)
}

// PublishRetained acts like Publish, but causes the message to be stored on the
// server, so that they can be delivered to future subscribers.
func (c *Client) PublishRetained(topic string, message []byte, deliver QoS) error {
	return c.publish(topic, message, deliver, true)
}

func (c *Client) publish(topic string, message []byte, deliver QoS, retain bool) error {
	if deliver == AtMostOnce {
		packet := newPubMsg(topic, message)
		defer packetPool.Put(packet)
		if retain {
			packet.buf[0] |= retainFlag
		}
		return c.write(packet.buf)
	}

	id, err := c.packetIDs.reserve()
	if err != nil {
		return err
	}
	packet := newPubMsgWithID(id, topic, message, deliver)
	defer packetPool.Put(packet)
	if retain {
		packet.buf[0] |= retainFlag
	}

	key := id | localPacketIDFlag
	err = c.Storage.Persist(key, packet.buf)
	if err != nil {
		return err
	}
	c.keyQ <- key
	return nil
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
	c.outQ <- pingPacket

	panic("TODO: await ack")
}

// Disconnect is a graceful termination, which also discards the Will.
// The underlying connection is closed.
func (c *Client) Disconnect() error {
	_, err := c.conn.Write(disconnPacket.buf)

	closeErr := c.conn.Close()
	if err == nil {
		err = closeErr
	}

	return err
}
