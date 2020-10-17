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
	Receive     // inbound destination
	Connecter   // remote link
	Persistence // session safeguard

	SessionConfig

	// Messages larger than InSizeLimit are not presented to Receive.
	// The protocol limit is 256 MiB.
	InSizeLimit int

	// Backoff on transport errors.
	RetryDelay time.Duration
}

// Client manages a single network connection.
type Client struct {
	ClientConfig // read-only

	conn net.Conn

	outQ chan *packet // high-prority send queue
	keyQ chan uint    // storage send queue

	closed chan struct{}

	// Semaphore allows for one ping request at a time.
	pingAck chan chan<- struct{}

	// Semaphore allows for one subscribe or unsubscribe request at a time.
	subscriptionAck chan chan<- byte

	atLeastOnceLine
	exactlyOnceLine
}

func NewClient(config *ClientConfig) *Client {
	c := &Client{
		ClientConfig:    *config, // copy
		pingAck:         make(chan chan<- struct{}, 1),
		subscriptionAck: make(chan chan<- byte, 1),
		closed:          make(chan struct{}),
	}

	if c.Will != nil {
		willCopy := *c.Will
		c.Will = &willCopy
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

		p = p[n:]
		n, err = c.conn.Write(p)
		// handle error in current loop
	}

	return nil
}

func (c *Client) writeBuffers(buffers net.Buffers) error {
	n, err := buffers.WriteTo(c.conn)
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

		var todo net.Buffers
		// don't modify original buffers; it may be used by Store
		for i, bytes := range buffers {
			if n >= int64(len(bytes)) {
				n -= int64(len(bytes))
				continue
			}
			todo = append(todo, bytes[n:])
			todo = append(todo, buffers[i+1:]...)
			break
		}
		buffers = todo

		delay := c.RetryDelay
		log.Print("mqtt: write retry in ", delay, " on ", err)
		time.Sleep(delay)

		n, err = buffers.WriteTo(c.conn)
		// handle error in current loop
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

var errPacketID = fmt.Errorf("%w: wrong packet identifier", errProtoReset)

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
			c.Receive(topic, message)

		case AtLeastOnce << 1:
			if c.Receive(topic, message) {
				p := packetPool.Get().(*packet)
				p.buf = append(p.buf[:0], pubAck<<4, 2, byte(packetID>>8), byte(packetID))
				c.outQ <- p
			}

		case ExactlyOnce << 1:
			bytes := make([]byte, len(topic)+1+len(message))
			copy(bytes, topic)
			copy(bytes[len(topic)+1:], message)
			err := c.Persistence.Store(packetID, net.Buffers{bytes})
			if err != nil {
				log.Print("mqtt: persistence malfuncion: ", err)
				return nil // don't confirm
			}

			p := packetPool.Get().(*packet)
			p.buf = append(p.buf[:0], pubReceived<<4, 2, byte(packetID>>8), byte(packetID))
			c.outQ <- p

		default:
			return fmt.Errorf("%w: received publish with reserved QoS", errProtoReset)
		}

	case pubRelease: // second round trip for ExactlyOnce reception
		if len(p) != 2 {
			return fmt.Errorf("%w: received publish release with remaining length %d", errProtoReset, len(p))
		}
		packetID := uint(binary.BigEndian.Uint16(p))
		bytes, err := c.Persistence.Load(packetID)
		if err != nil {
			log.Print("mqtt: persistence malfuncion: ", err)
			return nil
		}
		if bytes != nil {
			for i, b := range bytes {
				if b == 0 {
					topic := string(bytes[:i])
					message := bytes[i+1:]
					if !c.Receive(topic, message) {
						return nil // don't confirm; keep in storage
					}
					break
				}
			}
			c.Persistence.Delete(packetID)
		}

		p := packetPool.Get().(*packet)
		p.buf = append(p.buf[:0], pubComplete<<4, 2, byte(packetID>>8), byte(packetID))
		c.outQ <- p

	case pubAck: // confirm of Publish with AtLeastOnce
		if len(p) != 2 {
			return fmt.Errorf("%w: received publish ␆ with remaining length %d", errProtoReset, len(p))
		}
		packetID := uint(binary.BigEndian.Uint16(p))

		err := c.atLeastOnceLine.FreeID(packetID)
		if err != nil {
			return err
		}

		err = c.Persistence.Delete(packetID)
		if err != nil {
			return err
		}

	case pubReceived: // first confirm of Publish with ExactlyOnce
		if len(p) != 2 {
			return fmt.Errorf("%w: received publish received with remaining length %d", errProtoReset, len(p))
		}
		packetID := uint(binary.BigEndian.Uint16(p))

		err := c.exactlyOnceLine.ReleaseID(packetID)
		if err != nil {
			return err
		}

		p := packetPool.Get().(*packet)
		p.buf = append(p.buf[:0], pubRelease<<4, 2, byte(packetID>>8), byte(packetID))
		err = c.Persistence.Store(packetID, net.Buffers{p.buf})
		if err != nil {
			return err
		}
		c.outQ <- p

	case pubComplete: // second confirm of Publish with ExactlyOnce
		if len(p) != 2 {
			return fmt.Errorf("%w: received publish complete with remaining length %d", errProtoReset, len(p))
		}
		packetID := uint(binary.BigEndian.Uint16(p))

		err := c.exactlyOnceLine.FreeID(packetID)
		if err != nil {
			return err
		}
		err = c.Persistence.Delete(packetID)
		if err != nil {
			return err
		}

	case subAck:
		if len(p) < 3 {
			return fmt.Errorf("%w: subscribe ␆ with %d B remaining length", errProtoReset, len(p))
		}
		if packetID := binary.BigEndian.Uint16(p); packetID != subscriptionPacketID {
			return fmt.Errorf("%w: received subscribe ␆ %#04x, while one %#04x in use", errPacketID, packetID, subscriptionPacketID)
		}

		err := c.Persistence.Delete(subscriptionPacketID)
		if err != nil {
			return err
		}

		var ack chan<- byte
		select {
		case ack = <-c.subscriptionAck:
			defer close(ack)
		default:
			return nil // tolerate redundant ␆
		}
		for _, returnCode := range p[2:] {
			select {
			case ack <- returnCode:
				break // OK
			default:
				// This could happen on persistence failure when
				// a redundant ␆ is received before the unlock.
				return nil
			}
		}

	case unsubAck:
		if len(p) != 2 {
			return fmt.Errorf("%w: unsubscribe ␆ with %d B remaining length", errProtoReset, len(p))
		}
		if packetID := binary.BigEndian.Uint16(p); packetID != subscriptionPacketID {
			return fmt.Errorf("%w: received unsubscribe ␆ %#04x, while one %#04x in use", errPacketID, packetID, subscriptionPacketID)
		}

		err := c.Persistence.Delete(subscriptionPacketID)
		if err != nil {
			return err
		}

		var ack chan<- byte
		select {
		case ack = <-c.subscriptionAck:
			close(ack)
		default:
			break // tolerate redundant ␆
		}

	case pong:
		if len(p) != 0 {
			return fmt.Errorf("%w: got ping response with %d byte remaining length", errProtoReset, len(p))
		}
		select {
		case ack := <-c.pingAck:
			close(ack)
		default:
			break // tolerate unsolicited ping response
		}

	case connReq, subReq, unsubReq, ping, disconn:
		return fmt.Errorf("%w: received packet type %d", errProtoReset, packetType)

	case connAck:
		return fmt.Errorf("%w: received redundant connection ␆", errProtoReset)

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
	size := 6 // variable header

	var flags uint
	if c.UserName != "" {
		size += 2 + len(c.UserName)
		flags |= 1 << 7
	}
	if c.Password != nil {
		size += 2 + len(c.Password)
		flags |= 1 << 6
	}
	if w := c.Will; w != nil {
		size += 2 + len(w.Topic)
		size += 2 + len(w.Message)
		if w.Retain {
			flags |= 1 << 5
		}
		flags |= uint(w.Deliver) << 3
		flags |= 1 << 2
	}
	if c.CleanSession {
		flags |= 1 << 1
	}
	size += 2 + len(c.ClientID)

	p := packetPool.Get().(*packet)

	// compose header
	p.buf = append(p.buf[:0], connReq<<4)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf[:0], byte(size))

	p.buf = append(p.buf, 0, 4, 'M', 'Q', 'T', 'T', 4, byte(flags))

	// append payload
	p.addString(c.ClientID)
	if w := c.Will; w != nil {
		p.addString(w.Topic)
		p.addBytes(w.Message)
	}
	if c.UserName != "" {
		p.addString(c.UserName)
	}
	if c.Password != nil {
		p.addBytes(c.Password)
	}

	if err := c.write(p.buf); err != nil {
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
			return fmt.Errorf("mqtt: connect ␆ remaining length is %d instead of 2—connection closed", buf[1])
		}
		if n > 2 && buf[2] > 1 {
			return fmt.Errorf("mqtt: received reserved connect ␆ flags %#x—connection closed", buf[2])
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
// than ErrTopicName, ErrMessageSize and ErrRequestLimit signal fatal storage
// malfunction. Thus the actual publication is decoupled from the invokation.
//
// Deliver AtMostOnce causes message to be send the server, and that'll be the
// end of operation. Subscribers may or may not receive the message when subject
// to error. Use AtLeastOnce or ExactlyOne for more protection, at the cost of
// higher (performance) overhead.
//
// Multiple goroutines may invoke Publish simultaneously.
func (c *Client) Publish(topic string, message []byte, deliver QoS) error {
	return c.publish(topic, message, deliver, 0)
}

// PublishRetained acts like Publish, but causes the message to be stored on the
// server, so that they can be delivered to future subscribers.
func (c *Client) PublishRetained(topic string, message []byte, deliver QoS) error {
	return c.publish(topic, message, deliver, retainFlag)
}

func (c *Client) publish(topic string, message []byte, deliver QoS, flags byte) error {
	var packetID uint

	switch deliver {
	case AtMostOnce:
		p := packetPool.Get().(*packet)
		defer packetPool.Put(p)
		p.buf = append(p.buf[:0], pubMsg<<4|flags)

		size := 2 + len(topic) + len(message)
		for size > 127 {
			p.buf = append(p.buf, byte(size|128))
			size >>= 7
		}
		p.buf = append(p.buf, byte(size))

		p.addString(topic)
		p.buf = append(p.buf, message...)

		return c.write(p.buf)

	case AtLeastOnce:
		packetID = c.atLeastOnceLine.AssignID()

	case ExactlyOnce:
		packetID = c.exactlyOnceLine.AssignID()
	}

	p := packetPool.Get().(*packet)
	defer packetPool.Put(p)
	p.buf = append(p.buf[:0], pubMsg<<4|byte(deliver)<<1|flags)

	size := 4 + len(topic) + len(message)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf, byte(size))

	p.addString(topic)
	p.buf = append(p.buf, byte(packetID>>8), byte(packetID))

	buffers := net.Buffers{p.buf, message}

	key := packetID | localPacketIDFlag
	err := c.Persistence.Store(key, buffers)
	if err != nil {
		return err
	}
	c.keyQ <- key
	return nil
}

// Subscribe requests a subscription for all topics that match any of the filters.
// The requested quality of service is a maximum for the server.
func (c *Client) Subscribe(min, max QoS, topicFilters ...string) error {
	if len(topicFilters) == 0 {
		return nil
	}

	// measure & validate
	size := 2 + len(topicFilters)*3
	for _, s := range topicFilters {
		if err := stringCheck(s); err != nil {
			return err
		}
		size += len(s)
	}
	if size > packetMax {
		return errPacketMax
	}

	p := packetPool.Get().(*packet)
	defer packetPool.Put(p)
	if cap(p.buf) < size+5 {
		p.buf = make([]byte, 0, size)
	}

	p.buf = append(p.buf[:0], subReq<<4)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf, byte(size), 0, 1) // including subscriptionPacketID
	for _, s := range topicFilters {
		p.buf = append(p.buf, byte(len(s)>>8), byte(len(s)))
		p.buf = append(p.buf, s...)
		p.buf = append(p.buf, byte(max))
	}

	returnCodes := make(chan byte, len(topicFilters))
	c.subscriptionAck <- returnCodes // lock
	err := c.Persistence.Store(uint(subscriptionPacketID|localPacketIDFlag), net.Buffers{p.buf})
	if err != nil {
		<-c.subscriptionAck // unlock
		return err
	}
	if err := c.write(p.buf); err != nil {
		panic("TODO(pascaldekloe): Trigger reset")
	}

	var illegalCode bool
	var tooLows, failures []int
	var i int
	for code := range returnCodes {
		switch code {
		case AtMostOnce, AtLeastOnce, ExactlyOnce:
			if QoS(code) < min {
				tooLows = append(tooLows, i)
			}
		case 0x80:
			failures = append(failures, i)
		default:
			illegalCode = true
		}
		i++
	}
	if illegalCode {
		panic("TODO(pascaldekloe): Trigger reset")
	}
	if i > len(topicFilters) {
		return fmt.Errorf("mqtt: subscription ␆ got %d return codes for %d topic filters", i, len(topicFilters))
	}
	if len(failures) != 0 {
		return fmt.Errorf("mqtt: subscription ␆ got return code failure for topic filters %d", failures)
	}
	if len(tooLows) != 0 {
		return fmt.Errorf("mqtt: subscription ␆ QoS level lower than minimum for topic filters %d", tooLows)
	}
	return nil
}

// Unsubscribe requests a Subscribe cancelation.
func (c *Client) Unsubscribe(topicFilters ...string) error {
	if len(topicFilters) == 0 {
		return nil
	}

	// measure & validate
	size := 2 + len(topicFilters)*2
	for _, s := range topicFilters {
		if err := stringCheck(s); err != nil {
			return err
		}
		size += len(s)
	}
	if size > packetMax {
		return errPacketMax
	}

	p := packetPool.Get().(*packet)
	defer packetPool.Put(p)
	if cap(p.buf) < size+5 {
		p.buf = make([]byte, 0, size)
	}

	p.buf = append(p.buf[:0], unsubReq<<4)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf, byte(size), 0, 1) // including subscriptionPacketID
	for _, s := range topicFilters {
		p.buf = append(p.buf, byte(len(s)>>8), byte(len(s)))
		p.buf = append(p.buf, s...)
	}

	returnCodes := make(chan byte)
	c.subscriptionAck <- returnCodes // lock
	err := c.Persistence.Store(uint(subscriptionPacketID|localPacketIDFlag), net.Buffers{p.buf})
	if err != nil {
		<-c.subscriptionAck // unlock
		return err
	}
	if err := c.write(p.buf); err != nil {
		panic("TODO(pascaldekloe): Trigger reset")
	}

	for range returnCodes {
		panic("TODO(pascaldekloe): Trigger reset")
	}
	return nil
}

// Ping makes a roundtrip to validate the connection.
func (c *Client) Ping() error {
	ch := make(chan struct{})
	c.pingAck <- ch // lock

	if err := c.write(pingPacket.buf); err != nil {
		<-c.pingAck // unlock
		// TODO(pascaldekloe): Trigger reset
		return err
	}

	<-ch
	return nil
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
