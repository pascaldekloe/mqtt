package mqtt

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// ReadBufSize covers inbound packet reception. BigMessage still uses the buffer
// to parse everything up until the message payload, which makes a worst-case of
// 2 B size prefix + 64 KiB topic + 2 B packet identifier.
var readBufSize = 128 * 1024

// ErrDown signals no-service after a failed connect attempt.
// The error state will clear once a connect retry succeeds.
var ErrDown = errors.New("mqtt: connection unavailable")

// ErrClosed signals use after Close. The state is permanent.
// Further invocation will again result in the same error.
var ErrClosed = errors.New("mqtt: client closed")

// ErrBrokerTerm signals connection loss for unknown reasons.
var errBrokerTerm = fmt.Errorf("mqtt: broker closed the connection (%w)", io.EOF)

// ErrProtoReset signals illegal reception.
var errProtoReset = errors.New("mqtt: connection reset on protocol violation by the broker")

// “SUBSCRIBE, UNSUBSCRIBE, and PUBLISH (in cases where QoS > 0) Control Packets
// MUST contain a non-zero 16-bit Packet Identifier.”
// — MQTT Version 3.1.1, conformance statement MQTT-2.3.1-1
var errPacketIDZero = fmt.Errorf("%w: packet identifier zero", errProtoReset)

// A broker may send none of these packet types.
var (
	errRESERVED0      = fmt.Errorf("%w: reserved packet type 0 is forbidden", errProtoReset)
	errGotCONNECT     = fmt.Errorf("%w: inbound CONNECT packet", errProtoReset)
	errCONNACKTwo     = fmt.Errorf("%w: second CONNACK packet", errProtoReset)
	errGotSUBSCRIBE   = fmt.Errorf("%w: inbound SUBSCRIBE packet", errProtoReset)
	errGotUNSUBSCRIBE = fmt.Errorf("%w: inbound UNSUBSCRIBE packet", errProtoReset)
	errGotPINGREQ     = fmt.Errorf("%w: inbound PINGREQ packet", errProtoReset)
	errGotDISCONNECT  = fmt.Errorf("%w: inbound DISCONNECT packet", errProtoReset)
	errRESERVED15     = fmt.Errorf("%w: reserved packet type 15 is forbidden", errProtoReset)
)

// Dialer abstracts the transport layer establishment.
type Dialer func(ctx context.Context) (net.Conn, error)

// NewDialer provides plain network connections.
// See net.Dial for details on the network & address syntax.
func NewDialer(network, address string) Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		// minimize timer use; covered by WireTimeout
		dialer := net.Dialer{KeepAlive: -1}
		return dialer.DialContext(ctx, network, address)
	}
}

// NewTLSDialer provides secured network connections.
// See net.Dial for details on the network & address syntax.
func NewTLSDialer(network, address string, config *tls.Config) Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		dialer := tls.Dialer{
			// minimize timer use; covered by WireTimeout
			NetDialer: &net.Dialer{KeepAlive: -1},
			Config:    config,
		}
		return dialer.DialContext(ctx, network, address)
	}
}

// Config is a Client configuration. Dialer and Store are the only required
// fields, although the WireTimeout comes highly recommended.
type Config struct {
	Dialer // chooses the broker
	Store  // persists the session

	// WireTimeout sets the minimim transfer rate as one byte per duration.
	// Zero disables timeout protection, which leaves the Client vulnerable
	// to blocking on stale connections.
	//
	// Any pauses during MQTT packet submission that exceed the timeout will
	// be treated as fatal to the connection, if detected in time. Expiry
	// causes automated reconnects just like any other fatal network error.
	// Operations which got interupted by WireTimeout receive a net.Error
	// with Timeout true.
	WireTimeout time.Duration

	// The maximum number of transactions at a time. Excess is denied with
	// ErrMax. Zero effectively disables the respective quality-of-service
	// level.
	AtLeastOnceMax, ExactlyOnceMax int

	// The user name may be used by the broker for authentication and/or
	// authorization purposes. An empty string omits the option, except
	// for when password is not nil.
	UserName string
	Password []byte // option omitted when nil

	// The Will Message is published when the connection terminates
	// without Disconnect. A nil Message disables the Will option.
	Will struct {
		Topic   string // destination
		Message []byte // payload

		Retain      bool // see PublishRetained
		AtLeastOnce bool // see PublishAtLeastOnce
		ExactlyOnce bool // overrides AtLeastOnce
	}

	KeepAlive uint16 // timeout in seconds (disabled with zero)

	// Brokers must resume communications with the client (identified by
	// ClientID) when CleanSession is false. Otherwise, brokers must create
	// a new session when either CleanSession is true or when no session is
	// associated to the client identifier.
	CleanSession bool
}

// NewCONNREQ returns a new packet.
func (c *Config) newCONNREQ() ([]byte, error) {
	clientID, err := c.Load(clientIDKey)
	if err != nil {
		return nil, err
	}
	err = stringCheck(string(clientID))
	if err != nil {
		return nil, fmt.Errorf("mqtt: illegal client identifier: %w", err)
	}

	size := 12 + len(clientID)
	var flags uint

	if err := stringCheck(c.UserName); err != nil {
		return nil, fmt.Errorf("mqtt: illegal user name: %w", err)
	}
	// Supply an empty user name when the password is set to comply with “If
	// the User Name Flag is set to 0, the Password Flag MUST be set to 0.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.1.2-22
	if c.UserName != "" || c.Password != nil {
		size += 2 + len(c.UserName)
		flags |= 1 << 7
	}

	if len(c.Password) > stringMax {
		return nil, fmt.Errorf("mqtt: password exceeds %d bytes", stringMax)
	}
	if c.Password != nil {
		size += 2 + len(c.Password)
		flags |= 1 << 6
	}

	if c.Will.Message != nil {
		if err := stringCheck(c.Will.Topic); err != nil {
			return nil, fmt.Errorf("mqtt: illegal will topic: %w", err)
		}
		if len(c.Will.Message) > stringMax {
			return nil, fmt.Errorf("mqtt: will message exceeds %d bytes", stringMax)
		}
		size += 4 + len(c.Will.Topic) + len(c.Will.Message)
		if c.Will.Retain {
			flags |= 1 << 5
		}
		switch {
		case c.Will.ExactlyOnce:
			flags |= exactlyOnceLevel << 3
		case c.Will.AtLeastOnce:
			flags |= atLeastOnceLevel << 3
		}
		flags |= 1 << 2
	}

	if c.CleanSession {
		flags |= 1 << 1
	}

	// encode packet
	packet := make([]byte, 0, size+2)
	packet = append(packet, typeCONNECT<<4)
	l := uint(size)
	for ; l > 0x7f; l >>= 7 {
		packet = append(packet, byte(l|0x80))
	}
	packet = append(packet, byte(l),
		0, 4, 'M', 'Q', 'T', 'T', 4, byte(flags),
		byte(c.KeepAlive>>8), byte(c.KeepAlive),
		byte(len(clientID)>>8), byte(len(clientID)),
	)
	packet = append(packet, clientID...)
	if c.Will.Message != nil {
		packet = append(packet, byte(len(c.Will.Topic)>>8), byte(len(c.Will.Topic)))
		packet = append(packet, c.Will.Topic...)
		packet = append(packet, byte(len(c.Will.Message)>>8), byte(len(c.Will.Message)))
		packet = append(packet, c.Will.Message...)
	}
	if c.UserName != "" || c.Password != nil {
		packet = append(packet, byte(len(c.UserName)>>8), byte(len(c.UserName)))
		packet = append(packet, c.UserName...)
	}
	if c.Password != nil {
		packet = append(packet, byte(len(c.Password)>>8), byte(len(c.Password)))
		packet = append(packet, c.Password...)
	}
	return packet, nil
}

// Client manages a network connection. A single goroutine must invoke
// ReadSlices consecutively until ErrClosed. Some backoff on error reception
// comes recommended though.
//
// Multiple goroutines may invoke methods on a Conn simultaneously, except for
// ReadSlices.
type Client struct {
	Config // read-only

	// The read routine controls the connection, including reconnects.
	readConn net.Conn
	r        *bufio.Reader // conn buffered
	peek     []byte        // pending slice from bufio.Reader

	// The semaphore locks connection control. A nil entry implies no
	// successful connect yet.
	connSem chan net.Conn

	// The context is fed to Dialer for fast aborts during a Close.
	dialCtx    context.Context
	dialCancel context.CancelFunc

	// Write operations have four states:
	// * pending (re)connect with a .writeBlock entry
	// * connected with a non nil .connSem entry
	// * ErrDown (after a failed connect) with a nil .connSem entry
	// * ErrClosed with writeSem closed

	// The semaphore allows for ordered output, as required by the protocol.
	writeSem chan net.Conn

	// Writes signal the block channel on fatal errors, leaving writeSem
	// empty/locked. The connection must be closed (if it wasn't already).
	writeBlock chan struct{}

	// The semaphore allows for one ping request at a time.
	pingAck chan chan<- error

	// The semaphores lock the respective acknowledge queues with a
	// submission counter. Overflows are acceptable.
	atLeastOnceSem, exactlyOnceSem chan uint

	// Outbout PUBLISH acknowledgement is traced by a callback channel.
	ackQ, recQ, compQ chan chan<- error

	orderedTxs
	unorderedTxs

	// The read routine uses this reusable buffer for packet submission.
	pendingAck [4]byte
	// The read routine parks reception beyond readBufSize.
	bigMessage *BigMessage
}

// NewClient returns a new Client. Configuration errors result in IsDeny on
// ReadSlices.
func NewClient(config *Config) *Client {
	if config.Dialer == nil {
		panic("nil Dialer")
	}
	if config.Store == nil {
		panic("nil Store")
	}
	// need 1 packet identifier free to determine the first and last entry
	if config.AtLeastOnceMax < 0 || config.AtLeastOnceMax > publishIDMask {
		config.AtLeastOnceMax = publishIDMask
	}
	if config.ExactlyOnceMax < 0 || config.ExactlyOnceMax > publishIDMask {
		config.ExactlyOnceMax = publishIDMask
	}

	c := &Client{
		Config:         *config, // copy
		connSem:        make(chan net.Conn, 1),
		writeSem:       make(chan net.Conn, 1),
		writeBlock:     make(chan struct{}, 1),
		pingAck:        make(chan chan<- error, 1),
		atLeastOnceSem: make(chan uint, 1),
		exactlyOnceSem: make(chan uint, 1),
		ackQ:           make(chan chan<- error, config.AtLeastOnceMax),
		recQ:           make(chan chan<- error, config.ExactlyOnceMax),
		compQ:          make(chan chan<- error, config.ExactlyOnceMax),
		unorderedTxs: unorderedTxs{
			perPacketID: make(map[uint16]unorderedCallback),
		},
	}
	c.connSem <- nil
	c.dialCtx, c.dialCancel = context.WithCancel(context.Background())
	c.writeBlock <- struct{}{}
	c.atLeastOnceSem <- 0
	c.exactlyOnceSem <- 0
	return c
}

// TermConn hijacks connection access. Further connect, write and writeAll
// requests are denied with ErrClosed, regardless of the error return.
func (c *Client) termConn(quit <-chan struct{}) (net.Conn, error) {
	// terminate connection control
	c.dialCancel()
	conn, ok := <-c.connSem
	if !ok {
		return nil, ErrClosed
	}
	close(c.connSem)

	// terminate write operations
	defer close(c.writeSem)
	select {
	case <-c.writeBlock:
		return nil, ErrDown

	case conn := <-c.writeSem:
		if conn == nil {
			return nil, ErrDown
		}
		return conn, nil

	case <-quit:
		if conn != nil {
			conn.Close()
		}
		// connection closed now; writes won't block
		select {
		case <-c.writeSem:
		case <-c.writeBlock:
		}
		return nil, ErrCanceled
	}
}

// Close terminates the connection establishment.
// The Client is closed regardless of the error return.
// Closing an already closed Client has no effect.
func (c *Client) Close() error {
	quit := make(chan struct{})
	close(quit) // no waiting
	conn, err := c.termConn(quit)
	switch err {
	case nil:
		err = conn.Close()
	case ErrCanceled, ErrDown:
		err = nil
	case ErrClosed:
		return nil
	}
	c.termCallbacks()
	return err
}

// Disconnect tries a graceful termination, which discards the Will.
// The Client is closed regardless of the error return.
//
// Quit is optional, as nil just blocks. Appliance of quit will strictly result
// in ErrCanceled.
//
// BUG(pascaldekloe): The MQTT protocol has no confirmation for the
// disconnect request. As a result, a client can never know for sure
// whether the operation actually succeeded.
func (c *Client) Disconnect(quit <-chan struct{}) error {
	conn, err := c.termConn(quit)
	if err == ErrClosed {
		return ErrClosed
	}
	c.termCallbacks()
	if err != nil {
		return fmt.Errorf("mqtt: DISCONNECT not send: %w", err)
	}

	// “After sending a DISCONNECT Packet the Client MUST NOT send
	// any more Control Packets on that Network Connection.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.14.4-2
	writeErr := write(conn, packetDISCONNECT, c.WireTimeout)
	closeErr := conn.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func (c *Client) termCallbacks() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-c.atLeastOnceSem      // lock
		close(c.atLeastOnceSem) // terminate
		// flush queue
		close(c.ackQ)
		for ch := range c.ackQ {
			select {
			case ch <- ErrClosed:
			default: // won't block
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-c.exactlyOnceSem      // lock
		close(c.exactlyOnceSem) // terminate
		// flush queues
		close(c.recQ)
		for ch := range c.recQ {
			select {
			case ch <- ErrClosed:
			default: // won't block
			}
		}
		close(c.compQ)
		for ch := range c.compQ {
			select {
			case ch <- ErrClosed:
			default: // won't block
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.unorderedTxs.close()
	}()

	select {
	case ack := <-c.pingAck:
		ack <- ErrClosed
	default:
		break
	}
	wg.Wait()
}

func (c Client) lockWrite(quit <-chan struct{}) (net.Conn, error) {
	select {
	case <-quit:
		return nil, ErrCanceled
	case conn, ok := <-c.writeSem: // locks writes
		if !ok {
			return nil, ErrClosed
		}
		if conn == nil {
			c.writeSem <- nil // unlocks writes
			return nil, ErrDown
		}
		return conn, nil
	}
}

// Write submits the packet. Keep synchronised with writeAll!
func (c *Client) write(quit <-chan struct{}, p []byte) error {
	for {
		conn, err := c.lockWrite(quit)
		if err != nil {
			return err
		}

		switch err := write(conn, p, c.WireTimeout); {
		case err == nil:
			c.writeSem <- conn // unlocks writes
			return nil

		case errors.Is(err, net.ErrClosed), errors.Is(err, io.ErrClosedPipe):
			// got interrupted; read routine will determine next course
			c.writeBlock <- struct{}{} // parks writes

		default:
			conn.Close()               // interrupts read routine
			c.writeBlock <- struct{}{} // parks writes
			return err
		}
	}
}

// WriteAll submits the packet. Keep synchronised with write!
func (c *Client) writeAll(quit <-chan struct{}, p net.Buffers) error {
	for {
		conn, err := c.lockWrite(quit)
		if err != nil {
			return err
		}

		switch err := writeAll(conn, p, c.WireTimeout); {
		case err == nil:
			c.writeSem <- conn // unlocks writes
			return nil

		case errors.Is(err, net.ErrClosed), errors.Is(err, io.ErrClosedPipe):
			// got interrupted; read routine will determine next course
			c.writeBlock <- struct{}{} // parks writes

		default:
			conn.Close()               // interrupts read routine
			c.writeBlock <- struct{}{} // parks writes
			return err
		}
	}
}

// Write submits the packet. Keep synchronised with writeAll!
func write(conn net.Conn, p []byte, idleTimeout time.Duration) error {
	if idleTimeout != 0 {
		// Abandon timer to prevent waking up the system for no good reason.
		// https://developer.apple.com/library/archive/documentation/Performance/Conceptual/EnergyGuide-iOS/MinimizeTimerUse.html
		defer conn.SetWriteDeadline(time.Time{})
	}

	for {
		if idleTimeout != 0 {
			err := conn.SetWriteDeadline(time.Now().Add(idleTimeout))
			if err != nil {
				return err // deemed critical
			}
		}
		n, err := conn.Write(p)
		if err == nil { // OK
			return nil
		}
		// Allow deadline expiry if at least one byte was transferred.
		var ne net.Error
		if n == 0 || !errors.As(err, &ne) || !ne.Timeout() {
			return err
		}

		p = p[n:] // continue with remaining
	}
}

// WriteAll submits the packet. Keep synchronised with write!
func writeAll(conn net.Conn, p net.Buffers, idleTimeout time.Duration) error {
	if idleTimeout != 0 {
		// Abandon timer to prevent waking up the system for no good reason.
		// https://developer.apple.com/library/archive/documentation/Performance/Conceptual/EnergyGuide-iOS/MinimizeTimerUse.html
		defer conn.SetWriteDeadline(time.Time{})
	}

	for {
		if idleTimeout != 0 {
			err := conn.SetWriteDeadline(time.Now().Add(idleTimeout))
			if err != nil {
				return err // deemed critical
			}
		}
		n, err := p.WriteTo(conn)
		if err == nil { // OK
			return nil
		}
		// Allow deadline expiry if at least one byte was transferred.
		var ne net.Error
		if n == 0 || !errors.As(err, &ne) || !ne.Timeout() {
			return err
		}

		// Don't modify the original buffers.
		var remaining net.Buffers
		offset := int(n) // size limited by packetMax
		for i, buf := range p {
			if len(buf) > offset {
				remaining = append(remaining, buf[offset:])
				remaining = append(remaining, p[i+1:]...)
				break
			}
			offset -= len(buf)
		}
		p = remaining
	}
}

// PeekPacket slices a packet payload from the read buffer into c.peek.
func (c *Client) peekPacket() (head byte, err error) {
	head, err = c.r.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = errBrokerTerm
		}
		return 0, err
	}

	if c.WireTimeout != 0 {
		// Abandon timer to prevent waking up the system for no good reason.
		// https://developer.apple.com/library/archive/documentation/Performance/Conceptual/EnergyGuide-iOS/MinimizeTimerUse.html
		defer c.readConn.SetReadDeadline(time.Time{})
	}

	// decode “remaining length”
	var size int
	for shift := uint(0); ; shift += 7 {
		if c.r.Buffered() == 0 && c.WireTimeout != 0 {
			err := c.readConn.SetReadDeadline(time.Now().Add(c.WireTimeout))
			if err != nil {
				return 0, err // deemed critical
			}
		}
		b, err := c.r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return 0, fmt.Errorf("mqtt: header from packet %#b incomplete: %w", head, err)
		}
		size |= int(b&0x7f) << shift
		if b&0x80 == 0 {
			break
		}
		if shift > 21 {
			return 0, fmt.Errorf("%w: remaining length encoding from packet %#b exceeds 4 bytes", errProtoReset, head)
		}
	}

	// slice payload form read buffer
	for {
		if c.r.Buffered() < size {
			err := c.readConn.SetReadDeadline(time.Now().Add(c.WireTimeout))
			if err != nil {
				return 0, err // deemed critical
			}
		}

		lastN := len(c.peek)
		c.peek, err = c.r.Peek(size)
		switch {
		case err == nil: // OK
			return head, err
		case head>>4 == typePUBLISH && errors.Is(err, bufio.ErrBufferFull):
			return head, &BigMessage{Client: c, Size: size}
		}

		// Allow deadline expiry if at least one byte was transferred.
		var ne net.Error
		if len(c.peek) > lastN && errors.As(err, &ne) && ne.Timeout() {
			continue
		}

		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, fmt.Errorf("mqtt: got %d out of %d bytes from packet %#b: %w", len(c.peek), size, head, err)
	}
}

// Connect installs the transport layer. The current
// connection must be closed in case of a reconnect.
func (c *Client) connect() error {
	packet, err := c.newCONNREQ()
	if err != nil {
		return err
	}

	// The semaphores wont block with a closed connection.
	oldConn, ok := <-c.connSem // locks connection control
	if !ok {
		return ErrClosed
	}
	select { // locks write
	case <-c.writeSem:
	case <-c.writeBlock:
	}

	// Reconnects shouldn't reset the session.
	if oldConn != nil && c.CleanSession {
		c.CleanSession = false
	}
	conn, err := c.Dialer(c.dialCtx)
	if err != nil {
		c.connSem <- oldConn // unlock for next attempt
		c.writeSem <- nil    // causes ErrDown
		// FIXME(pascaldekloe): Error string matching is supported
		// according to <https://github.com/golang/go/issues/36208>.
		if strings.Contains(err.Error(), "operation was canceled") {
			return ErrClosed
		}
		return err
	}
	// “After a Network Connection is established by a Client to a Server,
	// the first Packet sent from the Client to the Server MUST be a CONNECT
	// Packet.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.1.0-1

	c.connSem <- conn // release early for interruption by Close

	r, err := c.handshake(conn, packet)
	if err != nil {
		conn.Close()      // abandon
		c.writeSem <- nil // causes ErrDown
		return err
	}

	// install
	c.writeSem <- conn
	c.readConn = conn
	c.r = r
	c.peek = nil // applied to prevous r if any
	return nil
}

func (c *Client) handshake(conn net.Conn, requestPacket []byte) (*bufio.Reader, error) {
	err := write(conn, requestPacket, c.WireTimeout)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReaderSize(conn, readBufSize)

	// Apply the deadline to the "entire" 4-byte response.
	if c.WireTimeout != 0 {
		err := conn.SetReadDeadline(time.Now().Add(c.WireTimeout))
		if err != nil {
			return nil, err // deemed critical
		}
		defer conn.SetReadDeadline(time.Time{})
	}

	// “The first packet sent from the Server to the Client MUST be a
	// CONNACK Packet.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.2.0-1
	packet, err := r.Peek(4)
	switch {
	case len(packet) > 1 && (packet[0] != typeCONNACK<<4 || packet[1] != 2):
		return nil, fmt.Errorf("%w: want fixed CONNACK header 0x2002, got %#x", errProtoReset, packet)
	case c.dialCtx.Err() != nil:
		return nil, ErrClosed
	case len(packet) < 4:
		return nil, fmt.Errorf("mqtt: incomplete CONNACK %#x: %w", packet, err)
	case packet[3] != byte(accepted):
		return nil, connectReturn(packet[3])
	}
	r.Discard(len(packet)) // no errors guaranteed
	if errors.Is(err, io.EOF) {
		err = errBrokerTerm
	}
	return r, err
}

// ReadSlices should be invoked consecutively from a single goroutine until
// ErrClosed. An IsDeny implies permantent Config rejection.
//
// Both message and topic are slices from a read buffer. The bytes stop being
// valid at the next read.
//
// Each invocation acknowledges ownership of the previously returned if any.
// Alternatively, use either Disconnect or Close to prevent a confirmation from
// being send.
//
// BigMessage leaves the memory allocation choice to the consumer. Any other
// error puts the Client in an ErrDown state. Invocation should apply a backoff
// once down. Retries on IsConnectionRefused, if any, should probably apply a
// rather large backoff. See the NewClient example for a complete setup.
func (c *Client) ReadSlices() (message, topic []byte, err error) {
	// A pending BigMessage implies that the connection was functional on
	// the last return.
	switch {
	case c.bigMessage != nil:
		_, err = c.r.Discard(c.bigMessage.Size)
		if err != nil {
			c.block()
			return nil, nil, err
		}

	case c.readConn == nil:
		if err := c.connect(); err != nil {
			return nil, nil, err
		}

	default:
		// skip previous packet, if any
		c.r.Discard(len(c.peek)) // no errors guaranteed
		c.peek = nil             // flush
	}

	// acknowledge previous packet, if any
	if c.pendingAck[0] != 0 {
		if c.pendingAck[0]&0xf0 == typePUBREC<<4 {
			// BUG(pascaldekloe): Save errors from a Store may cause
			// duplicate reception for deliveries with the "exactly
			// once guarantee", if the respective Client goes down
			// before a recovery/retry succeeds.
			key := uint(binary.BigEndian.Uint16(c.pendingAck[2:4])) | remoteIDKeyFlag
			err = c.Store.Save(key, net.Buffers{c.pendingAck[:]})
			if err != nil {
				return nil, nil, err
			}
		}
		err := c.write(nil, c.pendingAck[:])
		if err != nil {
			return nil, nil, err
		}
		c.pendingAck[0], c.pendingAck[1], c.pendingAck[2], c.pendingAck[3] = 0, 0, 0, 0
	}

	// process packets until a PUBLISH appears
	for {
		head, err := c.peekPacket()
		switch {
		case err == nil:
			break

		case errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe):
			// got interrupted
			if err := c.connect(); err != nil {
				c.readConn = nil
				return nil, nil, err
			}

			continue // with new connection

		case errors.As(err, &c.bigMessage):
			if head>>4 == typePUBLISH {
				message, topic, err = c.onPUBLISH(head)
				// TODO(pascaldekloe): errDupe
				if err != nil {
					// If the packet is malformed then
					// BigMessage is not the issue anymore.
					c.bigMessage = nil
					c.block()
					return nil, nil, err
				}
				c.bigMessage.Topic = string(topic) // copy
				done := readBufSize - len(message)
				c.bigMessage.Size -= done
				c.r.Discard(done) // no errors guaranteed
			}
			c.peek = nil
			return nil, nil, c.bigMessage

		default:
			c.block()
			return nil, nil, err
		}

		switch head >> 4 {
		case typeRESERVED0:
			err = errRESERVED0
		case typeCONNECT:
			err = errGotCONNECT
		case typeCONNACK:
			err = errCONNACKTwo
		case typePUBLISH:
			message, topic, err = c.onPUBLISH(head)
			if err == nil {
				return message, topic, nil
			}
			if err == errDupe {
				err = nil // just skip
			}
		case typePUBACK:
			err = c.onPUBACK()
		case typePUBREC:
			err = c.onPUBREC()
		case typePUBREL:
			err = c.onPUBREL()
		case typePUBCOMP:
			err = c.onPUBCOMP()
		case typeSUBSCRIBE:
			err = errGotSUBSCRIBE
		case typeSUBACK:
			err = c.onSUBACK()
		case typeUNSUBSCRIBE:
			err = errGotUNSUBSCRIBE
		case typeUNSUBACK:
			err = c.onUNSUBACK()
		case typePINGREQ:
			err = errGotPINGREQ
		case typePINGRESP:
			err = c.onPINGRESP()
		case typeDISCONNECT:
			err = errGotDISCONNECT
		case typeRESERVED15:
			err = errRESERVED15
		}
		if err != nil {
			c.block()
			return nil, nil, err
		}

		// no errors guaranteed
		c.r.Discard(len(c.peek))
	}
}

func (c *Client) block() {
	select {
	case conn := <-c.writeSem:
		if conn != nil {
			conn.Close()
		}
	case <-c.writeBlock:
		// A write detected the problem in the mean time.
		// The signal implies that the connection was closed.
		break
	}
	c.writeBlock <- struct{}{}
	c.readConn = nil
}

// BigMessage signals reception beyond the read buffer capacity.
// Receivers may or may not allocate the memory with ReadAll.
// The next ReadSlices will acknowledge reception either way.
type BigMessage struct {
	*Client        // source
	Topic   string // destinition
	Size    int    // byte count
}

// Error implements the standard error interface.
func (e *BigMessage) Error() string {
	return fmt.Sprintf("mqtt: %d B message exceeds read buffer capacity", e.Size)
}

// ReadAll returns the message in a new/dedicated buffer. Messages can be read
// only once, after reception (from ReadSlices), and before the next ReadSlices.
// The invocation must occur from within the same routine.
func (e *BigMessage) ReadAll() ([]byte, error) {
	if e.bigMessage != e {
		return nil, errors.New("mqtt: read window expired for a big message")
	}
	e.bigMessage = nil

	message := make([]byte, e.Size)
	_, err := io.ReadFull(e.Client.r, message)
	if err != nil {
		e.Client.block()
		return nil, err
	}
	return message, nil
}

var errDupe = errors.New("mqtt: duplicate reception")

// OnPUBLISH slices an inbound message from Client.peek.
func (c *Client) onPUBLISH(head byte) (message, topic []byte, err error) {
	if len(c.peek) < 2 {
		return nil, nil, fmt.Errorf("%w: PUBLISH with %d byte remaining length", errProtoReset, len(c.peek))
	}
	i := int(uint(binary.BigEndian.Uint16(c.peek))) + 2
	if i > len(c.peek) {
		return nil, nil, fmt.Errorf("%w: PUBLISH topic exceeds remaining length", errProtoReset)
	}
	topic = c.peek[2:i]

	switch head & 0b0110 {
	case atMostOnceLevel << 1:
		break

	case atLeastOnceLevel << 1:
		if len(c.peek) < i+2 {
			return nil, nil, fmt.Errorf("%w: PUBLISH packet identifier exceeds remaining length", errProtoReset)
		}
		packetID := binary.BigEndian.Uint16(c.peek[i:])
		if packetID == 0 {
			return nil, nil, errPacketIDZero
		}
		i += 2

		// enqueue for next call
		c.pendingAck[0], c.pendingAck[1] = typePUBACK<<4, 2
		c.pendingAck[2], c.pendingAck[3] = byte(packetID>>8), byte(packetID)

	case exactlyOnceLevel << 1:
		if len(c.peek) < i+2 {
			return nil, nil, fmt.Errorf("%w: PUBLISH packet identifier exceeds remaining length", errProtoReset)
		}
		packetID := uint(binary.BigEndian.Uint16(c.peek[i:]))
		if packetID == 0 {
			return nil, nil, errPacketIDZero
		}
		i += 2

		bytes, err := c.Store.Load(packetID | remoteIDKeyFlag)
		if err != nil {
			return nil, nil, err
		}
		if bytes != nil {
			return nil, nil, errDupe
		}

		// enqueue for next call
		c.pendingAck[0], c.pendingAck[1] = typePUBREC<<4, 2
		c.pendingAck[2], c.pendingAck[3] = byte(packetID>>8), byte(packetID)

	default:
		return nil, nil, fmt.Errorf("%w: PUBLISH with reserved quality-of-service level 3", errProtoReset)
	}

	return c.peek[i:], topic, nil
}

// OnPUBREL applies the second round-trip for “exactly-once” reception.
func (c *Client) onPUBREL() error {
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBREL with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := uint(binary.BigEndian.Uint16(c.peek))
	if packetID == 0 {
		return errPacketIDZero
	}

	c.pendingAck[0], c.pendingAck[1] = typePUBCOMP<<4, 2
	c.pendingAck[2], c.pendingAck[3] = byte(packetID>>8), byte(packetID)
	err := c.Store.Save(packetID|remoteIDKeyFlag, net.Buffers{c.pendingAck[:4]})
	if err != nil {
		c.pendingAck[0], c.pendingAck[1], c.pendingAck[2], c.pendingAck[3] = 0, 0, 0, 0
		return err // causes resubmission of PUBREL
	}
	err = c.write(nil, c.pendingAck[:4])
	if err != nil {
		return err // causes resubmission of PUBCOMP
	}
	c.pendingAck[0], c.pendingAck[1], c.pendingAck[2], c.pendingAck[3] = 0, 0, 0, 0
	return nil
}
