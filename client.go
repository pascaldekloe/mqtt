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
// Further invocation will result again in an ErrClosed error.
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
		// minimize timer use; covered by PauseTimeout
		dialer := net.Dialer{KeepAlive: -1}
		return dialer.DialContext(ctx, network, address)
	}
}

// NewTLSDialer provides secured network connections.
// See net.Dial for details on the network & address syntax.
func NewTLSDialer(network, address string, config *tls.Config) Dialer {
	return func(ctx context.Context) (net.Conn, error) {
		dialer := tls.Dialer{
			// minimize timer use; covered by PauseTimeout
			NetDialer: &net.Dialer{KeepAlive: -1},
			Config:    config,
		}
		return dialer.DialContext(ctx, network, address)
	}
}

// Config is a Client configuration. Dialer is the only required field.
type Config struct {
	Dialer // chooses the broker

	// PauseTimeout sets the minimim transfer rate as one byte per duration.
	// Zero disables timeout protection entirely, which leaves the Client
	// vulnerable to blocking on stale connections.
	//
	// Any pauses during MQTT packet submission that exceed the timeout will
	// be treated as fatal to the connection, if they are detected in time.
	// Expiry causes automated reconnects just like any other fatal network
	// error. Operations which got interrupted by a PauseTimeout receive a
	// net.Error with Timeout true.
	PauseTimeout time.Duration

	// The maximum number of transactions at a time. Excess is denied with
	// ErrMax. Zero effectively disables the respective quality-of-service
	// level. Negative values default to the Client limit of 16,384. Higher
	// values are truncated silently.
	AtLeastOnceMax, ExactlyOnceMax int

	// The user name may be used by the broker for authentication and/or
	// authorization purposes. An empty string omits the option, except for
	// when password is not nil.
	UserName string
	Password []byte // option omitted when nil

	// The Will Message is published when the connection terminates without
	// Disconnect. A nil Message disables the Will option.
	Will struct {
		Topic   string // destination
		Message []byte // payload

		Retain      bool // see PublishRetained
		AtLeastOnce bool // see PublishAtLeastOnce
		ExactlyOnce bool // overrides AtLeastOnce
	}

	// KeepAlive sets the activity timeout in seconds, with zero for none.
	// The server must disconnect after no control-packet reception for one
	// and a half times the keep-alive duration. Use Ping when idle.
	KeepAlive uint16

	// Brokers must resume communications with the client (identified by
	// ClientID) when CleanSession is false. Otherwise, brokers must create
	// a new session when either CleanSession is true or when no session is
	// associated to the client identifier.
	CleanSession bool
}

func (c *Config) valid() error {
	if c.Dialer == nil {
		return errors.New("mqtt: no Dialer in Config")
	}
	if err := stringCheck(c.UserName); err != nil {
		return fmt.Errorf("mqtt: illegal user name: %w", err)
	}
	if len(c.Password) > stringMax {
		return fmt.Errorf("mqtt: illegal password: %w", errStringMax)
	}
	if len(c.Will.Message) > stringMax {
		return fmt.Errorf("mqtt: illegal will message: %w", errStringMax)
	}

	var err error
	if c.Will.Message != nil {
		err = topicCheck(c.Will.Topic)
	} else {
		err = stringCheck(c.Will.Topic)
	}
	if err != nil {
		return fmt.Errorf("mqtt: illegal will topic: %w", err)
	}

	return nil
}

// NewCONNREQ returns a new packet.
func (c *Config) newCONNREQ(clientID []byte) []byte {
	size := 12 + len(clientID)
	var flags uint

	// Supply an empty user name when the password is set to comply with “If
	// the User Name Flag is set to 0, the Password Flag MUST be set to 0.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.1.2-22
	if c.UserName != "" || c.Password != nil {
		size += 2 + len(c.UserName)
		flags |= 1 << 7
	}
	if c.Password != nil {
		size += 2 + len(c.Password)
		flags |= 1 << 6
	}

	if c.Will.Message != nil {
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
	return packet
}

// Client manages a network connection until Close or Disconnect. Clients always
// start in the Offline state. The (un)subscribe, publish and ping methods block
// until the first connect attempt (from ReadSlices) completes. When the connect
// attempt fails, then requests receive ErrDown until a retry succeeds. The same
// goes for the automatic reconnects on connection loss.
//
// A single goroutine must invoke ReadSlices consecutively until ErrClosed. Some
// backoff on error reception comes recommended though.
//
// Multiple goroutines may invoke methods on a Client simultaneously, except for
// ReadSlices.
type Client struct {
	Config // read-only

	persistence Persistence // tracks the session

	// Signal channels are closed once their respective state occurs.
	// Each read must restore or replace the signleton value.
	onlineSig, offlineSig chan chan struct{}

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

	atLeastOnce, exactlyOnce transfer

	orderedTxs
	unorderedTxs

	// The read routine sends its content on the next ReadSlices.
	pendingAck []byte

	// The read routine parks reception beyond readBufSize.
	bigMessage *BigMessage
}

// Transfer holds state of an outbound exchange-type.
type transfer struct {
	// The semaphore locks q with a submission counter.
	// Overflows are permitted.
	seqNoSem chan uint

	// Acknowledgement is traced by a callback channel.
	q chan chan<- error

	// Submissions signal the block channel on errors,
	// while leaving seqNoSem empty/locked.
	block chan holdup
}

func newClient(p Persistence, config *Config) *Client {
	// need 1 packet identifier free to determine the first and last entry
	if config.AtLeastOnceMax < 0 || config.AtLeastOnceMax > publishIDMask {
		config.AtLeastOnceMax = publishIDMask
	}
	if config.ExactlyOnceMax < 0 || config.ExactlyOnceMax > publishIDMask {
		config.ExactlyOnceMax = publishIDMask
	}

	c := &Client{
		Config:      *config, // copy
		persistence: p,
		onlineSig:   make(chan chan struct{}, 1),
		offlineSig:  make(chan chan struct{}, 1),
		connSem:     make(chan net.Conn, 1),
		writeSem:    make(chan net.Conn, 1),
		writeBlock:  make(chan struct{}, 1),
		pingAck:     make(chan chan<- error, 1),
		atLeastOnce: transfer{
			seqNoSem: make(chan uint, 1),
			q:        make(chan chan<- error, config.AtLeastOnceMax),
			block:    make(chan holdup, 1),
		},
		exactlyOnce: transfer{
			seqNoSem: make(chan uint, 1),
			q:        make(chan chan<- error, config.ExactlyOnceMax),
			block:    make(chan holdup, 1),
		},
		unorderedTxs: unorderedTxs{
			perPacketID: make(map[uint16]unorderedCallback),
		},
	}

	// start in offline state
	c.onlineSig <- make(chan struct{})
	released := make(chan struct{})
	close(released)
	c.offlineSig <- released

	c.connSem <- nil
	c.dialCtx, c.dialCancel = context.WithCancel(context.Background())
	c.writeBlock <- struct{}{}
	c.atLeastOnce.seqNoSem <- 0
	c.exactlyOnce.seqNoSem <- 0
	return c
}

// TermConn hijacks connection access. Further connect, write and writeBuffers
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
	case ErrCanceled, ErrDown, ErrClosed:
		err = nil
	}
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
	if err != nil {
		return fmt.Errorf("mqtt: DISCONNECT not send: %w", err)
	}

	// “After sending a DISCONNECT Packet the Client MUST NOT send
	// any more Control Packets on that Network Connection.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.14.4-2
	writeErr := write(conn, packetDISCONNECT, c.PauseTimeout)
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

		select {
		case _, ok := <-c.atLeastOnce.seqNoSem:
			if !ok { // already terminated
				return
			}
		case <-c.atLeastOnce.block:
		}
		close(c.atLeastOnce.seqNoSem) // terminate

		// flush queue
		err := fmt.Errorf("%w; PUBLISH not confirmed", ErrClosed)
		close(c.atLeastOnce.q)
		for ch := range c.atLeastOnce.q {
			select {
			case ch <- err:
			default: // won't block
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case _, ok := <-c.exactlyOnce.seqNoSem:
			if !ok { // already terminated
				return
			}
		case <-c.exactlyOnce.block:
		}
		close(c.exactlyOnce.seqNoSem) // terminate

		// flush queue
		err := fmt.Errorf("%w; PUBLISH not confirmed", ErrClosed)
		close(c.exactlyOnce.q)
		for ch := range c.exactlyOnce.q {
			select {
			case ch <- err:
			default: // won't block
			}
		}
	}()

	select {
	case ack, ok := <-c.pingAck:
		if ok {
			ack <- fmt.Errorf("%w; PING not confirmed", ErrBreak)
		}
	default:
		break
	}
	wg.Wait()

	c.unorderedTxs.breakAll()
}

// Online returns a chanel that's closed when the client has a connection.
func (c *Client) Online() <-chan struct{} {
	ch := <-c.onlineSig
	c.onlineSig <- ch
	return ch
}

// Offline returns a chanel that's closed when the client has no connection.
func (c *Client) Offline() <-chan struct{} {
	ch := <-c.offlineSig
	c.offlineSig <- ch
	return ch
}

func (c *Client) toOnline() {
	on := <-c.onlineSig
	select {
	case <-on:
		break // released already
	default:
		close(on)
	}
	c.onlineSig <- on

	off := <-c.offlineSig
	select {
	case <-off:
		c.offlineSig <- make(chan struct{})
	default:
		c.offlineSig <- off
	}
}

func (c *Client) toOffline() {
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

	off := <-c.offlineSig
	select {
	case <-off:
		break // released already
	default:
		close(off)
	}
	c.offlineSig <- off

	on := <-c.onlineSig
	select {
	case <-on:
		c.onlineSig <- make(chan struct{})
	default:
		c.onlineSig <- on
	}

	select {
	case ack := <-c.pingAck:
		ack <- ErrBreak
	default:
		break
	}
	c.unorderedTxs.breakAll()
}

func (c *Client) lockWrite(quit <-chan struct{}) (net.Conn, error) {
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

// Write submits the packet. Keep synchronised with writeBuffers!
func (c *Client) write(quit <-chan struct{}, p []byte) error {
	for {
		conn, err := c.lockWrite(quit)
		if err != nil {
			return err
		}

		switch err := write(conn, p, c.PauseTimeout); {
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

// WriteBuffers submits the packet. Keep synchronised with write!
func (c *Client) writeBuffers(quit <-chan struct{}, p net.Buffers) error {
	for {
		conn, err := c.lockWrite(quit)
		if err != nil {
			return err
		}

		switch err := writeBuffers(conn, p, c.PauseTimeout); {
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

// Write submits the packet. Keep synchronised with writeBuffers!
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

// WriteBuffers submits the packet. Keep synchronised with write!
func writeBuffers(conn net.Conn, p net.Buffers, idleTimeout time.Duration) error {
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

	if c.PauseTimeout != 0 {
		// Abandon timer to prevent waking up the system for no good reason.
		// https://developer.apple.com/library/archive/documentation/Performance/Conceptual/EnergyGuide-iOS/MinimizeTimerUse.html
		defer c.readConn.SetReadDeadline(time.Time{})
	}

	// decode “remaining length”
	var size int
	for shift := uint(0); ; shift += 7 {
		if c.r.Buffered() == 0 && c.PauseTimeout != 0 {
			err := c.readConn.SetReadDeadline(time.Now().Add(c.PauseTimeout))
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
			err := c.readConn.SetReadDeadline(time.Now().Add(c.PauseTimeout))
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
	clientID, err := c.persistence.Load(clientIDKey)
	if err != nil {
		return err
	}
	packet := c.newCONNREQ(clientID)

	<-c.Offline() // extra verification

	oldConn, ok := <-c.connSem // locks connection control
	if !ok {
		return ErrClosed
	}
	// No need for further closed channel checks as the
	// connsem lock is required to close any of them.

	select { // locks write
	case <-c.writeSem:
	case <-c.writeBlock:
	}

	var atLeastOnceSeqNo, exactlyOnceSeqNo uint
	select { // locks publish submission
	case atLeastOnceSeqNo = <-c.atLeastOnce.seqNoSem:
		break
	case holdup := <-c.atLeastOnce.block:
		atLeastOnceSeqNo = holdup.UntilSeqNo + 1
	}
	select { // locks publish submission
	case exactlyOnceSeqNo = <-c.exactlyOnce.seqNoSem:
		break
	case holdup := <-c.exactlyOnce.block:
		exactlyOnceSeqNo = holdup.UntilSeqNo + 1
	}

	// Reconnects shouldn't reset the session.
	if oldConn != nil && c.CleanSession {
		c.CleanSession = false
	}
	ctx, cancel := context.WithTimeout(c.dialCtx, c.PauseTimeout)
	defer cancel()
	conn, err := c.Dialer(ctx)
	if err != nil {
		c.connSem <- oldConn // unlock for next attempt
		c.writeSem <- nil    // causes ErrDown
		n := uint(len(c.atLeastOnce.q))
		if n == 0 {
			c.atLeastOnce.seqNoSem <- atLeastOnceSeqNo
		} else {
			c.atLeastOnce.block <- holdup{atLeastOnceSeqNo - n, atLeastOnceSeqNo - 1}
		}
		n = uint(len(c.exactlyOnce.q))
		if n == 0 {
			c.exactlyOnce.seqNoSem <- exactlyOnceSeqNo
		} else {
			c.exactlyOnce.block <- holdup{exactlyOnceSeqNo - n, exactlyOnceSeqNo - 1}
		}

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
		if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
			// connSem entry closed
			err = ErrClosed
		}
		n := uint(len(c.atLeastOnce.q))
		if n == 0 {
			c.atLeastOnce.seqNoSem <- atLeastOnceSeqNo
		} else {
			c.atLeastOnce.block <- holdup{atLeastOnceSeqNo - n, atLeastOnceSeqNo - 1}
		}
		n = uint(len(c.exactlyOnce.q))
		if n == 0 {
			c.exactlyOnce.seqNoSem <- exactlyOnceSeqNo
		} else {
			c.exactlyOnce.block <- holdup{exactlyOnceSeqNo - n, exactlyOnceSeqNo - 1}
		}
		return err
	}

	c.toOnline()
	// install connection
	c.writeSem <- conn
	c.readConn = conn
	c.r = r
	c.peek = nil // applied to prevous r if any

	// Resend any pending PUBLISH and/or PUBREL entries from Persistence.
	// The queues are locked because this runs within the read-routine and new
	// submission requires either the sequence number semaphore or a holdup block.
	if n := uint(len(c.atLeastOnce.q)); n != 0 {
		err := c.resendPublishPackets(atLeastOnceSeqNo-n, atLeastOnceSeqNo-1, atLeastOnceIDSpace)
		if err != nil {
			c.toOffline()
			c.atLeastOnce.block <- holdup{atLeastOnceSeqNo - n, atLeastOnceSeqNo - 1}
			n = uint(len(c.exactlyOnce.q))
			c.exactlyOnce.block <- holdup{exactlyOnceSeqNo - n, exactlyOnceSeqNo - 1}
			return err
		}
	}
	c.atLeastOnce.seqNoSem <- atLeastOnceSeqNo
	if n := uint(len(c.exactlyOnce.q)); n != 0 {
		err := c.resendPublishPackets(exactlyOnceSeqNo-n, exactlyOnceSeqNo-1, exactlyOnceIDSpace)
		if err != nil {
			c.toOffline()
			c.exactlyOnce.block <- holdup{exactlyOnceSeqNo - n, exactlyOnceSeqNo - 1}
			return err
		}
	}
	c.exactlyOnce.seqNoSem <- exactlyOnceSeqNo

	return nil
}

func (c *Client) resendPublishPackets(firstSeqNo, lastSeqNo uint, space uint) error {
	for seqNo := firstSeqNo; seqNo <= lastSeqNo; seqNo++ {
		key := seqNo&publishIDMask | space
		packet, err := c.persistence.Load(key)
		if err != nil {
			return err
		}
		if len(packet) == 0 {
			return fmt.Errorf("mqtt: persistence key %#04x gone missing 👻", key)
		}
		if packet[0]>>4 == typePUBLISH {
			packet[0] |= dupeFlag
		}
		err = c.write(nil, packet)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) handshake(conn net.Conn, requestPacket []byte) (*bufio.Reader, error) {
	err := write(conn, requestPacket, c.PauseTimeout)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReaderSize(conn, readBufSize)

	// Apply the deadline to the "entire" 4-byte response.
	if c.PauseTimeout != 0 {
		err := conn.SetReadDeadline(time.Now().Add(c.PauseTimeout))
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
	case c.dialCtx.Err() != nil:
		err = ErrClosed
	case len(packet) > 1 && (packet[0] != typeCONNACK<<4 || packet[1] != 2):
		return nil, fmt.Errorf("%w: want fixed CONNACK header 0x2002, got %#x", errProtoReset, packet)
	case len(packet) > 3 && connectReturn(packet[3]) != accepted:
		return nil, connectReturn(packet[3])
	case err == nil:
		r.Discard(len(packet)) // no errors guaranteed
		return r, nil
	case errors.Is(err, io.EOF): // doesn't match io.ErrUnexpectedEOF
		err = errBrokerTerm
	}
	if len(packet) != 4 {
		err = fmt.Errorf("%w; CONNECT not confirmed", err)
	}
	return nil, err
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
// rather large backoff. See the Client example for a complete setup.
func (c *Client) ReadSlices() (message, topic []byte, err error) {
	message, topic, err = c.readSlices()
	switch {
	case err == c.bigMessage: // either nil or BigMessage
		break
	case errors.Is(err, ErrClosed):
		c.termCallbacks()
	}
	return
}

func (c *Client) readSlices() (message, topic []byte, err error) {
	// A pending BigMessage implies that the connection was functional on
	// the last return.
	switch {
	case c.bigMessage != nil:
		<-c.Online() // extra verification
		_, err = c.r.Discard(c.bigMessage.Size)
		if err != nil {
			c.toOffline()
			return nil, nil, err
		}

	case c.readConn == nil:
		if err := c.connect(); err != nil {
			return nil, nil, err
		}
		<-c.Online() // extra verification

	default:
		<-c.Online() // extra verification
		// skip previous packet, if any
		c.r.Discard(len(c.peek)) // no errors guaranteed
		c.peek = nil             // flush
	}

	// acknowledge previous packet, if any
	if len(c.pendingAck) != 0 {
		if c.pendingAck[0]>>4 == typePUBREC {
			// BUG(pascaldekloe): Save errors from a Persistence may
			// cause duplicate reception for deliveries with an
			// “exactly once guarantee”, if the respective Client
			// goes down before a recovery/retry succeeds.
			key := uint(binary.BigEndian.Uint16(c.pendingAck[2:4])) | remoteIDKeyFlag
			err = c.persistence.Save(key, net.Buffers{c.pendingAck})
			if err != nil {
				return nil, nil, err
			}
		}
		err := c.write(nil, c.pendingAck)
		if err != nil {
			return nil, nil, err // keeps pendingAck to retry
		}
		c.pendingAck = c.pendingAck[:0]
	}

	// process packets until a PUBLISH appears
	for {
		head, err := c.peekPacket()
		switch {
		case err == nil:
			break

		case errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe):
			// got interrupted
			c.toOffline()
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
					c.toOffline()
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
			c.toOffline()
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
			c.toOffline()
			return nil, nil, err
		}

		// no errors guaranteed
		c.r.Discard(len(c.peek))
	}
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
		e.Client.toOffline()
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
		c.pendingAck = append(c.pendingAck, typePUBACK<<4, 2, byte(packetID>>8), byte(packetID))

	case exactlyOnceLevel << 1:
		if len(c.peek) < i+2 {
			return nil, nil, fmt.Errorf("%w: PUBLISH packet identifier exceeds remaining length", errProtoReset)
		}
		packetID := uint(binary.BigEndian.Uint16(c.peek[i:]))
		if packetID == 0 {
			return nil, nil, errPacketIDZero
		}
		i += 2

		bytes, err := c.persistence.Load(packetID | remoteIDKeyFlag)
		if err != nil {
			return nil, nil, err
		}
		if bytes != nil {
			return nil, nil, errDupe
		}

		// enqueue for next call
		c.pendingAck = append(c.pendingAck, typePUBREC<<4, 2, byte(packetID>>8), byte(packetID))

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

	err := c.persistence.Delete(packetID | remoteIDKeyFlag)
	if err != nil {
		return err // causes resubmission of PUBREL
	}
	// Use pendingAck as a buffer here.
	c.pendingAck = append(c.pendingAck[:0], typePUBCOMP<<4, 2, byte(packetID>>8), byte(packetID))
	err = c.write(nil, c.pendingAck)
	if err != nil {
		return err // causes resubmission of PUBCOMP
	}
	c.pendingAck = c.pendingAck[:0]
	return nil
}
