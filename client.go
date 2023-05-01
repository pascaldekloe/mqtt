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
	//
	// Reconnects do not clean the session, regardless of this setting.
	CleanSession bool
}

func (c *Config) valid() error {
	if c.Dialer == nil {
		return errors.New("mqtt: no Dialer in Config")
	}
	if err := stringCheck(c.UserName); err != nil {
		return fmt.Errorf("%w; illegal user name", err)
	}
	if len(c.Password) > stringMax {
		return fmt.Errorf("%w; illegal password", errStringMax)
	}
	if len(c.Will.Message) > stringMax {
		return fmt.Errorf("%w; illegal will message", errStringMax)
	}

	var err error
	if c.Will.Message != nil {
		err = topicCheck(c.Will.Topic)
	} else {
		err = stringCheck(c.Will.Topic)
	}
	if err != nil {
		return fmt.Errorf("%w; illegal will topic", err)
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
// goes for reconnects on connection loss.
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
	bufr     *bufio.Reader // readConn buffered
	peek     []byte        // pending slice from bufio.Reader

	// Context is applied durring connect for faster aborts.
	ctx    context.Context
	cancel context.CancelFunc // Close may cancel the context

	// The semaphore locks connection control. A nil entry implies no
	// connect yet. ConnSem must be held to close writeSem.
	connSem chan net.Conn

	// Writes may happen from multiple goroutines. The semaphore contains
	// either a signal placeholder or the active connection. A connPending
	// entry signals a first (re)connect attempt, and connDown signals a
	// failed (re)connect attempt
	writeSem chan net.Conn

	// The semaphore allows for one ping request at a time.
	pingAck chan chan<- error

	atLeastOnce, exactlyOnce outbound

	// outbound transaction tracking
	orderedTxs
	unorderedTxs

	pendingAck []byte // enqueued packet submission

	// The read routine parks reception beyond readBufSize.
	bigMessage *BigMessage
}

// Outbound submission may face multiple goroutines.
type outbound struct {
	seqSem chan seq // sequence semaphore singleton

	// Acknowledgement is traced with a callback channel.
	// Insertion requires seqSem. Close requires connSem.
	q chan chan<- error
}

// Sequence tracks outbound submission.
type seq struct {
	// Sequence number n applies to the next submission, starting with zero.
	// Its value is used to calculate the respective MQTT packet identifier.
	n uint

	// When backlog is less than n, then packets since backlog until n - 1
	// await submission. This may happen during connection absence.
	backlog uint
}

func newClient(p Persistence, config *Config) *Client {
	if config.AtLeastOnceMax < 0 || config.AtLeastOnceMax > publishIDMask {
		config.AtLeastOnceMax = publishIDMask + 1
	}
	if config.ExactlyOnceMax < 0 || config.ExactlyOnceMax > publishIDMask {
		config.ExactlyOnceMax = publishIDMask + 1
	}

	c := &Client{
		Config:      *config, // copy
		persistence: p,
		onlineSig:   make(chan chan struct{}, 1),
		offlineSig:  make(chan chan struct{}, 1),
		connSem:     make(chan net.Conn, 1),
		writeSem:    make(chan net.Conn, 1),
		pingAck:     make(chan chan<- error, 1),
		atLeastOnce: outbound{
			seqSem: make(chan seq, 1),
			q:      make(chan chan<- error, config.AtLeastOnceMax),
		},
		exactlyOnce: outbound{
			seqSem: make(chan seq, 1),
			q:      make(chan chan<- error, config.ExactlyOnceMax),
		},
		unorderedTxs: unorderedTxs{
			perPacketID: make(map[uint16]unorderedCallback),
		},
	}

	// start in offline state
	c.onlineSig <- make(chan struct{}) // blocks
	released := make(chan struct{})
	close(released)
	c.offlineSig <- released

	c.connSem <- nil
	c.writeSem <- connPending

	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.atLeastOnce.seqSem <- seq{backlog: ^uint(0)}
	c.exactlyOnce.seqSem <- seq{backlog: ^uint(0)}
	return c
}

// Close terminates the connection establishment.
// The Client is closed regardless of the error return.
// Closing an already closed Client has no effect.
func (c *Client) Close() error {
	// halt context (interrupts dial & connect)
	c.cancel()

	// block connection control
	conn, ok := <-c.connSem
	if !ok {
		// allready closed
		return nil
	}
	defer func() {
		// signal offline
		blockSignalChan(c.onlineSig)
		clearSignalChan(c.offlineSig)
		// signal closed
		close(c.writeSem)
		close(c.connSem)
	}()

	// block write, close connection
	select {
	case conn = <-c.writeSem:
		switch conn {
		case connPending, connDown:
			return nil // already offline
		}
		return conn.Close()
	default: // no wait for write
		var err error
		if conn != nil {
			err = conn.Close() // may interrupt write
		}
		<-c.writeSem // won't block for long now
		return err
	}
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
	// halt context (interrupts dial & connect)
	c.cancel()

	// block connection control
	conn, ok := <-c.connSem
	if !ok {
		return fmt.Errorf("%w; DISCONNECT not send", ErrClosed)
	}
	defer func() {
		// signal offline
		blockSignalChan(c.onlineSig)
		clearSignalChan(c.offlineSig)
		// signal closed
		close(c.writeSem)
		close(c.connSem)
	}()

	// block write, send disconnect, close connection
	select {
	case <-quit:
		if conn != nil {
			conn.Close() // may interrupt write
		}
		<-c.writeSem // won't block for long now
		return fmt.Errorf("%w; DISCONNECT not send", ErrCanceled)

	case conn = <-c.writeSem:
		switch conn {
		case connPending, connDown:
			return fmt.Errorf("%w; DISCONNECT not send", ErrDown)
		}

		// “After sending a DISCONNECT Packet the Client MUST NOT send
		// any more Control Packets on that Network Connection.”
		// — MQTT Version 3.1.1, conformance statement MQTT-3.14.4-2
		writeErr := write(conn, packetDISCONNECT, c.PauseTimeout)
		closeErr := conn.Close()
		if writeErr != nil {
			return fmt.Errorf("%w; DISCONNECT lost", writeErr)
		}
		return closeErr
	}
}

func (c *Client) termCallbacks() {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		_, ok := <-c.atLeastOnce.seqSem
		if !ok { // already terminated
			return
		}
		close(c.atLeastOnce.seqSem) // terminate

		// flush queue
		err := fmt.Errorf("%w; PUBLISH not confirmed", ErrClosed)
		close(c.atLeastOnce.q)
		for ch := range c.atLeastOnce.q {
			ch <- err // won't block
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		_, ok := <-c.exactlyOnce.seqSem
		if !ok { // already terminated
			return
		}
		close(c.exactlyOnce.seqSem) // terminate

		// flush queue
		err := fmt.Errorf("%w; PUBLISH not confirmed", ErrClosed)
		close(c.exactlyOnce.q)
		for ch := range c.exactlyOnce.q {
			ch <- err // won't block
		}
	}()

	select {
	case ack := <-c.pingAck:
		ack <- fmt.Errorf("%w; PING not confirmed", ErrBreak)
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

func clearSignalChan(ch chan chan struct{}) {
	sig := <-ch
	select {
	case <-sig:
		break // released already
	default:
		close(sig) // release
	}
	ch <- sig
}

func blockSignalChan(ch chan chan struct{}) {
	sig := <-ch
	select {
	case <-sig:
		ch <- make(chan struct{}) // block
	default:
		ch <- sig // blocks already
	}
}

func (c *Client) toOffline() {
	select {
	case _, ok := <-c.writeSem:
		if !ok {
			return // ErrClosed
		}
		c.readConn.Close()
	default:
		c.readConn.Close() // interrupt write
		_, ok := <-c.writeSem
		if !ok {
			return // ErrClosed
		}
	}
	blockSignalChan(c.onlineSig)
	clearSignalChan(c.offlineSig)
	c.writeSem <- connPending

	c.readConn = nil
	c.bigMessage = nil // lost
	c.bufr = nil
	c.peek = nil // applied to prevous r, if any

	select {
	case ack := <-c.pingAck:
		ack <- ErrBreak
	default:
		break
	}

	c.unorderedTxs.breakAll()
}

// LockWrite acquires the write semaphore.
func (c *Client) lockWrite(quit <-chan struct{}) (net.Conn, error) {
	var checkConnect *time.Ticker

	for {
		select {
		case <-quit:
			return nil, ErrCanceled
		case conn, ok := <-c.writeSem: // lock
			switch {
			case !ok:
				return nil, ErrClosed
			case conn == connDown:
				c.writeSem <- connDown // unlock
				return nil, ErrDown
			case conn == connPending:
				c.writeSem <- connPending // unlock
				break
			default:
				return conn, nil
			}

			if checkConnect == nil {
				checkConnect = time.NewTicker(20 * time.Millisecond)
				defer checkConnect.Stop()
			}
			select {
			case <-c.ctx.Done():
				return nil, ErrClosed
			case <-c.Online():
				break // connect succeeded
			case <-checkConnect.C:
				break // connect may have failed
			}
		}
	}
}

// Write submits the packet. Keep synchronised with writeBuffers!
func (c *Client) write(quit <-chan struct{}, p []byte) error {
	conn, err := c.lockWrite(quit)
	if err != nil {
		return err
	}

	switch err := write(conn, p, c.PauseTimeout); {
	case err == nil:
		c.writeSem <- conn // unlock write
		return nil

	case errors.Is(err, net.ErrClosed), errors.Is(err, io.ErrClosedPipe):
		// read routine closed the connection
		c.writeSem <- connPending // unlock write; pending connect
		return fmt.Errorf("mqtt: submission interupted: %w", err)

	default:
		conn.Close()              // signal read routine
		c.writeSem <- connPending // unlock write; pending connect
		return err
	}
}

// WriteBuffers submits the packet. Keep synchronised with write!
func (c *Client) writeBuffers(quit <-chan struct{}, p net.Buffers) error {
	conn, err := c.lockWrite(quit)
	if err != nil {
		return err
	}

	switch err := writeBuffers(conn, p, c.PauseTimeout); {
	case err == nil:
		c.writeSem <- conn // unlock write
		return nil

	case errors.Is(err, net.ErrClosed), errors.Is(err, io.ErrClosedPipe):
		// read routine closed the connection
		c.writeSem <- connPending // unlock write; pending connect
		return fmt.Errorf("mqtt: submission interupted: %w", err)

	default:
		conn.Close()              // signal read routine
		c.writeSem <- connPending // unlock write; pending connect
		return err
	}
}

// WriteBuffersNoWait does not wait for pending connects.
func (c *Client) writeBuffersNoWait(p net.Buffers) error {
	// lock write
	conn, ok := <-c.writeSem
	switch {
	case !ok:
		return ErrClosed
	case conn == connDown, conn == connPending:
		c.writeSem <- conn // unlock
		return ErrDown
	}

	// transfer
	switch err := writeBuffers(conn, p, c.PauseTimeout); {
	case err == nil:
		c.writeSem <- conn // unlock write
		return nil

	case errors.Is(err, net.ErrClosed), errors.Is(err, io.ErrClosedPipe):
		// read routine closed the connection
		c.writeSem <- connPending // unlock write; pending connect
		return fmt.Errorf("mqtt: submission interupted: %w", err)

	default:
		conn.Close()              // signal read routine
		c.writeSem <- connPending // unlock write; pending connect
		return err
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
		// ⚠️ reverse error check
		if err == nil {
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
		// ⚠️ reverse error check
		if err == nil {
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
	head, err = c.bufr.ReadByte()
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
		if c.bufr.Buffered() == 0 && c.PauseTimeout != 0 {
			err := c.readConn.SetReadDeadline(time.Now().Add(c.PauseTimeout))
			if err != nil {
				return 0, err // deemed critical
			}
		}
		b, err := c.bufr.ReadByte()
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
		if c.bufr.Buffered() < size {
			err := c.readConn.SetReadDeadline(time.Now().Add(c.PauseTimeout))
			if err != nil {
				return 0, err // deemed critical
			}
		}

		lastN := len(c.peek)
		c.peek, err = c.bufr.Peek(size)
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

// Connect installs the transport layer.
//
// The current connection must be closed in case of a reconnect.
func (c *Client) connect() error {
	previousConn, ok := <-c.connSem // locks connection control
	if !ok {
		return ErrClosed
	}
	// No need for further closed channel checks as the
	// connSem lock is required to close any of them.

	config := c.Config // copy
	// Reconnects shouldn't reset the session.
	if previousConn != nil {
		config.CleanSession = false
	}
	conn, bufr, err := c.dialAndConnect(&config)
	switch err {
	case nil:
		break

	case context.Canceled:
		// Close or Disconnect interrupted dial
		c.connSem <- previousConn // unlock
		return ErrClosed

	default:
		// ErrDown after failed connect
		<-c.writeSem
		c.writeSem <- connDown

		c.connSem <- previousConn // unlock
		return err
	}

	// lock sequences until resubmission (checks) complete
	atLeastOnceSeq := <-c.atLeastOnce.seqSem
	exactlyOnceSeq := <-c.exactlyOnce.seqSem

	// lock write in sequence locks, conform submitPersisted
	<-c.writeSem

	c.connSem <- conn // unlock (for interruption of resends)

	err = c.resend(conn, c.orderedTxs.Acked, &atLeastOnceSeq, atLeastOnceIDSpace)
	c.atLeastOnce.seqSem <- atLeastOnceSeq // unlock
	if err != nil {
		c.exactlyOnce.seqSem <- exactlyOnceSeq // unlock
		conn.Close()
		c.writeSem <- connDown
		return err
	}
	err = c.resend(conn, c.orderedTxs.Completed, &exactlyOnceSeq, exactlyOnceIDSpace)
	c.exactlyOnce.seqSem <- exactlyOnceSeq // unlock
	if err != nil {
		conn.Close()
		c.writeSem <- connDown
		return err
	}

	// update signals
	blockSignalChan(c.offlineSig)
	clearSignalChan(c.onlineSig)
	// release
	c.writeSem <- conn
	c.readConn = conn
	c.bufr = bufr
	return nil
}

func (c *Client) dialAndConnect(config *Config) (net.Conn, *bufio.Reader, error) {
	// connect request packet
	clientID, err := c.persistence.Load(clientIDKey)
	if err != nil {
		return nil, nil, err
	}
	packet := config.newCONNREQ(clientID)

	// network connection
	ctx := c.ctx
	if c.PauseTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.PauseTimeout)
		defer cancel()
	}
	conn, err := c.Dialer(ctx)
	if err != nil {
		if e := c.ctx.Err(); e != nil {
			return nil, nil, e
		}
		return nil, nil, err
	}
	// “After a Network Connection is established by a Client to a Server,
	// the first Packet sent from the Client to the Server MUST be a CONNECT
	// Packet.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.1.0-1

	// Don't make Close wait on a slow connect.
	done := make(chan struct{})
	defer close(done)
	abort := make(chan error, 1)
	go func() {
		defer close(abort)
		select {
		case <-c.ctx.Done():
			conn.Close() // interrupt
			abort <- ErrClosed
		case <-done:
			break
		}
	}()

	bufr, err := c.handshake(conn, packet)
	// ⚠️ delayed error check

	done <- struct{}{}
	e := <-abort
	if e != nil {
		// abort closed connection
		return nil, nil, e
	}

	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, bufr, nil
}

// Resend submits any and all pending since seqNoOffset.
func (c *Client) resend(conn net.Conn, seqNoOffset uint, seq *seq, space uint) error {
	for seqNo := seqNoOffset; seqNo < seq.n; seqNo++ {
		key := seqNo&publishIDMask | space
		packet, err := c.persistence.Load(uint(key))
		if err != nil {
			return err
		}
		if packet == nil {
			return fmt.Errorf("mqtt: persistence key %#04x gone missing 👻", key)
		}

		if seqNo < seq.backlog && packet[0]>>4 == typePUBLISH {
			packet[0] |= dupeFlag
		}

		err = write(conn, packet, c.PauseTimeout)
		if err != nil {
			return err
		}
		if seqNo >= seq.backlog {
			seq.backlog = seqNo + 1
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
// ErrClosed. Both message and topic are slices from a read buffer. The bytes
// stop being valid at the next read. Care must be taken to avoid freezing the
// Client. Use any of the following techniques for blocking operations.
//
//   - start a goroutine with a copy of message and/or topic
//   - start a goroutine with the bytes parsed/unmarshalled
//   - persist message and/or topic; then continue from there
//   - apply low timeouts in a strict manner
//
// Each invocation acknowledges ownership of the previous returned, if any,
// including BigMessage.
//
// BigMessage leaves memory allocation beyond the read buffer as a choice to the
// consumer. Any error other than BigMessage puts the Client in an ErrDown state.
// Invocation should apply a backoff once down. Retries on IsConnectionRefused,
// if any, should probably apply a rather large backoff. See the Client example
// for a complete setup.
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
	if c.readConn == nil {
		if err = c.connect(); err != nil {
			return nil, nil, err
		}
	}

	if c.bigMessage != nil {
		_, err = c.bufr.Discard(c.bigMessage.Size)
		if err != nil {
			c.toOffline()
			return nil, nil, err
		}

		c.bigMessage = nil
	}

	if pending := len(c.peek); pending != 0 {
		// skip previous packet, if any
		c.bufr.Discard(pending) // no error guaranteed
		c.peek = nil            // clear
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
			c.toOffline()
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
			// closed by either Close, Disconnect, or failed write
			c.toOffline()
			if err := c.connect(); err != nil {
				return nil, nil, err
			}
			continue // with new connection

		case errors.As(err, &c.bigMessage):
			if head>>4 == typePUBLISH {
				message, topic, err = c.onPUBLISH(head)
				// TODO(pascaldekloe): errDupe
				if err != nil {
					c.toOffline()
					return nil, nil, err
				}
				c.bigMessage.Topic = string(topic) // copy
				done := readBufSize - len(message)
				c.bigMessage.Size -= done
				c.bufr.Discard(done) // no errors guaranteed
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
		c.bufr.Discard(len(c.peek))
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
	_, err := io.ReadFull(e.Client.bufr, message)
	if err != nil {
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

// The write semaphore may hold a connSignal when not connected.
const (
	connPending connSignal = iota // first (re)connect attempt.
	connDown                      // failed (re)connect attempt.
)

// ConnSignal is a net.Conn.
type connSignal int

const connSignalInvoke = "signal invoked as connection"

func (c connSignal) Read(b []byte) (n int, err error) { panic(connSignalInvoke) }
func (c connSignal) Write(b []byte) (int, error)      { panic(connSignalInvoke) }
func (c connSignal) Close() error                     { panic(connSignalInvoke) }
func (c connSignal) LocalAddr() net.Addr              { panic(connSignalInvoke) }
func (c connSignal) RemoteAddr() net.Addr             { panic(connSignalInvoke) }
func (c connSignal) SetDeadline(time.Time) error      { panic(connSignalInvoke) }
func (c connSignal) SetReadDeadline(time.Time) error  { panic(connSignalInvoke) }
func (c connSignal) SetWriteDeadline(time.Time) error { panic(connSignalInvoke) }
