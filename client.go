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
	"sync/atomic"
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

	// Channels returned by ReadBackoff will block for at least this amount
	// of time on connection loss. Zero defaults to one second. Retries at a
	// low interval may induce ill side-effects such as network strain, log
	// flooding and battery consumption.
	ReconnectWaitMin time.Duration

	// Channels returned by ReadBackoff will each double the amount of time
	// they block on consecutive failure to connect, up to this maximum. The
	// value defaults to one minute. The maximum is raised to the effective
	// minimum (from ReconnectWaitMin) when short. Connection refusal by the
	// broker directly applies to the maximum, without ramp up.
	ReconnectWaitMax time.Duration

	// PauseTimeout sets the minimum transfer rate as one byte per duration.
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
	// The broker must disconnect after no control-packet reception for one
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
// applies to reconnects.
//
// A single goroutine must invoke ReadSlices consecutively until ErrClosed.
// Appliance of ReadBackoff comes recommended though.
//
// Multiple goroutines may invoke methods on a Client simultaneously, except for
// ReadSlices and ReadBackoff.
type Client struct {
	// The applied settings are read only.
	Config

	// InNewSession gets flagged when the broker confirms a connect without
	// without “session present”, a.k.a. the SP¹ indicator. Note how this
	// includes any automatic reconnects after protocol or connection failure.
	// Users should await the Online channel before Load of the atomic.
	InNewSession atomic.Bool

	// The session track is dedicated to the respective client identifier.
	persistence Persistence

	// Signal channels are closed once their respective state occurs.
	// Each read must restore or replace the signleton value.
	onlineSig, offlineSig chan chan struct{}

	// The read routine controls the connection, including reconnects.
	readConn net.Conn
	bufr     *bufio.Reader // readConn buffered
	peek     []byte        // pending slice from bufio.Reader

	// Context is applied during connect for faster aborts.
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

	// shared backoff on ErrMax prevents timer flood
	backoffOnMax atomic.Pointer[<-chan struct{}]

	// backoff ramp-up
	reconnectWait time.Duration
}

// Outbound submission may face multiple goroutines.
type outbound struct {
	// The sequence semaphore is a singleton instance.
	seqSem chan seq

	// Acknowledgement is traced with a callback channel per request.
	// Insertion requires a seqSem lock as the queue order must match its
	// respective sequence number. Close of the queue requires connSem to
	// prevent panic on double close [race].
	queue chan chan<- error
}

// Sequence tracks outbound submission.
type seq struct {
	// AcceptN has the sequence number for the next submission. Counting
	// starts at zero. The value is used to calculate the respective MQTT
	// packet identifiers.

	// Packets are accepted once they are persisted. The count is used as a
	// sequence number (starting with zero) in packet identifiers.
	acceptN uint

	// Any packets between submitN and acceptN are still pending network
	// submission. Such backlog may happen due to connectivity failure.
	submitN uint
}

func newClient(p Persistence, config *Config) *Client {
	if config.ReconnectWaitMin == 0 {
		config.ReconnectWaitMin = time.Second
	}
	if config.ReconnectWaitMin < 0 {
		config.ReconnectWaitMin = 0
	}
	if config.ReconnectWaitMax < config.ReconnectWaitMin {
		config.ReconnectWaitMax = config.ReconnectWaitMin
	}

	if config.AtLeastOnceMax < 0 || config.AtLeastOnceMax > publishIDMask {
		config.AtLeastOnceMax = publishIDMask + 1
	}
	if config.ExactlyOnceMax < 0 || config.ExactlyOnceMax > publishIDMask {
		config.ExactlyOnceMax = publishIDMask + 1
	}

	c := Client{
		Config:      *config, // copy
		persistence: p,
		onlineSig:   make(chan chan struct{}, 1),
		offlineSig:  make(chan chan struct{}, 1),
		connSem:     make(chan net.Conn, 1),
		writeSem:    make(chan net.Conn, 1),
		pingAck:     make(chan chan<- error, 1),
		atLeastOnce: outbound{
			seqSem: make(chan seq, 1), // must singleton
			queue:  make(chan chan<- error, config.AtLeastOnceMax),
		},
		exactlyOnce: outbound{
			seqSem: make(chan seq, 1), // must singleton
			queue:  make(chan chan<- error, config.ExactlyOnceMax),
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
	c.atLeastOnce.seqSem <- seq{}
	c.exactlyOnce.seqSem <- seq{}
	return &c
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
		// already closed
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
		writeErr := writeTo(conn, packetDISCONNECT, c.PauseTimeout)
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
		// seqSem lock required for close:
		close(c.atLeastOnce.queue)
		for ch := range c.atLeastOnce.queue {
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
		// seqSem lock required for close:
		close(c.exactlyOnce.queue)
		for ch := range c.exactlyOnce.queue {
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

var connClosedErrors = []error{net.ErrClosed, io.ErrClosedPipe}

// Write submits the packet. Keep synchronised with writeBuffers!
func (c *Client) write(quit <-chan struct{}, p []byte) error {
	conn, err := c.lockWrite(quit)
	if err != nil {
		return err
	}

	err = writeTo(conn, p, c.PauseTimeout)
	if err != nil {
		if !nonNilIsAny(err, connClosedErrors) {
			conn.Close() // signal read routine
		}
		c.writeSem <- connPending // unlock write; pending connect
		return errors.Join(ErrSubmit, err)
	}

	c.writeSem <- conn // unlock write
	return nil
}

// WriteBuffers submits the packet. Keep synchronised with write!
func (c *Client) writeBuffers(quit <-chan struct{}, p net.Buffers) error {
	conn, err := c.lockWrite(quit)
	if err != nil {
		return err
	}

	err = writeBuffersTo(conn, p, c.PauseTimeout)
	if err != nil {
		if !nonNilIsAny(err, connClosedErrors) {
			conn.Close() // signal read routine
		}
		// unlock write; pending connect
		c.writeSem <- connPending
		return errors.Join(ErrSubmit, err)
	}

	c.writeSem <- conn // unlock write
	return nil
}

// WriteBuffersNoWait is like writeBuffers, yet it does not wait for pending
// connects.
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
	err := writeBuffersTo(conn, p, c.PauseTimeout)
	if err != nil {
		if !nonNilIsAny(err, connClosedErrors) {
			conn.Close() // signal read routine
		}
		// unlock write; pending connect
		c.writeSem <- connPending
		return errors.Join(ErrSubmit, err)
	}

	c.writeSem <- conn // unlock write
	return nil
}

// WriteTo submits the packet. Keep synchronised with writeBuffers!
func writeTo(conn net.Conn, p []byte, idleTimeout time.Duration) error {
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
		if err == nil {
			return nil
		}

		// Allow deadline expiry if at least one byte was transferred.
		var ne net.Error
		if n == 0 || !errors.As(err, &ne) || !ne.Timeout() {
			return err
		}

		p = p[n:]
	}
}

// WriteBuffersTo submits the packet. Keep synchronised with write!
func writeBuffersTo(conn net.Conn, p net.Buffers, idleTimeout time.Duration) error {
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
		if c.bufr.Buffered() < size && c.PauseTimeout != 0 {
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
		return 0, fmt.Errorf("mqtt: got %d out of %d bytes from packet %#b: %w",
			len(c.peek), size, head, err)
	}
}

// Discard skips n bytes from the network connection.
func (c *Client) discard(n int) error {
	if c.PauseTimeout != 0 {
		// Abandon timer to prevent waking up the system for no good reason.
		// https://developer.apple.com/library/archive/documentation/Performance/Conceptual/EnergyGuide-iOS/MinimizeTimerUse.html
		defer c.readConn.SetReadDeadline(time.Time{})
	}

	for {
		if c.PauseTimeout != 0 {
			err := c.readConn.SetReadDeadline(time.Now().Add(c.PauseTimeout))
			if err != nil {
				return err // deemed critical
			}
		}

		done, err := c.bufr.Discard(n)
		if err == nil {
			return nil
		}

		// Allow deadline expiry if at least one byte was transferred.
		var ne net.Error
		if done != 0 && errors.As(err, &ne) && ne.Timeout() {
			n -= done
			continue
		}

		return fmt.Errorf("mqtt: %d bytes remaining of packet discard: %w",
			n, err)
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
	// reset backoff ramp-up
	c.reconnectWait = 0
	return nil
}

func (c *Client) dialAndConnect(config *Config) (net.Conn, *bufio.Reader, error) {
	clientID, err := c.persistence.Load(clientIDKey)
	if err != nil {
		return nil, nil, err
	}

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

	bufr, err := c.handshake(conn, config, clientID)
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

// Resend submits any and all pending since seqNoOffset. Sequence numbers count
// from zero. Each sequence number is one less than the respective accept count
// was at the time.
func (c *Client) resend(conn net.Conn, seqNoOffset uint, seq *seq, space uint) error {
	for seqNo := seqNoOffset; seqNo < seq.acceptN; seqNo++ {
		key := seqNo&publishIDMask | space
		packet, err := c.persistence.Load(uint(key))
		if err != nil {
			return err
		}
		if packet == nil {
			return fmt.Errorf("mqtt: persistence key %#04x gone missing 👻", key)
		}

		if seqNo < seq.submitN && packet[0]>>4 == typePUBLISH {
			packet[0] |= dupeFlag
		}

		err = writeTo(conn, packet, c.PauseTimeout)
		if err != nil {
			return err
		}

		if seqNo >= seq.submitN {
			seq.submitN = seqNo + 1
		}
	}
	return nil
}

func (c *Client) handshake(conn net.Conn, config *Config, clientID []byte) (*bufio.Reader, error) {
	// send request
	err := writeTo(conn, config.newCONNREQ(clientID), c.PauseTimeout)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReaderSize(conn, readBufSize)

	// Apply the timeout to the "entire" 4-byte response.
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
	// A smaller packet may cause timeout errors. 😉
	if len(packet) > 1 && (packet[0] != typeCONNACK<<4 || packet[1] != 2) {
		return nil, fmt.Errorf("%w: want fixed CONNACK header 0x2002, got %#x", errProtoReset, packet)
	}
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = errBrokerTerm
		}
		return nil, fmt.Errorf("%w; CONNECT not confirmed", err)
	}

	// Check the return code first to prevent confusion with flag appliance.
	//
	// “If a server sends a CONNACK packet containing a non-zero return code
	// it MUST set Session Present to 0.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.2.2-4
	if r := connectReturn(packet[3]); r != accepted {
		return nil, r
	}

	switch flags := packet[2]; flags {
	// “Bits 7-1 are reserved and MUST be set to 0.”
	default:
		return nil, fmt.Errorf("%w: CONNACK with reserved flags %#b",
			errProtoReset, flags)

	// “Bit 0 (SP1) is the Session Present Flag.”
	case 1:
		// “If the Server accepts a connection with CleanSession set to
		// 1, the Server MUST set Session Present to 0 in the CONNACK …”
		// — MQTT Version 3.1.1, conformance statement MQTT-3.2.2-1
		if config.CleanSession {
			return nil, fmt.Errorf("%w: CONNACK with session-present for clean-session request",
				errProtoReset)
		}

		// don't clear InNewSession (on reconnects)

	case 0:
		c.InNewSession.Store(true)
	}

	r.Discard(len(packet)) // no errors guaranteed
	return r, nil
}

var closed = make(chan struct{})

func init() {
	close(closed)
}

// ReadBackoff returns a channel which is closed once ReadSlices should be
// invoked again. The return is nil when ReadSlices was fatal, i.e., ErrClosed
// gets a nil channel which blocks. Idle time on connection loss is subject to
// ReconnectWaitMin and ReconnectWaitMax from Config.
func (c *Client) ReadBackoff(err error) <-chan struct{} {
	var idle time.Duration
	switch {
	case err == nil, c.bigMessage != nil:
		return closed // no backoff

	case errors.Is(err, ErrClosed):
		return nil // blocks

	case c.readConn != nil:
		// error came from Persistence ☠️
		idle = time.Second

	case IsConnectionRefused(err):
		// documented behaviour
		idle = c.ReconnectWaitMax

	default:
		// need reconnect
		idle = c.reconnectWait
		idle = max(idle, c.ReconnectWaitMin)
		idle = min(idle, c.ReconnectWaitMax)
		// exponential ramp-up is documented behaviour
		c.reconnectWait = idle * 2
	}

	wait := make(chan struct{})
	time.AfterFunc(idle, func() { close(wait) })
	return wait
}

// ReadSlices should be invoked consecutively from a single goroutine until
// ErrClosed. Each invocation acknowledges ownership of the previous return.
//
// Both message and topic are sliced from a read buffer. The bytes stop being
// valid at the next read. BigMessage leaves memory allocation beyond the read
// buffer as a choice to the consumer.
//
// Slow processing of the return may freeze the Client. Blocking operations
// require counter measures for stability:
//
//   - Start a goroutine with a copy of message and/or topic.
//   - Start a goroutine with the bytes parsed/unmarshalled.
//   - Persist message and/or topic. Then, continue from there.
//   - Apply low timeouts in a strict manner.
//
// Invocation should apply some backoff after errors other than BigMessage.
// Use of ReadBackoff comes recommended. See the Client example for a setup.
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
	// auto connect
	if c.readConn == nil {
		if err = c.connect(); err != nil {
			return nil, nil, err
		}
	}

	// flush big message if any
	if c.bigMessage != nil {
		remaining := c.bigMessage.Size
		c.bigMessage = nil

		err = c.discard(remaining)
		if err != nil {
			c.toOffline()
			return nil, nil, err
		}
	}

	// skip previous packet, if any
	c.bufr.Discard(len(c.peek)) // no error guaranteed
	c.peek = nil

	// acknowledge previous packet, if any
	if len(c.pendingAck) != 0 {
		// BUG(pascaldekloe): Save errors from Persistence can cause
		// duplicate reception of messages with the “exactly once”
		// guarantee, but only in a follow-up with AdoptSession, and
		// only if the Client which encountered Persistence failure
		// goes down before automatic-recovery in ReadSlices succeded.
		if c.pendingAck[0]>>4 == typePUBREC {
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
			// keys + topic under readBufSize thus in c.peek
			partialMessage, topic, err := c.onPUBLISH(head)
			if err != nil {
				if err != errDupe {
					c.toOffline()
					return nil, nil, err
				}

				// can just skip the already received
				payloadSize := c.bigMessage.Size
				c.bigMessage = nil
				c.peek = nil
				err := c.discard(payloadSize)
				if err != nil {
					return nil, nil, err
				}
				continue
			}

			// serve big message (as error)
			c.bigMessage.Topic = string(topic) // copy
			beforeMessage := readBufSize - len(partialMessage)
			c.bigMessage.Size -= beforeMessage
			c.peek = nil
			c.bufr.Discard(beforeMessage) // no errors guaranteed
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
				err = nil // can just skip
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
	Topic   string // destination
	Size    int    // byte count
}

// Error implements the standard error interface.
func (e *BigMessage) Error() string {
	return fmt.Sprintf("mqtt: %d-byte message exceeds read buffer capacity", e.Size)
}

// ReadAll returns the message in a new/dedicated buffer. Messages can be read
// once at most. Read fails on second attempt. Read also fails after followup by
// another ReadSlices.
func (e *BigMessage) ReadAll() ([]byte, error) {
	if e.Client.bigMessage != e {
		return nil, errors.New("mqtt: read window expired for a big message")
	}
	e.Client.bigMessage = nil

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
		if len(c.pendingAck) != 0 {
			return nil, nil, fmt.Errorf("mqtt: internal error: ack %#x pending during PUBLISH at least once reception", c.pendingAck)
		}
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
		if len(c.pendingAck) != 0 {
			return nil, nil, fmt.Errorf("mqtt: internal error: ack %#x pending during PUBLISH exactly once reception", c.pendingAck)
		}
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
	if len(c.pendingAck) != 0 {
		return fmt.Errorf("mqtt: internal error: ack %#x pending during PUBREL reception", c.pendingAck)
	}
	c.pendingAck = append(c.pendingAck, typePUBCOMP<<4, 2, byte(packetID>>8), byte(packetID))
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
