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

// A total for four types of client requests require a 16-bit packet identifier,
// namely SUBSCRIBE, UNSUBSCRIBE and PUBLISH at-least-once or exactly-once.
// Identifiers are assigned in segments to separate transit management per type.
const (
	// A 14-bit address space allows for up to 16,384 pending transactions.
	packetIDMask = 0x3fff

	// The 2 most-significant bits distinguish the packet type.
	subscriptionIDSpace = 0x4000
	atLeastOnceIDSpace  = 0x8000
	exactlyOnceIDSpace  = 0xc000
	// Note that packet identifier zero is not permitted.
)

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

// Connecter abstracts the transport layer establishment.
type Connecter func(ctx context.Context) (net.Conn, error)

// UnsecuredConnecter creates plain network connections.
// See net.Dial for details on the network & address syntax.
func UnsecuredConnecter(network, address string) Connecter {
	return func(ctx context.Context) (net.Conn, error) {
		// minimize timer use; covered by WireTimeout
		dialer := net.Dialer{KeepAlive: -1}
		return dialer.DialContext(ctx, network, address)
	}
}

// SecuredConnecter creates TLS network connections.
// See net.Dial for details on the network & address syntax.
func SecuredConnecter(network, address string, config *tls.Config) Connecter {
	return func(ctx context.Context) (net.Conn, error) {
		dialer := tls.Dialer{
			// minimize timer use; covered by WireTimeout
			NetDialer: &net.Dialer{KeepAlive: -1},
			Config:    config,
		}
		return dialer.DialContext(ctx, network, address)
	}
}

// ClientConfig is the configuration for a Client.
// Clients can't share a configuration because they can't share the Store.
type ClientConfig struct {
	*SessionConfig

	Connecter

	// BufSize defines the read buffer capacity, which goes up to 256 MiB.
	BufSize int

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
}

// Client manages a network connection. A single goroutine must invoke
// ReadSlices consecutively until ErrClosed. Some backoff on error reception
// comes recommended though.
//
// Multiple goroutines may invoke methods on a Conn simultaneously, except for
// ReadSlices.
type Client struct {
	ClientConfig // read-only

	// The read routine controls the connection, including reconnects.
	readConn net.Conn
	r        *bufio.Reader // conn buffered
	peek     []byte        // pending slice from bufio.Reader

	// The semaphore locks connection control. A nil entry implies no
	// successful connect yet.
	connSem chan net.Conn

	// The context is fed to the Connecter, which allows for faster aborts
	// during a Close.
	connectCtx    context.Context
	connectCancel context.CancelFunc

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

	subscription

	// The semaphores lock the respective acknowledge queues with a
	// submission counter. Overflows are acceptable.
	atLeastOnceSem, exactlyOnceSem chan uint

	// Outbout PUBLISH acknowledgement is traced by a callback channel.
	ackQ, recQ, compQ chan chan<- error

	txOut

	// The read routine uses this reusable buffer for packet submission.
	pendingAck [4]byte
}

// NewClient returns a new Client. Configuration errors result in IsDeny on
// ReadSlices.
func NewClient(config *ClientConfig) *Client {
	// need 1 packet identifier free to determine the first and last entry
	if config.AtLeastOnceMax < 0 || config.AtLeastOnceMax > packetIDMask {
		config.AtLeastOnceMax = packetIDMask
	}
	if config.ExactlyOnceMax < 0 || config.ExactlyOnceMax > packetIDMask {
		config.ExactlyOnceMax = packetIDMask
	}
	// apply defaults
	if config.SessionConfig == nil {
		config.SessionConfig = NewVolatileSessionConfig("")
	}

	c := &Client{
		ClientConfig: *config, // copy
		writeSem:     make(chan net.Conn, 1),
		writeBlock:   make(chan struct{}, 1),
		connSem:      make(chan net.Conn, 1),
		subscription: subscription{
			transit: make(map[uint16]*subscriptionCallback),
		},
		atLeastOnceSem: make(chan uint, 1),
		exactlyOnceSem: make(chan uint, 1),
		ackQ:           make(chan chan<- error, config.AtLeastOnceMax),
		recQ:           make(chan chan<- error, config.ExactlyOnceMax),
		compQ:          make(chan chan<- error, config.ExactlyOnceMax),
		pingAck:        make(chan chan<- error, 1),
	}
	c.connSem <- nil
	c.writeBlock <- struct{}{}
	c.atLeastOnceSem <- 0
	c.exactlyOnceSem <- 0
	c.connectCtx, c.connectCancel = context.WithCancel(context.Background())
	return c
}

// TermConn hijacks connection access. Further connect, write and writeAll
// requests are denied with ErrClosed, regardless of the error return.
func (c *Client) termConn(quit <-chan struct{}) (net.Conn, error) {
	// terminate connection control
	c.connectCancel()
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
		return nil, ErrAbort
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
	case ErrAbort, ErrDown:
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

	// Interrupt DISCONNECT (and cause ErrClosed) on quit receive.
	done := make(chan struct{})
	exit := make(chan error, 1)
	go func() {
		select {
		case <-quit:
			exit <- ErrAbort
			conn.Close()
		case <-done:
			exit <- conn.Close()
		}
	}()

	// “After sending a DISCONNECT Packet the Client MUST NOT send
	// any more Control Packets on that Network Connection.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.14.4-2
	writeErr := write(conn, packetDISCONNECT, c.WireTimeout)
	close(done)
	exitErr := <-exit
	if exitErr == ErrAbort {
		return ErrAbort
	}
	if writeErr != nil {
		return writeErr
	}
	return exitErr
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
		c.subscription.close()
	}()

	select {
	case ack := <-c.pingAck:
		ack <- ErrClosed
	default:
		break
	}
	wg.Wait()
}

// Write submits the packet. Keep synchronised with writeAll!
func (c *Client) write(p []byte) error {
	for {
		conn, ok := <-c.writeSem // locks writes
		if !ok {
			return ErrClosed
		}
		if conn == nil {
			c.writeSem <- nil // unlocks writes
			return ErrDown
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
func (c *Client) writeAll(p net.Buffers) error {
	for {
		conn, ok := <-c.writeSem // locks writes
		if !ok {
			return ErrClosed
		}
		if conn == nil {
			c.writeSem <- nil // unlocks writes
			return ErrDown
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
		if shift > 24 {
			return 0, fmt.Errorf("%w: remaining length encoding from packet %#b exceeds 4 bytes", errProtoReset, head)
		}
	}

	// slice payload form read buffer
	for {
		err := c.readConn.SetReadDeadline(time.Now().Add(c.WireTimeout))
		if err != nil {
			return 0, err // deemed critical
		}

		lastN := len(c.peek)
		c.peek, err = c.r.Peek(int(size))
		if err == nil { // OK
			return head, nil
		}

		// TODO(pascaldekloe): if errors.Is(err, bufio.ErrBufferFull) {
		//	return head, BigPacketError{c, io.MultiReader(bytes.NewReader(c.peek), io.LimitReader(c.r, l-len(c.peek)))}

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
	// “After a Network Connection is established by a Client to a Server,
	// the first Packet sent from the Client to the Server MUST be a CONNECT
	// Packet.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.1.0-1
	conn, err := c.Connecter(c.connectCtx)
	if err != nil {
		c.connSem <- oldConn // unlock for next attempt
		c.writeSem <- nil    // causes ErrDown
		// See <https://github.com/golang/go/issues/36208>.
		if c.connectCtx.Err() != nil {
			return ErrClosed
		}
		return err
	}

	c.connSem <- conn // release early for interruption by Close

	r, err := c.handshake(conn, packet)
	if err != nil {
		conn.Close()      // abandon
		c.writeSem <- nil // causes ErrDown
		return err
	}

	// install
	c.readConn = conn
	c.writeSem <- conn
	c.r = r
	c.peek = nil // applied to prevous r if any
	return nil
}

func (c *Client) handshake(conn net.Conn, requestPacket []byte) (*bufio.Reader, error) {
	err := write(conn, requestPacket, c.WireTimeout)
	if err != nil {
		return nil, err
	}

	r := bufio.NewReaderSize(conn, c.BufSize)

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
	case c.connectCtx.Err() != nil:
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

// ReadSlices MUST be invoked consecutively from a single goroutine.
//
// Both message and topic are slices from a read buffer. The bytes stop being
// valid at the next read.
//
// Each invocation acknowledges ownership of the previously returned if any.
// Alternatively, use either Disconnect or Close to prevent a confirmation from
// being send.
func (c *Client) ReadSlices() (message, topic []byte, err error) {
	// skip previous packet, if any
	c.r.Discard(len(c.peek)) // no errors guaranteed
	c.peek = nil             // flush

	if c.readConn == nil {
		if err := c.connect(); err != nil {
			return nil, nil, err
		}
	}

	// acknowledge previous packet, if any
	if c.pendingAck[0] != 0 {
		if c.pendingAck[0]&0xf0 == typePUBREC<<4 {
			// BUG(pascaldekloe): Store errors from a persistence
			// may cause duplicate reception on quality-of-service
			// level 2—breaking the "exactly once guarantee"—when
			// the respective Client goes down before the recovery
			// succeeds.
			key := uint(binary.BigEndian.Uint16(c.pendingAck[2:4])) | remotePacketIDSpace
			err = c.Store.Save(key, net.Buffers{c.pendingAck[:]})
			if err != nil {
				return nil, nil, err
			}
		}
		err := c.write(c.pendingAck[:])
		if err != nil {
			return nil, nil, err
		}
		c.pendingAck[0], c.pendingAck[1], c.pendingAck[2], c.pendingAck[3] = 0, 0, 0, 0
	}

	// process packets until a PUBLISH appears
	for {
		head, err := c.peekPacket()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
				// got interrupted
				if err := c.connect(); err != nil {
					c.readConn = nil
					return nil, nil, err
				}

				continue // with new connection
			}

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

		bytes, err := c.Store.Load(packetID | remotePacketIDSpace)
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
		return nil, nil, fmt.Errorf("%w: PUBLISH with reserved quality-of-service level", errProtoReset)
	}

	return c.peek[i:], topic, nil
}

// OnPUBREL applies the second round trip for “exactly-once” reception.
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
	err := c.Store.Save(packetID|remotePacketIDSpace, net.Buffers{c.pendingAck[:4]})
	if err != nil {
		c.pendingAck[0], c.pendingAck[1], c.pendingAck[2], c.pendingAck[3] = 0, 0, 0, 0
		return err // causes resubmission of PUBREL
	}
	err = c.write(c.pendingAck[:4])
	if err != nil {
		return err // causes resubmission of PUBCOMP
	}
	c.pendingAck[0], c.pendingAck[1], c.pendingAck[2], c.pendingAck[3] = 0, 0, 0, 0
	return nil
}
