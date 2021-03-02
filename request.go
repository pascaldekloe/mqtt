package mqtt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
)

// ErrMax denies a request on transit capacity, which prevents blocking.
// Ping has a limit of 1 slot. Subscribe and Unsubscribe share their slots.
// PublishAtLeastOnce and PublishExactlyOnce each have a configurable limit.
// The plain Publish has no limit though.
var ErrMax = errors.New("mqtt: maximum number of pending requests reached")

// ErrCanceled means that a quit signal got applied before the request was send.
// The transacion never happened, as opposed to ErrAbandoned.
var ErrCanceled = errors.New("mqtt: request canceled before submission")

// ErrAbandoned means that a quit signal got applied after the request was send.
// The broker received the request, yet the result/reponse remains unkown.
var ErrAbandoned = errors.New("mqtt: request abandoned after submission")

// ErrBreak means that the connection broke up after the request was send.
// The broker received the request, yet the result/reponse remains unkown.
var ErrBreak = errors.New("mqtt: connection lost while awaiting response")

// BufSize should fit topic names with a bit of overhead.
const bufSize = 128

// BufPool is used to construct packets for submission.
// Append will allocate the appropriate amount on overflows.
// The PUBLISH messages are not copied into these buffers.
var bufPool = sync.Pool{New: func() interface{} { return new([bufSize]byte) }}

// Ping makes a roundtrip to validate the connection.
// Only one request is permitted [ErrMax] at a time.
//
// Quit is optional, as nil just blocks. Appliance of quit will strictly result
// in either ErrCanceled or ErrAbandoned.
func (c *Client) Ping(quit <-chan struct{}) error {
	// install callback
	ch := make(chan error, 1)
	select {
	case c.pingAck <- ch:
		break // OK
	default:
		return ErrMax
	}

	// submit transaction
	if err := c.write(quit, packetPINGREQ); err != nil {
		select {
		case <-c.pingAck: // unlock
		default: // picked up by unrelated pong
		}
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-quit:
		select {
		case <-c.pingAck: // unlock
			return ErrAbandoned
		default: // picked up in mean time
			return <-ch
		}
	}
}

func (c *Client) onPINGRESP() error {
	if len(c.peek) != 0 {
		return fmt.Errorf("%w: PINGRESP with %d byte remaining length", errProtoReset, len(c.peek))
	}
	select {
	case ack := <-c.pingAck:
		close(ack)
	default:
		break // tolerates wandering pong
	}
	return nil
}

// SubscribeError holds one or more topic filters which were failed by the broker.
// The element order matches the originating request's.
type SubscribeError []string

// Error implements the standard error interface.
func (e SubscribeError) Error() string {
	return fmt.Sprintf("mqtt: broker failed %d topic filters", len(e))
}

// Reject no-ops to prevent programming mistakes.
var (
	// “The payload of a SUBSCRIBE packet MUST contain at least one
	// Topic Filter / QoS pair. A SUBSCRIBE packet with no payload
	// is a protocol violation.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.8.3-3
	errSubscribeNone = errors.New("mqtt: SUBSCRIBE without topic filters denied")

	// “The Payload of an UNSUBSCRIBE packet MUST contain at least
	// one Topic Filter. An UNSUBSCRIBE packet with no payload is a
	// protocol violation.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.10.3-2
	errUnsubscribeNone = errors.New("mqtt: UNSUBSCRIBE without topic filters denied")
)

// A total for four types of client requests require a 16-bit packet identifier,
// namely SUBSCRIBE, UNSUBSCRIBE and PUBLISH at-least-once or exactly-once.
// The outbound identifiers are assigned in segments per type. The non-zero
// prefixes/spaces also prevent use of the reserved packet identifier zero.
const (
	// A 14-bit address space allows for up to 16,384 pending transactions.
	publishIDMask = 0x3fff
	// The most-significant bit flags an ordered transaction for publish.
	// The second most-significant bit distinguises the QOS level.
	atLeastOnceIDSpace = 0x8000
	exactlyOnceIDSpace = 0xc000

	// A 13-bit address space allows for up to 8,192 pending transactions.
	unorderedIDMask    = 0x1fff
	subscribeIDSpace   = 0x6000
	unsubscribeIDSpace = 0x4000
)

// ErrPacketIDSpace signals a response packet with an identifier outside of the
// respective address spaces, defined by subscribeIDSpace, unsubscribeIDSpace,
// atLeastOnceIDSpace and exactlyOnceIDSpace. This extra check has a potential
// to detect corruptions which would otherwise go unnoticed.
var errPacketIDSpace = fmt.Errorf("%w: packet ID space mismatch", errProtoReset)

// UnorderedTxs tracks outbound transactions without sequence contraints.
type unorderedTxs struct {
	sync.Mutex
	n           uint                         // counter is permitted to overflow
	perPacketID map[uint16]unorderedCallback // transit state
}

type unorderedCallback struct {
	done         chan<- error
	topicFilters []string
}

// StartTx assigns a slot for either a subscribe or an unsubscribe.
// The filter slice is nil for unsubscribes only.
func (txs *unorderedTxs) startTx(topicFilters []string) (packetID uint16, done <-chan error, err error) {
	var space uint
	if topicFilters == nil {
		space = unsubscribeIDSpace
	} else {
		space = subscribeIDSpace
	}

	// Only one response error can be applied on done.
	ch := make(chan error, 1)

	txs.Lock()
	defer txs.Unlock()

	// By using only a small window of the actual space we
	// minimise any overlap risks with ErrAbandoned cases.
	if len(txs.perPacketID) > unorderedIDMask>>4 {
		return 0, nil, ErrMax
	}

	// Find a free identifier with the sequence counter.
	for {
		packetID = uint16(txs.n&unorderedIDMask | space)
		txs.n++
		if _, ok := txs.perPacketID[packetID]; ok {
			// Such collision indicates a very late response.
			continue // just skips the identifier
		}
		txs.perPacketID[packetID] = unorderedCallback{
			topicFilters: topicFilters,
			done:         ch,
		}
		return packetID, ch, nil
	}
}

// EndTx releases a slot. The filter slice is nil for unsubscribe requests.
func (txs *unorderedTxs) endTx(packetID uint16) (done chan<- error, topicFilters []string) {
	txs.Lock()
	defer txs.Unlock()
	callback := txs.perPacketID[packetID]
	delete(txs.perPacketID, packetID)
	return callback.done, callback.topicFilters
}

func (txs *unorderedTxs) breakAll() {
	txs.Lock()
	defer txs.Unlock()
	for packetID, callback := range txs.perPacketID {
		delete(txs.perPacketID, packetID)
		callback.done <- ErrBreak
	}
}

// Subscribe requests subscription for all topics that match any of the filter
// arguments.
//
// Quit is optional, as nil just blocks. Appliance of quit will strictly result
// in either ErrCanceled or ErrAbandoned.
func (c *Client) Subscribe(quit <-chan struct{}, topicFilters ...string) error {
	return c.subscribeLevel(quit, topicFilters, exactlyOnceLevel)
}

// SubscribeLimitAtMostOnce is like Subscribe, but limits the message reception
// to quality-of-service level 0: fire-and-forget.
func (c *Client) SubscribeLimitAtMostOnce(quit <-chan struct{}, topicFilters ...string) error {
	return c.subscribeLevel(quit, topicFilters, atMostOnceLevel)
}

// SubscribeLimitAtLeastOnce is like Subscribe, but limits the message reception
// to quality-of-service level 1: acknowledged transfer.
func (c *Client) SubscribeLimitAtLeastOnce(quit <-chan struct{}, topicFilters ...string) error {
	return c.subscribeLevel(quit, topicFilters, atLeastOnceLevel)
}

func (c *Client) subscribeLevel(quit <-chan struct{}, topicFilters []string, levelMax byte) error {
	if len(topicFilters) == 0 {
		return errSubscribeNone
	}
	size := 2 + len(topicFilters)*3
	for _, s := range topicFilters {
		if err := stringCheck(s); err != nil {
			return fmt.Errorf("mqtt: SUBSCRIBE request denied on topic filter: %w", err)
		}
		size += len(s)
	}
	if size > packetMax {
		return fmt.Errorf("mqtt: SUBSCRIBE request denied: %w", errPacketMax)
	}

	// slot assignment
	packetID, done, err := c.unorderedTxs.startTx(topicFilters)
	if err != nil {
		return err
	}

	// request packet composition
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet := append(buf[:0], typeSUBSCRIBE<<4|atLeastOnceLevel<<1)
	l := uint(size)
	for ; l > 0x7f; l >>= 7 {
		packet = append(packet, byte(l|0x80))
	}
	packet = append(packet, byte(l))
	packet = append(packet, byte(packetID>>8), byte(packetID))
	for _, s := range topicFilters {
		packet = append(packet, byte(len(s)>>8), byte(len(s)))
		packet = append(packet, s...)
		packet = append(packet, levelMax)
	}

	// network submission
	if err = c.write(quit, packet); err != nil {
		c.unorderedTxs.endTx(packetID) // releases slot
		return err
	}
	select {
	case err := <-done:
		return err
	case <-quit:
		c.unorderedTxs.endTx(packetID) // releases slot
		return ErrAbandoned
	}
}

func (c *Client) onSUBACK() error {
	if len(c.peek) < 3 {
		return fmt.Errorf("%w: SUBACK with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := binary.BigEndian.Uint16(c.peek)
	switch {
	case packetID == 0:
		return errPacketIDZero
	case packetID&^unorderedIDMask != subscribeIDSpace:
		return errPacketIDSpace
	}

	returnCodes := c.peek[2:]
	var failN int
	for _, code := range returnCodes {
		switch code {
		case atMostOnceLevel, atLeastOnceLevel, exactlyOnceLevel:
			break
		case 0x80:
			failN++
		default:
			return fmt.Errorf("%w: SUBACK with illegal return code %#02x", errProtoReset, code)
		}
	}

	// commit
	done, topicFilters := c.unorderedTxs.endTx(packetID)
	if done == nil { // hopefully due ErrAbandoned
		return nil
	}

	// “The SUBACK Packet sent by the Server to the Client MUST contain a
	// return code for each Topic Filter/QoS pair. …”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.8.4-5
	if len(topicFilters) != len(returnCodes) {
		done <- fmt.Errorf("mqtt: %d return codes for SUBSCRIBE with %d topic filters", len(returnCodes), len(topicFilters))
		return errProtoReset
	}

	if failN != 0 {
		var err SubscribeError
		for i, code := range returnCodes {
			if code == 0x80 {
				err = append(err, topicFilters[i])
			}
		}
		done <- err
	}
	close(done)
	return nil
}

// Unsubscribe requests subscription cancelation for each of the filter
// arguments.
//
// Quit is optional, as nil just blocks. Appliance of quit will strictly result
// in either ErrCanceled or ErrAbandoned.
func (c *Client) Unsubscribe(quit <-chan struct{}, topicFilters ...string) error {
	if len(topicFilters) == 0 {
		return errUnsubscribeNone
	}
	size := 2 + len(topicFilters)*2
	for _, s := range topicFilters {
		size += len(s)
		if err := stringCheck(s); err != nil {
			return fmt.Errorf("mqtt: UNSUBSCRIBE request denied on topic filter: %w", err)
		}
	}
	if size > packetMax {
		return fmt.Errorf("mqtt: UNSUBSCRIBE request denied: %w", errPacketMax)
	}

	// slot assignment
	packetID, done, err := c.unorderedTxs.startTx(nil)
	if err != nil {
		return err
	}

	// request packet composition
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	// header
	packet := append(buf[:0], typeUNSUBSCRIBE<<4|atLeastOnceLevel<<1)
	l := uint(size)
	for ; l > 0x7f; l >>= 7 {
		packet = append(packet, byte(l|0x80))
	}
	packet = append(packet, byte(l))
	packet = append(packet, byte(packetID>>8), byte(packetID))
	// payload
	for _, s := range topicFilters {
		packet = append(packet, byte(len(s)>>8), byte(len(s)))
		packet = append(packet, s...)
	}

	// network submission
	if err = c.write(quit, packet); err != nil {
		c.unorderedTxs.endTx(packetID) // releases slot
		return err
	}
	select {
	case err := <-done:
		return err
	case <-quit:
		c.unorderedTxs.endTx(packetID) // releases slot
		return ErrAbandoned
	}
}

func (c *Client) onUNSUBACK() error {
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: UNSUBACK with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := binary.BigEndian.Uint16(c.peek)
	switch {
	case packetID == 0:
		return errPacketIDZero
	case packetID&^unorderedIDMask != unsubscribeIDSpace:
		return errPacketIDSpace
	}
	done, _ := c.unorderedTxs.endTx(packetID)
	if done != nil {
		close(done)
	}
	return nil
}

// OrderedTxs tracks outbound transactions with sequence constraints.
// The counters are allowed to overflow.
type orderedTxs struct {
	Acked     uint // confirm count for PublishAtLeastOnce
	Received  uint // confirm count 1/2 for PublishExactlyOnce
	Completed uint // confirm count 2/2 for PublishExactlyOnce
}

type holdup struct {
	SinceSeqNo uint // oldest entry
	UntilSeqNo uint // latest entry
}

// Publish delivers the message with an “at most once” guarantee.
// Subscribers may or may not receive the message when subject to error.
// This delivery method is the most efficient option.
//
// Quit is optional, as nil just blocks. Appliance of quit will strictly result
// in ErrCanceled.
func (c *Client) Publish(quit <-chan struct{}, message []byte, topic string) error {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := appendPublishPacket(buf, message, topic, 0, typePUBLISH<<4)
	if err != nil {
		return err
	}
	return c.writeAll(quit, packet)
}

// PublishRetained is like Publish, but the broker should store the message, so
// that it can be delivered to future subscribers whose subscriptions match the
// topic name. The broker may choose to discard the message at any time though.
// Uppon reception, the broker must discard any message previously retained for
// the topic name.
func (c *Client) PublishRetained(quit <-chan struct{}, message []byte, topic string) error {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := appendPublishPacket(buf, message, topic, 0, typePUBLISH<<4|retainFlag)
	if err != nil {
		return err
	}
	return c.writeAll(quit, packet)
}

// PublishAtLeastOnce delivers the message with an “at least once” guarantee.
// Subscribers may receive the message more than once when subject to error.
// This delivery method requires a response transmission plus persistence on
// both client-side and broker-side.
//
// The exchange channel is closed uppon receival confirmation by the broker.
// ErrClosed leaves the channel blocked (with no further input).
func (c *Client) PublishAtLeastOnce(message []byte, topic string) (exchange <-chan error, err error) {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := appendPublishPacket(buf, message, topic, atLeastOnceIDSpace, typePUBLISH<<4|atLeastOnceLevel<<1)
	if err != nil {
		return nil, err
	}
	return c.submitPersisted(packet, c.atLeastOnceSem, c.ackQ, nil, c.atLeastOnceBlock)
}

// PublishAtLeastOnceRetained is like PublishAtLeastOnce, but the broker must
// store the message, so that it can be delivered to future subscribers whose
// subscriptions match the topic name. When a new subscription is established,
// the last retained message, if any, on each matching topic name must be sent
// to the subscriber.
func (c *Client) PublishAtLeastOnceRetained(message []byte, topic string) (exchange <-chan error, err error) {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := appendPublishPacket(buf, message, topic, atLeastOnceIDSpace, typePUBLISH<<4|atLeastOnceLevel<<1|retainFlag)
	if err != nil {
		return nil, err
	}
	return c.submitPersisted(packet, c.atLeastOnceSem, c.ackQ, nil, c.atLeastOnceBlock)
}

// PublishExactlyOnce delivers the message with an “exactly once” guarantee.
// This delivery method eliminates the duplicate-delivery risk from
// PublishAtLeastOnce at the expense of an additional network roundtrip.
func (c *Client) PublishExactlyOnce(message []byte, topic string) (exchange <-chan error, err error) {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := appendPublishPacket(buf, message, topic, exactlyOnceIDSpace, typePUBLISH<<4|exactlyOnceLevel<<1)
	if err != nil {
		return nil, err
	}
	return c.submitPersisted(packet, c.exactlyOnceSem, c.recQ, c.compQ, c.exactlyOnceBlock)
}

// PublishExactlyOnceRetained is like PublishExactlyOnce, but the broker must
// store the message, so that it can be delivered to future subscribers whose
// subscriptions match the topic name. When a new subscription is established,
// the last retained message, if any, on each matching topic name must be sent
// to the subscriber.
func (c *Client) PublishExactlyOnceRetained(message []byte, topic string) (exchange <-chan error, err error) {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := appendPublishPacket(buf, message, topic, exactlyOnceIDSpace, typePUBLISH<<4|exactlyOnceLevel<<1|retainFlag)
	if err != nil {
		return nil, err
	}
	return c.submitPersisted(packet, c.exactlyOnceSem, c.recQ, c.compQ, c.exactlyOnceBlock)
}

func (c *Client) submitPersisted(packet net.Buffers, sem chan uint, ackQ, ackQ2 chan chan<- error, block chan holdup) (exchange <-chan error, err error) {
	done := make(chan error, 2) // receives at most 1 write error + ErrClosed
	select {
	case counter, ok := <-sem:
		if !ok {
			return nil, ErrClosed
		}
		if cap(ackQ) == len(ackQ)+len(ackQ2) {
			sem <- counter // unlock
			return nil, ErrMax
		}
		packetID := applyPublishSeqNo(packet, counter)
		err = c.persistence.Save(packetID, packet)
		if err != nil {
			sem <- counter // unlock
			return nil, err
		}
		ackQ <- done // won't block due ErrMax check
		switch err := c.writeAll(c.Offline(), packet); {
		case err == nil:
			sem <- counter + 1
		case errors.Is(err, ErrCanceled), errors.Is(err, ErrDown):
			// don't report down
			block <- holdup{SinceSeqNo: counter, UntilSeqNo: counter}
		default:
			done <- err
			block <- holdup{SinceSeqNo: counter, UntilSeqNo: counter}
		}

	case holdup := <-block:
		if cap(ackQ) == len(ackQ)+len(ackQ2) {
			block <- holdup // unlock
			return nil, ErrMax
		}
		packetID := applyPublishSeqNo(packet, holdup.UntilSeqNo+1)
		err = c.persistence.Save(packetID, packet)
		if err != nil {
			block <- holdup // unlock
			return nil, err
		}
		ackQ <- done // won't block due ErrMax check
		holdup.UntilSeqNo++
		block <- holdup
	}

	return done, nil
}

func appendPublishPacket(buf *[bufSize]byte, message []byte, topic string, packetID uint, head byte) (net.Buffers, error) {
	if err := stringCheck(topic); err != nil {
		return nil, fmt.Errorf("mqtt: PUBLISH request denied due topic: %w", err)
	}
	size := 2 + len(topic) + len(message)
	if packetID != 0 {
		size += 2
	}
	if size < 0 || size > packetMax {
		return nil, fmt.Errorf("mqtt: PUBLISH request denied: %w", errPacketMax)
	}

	packet := append(buf[:0], head)
	l := uint(size)
	for ; l > 0x7f; l >>= 7 {
		packet = append(packet, byte(l|0x80))
	}
	packet = append(packet, byte(l))
	packet = append(packet, byte(len(topic)>>8), byte(len(topic)))
	packet = append(packet, topic...)
	if packetID != 0 {
		packet = append(packet, byte(packetID>>8), byte(packetID))
	}
	return net.Buffers{packet, message}, nil
}

// ApplyPublishSeqNo applies a sequence number to a appendPublishPublishPacket
// composition.
func applyPublishSeqNo(packet net.Buffers, seqNo uint) (packetID uint) {
	buf := packet[0]
	i := len(buf) - 2
	packetID = uint(binary.BigEndian.Uint16(buf[i:]))
	packetID |= seqNo & publishIDMask
	binary.BigEndian.PutUint16(buf[i:], uint16(packetID))
	return packetID
}

// OnPUBACK applies the confirm of a PublishAtLeastOnce.
func (c *Client) onPUBACK() error {
	// parse packet
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBACK with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := uint(binary.BigEndian.Uint16(c.peek))
	switch {
	case packetID == 0:
		return errPacketIDZero
	case packetID&^publishIDMask != atLeastOnceIDSpace:
		return errPacketIDSpace
	}

	// match identifier
	if len(c.ackQ) == 0 {
		return nil // tolerates wandering PUBACK
	}
	expect := c.orderedTxs.Acked&publishIDMask | atLeastOnceIDSpace
	if expect != packetID {
		return fmt.Errorf("mqtt: PUBACK %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.ackQ))
	}

	// ceil transaction
	err := c.persistence.Delete(packetID)
	if err != nil {
		return err // causes resubmission of PUBLISH
	}
	c.orderedTxs.Acked++
	close(<-c.ackQ)
	return nil
}

// OnPUBREC applies the first confirm of a PublishExactlyOnce.
func (c *Client) onPUBREC() error {
	// parse packet
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBREC with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := uint(binary.BigEndian.Uint16(c.peek))
	switch {
	case packetID == 0:
		return errPacketIDZero
	case packetID&^publishIDMask != exactlyOnceIDSpace:
		return errPacketIDSpace
	}

	// match identifier
	if len(c.recQ) == 0 {
		return nil // tolerates wandering PUBREC
	}
	expect := c.orderedTxs.Received&publishIDMask | exactlyOnceIDSpace
	if packetID != expect {
		return fmt.Errorf("mqtt: PUBREC %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.recQ))
	}

	// ceil receive with progress to release
	c.readBuf[0], c.readBuf[1] = typePUBREL<<4|atLeastOnceLevel<<1, 2
	c.readBuf[2], c.readBuf[3] = byte(packetID>>8), byte(packetID)
	c.pendingAck = c.readBuf[:4]
	err := c.persistence.Save(packetID, net.Buffers{c.pendingAck})
	if err != nil {
		return err // causes resubmission of PUBLISH (from persistence)
	}
	c.orderedTxs.Received++
	c.compQ <- <-c.recQ

	// errors cause resubmission of PUBREL (from persistence)
	err = c.write(nil, c.pendingAck)
	if err != nil {
		return err
	}
	c.pendingAck = nil
	return nil
}

// OnPUBCOMP applies the second (and final) confirm of a PublishExactlyOnce.
func (c *Client) onPUBCOMP() error {
	// parse packet
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBCOMP with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := uint(binary.BigEndian.Uint16(c.peek))
	switch {
	case packetID == 0:
		return errPacketIDZero
	case packetID&^publishIDMask != exactlyOnceIDSpace:
		return errPacketIDSpace
	}

	// match identifier
	if len(c.compQ) == 0 {
		return nil // tolerates wandering PUBCOMP
	}
	expect := c.orderedTxs.Completed&publishIDMask | exactlyOnceIDSpace
	if packetID != expect {
		return fmt.Errorf("mqtt: PUBCOMP %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.compQ))
	}

	// ceil transaction
	err := c.persistence.Delete(packetID)
	if err != nil {
		return err // causes resubmission of PUBREL (from persistence)
	}
	c.orderedTxs.Completed++
	close(<-c.compQ)
	return nil
}

// InitSession configures the Persistence for first use. Brokers use clientID to
// uniquely identify the session. The session may be continued with AdoptSession
// on another Client.
func InitSession(clientID string, p Persistence, c *Config) (*Client, error) {
	return initSession(clientID, &ruggedPersistence{Persistence: p}, c)
}

// VolatileSession operates solely in-memory. This setup is recommended for
// delivery with the “at most once” guarantee [Publish], and for reception
// without the “exactly once” guarantee [SubscribeLimitAtLeastOnce], and for
// testing.
//
// Brokers use clientID to uniquely identify the session. Volatile sessions may
// be continued by using the same clientID again. Use CleanSession to prevent
// reuse of an existing state.
func VolatileSession(clientID string, c *Config) (*Client, error) {
	return initSession(clientID, newVolatile(), c)
}

func initSession(clientID string, p Persistence, c *Config) (*Client, error) {
	if err := stringCheck(clientID); err != nil {
		return nil, fmt.Errorf("mqtt: illegal client identifier: %w", err)
	}
	if err := c.valid(); err != nil {
		return nil, err
	}

	// empty check
	keys, err := p.List()
	if err != nil {
		return nil, err
	}
	if len(keys) != 0 {
		return nil, errors.New("mqtt: init on non-empty persistence")
	}

	// install
	err = p.Save(clientIDKey, net.Buffers{[]byte(clientID)})
	if err != nil {
		return nil, err
	}

	return newClient(p, c), nil
}

// AdoptSession continues with a Persistence which had an InitSession already.
func AdoptSession(p Persistence, c *Config) (client *Client, warn []error, fatal error) {
	if err := c.valid(); err != nil {
		return nil, warn, err
	}

	keys, err := p.List()
	if err != nil {
		return nil, warn, err
	}

	// collect local packet identifiers
	seqNos := make(seqNos, len(keys))
	keyPerSeqNo := make(map[uint64]uint, len(keys))
	PUBRELPerKey := make(map[uint][]byte)
	for _, key := range keys {
		if key == clientIDKey || key&remoteIDKeyFlag != 0 {
			continue
		}
		value, err := p.Load(key)
		if err != nil {
			return nil, warn, err
		}

		switch packet, seqNo, ok := decodeValue(value); {
		case !ok:
			err := p.Delete(key)
			if err != nil {
				warn = append(warn, fmt.Errorf("mqtt: corrupt persistence record %#x not deleted: %w", key, err))
			} else {
				warn = append(warn, fmt.Errorf("mqtt: corrupt persistence record %#x deleted", key))
			}

		case len(packet) == 0:
			err := p.Delete(key)
			if err != nil {
				warn = append(warn, fmt.Errorf("mqtt: somehow empty persistence record %#x not deleted: %w", key, err))
			} else {
				warn = append(warn, fmt.Errorf("mqtt: somehow empty persistence record %#x deleted", key))
			}

		default:
			seqNos = append(seqNos, seqNo)
			keyPerSeqNo[seqNo] = key
			if packet[0]>>4 == typePUBREL {
				PUBRELPerKey[key] = packet
			}
		}
	}

	var atLeastOnceKeys, exactlyOnceKeys []uint
	sort.Sort(seqNos)
	for _, seqNo := range seqNos {
		key := keyPerSeqNo[seqNo]
		switch key &^ publishIDMask {
		case atLeastOnceIDSpace:
			atLeastOnceKeys = append(atLeastOnceKeys, key)
		case exactlyOnceIDSpace:
			exactlyOnceKeys = append(exactlyOnceKeys, key)
		}
	}
	atLeastOnceKeys = cleanSeq(atLeastOnceKeys, "at-least-once", p, &warn)
	exactlyOnceKeys = cleanSeq(exactlyOnceKeys, "exactly-once", p, &warn)

	switch {
	case len(atLeastOnceKeys) > c.AtLeastOnceMax:
		return nil, warn, fmt.Errorf("mqtt: %d AtLeastOnceMax is less than %d pending from Persistence", c.AtLeastOnceMax, len(atLeastOnceKeys))
	case len(exactlyOnceKeys) > c.ExactlyOnceMax:
		return nil, warn, fmt.Errorf("mqtt: %d ExactlyOnceMax is less than %d pending from Persistence", c.ExactlyOnceMax, len(exactlyOnceKeys))
	}
	client = newClient(&ruggedPersistence{Persistence: p}, c)

	// “When a Client reconnects with CleanSession set to 0, both the Client
	// and Server MUST re-send any unacknowledged PUBLISH Packets (where QoS
	// > 0) and PUBREL Packets using their original Packet Identifiers.”
	// — MQTT Version 3.1.1, conformance statement MQTT-4.4.0-1

	if len(atLeastOnceKeys) != 0 {
		for range atLeastOnceKeys {
			client.ackQ <- make(chan<- error, 1) // won't block due Max check above
		}
		<-client.atLeastOnceSem
		client.atLeastOnceBlock <- holdup{
			SinceSeqNo: atLeastOnceKeys[0],
			UntilSeqNo: atLeastOnceKeys[len(atLeastOnceKeys)-1],
		}
	}

	if len(exactlyOnceKeys) != 0 {
		var pubN int
		for i, key := range exactlyOnceKeys {
			packet, ok := PUBRELPerKey[key]
			if !ok {
				pubN = len(exactlyOnceKeys) - i
				break
			}
			// send all those 4-byte packets in one goal
			client.pendingAck = append(client.pendingAck, packet...)
			client.compQ <- make(chan<- error, 1) // won't block due Max check above
		}
		<-client.exactlyOnceSem
		if pubN == 0 {
			client.exactlyOnceSem <- exactlyOnceKeys[len(exactlyOnceKeys)-1] + 1
		} else {
			client.exactlyOnceBlock <- holdup{
				SinceSeqNo: exactlyOnceKeys[len(exactlyOnceKeys)-pubN],
				UntilSeqNo: exactlyOnceKeys[len(exactlyOnceKeys)-1],
			}
		}
	}

	return client, warn, nil
}

// SeqNos sorts ruggedPersistence sequence numbers chronologicaly.
type seqNos []uint64

// Len implements sort.Interface.
func (a seqNos) Len() int { return len(a) }

// Less implements sort.Interface.
func (a seqNos) Less(i, j int) bool { return a[i] < a[j] }

// Swap implements sort.Interface.
func (a seqNos) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// CleanSeq returns the last uninterrupted sequence from keys.
func cleanSeq(keys []uint, name string, p Persistence, warn *[]error) (cleanKeys []uint) {
	for len(keys) != 0 {
		last := keys[0]
		for i := 1; ; i++ {
			if i >= len(keys) {
				return keys
			}
			key := keys[i]

			if key&publishIDMask == (last+1)&publishIDMask {
				last = key
				continue // correct followup
			}

			*warn = append(*warn, fmt.Errorf("mqtt: %s persistence records %#x–%#x lost due gap until %#x, caused by delete or save failure", name, keys[0], last, key))
			for _, key := range keys[:i] {
				err := p.Delete(key)
				if err != nil {
					*warn = append(*warn, fmt.Errorf("mqtt: persistence record %#v not deleted: %w", key, err))
				}
			}
			keys = keys[i:]
			break
		}
	}
	return nil
}
