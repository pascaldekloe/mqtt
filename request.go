package mqtt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
)

// ErrMax denies a request on transit capacity, which prevents blocking.
// Ping has a limit of 1 slot. Subscribe and Unsubscribe share their slots.
// PublishAtLeastOnce and PublishExactlyOnce each have a configurable limit.
// The plain Publish has no limit though.
var ErrMax = errors.New("mqtt: maximum number of pending requests reached")

// ErrAbandon gives up on a pending request. Quit channel reception may cause
// ErrAbandon on the ack(nowledge) channel.
var ErrAbandon = errors.New("mqtt: request abandoned while awaiting response")

// BufSize should fit topic names with a bit of overhead.
const bufSize = 128

// BufPool is used to construct packets for submission.
// Append will allocate the appropriate amount on overflows.
// The PUBLISH messages are not copied into these buffers.
var bufPool = sync.Pool{New: func() interface{} { return new([bufSize]byte) }}

// Ping makes a roundtrip to validate the connection.
// Only one request is permitted [ErrMax] at a time.
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
	if err := c.write(packetPINGREQ); err != nil {
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
			return ErrAbandon
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
	// minimise any overlap risks with ErrAbandon cases.
	if len(txs.perPacketID) > unorderedIDMask>>4 {
		return 0, nil, ErrMax
	}

	// Find a free identifier with the sequence counter.
	for {
		packetID = uint16(txs.n&unorderedIDMask | space)
		txs.n++
		if c, ok := txs.perPacketID[packetID]; ok {
			c.done <- ErrAbandon
			delete(txs.perPacketID, packetID)
			// Skip the identifier for now minimise the chance of
			// collision with a very very late response.
			continue
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

func (txs *unorderedTxs) close() {
	txs.Lock()
	defer txs.Unlock()
	for packetID, callback := range txs.perPacketID {
		delete(txs.perPacketID, packetID)
		callback.done <- ErrClosed
	}
}

// Subscribe requests subscription for all topics that match any of the filter
// arguments.
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
			return fmt.Errorf("mqtt: SUBSCRIBE request denied on topic filter: %s", err)
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
	if err = c.write(packet); err != nil {
		c.unorderedTxs.endTx(packetID) // releases slot
		return err
	}
	select {
	case err := <-done:
		return err
	case <-quit:
		c.unorderedTxs.endTx(packetID) // releases slot
		return ErrAbandon
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
	if done == nil { // hopefully due ErrAbandon
		return nil
	}

	// “The SUBACK Packet sent by the Server to the Client MUST contain a
	// return code for each Topic Filter/QoS pair. …”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.8.4-5
	if len(topicFilters) != len(returnCodes) {
		done <- fmt.Errorf("mqtt: %d return codes for SUBSCRIBE with %d topic filters", len(returnCodes), len(topicFilters))
		return errProtoReset
	}

	if failN == 0 {
		close(done)
	} else {
		var err SubscribeError
		for i, code := range returnCodes {
			if code == 0x80 {
				err = append(err, topicFilters[i])
			}
		}
		done <- err
	}
	return nil
}

// Unsubscribe requests subscription cancelation for each of the filter
// arguments.
func (c *Client) Unsubscribe(quit <-chan struct{}, topicFilters ...string) error {
	if len(topicFilters) == 0 {
		return errUnsubscribeNone
	}
	size := 2 + len(topicFilters)*2
	for _, s := range topicFilters {
		size += len(s)
		if err := stringCheck(s); err != nil {
			return fmt.Errorf("mqtt: UNSUBSCRIBE request denied on topic filter: %s", err)
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
	if err = c.write(packet); err != nil {
		c.unorderedTxs.endTx(packetID) // releases slot
		return err
	}
	select {
	case err := <-done:
		return err
	case <-quit:
		c.unorderedTxs.endTx(packetID) // releases slot
		return ErrAbandon
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
	case packetID&^unorderedIDMask != subscribeIDSpace:
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

// Publish delivers the message with an “at most once” guarantee.
// Subscribers may or may not receive the message when subject to error.
// This delivery method is the most efficient option.
func (c *Client) Publish(message []byte, topic string) error {
	return c.publish(message, topic, 0)
}

// PublishRetained is like Publish, but the broker should store the message, so
// that it can be delivered to future subscribers whose subscriptions match the
// topic name. The broker may choose to discard the message at any time though.
// Uppon reception, the broker must discard any message previously retained for
// the topic name.
func (c *Client) PublishRetained(message []byte, topic string) error {
	return c.publish(message, topic, retainFlag)
}

// PublishAtLeastOnce delivers the message with an “at least once” guarantee.
// Subscribers may receive the message more than once when subject to error.
// This delivery method requires a response transmission plus persistence on
// both client-side and broker-side.
//
// Ack(nowledge) is an optional feedback channel. The channel is closed uppon
// receival confirmation by the broker. The channel should have a buffer to
// receive without blocking. A blocked send to ack from PublishAtLeastOnce may
// causes an error encounter from the first request submission to be returned
// instead. Similar, a blocked send from ReadSlices causes resubmission errors
// to be returned instead. Submission of ErrClosed will also not wait for a
// blocking ack. The nil channel always blocks, which effectively disables the
// feature.
func (c *Client) PublishAtLeastOnce(message []byte, topic string, ack chan<- error) error {
	return c.publishAtLeastOnceRetained(message, topic, ack, 0)
}

// PublishAtLeastOnceRetained is like PublishAtLeastOnce, but the broker must
// store the message, so that it can be delivered to future subscribers whose
// subscriptions match the topic name. When a new subscription is established,
// the last retained message, if any, on each matching topic name must be sent
// to the subscriber.
func (c *Client) PublishAtLeastOnceRetained(message []byte, topic string, ack chan<- error) error {
	return c.publishAtLeastOnceRetained(message, topic, ack, retainFlag)
}

// PublishExactlyOnce delivers the message with an “exactly once” guarantee.
// This delivery method eliminates the duplicate-delivery risk from
// PublishAtLeastOnce at the expense of an additional network roundtrip.
func (c *Client) PublishExactlyOnce(message []byte, topic string, ack chan<- error) error {
	return c.publishExactlyOnceRetained(message, topic, ack, 0)
}

// PublishExactlyOnceRetained is like PublishExactlyOnce, but the broker must
// store the message, so that it can be delivered to future subscribers whose
// subscriptions match the topic name. When a new subscription is established,
// the last retained message, if any, on each matching topic name must be sent
// to the subscriber.
func (c *Client) PublishExactlyOnceRetained(message []byte, topic string, ack chan<- error) error {
	return c.publishExactlyOnceRetained(message, topic, ack, retainFlag)
}

func (c *Client) publishAtLeastOnceRetained(message []byte, topic string, ack chan<- error, flags byte) error {
	counter, ok := <-c.atLeastOnceSem
	if !ok {
		return ErrClosed
	}
	if cap(c.ackQ)-len(c.ackQ) == 0 {
		c.exactlyOnceSem <- counter
		return ErrMax
	}
	err := c.publishPersisted(message, topic, counter&publishIDMask|atLeastOnceIDSpace, atLeastOnceLevel<<1|flags, ack, c.ackQ, &counter)
	c.atLeastOnceSem <- counter
	return err
}

func (c *Client) publishExactlyOnceRetained(message []byte, topic string, ack chan<- error, flags byte) error {
	counter, ok := <-c.exactlyOnceSem
	if !ok {
		return ErrClosed
	}
	if cap(c.recQ)-len(c.recQ)-len(c.compQ) == 0 {
		c.exactlyOnceSem <- counter
		return ErrMax
	}
	err := c.publishPersisted(message, topic, counter&publishIDMask|exactlyOnceIDSpace, exactlyOnceLevel<<1|flags, ack, c.recQ, &counter)
	c.exactlyOnceSem <- counter
	return err
}

// ⚠️ Keep synchronised with publishPersisted.
func (c *Client) publish(message []byte, topic string, flags byte) error {
	if err := stringCheck(topic); err != nil {
		return fmt.Errorf("mqtt: PUBLISH denied due topic: %w", err)
	}
	size := 2 + len(topic) + len(message)
	if size < 0 || size > packetMax {
		return fmt.Errorf("mqtt: PUBLISH denied: %w", errPacketMax)
	}

	// build request
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	head := append(buf[:0], typePUBLISH<<4|flags)
	l := uint(size)
	for ; l > 0x7f; l >>= 7 {
		head = append(head, byte(l|0x80))
	}
	head = append(head, byte(l))
	head = append(head, byte(len(topic)>>8), byte(len(topic)))
	head = append(head, topic...)

	// submit
	return c.writeAll(net.Buffers{head, message})
}

// ⚠️ Keep synchronised sync with publish.
func (c *Client) publishPersisted(message []byte, topic string, packetID uint, flags byte, ack chan<- error, q chan<- chan<- error, counter *uint) error {
	if err := stringCheck(topic); err != nil {
		return fmt.Errorf("mqtt: PUBLISH denied due topic: %w", err)
	}
	size := 4 + len(topic) + len(message)
	if size < 0 || size > packetMax {
		return fmt.Errorf("mqtt: PUBLISH denied: %w", errPacketMax)
	}

	// build request
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	head := append(buf[:0], typePUBLISH<<4|flags)
	l := uint(size)
	for ; l > 0x7f; l >>= 7 {
		head = append(head, byte(l|0x80))
	}
	head = append(head, byte(l))
	head = append(head, byte(len(topic)>>8), byte(len(topic)))
	head = append(head, topic...)
	head = append(head, byte(packetID>>8), byte(packetID))
	packet := net.Buffers{head, message}

	err := c.Store.Save(packetID, packet)
	if err != nil {
		return err
	}

	// commit
	q <- ack
	*counter++
	// submit
	if err := c.writeAll(packet); err != nil {
		select {
		case ack <- err:
			break
		default:
			return err
		}
	}
	return nil
}

// OnPUBACK applies the confirm of a PublishAtLeastOnce.
func (c *Client) onPUBACK() error {
	// parse packet
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBACK with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := binary.BigEndian.Uint16(c.peek)
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
	if expect != uint(packetID) {
		return fmt.Errorf("mqtt: PUBACK %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.ackQ))
	}

	// ceil transaction
	err := c.Store.Delete(uint(packetID))
	if err != nil {
		return err // causes resubmission of PUBLISH
	}
	c.orderedTxs.Acked++
	ack := <-c.ackQ
	if ack != nil {
		close(ack)
	}
	return nil
}

// OnPUBREC applies the first confirm of a PublishExactlyOnce.
func (c *Client) onPUBREC() error {
	// parse packet
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBREC with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := binary.BigEndian.Uint16(c.peek)
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
	if uint(packetID) != expect {
		return fmt.Errorf("mqtt: PUBREC %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.recQ))
	}

	// ceil receive with progress to release
	c.pendingAck[0], c.pendingAck[1] = typePUBREL<<4|atLeastOnceLevel<<1, 2
	c.pendingAck[2], c.pendingAck[3] = byte(packetID>>8), byte(packetID)
	err := c.Store.Save(uint(packetID), net.Buffers{c.pendingAck[:4]})
	if err != nil {
		return err // causes resubmission of PUBLISH (from persistence)
	}
	c.orderedTxs.Received++
	c.compQ <- <-c.recQ

	// errors cause resubmission of PUBREL (from persistence)
	err = c.write(c.pendingAck[:4])
	if err != nil {
		return err
	}
	binary.LittleEndian.PutUint32(c.pendingAck[:4], 0) // clear
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
	if uint(packetID) != expect {
		return fmt.Errorf("mqtt: PUBCOMP %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.compQ))
	}

	// ceil transaction
	err := c.Store.Delete(uint(packetID))
	if err != nil {
		return err // causes resubmission of PUBREL (from persistence)
	}
	c.orderedTxs.Completed++
	ack := <-c.compQ
	if ack != nil {
		close(ack)
	}
	return nil
}
