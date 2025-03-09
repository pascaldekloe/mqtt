package mqtt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

// ErrMax denies a request on transit capacity, which prevents the Client from
// blocking. Ping has a limit of 1 slot. Subscribe and Unsubscribe share a large
// number of slots. PublishAtLeastOnce and PublishExactlyOnce each have a limit
// defined by Config. A plain Publish (at most once) has no limit.
var ErrMax = errors.New("mqtt: maximum number of pending requests reached")

// ErrCanceled means that a quit signal got applied before the request was send.
// The transacion never happened, as opposed to ErrAbandoned.
var ErrCanceled = errors.New("mqtt: request canceled before submission")

// ErrAbandoned means that a quit signal got applied after the request was send.
// The broker received the request, yet the result/response remains unknown.
var ErrAbandoned = errors.New("mqtt: request abandoned after submission")

// ErrSubmit signals that the connection was lost during outbound transfer. The
// status of the execution remains unknown, because there is no telling how much
// of the payload actually reached the broker. Connection loss after submision
// causes ErrBreak instead.
var ErrSubmit = errors.New("mqtt: connection lost during submission")

// ErrBreak signals that the connection was lost after a request was send, and
// before a response was received. The status of the execution remains unknown,
// similar to ErrSubmit.
var ErrBreak = errors.New("mqtt: connection lost while awaiting response")

// BufSize should fit topic names with a bit of overhead.
const bufSize = 128

// BufPool is used to construct packets for submission.
// Append will allocate the appropriate amount on overflows.
// The PUBLISH messages are not copied into these buffers.
var bufPool = sync.Pool{New: func() interface{} { return new([bufSize]byte) }}

var denyAndEndErrs = append(append(
	make([]error, 0, len(denyErrs)+len(endErrs)),
	denyErrs...,
),
	endErrs...,
)

// Backoff returns a channel which is closed once the Client can (and possibly
// should) retry the error. The return is nil when retries are not applicable,
// i.e., any IsDeny, IsEnd, or SubscribeError gets a nil channel which blocks.
func (c *Client) Backoff(err error) <-chan struct{} {
	switch {
	case err == nil || nonNilIsAny(err, denyAndEndErrs):
		return nil

	case errors.Is(err, ErrMax):
		const timeout = time.Second / 4

		// shared backoff for too many requests
		for {
			shared := c.backoffOnMax.Load()
			if shared != nil {
				// maybe shared already expired
				select {
				case <-*shared:
					break // replace with new up next
				default:
					return *shared
				}
			}
			// shared nil or expired

			new := make(chan struct{})
			var readEnd <-chan struct{} = new
			if c.backoffOnMax.CompareAndSwap(shared, &readEnd) {
				// launch Timer only within singleton guarantee
				time.AfterFunc(timeout, func() { close(new) })
				return readEnd
			}
			// another goroutine placed a new backoff first
		}

	case errors.As(err, new(SubscribeError)):
		// server failed request
		return nil

	default:
		// connection was down, or it just broke
		return c.Online() // await reconnect
	}
}

// Ping makes a roundtrip to validate the connection. Client allows only one
// ping at a time. Redundant requests get ErrMax.
//
// Quit is optional, as nil just blocks. Appliance of quit will strictly result
// in either ErrCanceled or ErrAbandoned.
func (c *Client) Ping(quit <-chan struct{}) error {
	// install callback
	done := make(chan error, 1)
	select {
	case c.pingAck <- done:
		break // OK
	default:
		return fmt.Errorf("%w; PING unavailable", ErrMax)
	}

	// submit transaction
	if err := c.write(quit, packetPINGREQ); err != nil {
		select {
		case <-c.pingAck: // unlock
		default: // picked up by unrelated pong
		}
		if errors.Is(err, ErrSubmit) {
			return fmt.Errorf("%w; PING in limbo", err)
		}
		return fmt.Errorf("%w; PING not send", err)
	}

	select {
	case err := <-done:
		return err
	case <-quit:
		select {
		case <-c.pingAck: // unlock
			return fmt.Errorf("%w; PING not confirmed", ErrAbandoned)
		default: // picked up in mean time
			return <-done
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
		callback.done <- fmt.Errorf("%w; subscription change not confirmed", ErrBreak)
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

// SubscribeLimitAtMostOnce is like Subscribe, but it limits message reception
// to quality-of-service level 0—fire and forget.
func (c *Client) SubscribeLimitAtMostOnce(quit <-chan struct{}, topicFilters ...string) error {
	return c.subscribeLevel(quit, topicFilters, atMostOnceLevel)
}

// SubscribeLimitAtLeastOnce is like Subscribe, but it limits message reception
// to quality-of-service level 1—acknowledged transfer.
func (c *Client) SubscribeLimitAtLeastOnce(quit <-chan struct{}, topicFilters ...string) error {
	return c.subscribeLevel(quit, topicFilters, atLeastOnceLevel)
}

func (c *Client) subscribeLevel(quit <-chan struct{}, topicFilters []string, levelMax byte) error {
	if len(topicFilters) == 0 {
		return errSubscribeNone
	}
	size := 2 + len(topicFilters)*3
	for _, s := range topicFilters {
		if err := topicCheck(s); err != nil {
			return fmt.Errorf("%w; SUBSCRIBE request denied on topic filter", err)
		}
		size += len(s)
	}
	if size > packetMax {
		return fmt.Errorf("%w; SUBSCRIBE request denied", errPacketMax)
	}

	// slot assignment
	packetID, done, err := c.unorderedTxs.startTx(topicFilters)
	if err != nil {
		return fmt.Errorf("%w; SUBSCRIBE unavailable", err)
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
		if errors.Is(err, ErrSubmit) {
			return fmt.Errorf("%w; SUBSCRIBE in limbo", err)
		}
		return fmt.Errorf("%w; SUBSCRIBE not send", err)
	}

	select {
	case err := <-done:
		return err
	case <-quit:
		c.unorderedTxs.endTx(packetID) // releases slot
		return fmt.Errorf("%w; SUBSCRIBE not confirmed", ErrAbandoned)
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
		if err := topicCheck(s); err != nil {
			return fmt.Errorf("%w; UNSUBSCRIBE request denied on topic filter", err)
		}
	}
	if size > packetMax {
		return fmt.Errorf("%w; UNSUBSCRIBE request denied", errPacketMax)
	}

	// slot assignment
	packetID, done, err := c.unorderedTxs.startTx(nil)
	if err != nil {
		return fmt.Errorf("%w; UNSUBSCRIBE unavailable", err)
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
		if errors.Is(err, ErrSubmit) {
			return fmt.Errorf("%w; UNSUBSCRIBE in limbo", err)
		}
		return fmt.Errorf("%w; UNSUBSCRIBE not send", err)
	}

	select {
	case err := <-done:
		return err
	case <-quit:
		c.unorderedTxs.endTx(packetID) // releases slot
		return fmt.Errorf("%w; UNSUBSCRIBE not confirmed", ErrAbandoned)
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

// Publish delivers the message with an “at most once” guarantee.
// Subscribers may or may not receive the message when subject to error.
// This delivery method is the most efficient option.
//
// Quit is optional, as nil just blocks. Appliance of quit will strictly result
// in ErrCanceled.
func (c *Client) Publish(quit <-chan struct{}, message []byte, topic string) error {
	return c.publish(quit, message, topic, typePUBLISH<<4)
}

// PublishRetained is like Publish, but the broker must store the message, so
// that it can be delivered to future subscribers whose subscriptions match the
// topic name. The broker may choose to discard the message at any time though.
// Uppon reception, the broker must discard any message previously retained for
// the topic name.
func (c *Client) PublishRetained(quit <-chan struct{}, message []byte, topic string) error {
	return c.publish(quit, message, topic, typePUBLISH<<4|retainFlag)
}

func (c *Client) publish(quit <-chan struct{}, message []byte, topic string, head byte) error {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := publishPacket(buf, message, topic, 0, head)
	if err != nil {
		return err
	}

	err = c.writeBuffers(quit, packet)
	if err != nil {
		if errors.Is(err, ErrSubmit) {
			return fmt.Errorf("%w; PUBLISH in limbo", err)
		}
		return fmt.Errorf("%w; PUBLISH not send", err)
	}
	return nil
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
	packet, err := publishPacket(buf, message, topic, atLeastOnceIDSpace, typePUBLISH<<4|atLeastOnceLevel<<1)
	if err != nil {
		return nil, err
	}
	return c.submitPersisted(packet, c.atLeastOnce)
}

// PublishAtLeastOnceRetained is like PublishAtLeastOnce, but the broker must
// store the message, so that it can be delivered to future subscribers whose
// subscriptions match the topic name. When a new subscription is established,
// the last retained message, if any, on each matching topic name must be sent
// to the subscriber.
func (c *Client) PublishAtLeastOnceRetained(message []byte, topic string) (exchange <-chan error, err error) {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := publishPacket(buf, message, topic, atLeastOnceIDSpace, typePUBLISH<<4|atLeastOnceLevel<<1|retainFlag)
	if err != nil {
		return nil, err
	}
	return c.submitPersisted(packet, c.atLeastOnce)
}

// PublishExactlyOnce delivers the message with an “exactly once” guarantee.
// This delivery method eliminates the duplicate-delivery risk from
// PublishAtLeastOnce at the expense of an additional network roundtrip.
func (c *Client) PublishExactlyOnce(message []byte, topic string) (exchange <-chan error, err error) {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := publishPacket(buf, message, topic, exactlyOnceIDSpace, typePUBLISH<<4|exactlyOnceLevel<<1)
	if err != nil {
		return nil, err
	}
	return c.submitPersisted(packet, c.exactlyOnce)
}

// PublishExactlyOnceRetained is like PublishExactlyOnce, but the broker must
// store the message, so that it can be delivered to future subscribers whose
// subscriptions match the topic name. When a new subscription is established,
// the last retained message, if any, on each matching topic name must be sent
// to the subscriber.
func (c *Client) PublishExactlyOnceRetained(message []byte, topic string) (exchange <-chan error, err error) {
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	packet, err := publishPacket(buf, message, topic, exactlyOnceIDSpace, typePUBLISH<<4|exactlyOnceLevel<<1|retainFlag)
	if err != nil {
		return nil, err
	}
	return c.submitPersisted(packet, c.exactlyOnce)
}

func (c *Client) submitPersisted(packet net.Buffers, out outbound) (exchange <-chan error, err error) {
	// lock sequence
	seq, ok := <-out.seqSem
	if !ok {
		return nil, ErrClosed
	}
	defer func() {
		out.seqSem <- seq // unlock with updated
	}()

	hasBacklog := seq.submitN < seq.acceptN

	// persist
	done, err := c.applySeqNoAndEnqueue(packet, seq.acceptN, out)
	if err != nil {
		return nil, err
	}
	seq.acceptN++

	// submit
	if hasBacklog {
		// buffered channel won't block
		done <- fmt.Errorf("%w; PUBLISH enqueued", ErrDown)
	} else {
		err = c.writeBuffersNoWait(packet)
		if err != nil {
			// buffered channel won't block
			done <- fmt.Errorf("%w; PUBLISH enqueued", err)
		} else {
			seq.submitN = seq.acceptN
		}
	}

	return done, nil
}

func (c *Client) applySeqNoAndEnqueue(packet net.Buffers, seqNo uint, out outbound) (done chan error, err error) {
	if cap(out.queue) == len(out.queue) {
		return nil, fmt.Errorf("%w; PUBLISH unavailable", ErrMax)
	}

	// apply sequence number to packet
	buf := packet[0]
	i := len(buf) - 2
	packetID := uint(binary.BigEndian.Uint16(buf[i:]))
	packetID |= seqNo & publishIDMask
	binary.BigEndian.PutUint16(buf[i:], uint16(packetID))

	err = c.persistence.Save(packetID, packet)
	if err != nil {
		return nil, fmt.Errorf("%w; PUBLISH dropped", err)
	}

	done = make(chan error, 2) // receives at most 1 write error + ErrClosed
	out.queue <- done          // won't block due ErrMax check
	return done, nil
}

func publishPacket(buf *[bufSize]byte, message []byte, topic string, packetID uint, head byte) (net.Buffers, error) {
	if err := topicCheck(topic); err != nil {
		return nil, fmt.Errorf("%w; PUBLISH request denied on topic", err)
	}
	size := 2 + len(topic) + len(message)
	if packetID != 0 {
		size += 2
	}
	if size < 0 || size > packetMax {
		return nil, fmt.Errorf("%w; PUBLISH request denied", errPacketMax)
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

// OnPUBACK applies the confirm of a PublishAtLeastOnce.
func (c *Client) onPUBACK() error {
	// parse packet
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBACK with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := uint(binary.BigEndian.Uint16(c.peek))

	// match identifier
	expect := c.orderedTxs.Acked&publishIDMask | atLeastOnceIDSpace
	switch {
	case packetID == 0:
		return errPacketIDZero
	case packetID&^publishIDMask != atLeastOnceIDSpace:
		return errPacketIDSpace
	case expect != packetID:
		return fmt.Errorf("%w: PUBACK %#04x while %#04x next in line", errProtoReset, packetID, expect)
	case len(c.atLeastOnce.queue) == 0:
		return fmt.Errorf("%w: PUBACK precedes PUBLISH", errProtoReset)
	}

	// ceil transaction
	err := c.persistence.Delete(packetID)
	if err != nil {
		return err // causes resubmission of PUBLISH
	}
	c.orderedTxs.Acked++
	close(<-c.atLeastOnce.queue)
	return nil
}

// OnPUBREC applies the first confirm of a PublishExactlyOnce.
func (c *Client) onPUBREC() error {
	// parse packet
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBREC with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := uint(binary.BigEndian.Uint16(c.peek))

	// match identifier
	expect := c.orderedTxs.Received&publishIDMask | exactlyOnceIDSpace
	switch {
	case packetID == 0:
		return errPacketIDZero
	case packetID&^publishIDMask != exactlyOnceIDSpace:
		return errPacketIDSpace
	case packetID != expect:
		return fmt.Errorf("%w: PUBREC %#04x while %#04x next in line", errProtoReset, packetID, expect)
	case int(c.Received-c.Completed) >= len(c.exactlyOnce.queue):
		return fmt.Errorf("%w: PUBREC precedes PUBLISH", errProtoReset)
	}

	// Use pendingAck as a buffer here.
	c.pendingAck = append(c.pendingAck[:0], typePUBREL<<4|atLeastOnceLevel<<1, 2, byte(packetID>>8), byte(packetID))
	err := c.persistence.Save(packetID, net.Buffers{c.pendingAck})
	if err != nil {
		c.pendingAck = c.pendingAck[:0]
		return err // causes resubmission of PUBLISH (from persistence)
	}
	c.orderedTxs.Received++

	err = c.write(nil, c.pendingAck)
	if err != nil {
		return err // keeps pendingAck to retry
	}
	c.pendingAck = c.pendingAck[:0]
	return nil
}

// OnPUBCOMP applies the second (and final) confirm of a PublishExactlyOnce.
func (c *Client) onPUBCOMP() error {
	// parse packet
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: PUBCOMP with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := uint(binary.BigEndian.Uint16(c.peek))

	// match identifier
	expect := c.orderedTxs.Completed&publishIDMask | exactlyOnceIDSpace
	switch {
	case packetID == 0:
		return errPacketIDZero
	case packetID&^publishIDMask != exactlyOnceIDSpace:
		return errPacketIDSpace
	case packetID != expect:
		return fmt.Errorf("%w: PUBCOMP %#04x while %#04x next in line", errProtoReset, packetID, expect)
	case c.orderedTxs.Completed >= c.orderedTxs.Received || len(c.exactlyOnce.queue) == 0:
		return fmt.Errorf("%w: PUBCOMP precedes PUBREL", errProtoReset)
	}

	// ceil transaction
	err := c.persistence.Delete(packetID)
	if err != nil {
		return err // causes resubmission of PUBREL (from Persistence)
	}
	c.orderedTxs.Completed++
	close(<-c.exactlyOnce.queue)
	return nil
}

// InitSession configures the Persistence for first use. Brokers use clientID to
// uniquely identify the session. The session may be continued with AdoptSession
// on another Client.
//
// An error implies either a broken setup or Persistence failure. Connection
// issues, if any, are reported by ReadSlices.
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
//
// An error implies a broken setup. Connection issues, if any, are reported by
// ReadSlices.
func VolatileSession(clientID string, c *Config) (*Client, error) {
	return initSession(clientID, newVolatile(), c)
}

func initSession(clientID string, p Persistence, c *Config) (*Client, error) {
	if err := stringCheck(clientID); err != nil {
		return nil, fmt.Errorf("%w; illegal client identifier", err)
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
//
// A fatal implies either a broken setup or persistence failure. Connection
// issues, if any, are reported by ReadSlices. The Client recovers from corrupt
// states (in Persistence) automatically with warn entries.
func AdoptSession(p Persistence, c *Config) (client *Client, warn []error, fatal error) {
	if err := c.valid(); err != nil {
		return nil, warn, err
	}

	keys, err := p.List()
	if err != nil {
		return nil, warn, err
	}

	// storage includes a sequence number
	storeOrderPerKey := make(map[uint]uint64, len(keys))

	// “When a Client reconnects with CleanSession set to 0, both the Client
	// and Server MUST re-send any unacknowledged PUBLISH Packets (where QoS
	// > 0) and PUBREL Packets using their original Packet Identifiers.”
	// — MQTT Version 3.1.1, conformance statement MQTT-4.4.0-1
	var publishAtLeastOnceKeys, publishExactlyOnceKeys, publishReleaseKeys []uint
	for _, key := range keys {
		if key == clientIDKey || key&remoteIDKeyFlag != 0 {
			continue
		}
		value, err := p.Load(key)
		if err != nil {
			return nil, warn, err
		}

		packet, storageSeqNo, err := decodeValue(value)
		if err != nil {
			delErr := p.Delete(key)
			if delErr != nil {
				warn = append(warn, fmt.Errorf("%w; record %#x not deleted: %w", err, key, delErr))
			} else {
				warn = append(warn, fmt.Errorf("%w; record %#x deleted", err, key))
			}

			continue
		}

		storeOrderPerKey[key] = storageSeqNo

		switch packet[0] >> 4 {
		case typePUBLISH:
			switch key &^ publishIDMask {
			case atLeastOnceIDSpace:
				publishAtLeastOnceKeys = append(publishAtLeastOnceKeys, key)
			case exactlyOnceIDSpace:
				publishExactlyOnceKeys = append(publishExactlyOnceKeys, key)
			}
		case typePUBREL:
			publishReleaseKeys = append(publishReleaseKeys, key)
		}
	}

	// sort by persistence sequence number
	sort.Slice(publishAtLeastOnceKeys, func(i, j int) (less bool) {
		return storeOrderPerKey[publishAtLeastOnceKeys[i]] < storeOrderPerKey[publishAtLeastOnceKeys[j]]
	})
	sort.Slice(publishExactlyOnceKeys, func(i, j int) (less bool) {
		return storeOrderPerKey[publishExactlyOnceKeys[i]] < storeOrderPerKey[publishExactlyOnceKeys[j]]
	})
	sort.Slice(publishReleaseKeys, func(i, j int) (less bool) {
		return storeOrderPerKey[publishReleaseKeys[i]] < storeOrderPerKey[publishReleaseKeys[j]]
	})
	// ensure continuous sequence
	publishAtLeastOnceKeys = cleanSequence(publishAtLeastOnceKeys, "PUBLISH at-least-once", &warn)
	publishExactlyOnceKeys = cleanSequence(publishExactlyOnceKeys, "PUBLISH exactly-once", &warn)
	publishReleaseKeys = cleanSequence(publishReleaseKeys, "PUBREL", &warn)
	if len(publishExactlyOnceKeys) != 0 && len(publishReleaseKeys) != 0 {
		n := publishExactlyOnceKeys[0] & publishIDMask
		p := publishReleaseKeys[len(publishReleaseKeys)-1] & publishIDMask
		if n-p != 1 && !(n == 0 && p == publishIDMask) {
			warn = append(warn, fmt.Errorf("mqtt: PUBREL %#x–%#x dropped ☠️ due gap until PUBLISH %#x",
				publishReleaseKeys[0], publishReleaseKeys[len(publishReleaseKeys)-1], publishExactlyOnceKeys[0]))
		}
	}

	// instantiate client
	if n := len(publishAtLeastOnceKeys); n > c.AtLeastOnceMax {
		return nil, warn, fmt.Errorf("mqtt: %d AtLeastOnceMax is less than the %d pending in session", c.AtLeastOnceMax, n)
	}
	if n := len(publishExactlyOnceKeys) + len(publishReleaseKeys); n > c.ExactlyOnceMax {
		return nil, warn, fmt.Errorf("mqtt: %d ExactlyOnceMax is less than the %d pending in session", c.ExactlyOnceMax, n)
	}
	client = newClient(&ruggedPersistence{Persistence: p}, c)

	// check for outbound publish pending confirmation
	if keys = publishAtLeastOnceKeys; len(keys) != 0 {
		// install sequence counts; txs.Acked < seq.acceptN
		// and: seq.acceptN − txs.Acked ≤ publishIDMask

		client.orderedTxs.Acked = keys[0] & publishIDMask
		last := keys[len(keys)-1] & publishIDMask
		if last < client.orderedTxs.Acked {
			// range overflows address space
			last += publishIDMask + 1
		}
		seq := <-client.atLeastOnce.seqSem
		seq.acceptN = last + 1
		// BUG(pascaldekloe):
		//  AdoptSession assumes that all publish-at-least-once packets
		//  were submitted before already. Persisting the actual state
		//  after each network submission seems like too much just for
		//  the DUP flag to be slightly more precise.
		seq.submitN = seq.acceptN
		client.atLeastOnce.seqSem <- seq
	}

	// check for outbound publish pending confirmation
	if publishKeys, releaseKeys := publishExactlyOnceKeys, publishReleaseKeys; len(publishKeys) != 0 || len(releaseKeys) != 0 {
		// install sequence counts; txs.Completed < seq.acceptN
		// and: txs.Completed ≤ txs.Received ≤ seq.acceptN
		// and: seq.acceptN − txs.Completed ≤ publishIDMask

		txs := &client.orderedTxs
		if len(releaseKeys) == 0 { // implies len(publishKeys) != 0
			txs.Completed = publishKeys[0] & publishIDMask
			txs.Received = txs.Completed
		} else {
			txs.Completed = releaseKeys[0] & publishIDMask
			txs.Received = releaseKeys[len(releaseKeys)-1]&publishIDMask + 1
			if txs.Received < txs.Completed {
				// range overflows address space
				txs.Received += publishIDMask + 1
			}
		}

		var last uint
		if len(publishKeys) != 0 {
			last = publishKeys[len(publishKeys)-1] & publishIDMask
		} else {
			last = releaseKeys[len(releaseKeys)-1] & publishIDMask
		}
		if last < txs.Received {
			// range overflows address space
			last += publishIDMask + 1
		}
		seq := <-client.exactlyOnce.seqSem
		seq.acceptN = last + 1
		// BUG(pascaldekloe):
		//  AdoptSession assumes that all publish-exactly-once packets
		//  were submitted before already. Persisting the actual state
		//  after each network submission seems like too much just for
		//  the DUP flag to be slightly more precise.
		seq.submitN = seq.acceptN
		client.exactlyOnce.seqSem <- seq
	}

	// install callback placeholders; won't block due Max check above
	for range publishAtLeastOnceKeys {
		client.atLeastOnce.queue <- make(chan<- error, 1)
	}
	for range publishExactlyOnceKeys {
		client.exactlyOnce.queue <- make(chan<- error, 1)
	}
	for range publishReleaseKeys {
		client.exactlyOnce.queue <- make(chan<- error, 1)
	}

	return client, warn, nil
}

func cleanSequence(keys []uint, name string, warn *[]error) []uint {
	for i := 1; i < len(keys); i++ {
		n := keys[i] & publishIDMask
		p := keys[i-1] & publishIDMask
		if n-p == 1 || n == 0 && p == publishIDMask {
			continue
		}

		*warn = append(*warn, fmt.Errorf("mqtt: %s %#x–%#x dropped ☠️ due gap until %#x", name, keys[0], keys[i-1], keys[i]))

		keys = keys[i:]
		i = 0
	}
	return keys
}
