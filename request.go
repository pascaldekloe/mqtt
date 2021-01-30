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

// ErrAbort signals reception on a quit channel after sending the request.
var ErrAbort = errors.New("mqtt: request aborted while awaiting response")

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
			return ErrAbort
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

// SubscribeError holds one or more topic filters which were failed by the broker.
// The element order matches the originating request's.
type SubscribeError []string

// Error implements the standard error interface.
func (e SubscribeError) Error() string {
	return fmt.Sprintf("mqtt: broker failed %d topic filters", len(e))
}

type subscription struct {
	sync.Mutex
	n       uint                             // counter is allowed to overflow
	transit map[uint16]*subscriptionCallback // topic filters per packet identifier
}

type subscriptionCallback struct {
	done         chan<- error
	topicFilters []string
}

func (subs *subscription) startTx(done chan<- error, topicFilters []string) (packetID uint16, err error) {
	subs.Lock()
	defer subs.Unlock()
	// apply arbitrary high limit
	if len(subs.transit) > 255 {
		return 0, ErrMax
	}
	for {
		packetID = uint16(subs.n&packetIDMask | subscriptionIDSpace)
		subs.n++
		if _, ok := subs.transit[packetID]; ok {
			continue // pick another identifier
		}
		subs.transit[packetID] = &subscriptionCallback{
			topicFilters: topicFilters,
			done:         done,
		}
		return packetID, nil
	}
}

// EndTx releases the packet identifier.
// The caller must either send to done (or resubmit with startTx).
func (subs *subscription) endTx(packetID uint16) (done chan<- error, topicFilters []string) {
	subs.Lock()
	defer subs.Unlock()
	callback := subs.transit[packetID]
	delete(subs.transit, packetID)
	return callback.done, callback.topicFilters
}

func (subs *subscription) close() {
	subs.Lock()
	defer subs.Unlock()
	for packetID, callback := range subs.transit {
		delete(subs.transit, packetID)
		callback.done <- ErrClosed
	}
}

// Subscribe requests subscription for all topics that match any of the filter
// arguments.
func (c *Client) Subscribe(quit <-chan struct{}, topicFilters ...string) error {
	return c.subscribeQOS(quit, topicFilters, exactlyOnceLevel)
}

// SubscribeLimitAtMostOnce is like Subscribe, but limits the message reception
// to quality-of-service level 0: fire-and-forget.
func (c *Client) SubscribeLimitAtMostOnce(quit <-chan struct{}, topicFilters ...string) error {
	return c.subscribeQOS(quit, topicFilters, atMostOnceLevel)
}

// SubscribeLimitAtLeastOnce is like Subscribe, but limits the message reception
// to quality-of-service level 1: acknowledged transfer.
func (c *Client) SubscribeLimitAtLeastOnce(quit <-chan struct{}, topicFilters ...string) error {
	return c.subscribeQOS(quit, topicFilters, atLeastOnceLevel)
}

func (c *Client) subscribeQOS(quit <-chan struct{}, topicFilters []string, levelMax byte) error {
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

	// start transaction
	done := make(chan error, 1)
	packetID, err := c.subscription.startTx(done, topicFilters)
	if err != nil {
		return err
	}

	// compose request
	buf := bufPool.Get().(*[bufSize]byte)
	defer bufPool.Put(buf)
	// header
	packet := append(buf[:0], typeSUBSCRIBE<<4|atLeastOnceLevel<<1)
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
		packet = append(packet, levelMax)
	}

	// submit
	if err = c.write(packet); err != nil {
		c.subscription.endTx(packetID)
		return err
	}
	select {
	case err := <-done:
		return err
	case <-quit:
		c.subscription.endTx(packetID)
		return ErrAbort
	}
}

func (c *Client) onSUBACK() error {
	if len(c.peek) < 3 {
		return fmt.Errorf("%w: SUBACK with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := binary.BigEndian.Uint16(c.peek)
	if packetID == 0 {
		return errPacketIDZero
	}
	done, topicFilters := c.subscription.endTx(packetID)
	switch {
	case done == nil:
		return nil // tolerate wandering SUBACK

	// “The SUBACK Packet sent by the Server to the Client MUST contain a
	// return code for each Topic Filter/QoS pair. …”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.8.4-5
	case len(topicFilters) == len(c.peek)-2:
		break

	default:
		return fmt.Errorf("%w: SUBACK with %d return codes, requested %d topic filters", errProtoReset, len(c.peek)-2, len(topicFilters))
	}

	var err SubscribeError
	for i, returnCode := range c.peek[2:] {
		// “The order of Reason Codes in the SUBACK packet MUST match
		// the order of Topic Filters in the SUBSCRIBE packet.”
		// — MQTT Version 3.1.1, conformance statement MQTT-3.9.3-1
		switch returnCode {
		case atMostOnceLevel, atLeastOnceLevel, exactlyOnceLevel:
			break // assumes correct level
		case 0x80:
			err = append(err, topicFilters[i])
		default:
			return fmt.Errorf("%w: SUBACK with illegal return code %#02x", errProtoReset, returnCode)
		}
	}

	if len(err) != 0 {
		done <- err
	} else {
		close(done)
	}
	return nil
}

// Unsubscribe requests subscription cancelation for each of the filter
// arguments.
//
// The request will continue if the return is a SendError. An ErrMax should be
// retried with some backoff only. All other error return options imply illegal
// invocation—the same arguments will produce the same error again.
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

	// start transaction
	done := make(chan error, 1)
	packetID, err := c.subscription.startTx(done, topicFilters)
	if err != nil {
		return err
	}

	// compose request packet
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

	// submit
	if err = c.write(packet); err != nil {
		c.subscription.endTx(packetID)
		return err
	}
	select {
	case err := <-done:
		return err
	case <-quit:
		c.subscription.endTx(packetID)
		return ErrAbort
	}
}

func (c *Client) onUNSUBACK() error {
	if len(c.peek) != 2 {
		return fmt.Errorf("%w: UNSUBACK with %d byte remaining length", errProtoReset, len(c.peek))
	}
	packetID := binary.BigEndian.Uint16(c.peek)
	if packetID == 0 {
		return errPacketIDZero
	}
	done, _ := c.subscription.endTx(packetID)
	if done != nil {
		close(done)
	}
	return nil
}

// TxOut tracks outbound transactions.
// The counters are allowed to overflow.
type txOut struct {
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
	err := c.publishPersisted(message, topic, counter&packetIDMask|atLeastOnceIDSpace, atLeastOnceLevel<<1|flags, ack, c.ackQ, &counter)
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
	err := c.publishPersisted(message, topic, counter&packetIDMask|exactlyOnceIDSpace, exactlyOnceLevel<<1|flags, ack, c.recQ, &counter)
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

	err := c.Store.Save(packetID|localPacketIDSpace, packet)
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
	packetID := uint(binary.BigEndian.Uint16(c.peek))
	if packetID == 0 {
		return errPacketIDZero
	}
	if len(c.ackQ) == 0 {
		return fmt.Errorf("%w: wandering PUBACK %#04x", errProtoReset, packetID)
	}

	// match identifier
	expect := c.txOut.Acked&packetIDMask | atLeastOnceIDSpace
	if packetID != expect {
		return fmt.Errorf("mqtt: PUBACK %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.ackQ))
	}

	// ceil transaction
	err := c.Store.Delete(packetID | localPacketIDSpace)
	if err != nil {
		return err // causes resubmission of PUBLISH
	}
	c.txOut.Acked++
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
	packetID := uint(binary.BigEndian.Uint16(c.peek))
	if packetID == 0 {
		return errPacketIDZero
	}
	if len(c.recQ) == 0 {
		return fmt.Errorf("%w: wandering PUBREC %#04x", errProtoReset, packetID)
	}

	// match identifier
	expect := c.txOut.Received&packetIDMask | exactlyOnceIDSpace
	if packetID != expect {
		return fmt.Errorf("mqtt: PUBREC %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.recQ))
	}

	// ceil receive with progress to release
	c.pendingAck[0], c.pendingAck[1] = typePUBREL<<4|atLeastOnceLevel<<1, 2
	c.pendingAck[2], c.pendingAck[3] = byte(packetID>>8), byte(packetID)
	err := c.Store.Save(packetID|localPacketIDSpace, net.Buffers{c.pendingAck[:4]})
	if err != nil {
		return err // causes resubmission of PUBLISH (from persistence)
	}
	c.txOut.Received++
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
	if packetID == 0 {
		return errPacketIDZero
	}
	if len(c.compQ) == 0 {
		return nil // tolerate wandering PUBCOMP
	}

	// match identifier
	expect := c.txOut.Completed&packetIDMask | exactlyOnceIDSpace
	if packetID != expect {
		return fmt.Errorf("mqtt: PUBCOMP %#04x while %#04x is next in line (of %d)", packetID, expect, len(c.compQ))
	}

	// ceil transaction
	err := c.Store.Delete(packetID | localPacketIDSpace)
	if err != nil {
		return err // causes resubmission of PUBREL (from persistence)
	}
	c.txOut.Completed++
	ack := <-c.compQ
	if ack != nil {
		close(ack)
	}
	return nil
}
