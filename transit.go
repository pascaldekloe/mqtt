package mqtt

import (
	"fmt"
	"sync/atomic"
)

// SubscrptionPacketID fixes the packet identifier for subscribe and unsubscribe
// requests. Only one request can be in use at a time.
const subscriptionPacketID = 0x0001

// AtLeastOnceLine manages packet identifiers for publications with QoS level 1.
// Only 255 identifiers can be in use at a time (mask 0xFF00, excluding zero).
type atLeastOnceLine uint64

// AssignID returns the next packet identifier in line.
func (line *atLeastOnceLine) AssignID() (packetID uint) {
	u := atomic.LoadUint64((*uint64)(line))

	// “… PUBLISH (in cases where QoS > 0) Control Packets MUST contain a
	// non-zero 16-bit Packet Identifier”
	// — MQTT Version 3.1.1, conformance statement MQTT-2.3.1-1
	if u&0xff == 0 {
		u++ // skip zero and move to one
	}
	packetID = uint(u&0xff) << 8

	// Increment to next identifier with a 31-bit overflow.
	atomic.StoreUint64((*uint64)(line), (u+1)&^(1<<31))

	return packetID
}

// FreeID reuses the oldest packet identifier from AssignID.
//
// “… MUST send PUBACK packets in the order in which the corresponding
// PUBLISH packets were received”
// — MQTT Version 3.1.1, conformance statement MQTT-4.6.0-2
func (line *atLeastOnceLine) FreeID(packetID uint) error {
	u := atomic.LoadUint64((*uint64)(line))

	// Normalise both the offset (in the 32 most significant bits)
	// an the next in line (in the 32 least significant bits).
	if u&0xff == 0 {
		u++ // skip zero and move to one
	}
	if u&(0xff<<32) == 0 {
		u += 1 << 32 // skip zero and move to one
	}
	if u>>32 == u&(1<<32-1) { // offset equals next in line
		return nil // tolerate ␆ without any pending requests
	}

	expect := uint(u>>24) & 0xff00
	if packetID == expect {
		// Increment identifier offset with a 31-bit overflow.
		atomic.StoreUint64((*uint64)(line), (u+1<<32)&^(1<<63))
		return nil // happy flow
	}

	// figure out what's wrong
	switch next := uint(u<<8) & 0xff00; {
	case packetID&0x00ff != 0: // not in use for AtLeastOnce
		return fmt.Errorf("mqtt: publish at least once ␆ %#04x never send—wrong range", packetID)
	case expect < next:
		if packetID < expect || packetID >= next {
			return nil // tolerate ␆ not in line
		}
	case expect > next: // 8-bit overflow */
		if packetID < expect && packetID >= next {
			return nil // tolerate ␆ not in line
		}
	}
	return fmt.Errorf("mqtt: publish at least once ␆ %#04x skipped ␆ %#04x next in line", packetID, expect)
}

// ExactlyOnceLine manages packet identifiers for publications with QoS level 2.
// Only 127 identifiers can be in use at a time (mask 0x00FE, excluding zero).
type exactlyOnceLine uint64

// AssignID returns the next packet identifier in line.
func (line *exactlyOnceLine) AssignID() (packetID uint) {
	u := atomic.LoadUint64((*uint64)(line))

	// “… PUBLISH (in cases where QoS > 0) Control Packets MUST contain a
	// non-zero 16-bit Packet Identifier”
	// — MQTT Version 3.1.1, conformance statement MQTT-2.3.1-1
	if u&0x7f == 0 {
		u++ // skip zero and move to one
	}
	packetID = uint(u&0x7f) << 1

	// Increment to next identifier with a 31-bit overflow.
	atomic.StoreUint64((*uint64)(line), (u+1)&^(1<<31))

	return packetID
}

// ReleaseID releases the oldest packet identifier from AssignID.
//
// “… MUST send PUBREC packets in the order in which the corresponding
// PUBLISH packets were received”
// — MQTT Version 3.1.1, conformance statement MQTT-4.6.0-3
func (line *exactlyOnceLine) ReleaseID(packetID uint) error {
	panic("TODO")
}

// FreeID reuses the oldest packet identifier from ReleaseID.
//
// “… MUST send PUBREL packets in the order in which the corresponding
// PUBREC packets were received”
// — MQTT Version 3.1.1, conformance statement MQTT-4.6.0-4
func (line *exactlyOnceLine) FreeID(packetID uint) error {
	u := atomic.LoadUint64((*uint64)(line))

	// Normalise both the offset (in the 32 most significant bits)
	// an the next in line (in the 32 least significant bits).
	if u&0x7f == 0 {
		u++ // skip zero and move to one
	}
	if u&(0x7f<<32) == 0 {
		u += 1 << 32 // skip zero and move to one
	}
	if u>>32 == u&(1<<32-1) { // offset equals next in line
		return nil // tolerate ␆ without any pending requests
	}

	expect := uint(u>>31) & 0x00fe
	if packetID == expect {
		// Increment identifier offset with a 31-bit overflow.
		atomic.StoreUint64((*uint64)(line), (u+1<<32)&^(1<<63))
		return nil // happy flow
	}

	// figure out what's wrong
	switch next := uint(u<<1) & 0x00fe; {
	case packetID&0x00ff != 0: // not in use for AtLeastOnce
		return fmt.Errorf("mqtt: publish at least once ␆ %#04x never send—wrong range", packetID)
	case expect < next:
		if packetID < expect || packetID >= next {
			return nil // tolerate ␆ not in line
		}
	case expect > next: // 8-bit overflow */
		if packetID < expect && packetID >= next {
			return nil // tolerate ␆ not in line
		}
	}
	return fmt.Errorf("mqtt: publish at least once ␆ %#04x skipped ␆ %#04x next in line", packetID, expect)
}
