// Package mqtt implements the Message Queuing Telemetry Transport (MQTT)
// protocol.
//
// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
package mqtt

import (
	"errors"
	"fmt"
	"net"
	"sync"
)

// Fixed Packets
var (
	pingPacket    = []byte{ping << 4, 0}
	pongPacket    = []byte{pong << 4, 0}
	disconnPacket = []byte{disconn << 4, 0}
)

const (
	packetMax = 268_435_455 // 4-byte varint
	stringMax = 65535       // 16-bit size prefixes
)

var (
	errPacketMax = errors.New("mqtt: packet limit of 256 MiB exceeded")
	errStringMax = errors.New("mqtt: string exceeds 65535 bytes")

	errUTF8 = errors.New("mqtt: invalid UTF-8 byte sequence")
	errNull = errors.New("mqtt: string contains null characer")
)

func stringCheck(s string) error {
	if len(s) > stringMax {
		return errStringMax
	}
	for _, r := range s {
		// “The character data in a UTF-8 encoded string MUST be
		// well-formed UTF-8 as defined by the Unicode specification …”
		// — MQTT Version 3.1.1, conformance statement MQTT-1.5.3-1
		if r == '\uFFFD' {
			return errUTF8
		}

		// “A UTF-8 encoded string MUST NOT include an encoding of the
		// null character U+0000.”
		// — MQTT Version 3.1.1, conformance statement MQTT-1.5.3-2
		if r == 0 {
			return errNull
		}
	}
	return nil
}

// ErrRequestLimit signals too many client requests.
var ErrRequestLimit = errors.New("mqtt: maximum number of pending requests reached")

// Protocol Constraints
var (
	ErrTopicName   = errors.New("mqtt: malformed topic name: null [U+0000] character, illegal UTF-8 sequence or size exceeds 64 kiB")
	ErrMessageSize = errors.New("mqtt: message size exceeds 268,435,451 B minus UTF-8 length of topic name")
)

// Quality of Service (QoS) are defined as numeric levels.
const (
	atMostOnce  = iota // fire and forget
	atLeastOnce        // network round trip + Persistence
	exactlyOnce        // two network round trips + Persistence
)

// Control Packet Types
const (
	connReq = iota + 1
	connAck
	pubMsg
	pubAck      // QoS 1 confirm
	pubReceived // QoS 2 confirm, part Ⅰ
	pubRelease  // QoS 2 confirm, part Ⅱ
	pubComplete // QoS 2 confirm, part Ⅲ
	subReq
	subAck
	unsubReq
	unsubAck
	ping
	pong
	disconn
)

// Publication Flags
const (
	// Indicates that this might be re-delivery of an earlier attempt.
	dupeFlag = 8
	// Store the application message and its QoS, so that it can be
	// delivered to future subscribers.
	retainFlag = 1
)

// SessionConfig defines protocol settings.
type SessionConfig struct {
	// Required for the server to uniquely identify the session.
	ClientID string

	// Optional identification claim.
	UserName string
	// Optional credentials.
	Password []byte

	// Discard any previous session (on the server) and start a new one.
	CleanSession bool

	// Will is a message publication to be send when the
	// connection terminates without a disconnect package.
	Will struct {
		// Destination for the message.
		Topic string

		// The Will feature is disabled when nil.
		Message []byte

		// Persistence constraints.
		Retain bool

		// Delivery constraints.
		AtLeastOnce bool // raises the QoS to level 1
		ExactlyOnce bool // raises the QoS to level 2
	}

	// Timeout in seconds or disabled when zero.
	KeepAlive uint16
}

// Valid returns a protocol constraint violation, if any.
func (c *SessionConfig) Valid() error {
	if err := stringCheck(c.ClientID); err != nil {
		return fmt.Errorf("mqtt: illegal client identifier: %w", err)
	}
	if err := stringCheck(c.UserName); err != nil {
		return fmt.Errorf("mqtt: illegal user name: %w", err)
	}
	if len(c.Password) > stringMax {
		return fmt.Errorf("mqtt: password exceeds %d bytes", stringMax)
	}
	if c.Will.Message != nil {
		// MQTT Version 3.1.1, conformance statement MQTT-3.1.3-10
		if err := stringCheck(c.Will.Topic); err != nil {
			return fmt.Errorf("mqtt: illegal will topic: %w", err)
		}
		if len(c.Will.Message) > stringMax {
			return fmt.Errorf("mqtt: will message exceeds %d bytes", stringMax)
		}
	}
	return nil
}

func (c *SessionConfig) appendConnReq(dst []byte) []byte {
	size := 6 + 1 + 1 + 2 // protocol name + protocol level + connect flags + keep alive
	size += 2 + len(c.ClientID)
	var flags uint
	if c.UserName != "" {
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
			flags |= exactlyOnce << 3
		case c.Will.AtLeastOnce:
			flags |= atLeastOnce << 3
		}
		flags |= 1 << 2
	}
	if c.CleanSession {
		flags |= 1 << 1
	}

	// encode packet
	dst = append(dst, connReq<<4)
	for size > 127 {
		dst = append(dst, byte(size|128))
		size >>= 7
	}
	dst = append(dst, byte(size),
		0, 4, 'M', 'Q', 'T', 'T', 4, byte(flags),
		byte(c.KeepAlive>>8), byte(c.KeepAlive),
		byte(len(c.ClientID)>>8), byte(len(c.ClientID)),
	)
	dst = append(dst, c.ClientID...)
	if c.Will.Message != nil {
		dst = append(dst, byte(len(c.Will.Topic)>>8), byte(len(c.Will.Topic)))
		dst = append(dst, c.Will.Topic...)
		dst = append(dst, byte(len(c.Will.Message)>>8), byte(len(c.Will.Message)))
		dst = append(dst, c.Will.Message...)
	}
	if c.UserName != "" {
		dst = append(dst, byte(len(c.UserName)>>8), byte(len(c.UserName)))
		dst = append(dst, c.UserName...)
	}
	if c.Password != nil {
		dst = append(dst, byte(len(c.Password)>>8), byte(len(c.Password)))
		dst = append(dst, c.Password...)
	}
	return dst
}

// LocalPacketIDFlag marks the key in the a local address space.
const localPacketIDFlag = 1 << 16

// Persistence abstracts session storage, for continuation inbetween connects.
// Content is addressed by a 17-bit key (range 0 to 131071).
type Persistence interface {
	// Resolves data under the key. Malfunction may cause submission delay.
	Load(key uint) ([]byte, error)

	// Upserts data under the key. Malfunction may cause resubmission with
	// OnceOrMore or ExactlyOnce deliveries. The Client Publish operation
	// fails entirely on Persist errors.
	Store(key uint, data net.Buffers) error

	// Removes data under the key. Malfunction may threaten the availability
	// of Persist in terms of storage space.
	Delete(key uint) error

	// Enumerates available data. Malfunction may prevent new connection
	// establishment when an existing session is continued!
	List() (keys []uint, err error)
}

// ConnectReturn is a response code for connect requests.
type connectReturn byte

const (
	accepted connectReturn = iota

	// ErrProto means that the server does not support the level of the MQTT
	// protocol requested by the client.
	ErrProto

	// ErrClientID means that the client identifier is correct UTF-8 but not
	// allowed by the Server.
	ErrClientID

	// ErrUnavailable means that the network connection has been made but
	// the MQTT service is unavailable.
	ErrUnavailable

	// ErrAuthBad means that the data in the user name or password is
	// malformed.
	ErrAuthBad

	// ErrAuth means that the client is not authorized to connect.
	ErrAuth
)

// Error implements the standard error interface.
func (code connectReturn) Error() string {
	const refuse = "mqtt: connection refused: "

	switch code {
	case accepted:
		panic("connect return 0 [accepted] used as an error")
	case ErrProto:
		return refuse + "unacceptable protocol version"
	case ErrClientID:
		return refuse + "client identifier rejected"
	case ErrUnavailable:
		return refuse + "server unavailable"
	case ErrAuthBad:
		return refuse + "malformed user name or password"
	case ErrAuth:
		return refuse + "not authorized"
	default:
		return fmt.Sprintf(refuse+"reserved return code %d", code)
	}
}

// NewVolatile returns a new implementation, which lives in memory only.
// On the bright side, all volatile operations are entirely error free.
func NewVolatile() Persistence {
	return &volatile{perKey: make(map[uint][]byte)}
}

type volatile struct {
	sync.Mutex
	perKey map[uint][]byte
}

// List implements the Persistence interface.
func (m *volatile) Load(key uint) ([]byte, error) {
	m.Lock()
	defer m.Unlock()
	return m.perKey[key], nil
}

// List implements the Persistence interface.
func (m *volatile) Store(key uint, data net.Buffers) error {
	var n int
	for _, b := range data {
		n += len(b)
	}
	buf := make([]byte, n)

	i := 0
	for _, b := range data {
		copy(buf[i:], b)
		i += len(b)
	}

	m.Lock()
	defer m.Unlock()
	m.perKey[key] = buf
	return nil
}

// List implements the Persistence interface.
func (m *volatile) Delete(key uint) error {
	m.Lock()
	defer m.Unlock()
	delete(m.perKey, key)
	return nil
}

// List implements the Persistence interface.
func (m *volatile) List() (keys []uint, err error) {
	m.Lock()
	defer m.Unlock()
	keys = make([]uint, 0, len(m.perKey))
	for k := range m.perKey {
		keys = append(keys, k)
	}
	return
}
