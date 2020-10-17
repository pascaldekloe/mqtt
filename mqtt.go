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

// Packet is an encoding buffer.
type packet struct {
	buf []byte
}

func (p *packet) addString(s string) {
	p.buf = append(p.buf, byte(len(s)>>8), byte(len(s)))
	p.buf = append(p.buf, s...)
}

func (p *packet) addBytes(b []byte) {
	p.buf = append(p.buf, byte(len(b)>>8), byte(len(b)))
	p.buf = append(p.buf, b...)
}

// Fixed Packets
var (
	pingPacket    = &packet{[]byte{ping << 4, 0}}
	pongPacket    = &packet{[]byte{pong << 4, 0}}
	disconnPacket = &packet{[]byte{disconn << 4, 0}}
)

var packetPool = sync.Pool{New: func() interface{} { return new(packet) }}

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

// ErrClosed signals an attempt to use the connection after disconnect.
// Network errors that are not temporary [net.Error.Temporary] cause an
// automatic disconnect [net.Conn.Close], conform the MQTT protocol.
var ErrClosed = errors.New("mqtt: connection closed")

// ErrRequestLimit signals too many client requests.
var ErrRequestLimit = errors.New("mqtt: maximum number of pending requests reached")

// Protocol Constraints
var (
	ErrTopicName   = errors.New("mqtt: malformed topic name: null [U+0000] character, illegal UTF-8 sequence or size exceeds 64 kiB")
	ErrMessageSize = errors.New("mqtt: message size exceeds 268,435,451 B minus UTF-8 length of topic name")
)

// QoS defines the delivery quality of service.
type QoS uint

// Quality of Service Levels
const (
	AtMostOnce   = iota // fire and forget
	AtLeastOnce         // network round trip + Persistence
	ExactlyOnce         // two network round trips + Persistence
	reservedQoS3        // must not be used
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

	// Termination contract or disabled when nil.
	Will *Will

	// Timeout in seconds or disabled when zero.
	KeepAlive uint16
}

// Will is a message publication to be send when the
// connection terminates without a disconnect package.
type Will struct {
	// Destination for the message.
	Topic string

	// Actual payload.
	Message []byte

	// Delivery constraints.
	Deliver QoS

	// Persistence constraints.
	Retain bool
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

// Error honors the standard error interface.
func (code connectReturn) Error() string {
	const refuse = "mqtt: connection refused: "

	switch code {
	case accepted:
		return "OK" // not an error
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
