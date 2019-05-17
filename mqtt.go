// Package mqtt implements the Message Queuing Telemetry Transport (MQTT)
// protocol.
//
// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
package mqtt

import (
	"errors"
	"fmt"
)

// ErrClosed signals an attempt to use the connection after disconnect.
// Network errors that are not temporary [net.Error.Temporary] cause an
// automatic disconnect [net.Conn.Close], conform the MQTT protocol.
var ErrClosed = errors.New("mqtt: connection closed")

// QoS is a delivery definition about the quality of service.
type QoS uint

// Quality of Service Levels
const (
	AtMostOnce QoS = iota
	AtLeastOnce
	ExactlyOnce
)

// Control Packet Types
const (
	connReq = iota + 1
	connAck
	pubReq
	pubAck      // QoS 1
	pubReceived // QoS 2, part Ⅰ
	pubRelease  // QoS 2, part Ⅱ
	pubComplete // QoS 2, part Ⅲ
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

// Attributes define the session configuration.
type Attributes struct {
	// Required for the server to uniquely
	// identify the client (session).
	ClientID string

	// Optional identification claim.
	UserName string
	// Optional credentials.
	Password []byte

	// Discards any state at the server.
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

// ConnectReturn is a response code for connect requests.
type connectReturn byte

const (
	accepted connectReturn = iota
	// The Server does not support the level of the MQTT protocol requested
	// by the client.
	ErrProto
	// The Client identifier is correct UTF-8 but not allowed by the Server.
	ErrClientID
	// The Network Connection has been made but the MQTT service is unavailable.
	ErrUnavailable
	// The data in the user name or password is malformed.
	ErrAuthBad
	// The Client is not authorized to connect.
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
