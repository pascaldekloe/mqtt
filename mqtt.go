// Package mqtt implements the Message Queuing Telemetry Transport (MQTT)
// protocol.
//
// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
package mqtt

import "fmt"

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
	pubAck
	pubReceived
	pubRelease
	pubComplete
	subReq
	subAck
	unsubReq
	unsubAck
	ping
	pong
	disconn
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
type ConnectReturn byte

// The standard defines 6 response codes.
const (
	Accepted ConnectReturn = iota
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
func (code ConnectReturn) Error() string {
	const refuse = "mqtt: connection refused: "

	switch code {
	case Accepted:
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
