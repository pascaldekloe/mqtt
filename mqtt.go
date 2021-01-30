// Package mqtt provides a client for the Message Queuing Telemetry Transport
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

// Control packets have a 4-bit type code in the first byte.
const (
	typeRESERVED0 = iota
	typeCONNECT
	typeCONNACK
	typePUBLISH
	typePUBACK  // QOS level 1 confirm
	typePUBREC  // QOS level 2 confirm, part Ⅰ
	typePUBREL  // QOS level 2 confirm, part Ⅱ
	typePUBCOMP // QOS level 2 confirm, part Ⅲ
	typeSUBSCRIBE
	typeSUBACK
	typeUNSUBSCRIBE
	typeUNSUBACK
	typePINGREQ
	typePINGRESP
	typeDISCONNECT
	typeRESERVED15
)

// The quality-of-service is defined as a numeric level.
const (
	atMostOnceLevel  = iota // fire and forget
	atLeastOnceLevel        // network round trip + persistence
	exactlyOnceLevel        // two network round trips + persistence
)

// PUBLISH packets may have flags in the header.
const (
	dupeFlag   = 0b1000 // retry of an earlier attempt
	retainFlag = 0b0001 // store for future subscribers
)

// Packets without payload are kept static.
var (
	packetDISCONNECT = []byte{typeDISCONNECT << 4, 0}
	packetPINGREQ    = []byte{typePINGREQ << 4, 0}
)

// Capacity limitations are defined by their respective size prefix.
const (
	// See MQTT Version 3.1.1, table 2.4: “Size of Remaining Length field”.
	packetMax = 1<<(4*7) - 1 // 4-byte varint

	// “Unless stated otherwise all UTF-8 encoded strings can have any
	// length in the range 0 to 65535 bytes.”
	// — MQTT Version 3.1.1, section 1.5.3
	stringMax = 1<<16 - 1 // 16-bit size prefixes
)

// Validation errors are expected to be prefixed according to the context.
var (
	// ErrPacketMax enforces packetMax.
	errPacketMax = errors.New("packet payload exceeds 256 MiB")
	// ErrStringMax enforces stringMax.
	errStringMax = errors.New("string exceeds 64 KiB")

	errUTF8 = errors.New("invalid UTF-8 byte sequence")
	errNull = errors.New("string contains null character")
)

// Validation errors are expected to be prefixed according to the context.
func stringCheck(s string) error {
	if len(s) > stringMax {
		return errStringMax
	}
	for _, r := range s {
		switch r {
		// “The character data in a UTF-8 encoded string MUST be
		// well-formed UTF-8 as defined by the Unicode specification
		// and restated in RFC 3629.”
		// — MQTT Version 3.1.1, conformance statement MQTT-1.5.3-1
		case '\uFFFD':
			return errUTF8

		// “A UTF-8 encoded string MUST NOT include an encoding of the
		// null character U+0000.”
		// — MQTT Version 3.1.1, conformance statement MQTT-1.5.3-2
		case 0:
			return errNull
		}
	}
	return nil
}

// IsDeny returns whether the request was rejected by a Client based on
// validation constraints. The rejection is permanent in such case. Another
// invocation with the same arguments will result in the same error again.
func IsDeny(err error) bool {
	for err != nil {
		switch err {
		case errPacketMax, errStringMax, errUTF8, errNull,
			errSubscribeNone, errUnsubscribeNone:
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// SessionConfig is a configuration for the connection state.
// Clients can't share a configuration because they can't share the Store.
type SessionConfig struct {
	// Store persists a session. Use CleanSession to reset the content.
	Store

	// The user name may be used by the broker for authentication and/or
	// authorization purposes. An empty string omits the option, except
	// for when password is not nil.
	UserName string
	Password []byte // option omitted when nil

	// The Will Message is published when the connection terminates
	// without Disconnect. A nil Message disables the Will option.
	Will struct {
		Topic   string // destination
		Message []byte // payload

		Retain      bool // see PublishRetained
		AtLeastOnce bool // see PublishAtLeastOnce
		ExactlyOnce bool // overrides AtLeastOnce
	}

	KeepAlive uint16 // timeout in seconds (disabled with zero)

	// Brokers must resume communications with the client (identified by
	// ClientID) when CleanSession is false. Otherwise, brokers must create
	// a new session when either CleanSession is true or when no session is
	// associated to the client identifier.
	CleanSession bool
}

// NewVolatileSessionConfig returns a new configuration with a Store which
// resides in memory only. This configuration is only recommended for testing or
// deliveries with the “at most once” guarantee [Publish] or subscriptions
// without the “exactly once” guarantee [SubscribeLimitAtLeastOnce].
// Brokers use clientID to uniquely identify the session.
func NewVolatileSessionConfig(clientID string) *SessionConfig {
	return &SessionConfig{Store: newVolatile(clientID)}
}

// NewSessionConfig returns a new configuration for the Store. Brokers use the
// clientID to uniquely identify the session. If the Store was not used before,
// then the content is initialized for clientID. Otherwise clientID must match
// the initialization.
//
// A Store can serve only one Client at a time!
func NewSessionConfig(clientID string, store Store) (*SessionConfig, error) {
	if err := stringCheck(clientID); err != nil {
		return nil, fmt.Errorf("mqtt: illegal client identifier: %w", err)
	}

	bytes, err := store.Load(clientIDKey)
	switch {
	case err != nil:
		return nil, err
	case bytes == nil:
		err := store.Save(clientIDKey, net.Buffers{[]byte(clientID)})
		if err != nil {
			return nil, err
		}
	case string(bytes) != clientID:
		return nil, fmt.Errorf("mqtt: store mismatch on client identifier %q", bytes)
	}

	return &SessionConfig{Store: store}, nil
}

// NewCONNREQ returns a new packet.
func (c *SessionConfig) newCONNREQ() ([]byte, error) {
	clientID, err := c.Load(clientIDKey)
	if err != nil {
		return nil, err
	}
	err = stringCheck(string(clientID))
	if err != nil {
		return nil, fmt.Errorf("mqtt: illegal client identifier: %w", err)
	}

	size := 12 + len(clientID)
	var flags uint

	if err := stringCheck(c.UserName); err != nil {
		return nil, fmt.Errorf("mqtt: illegal user name: %w", err)
	}
	// Supply an empty user name when the password is set to comply with “If
	// the User Name Flag is set to 0, the Password Flag MUST be set to 0.”
	// — MQTT Version 3.1.1, conformance statement MQTT-3.1.2-22
	if c.UserName != "" || c.Password != nil {
		size += 2 + len(c.UserName)
		flags |= 1 << 7
	}

	if len(c.Password) > stringMax {
		return nil, fmt.Errorf("mqtt: password exceeds %d bytes", stringMax)
	}
	if c.Password != nil {
		size += 2 + len(c.Password)
		flags |= 1 << 6
	}

	if c.Will.Message != nil {
		if err := stringCheck(c.Will.Topic); err != nil {
			return nil, fmt.Errorf("mqtt: illegal will topic: %w", err)
		}
		if len(c.Will.Message) > stringMax {
			return nil, fmt.Errorf("mqtt: will message exceeds %d bytes", stringMax)
		}
		size += 4 + len(c.Will.Topic) + len(c.Will.Message)
		if c.Will.Retain {
			flags |= 1 << 5
		}
		switch {
		case c.Will.ExactlyOnce:
			flags |= exactlyOnceLevel << 3
		case c.Will.AtLeastOnce:
			flags |= atLeastOnceLevel << 3
		}
		flags |= 1 << 2
	}

	if c.CleanSession {
		flags |= 1 << 1
	}

	// encode packet
	packet := make([]byte, 0, size+2)
	packet = append(packet, typeCONNECT<<4)
	l := uint(size)
	for ; l > 0x7f; l >>= 7 {
		packet = append(packet, byte(l|0x80))
	}
	packet = append(packet, byte(l),
		0, 4, 'M', 'Q', 'T', 'T', 4, byte(flags),
		byte(c.KeepAlive>>8), byte(c.KeepAlive),
		byte(len(clientID)>>8), byte(len(clientID)),
	)
	packet = append(packet, clientID...)
	if c.Will.Message != nil {
		packet = append(packet, byte(len(c.Will.Topic)>>8), byte(len(c.Will.Topic)))
		packet = append(packet, c.Will.Topic...)
		packet = append(packet, byte(len(c.Will.Message)>>8), byte(len(c.Will.Message)))
		packet = append(packet, c.Will.Message...)
	}
	if c.UserName != "" || c.Password != nil {
		packet = append(packet, byte(len(c.UserName)>>8), byte(len(c.UserName)))
		packet = append(packet, c.UserName...)
	}
	if c.Password != nil {
		packet = append(packet, byte(len(c.Password)>>8), byte(len(c.Password)))
		packet = append(packet, c.Password...)
	}
	return packet, nil
}

// ConnectReturn is the response code CONNACK.
type connectReturn byte

// Connect return errors are predefined reasons for a broker to deny a session.
const (
	accepted connectReturn = iota

	// ErrProtocolLevel means that the server does not support the level of
	// the MQTT protocol requested by the Client.
	ErrProtocolLevel

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
		return "mqtt: connect return code 0 “Connection Accepted” used as an error"
	case ErrProtocolLevel:
		return refuse + "unacceptable protocol version"
	case ErrClientID:
		return refuse + "client identifier rejected"
	case ErrUnavailable:
		return refuse + "server unavailable"
	case ErrAuthBad:
		return refuse + "bad user name or password"
	case ErrAuth:
		return refuse + "not authorized"
	default:
		return fmt.Sprintf(refuse+"connect return code %d reserved for future use", code)
	}
}

// Store keys correspond to MQTT packet identifiers.
const (
	// Packet identifier zero is not in use by the protocol.
	clientIDKey = 0

	// The most-significant bit distinguishes between local and the remote registry.
	localPacketIDSpace  = 0 << 16
	remotePacketIDSpace = 1 << 16
)

// Store defines session persistence for a Client.
//
// Content is addressed by a 17-bit key, mask 0x1ffff.
// The maximum size for entries is 256 MiB + 5 B.
// Methods on a Store may be invoked simultaneously.
type Store interface {
	// Load resolves content under a key. A nil return means “not found”.
	Load(key uint) ([]byte, error)

	// Save upserts content under a key. The method is expected to operate
	// in an atomic manner. That is, an error should imply that the content
	// remains unmodified.
	Save(key uint, data net.Buffers) error

	// Removes all content under a key in an atomic manner. Deletes of a non
	// existent content are not considdered to be an error.
	Delete(key uint) error

	// Enumerates all available content.
	List() (keys []uint, err error)
}

// Volatile is an in-memory Store.
type volatile struct {
	sync.Mutex
	perKey map[uint][]byte
}

func newVolatile(clientID string) Store {
	store := &volatile{perKey: make(map[uint][]byte)}
	if clientID != "" {
		store.perKey[clientIDKey] = []byte(clientID)
	}
	return store
}

// Load implements the Store interface.
func (m *volatile) Load(key uint) ([]byte, error) {
	m.Lock()
	defer m.Unlock()
	return m.perKey[key], nil
}

// Save implements the Store interface.
func (m *volatile) Save(key uint, data net.Buffers) error {
	var n int
	for _, b := range data {
		n += len(b)
	}
	buf := make([]byte, n)
	i := 0
	for _, b := range data {
		i += copy(buf[i:], b)
	}

	m.Lock()
	defer m.Unlock()
	m.perKey[key] = buf
	return nil
}

// Delete implements the Store interface.
func (m *volatile) Delete(key uint) error {
	m.Lock()
	defer m.Unlock()
	delete(m.perKey, key)
	return nil
}

// List implements the Store interface.
func (m *volatile) List() (keys []uint, err error) {
	m.Lock()
	defer m.Unlock()
	keys = make([]uint, 0, len(m.perKey))
	for k := range m.perKey {
		keys = append(keys, k)
	}
	return keys, nil
}
