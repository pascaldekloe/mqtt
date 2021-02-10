// Package mqtt provides a client for the Message Queuing Telemetry Transport
// protocol.
//
// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
package mqtt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"sync"
	"sync/atomic"
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

// IsDeny returns whether execution was rejected by the Client based on some
// validation constraint, like size limitations or an illegal UTF-8 encoding.
// The rejection is permanent in such case. Another invocation with the same
// arguments will result in the same error again.
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

// IsConnectionRefused returns whether the broker denied a connect request from
// the Client.
func IsConnectionRefused(err error) bool {
	var code connectReturn
	if errors.As(err, &code) {
		return code != accepted
	}
	return false
}

// Store keys correspond to MQTT packet identifiers.
const (
	// Packet identifiers on inbound and outbound requests each have their
	// own namespace. The most-significant bit on a Store key makes the
	// distinction.
	remoteIDKeyFlag = 1 << 16
)

// Store defines session persistence for a Client. A Store may serve only one
// Client at a time.
//
// Content is addressed by a 17-bit key, mask 0x1ffff.
// The minimum size for entries is 12 B.
// The maximum size for entries is 256 MiB + 17 B.
// Clients apply integrity checks on content.
//
// Methods on a Store may be invoked simultaneously.
type Store interface {
	// Load resolves content under a key. A nil return means “not found”.
	Load(key uint) ([]byte, error)

	// Save upserts content under a key. The method is expected to operate
	// in an atomic manner. That is, an error should imply that the content
	// remains unmodified.
	Save(key uint, content net.Buffers) error

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

func newVolatileStore() Store {
	return &volatile{perKey: make(map[uint][]byte)}
}

// Load implements the Store interface.
func (m *volatile) Load(key uint) ([]byte, error) {
	m.Lock()
	defer m.Unlock()
	return m.perKey[key], nil
}

// Save implements the Store interface.
func (m *volatile) Save(key uint, content net.Buffers) error {
	var n int
	for _, b := range content {
		n += len(b)
	}
	buf := make([]byte, n)
	i := 0
	for _, b := range content {
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

// ruggedStore applies a sequence number plus content integrity checks.
type ruggedStore struct {
	Store
	// Store content is ordered based on this sequence number.
	// Zero is reserved for the clientIDKey content.
	seqNo uint64
}

func (r *ruggedStore) Load(key uint) ([]byte, error) {
	value, err := r.Store.Load(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	value, _, ok := decodeStoreContent(value)
	if !ok {
		return nil, fmt.Errorf("mqtt: persistence content from key %#x corrupt", key)
	}
	return value, nil
}

func (rugged *ruggedStore) Save(key uint, content net.Buffers) error {
	content = encodeStoreContent(content, atomic.AddUint64(&rugged.seqNo, 1))
	return rugged.Store.Save(key, content)
}

var checkTable = crc32.MakeTable(crc32.Castagnoli)

func encodeStoreContent(packet net.Buffers, seqNo uint64) net.Buffers {
	var sum uint32
	for _, buf := range packet {
		sum = crc32.Update(sum, checkTable, buf)
	}
	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:8], seqNo)
	sum = crc32.Update(sum, checkTable, buf[:8])
	binary.LittleEndian.PutUint32(buf[8:], sum)
	return append(packet, buf[:])
}

func decodeStoreContent(buf []byte) (packet []byte, seqNo uint64, ok bool) {
	if len(buf) < 12 {
		return nil, 0, false
	}
	if crc32.Checksum(buf[:len(buf)-4], checkTable) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, 0, false
	}
	return buf[:len(buf)-12], binary.LittleEndian.Uint64(buf[len(buf)-12:]), true
}
