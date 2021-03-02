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
	"os"
	"strconv"
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

// Some packet types do not carry any payload.
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
	// — MQTT Version 3.1.1, subsection 1.5.3
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
// validation constraint, like size limitation or an illegal UTF-8 encoding.
// The rejection is permanent in such case. Another invocation with the same
// arguments will result in the same error again.
func IsDeny(err error) bool {
	for err != nil {
		switch err {
		case errPacketMax, errStringMax, errUTF8, errNull, errSubscribeNone, errUnsubscribeNone:
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// ConnectReturn is the response code from CONNACK.
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

// Persistence keys correspond to MQTT packet identifiers.
const (
	// The 16-bit packet identifiers on inbound and outbound requests each
	// have their own namespace. The most-significant bit from a key makes
	// the distinction.
	remoteIDKeyFlag = 1 << 16

	// Packet identifier zero is not in use by the protocol.
	clientIDKey = 0
)

// Persistence tracks the session state as a key–value store. An instance may
// serve only one Client at a time.
//
// Values are addressed by a 17-bit key, mask 0x1ffff. The minimum size is 12 B.
// The maximum size is 256 MiB + 17 B. Clients apply integrity checks all round.
//
// Multiple goroutines may invoke methods on a Persistence simultaneously.
type Persistence interface {
	// Load resolves the value of a key. A nil return means “not found”.
	Load(key uint) ([]byte, error)

	// Save defines the value of a key.
	Save(key uint, value net.Buffers) error

	// Delete clears the value of a key, whether it existed or not. Failures
	// will be overwitten eventually due to the limited address space.
	Delete(key uint) error

	// List enumerates all available in any order.
	List() (keys []uint, err error)
}

// Volatile is an in-memory Persistence.
type volatile struct {
	sync.Mutex
	perKey map[uint][]byte
}

func newVolatile() Persistence {
	return &volatile{perKey: make(map[uint][]byte)}
}

// Load implements the Persistence interface.
func (m *volatile) Load(key uint) ([]byte, error) {
	m.Lock()
	defer m.Unlock()
	return m.perKey[key], nil
}

// Save implements the Persistence interface.
func (m *volatile) Save(key uint, value net.Buffers) error {
	var n int
	for _, buf := range value {
		n += len(buf)
	}
	bytes := make([]byte, n)
	i := 0
	for _, buf := range value {
		i += copy(bytes[i:], buf)
	}

	m.Lock()
	defer m.Unlock()
	m.perKey[key] = bytes
	return nil
}

// Delete implements the Persistence interface.
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
	return keys, nil
}

type fileSystem string

// FileSystem stores values per file in a directory.
func FileSystem(dir string) Persistence {
	if dir == "" || dir[len(dir)-1] != os.PathSeparator {
		dir += string([]rune{os.PathSeparator})
	}
	return fileSystem(dir)
}

func (dir fileSystem) file(key uint) string {
	return fmt.Sprintf("%s%05x", dir, key)
}

func (dir fileSystem) spoolFile(key uint) string {
	return fmt.Sprintf("%s%05x.spool", dir, key)
}

// Load implements the Persistence interface.
func (dir fileSystem) Load(key uint) ([]byte, error) {
	value, err := os.ReadFile(dir.file(key))
	if err != nil && errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return value, err
}

// Save implements the Persistence interface.
func (dir fileSystem) Save(key uint, value net.Buffers) error {
	f, err := os.Create(dir.spoolFile(key))
	if err != nil {
		return err
	}
	// ⚠️ inverse error checks
	_, err = value.WriteTo(f)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if closeErr != nil {
		if err == nil {
			err = closeErr
		} else {
			err = fmt.Errorf("%w; file descriptor leak: %s", err, closeErr)
		}
	}
	if err == nil {
		err = os.Rename(f.Name(), dir.file(key))
	}
	if err == nil {
		return nil // OK
	}
	removeErr := os.Remove(f.Name())
	if removeErr != nil {
		err = fmt.Errorf("%w; file leak: %s", err, removeErr)
	}
	return err
}

// Delete implements the Persistence interface.
func (dir fileSystem) Delete(key uint) error {
	err := os.Remove(dir.file(key))
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// List implements the Persistence interface.
func (dir fileSystem) List() (keys []uint, err error) {
	f, err := os.Open(string(dir))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	keys = make([]uint, 0, len(names))
	for _, name := range names {
		if len(name) != 5 {
			continue
		}
		u, err := strconv.ParseUint(name, 16, 17)
		if err != nil {
			continue
		}
		keys = append(keys, uint(u))
	}
	return keys, nil
}

// ruggedPersistence applies a sequence number plus integrity checks.
type ruggedPersistence struct {
	Persistence
	// Content is ordered based on this sequence number.
	// Zero gets applied to the clientIDKey value.
	seqNo uint64
}

// Load implements the Persistence interface.
func (r *ruggedPersistence) Load(key uint) ([]byte, error) {
	value, err := r.Persistence.Load(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	value, _, ok := decodeValue(value)
	if !ok {
		return nil, fmt.Errorf("mqtt: persistence value from key %#x corrupt", key)
	}
	return value, nil
}

// Save implements the Persistence interface.
func (r *ruggedPersistence) Save(key uint, value net.Buffers) error {
	return r.Persistence.Save(key, encodeValue(value, atomic.AddUint64(&r.seqNo, 1)))
}

var checkTable = crc32.MakeTable(crc32.Castagnoli)

func encodeValue(packet net.Buffers, seqNo uint64) net.Buffers {
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

func decodeValue(buf []byte) (packet []byte, seqNo uint64, ok bool) {
	if len(buf) < 12 {
		return nil, 0, false
	}
	if crc32.Checksum(buf[:len(buf)-4], checkTable) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, 0, false
	}
	return buf[:len(buf)-12], binary.LittleEndian.Uint64(buf[len(buf)-12:]), true
}
