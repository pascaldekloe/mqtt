// Package mqtt provides a client for the Message Queuing Telemetry Transport
// protocol.
//
// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html
//
// Publish and Disconnect do a fire-and-forget submission. ErrClosed, ErrDown,
// ErrCanceled or an IsDeny [Publish only] imply no request submission. Any
// other error implies that the request submission was interrupted by either a
// connection failure or a by PauseTimeout appliance.
//
// Ping, Subscribe and Unsubscribe await response from the broker. ErrClosed,
// ErrDown, ErrMax, ErrCanceled or an IsDeny [Subscribe and Unsubscribe only]
// imply no request submission. ErrBreak and ErrAbandoned leave with the broker
// response unknown. Subscribe responses may cause an SubscribeError. Any other
// error implies that the request submission was interrupted by either a
// connection failure or by a PauseTimeout appliance.
//
// PublishAtLeastOnce and PublishExactlyOnce enqueue requests to a Persistence.
// Errors (either ErrClosed, ErrMax, IsDeny, or a Save return) imply that the
// message was dropped. Once persisted, the client will execute the transfer
// with endless retries, and report to the respective exchange channel.
package mqtt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
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

var (
	// ErrPacketMax enforces packetMax.
	errPacketMax = errors.New("mqtt: packet reached 256 MiB limit")
	// ErrStringMax enforces stringMax.
	errStringMax = errors.New("mqtt: string reached 64 KiB limit")

	errUTF8 = errors.New("mqtt: invalid UTF-8 byte sequence")
	errNull = errors.New("mqtt: string contains null character")
	errZero = errors.New("mqtt: string is empty")
)

func stringCheck(s string) error {
	if len(s) > stringMax {
		return errStringMax
	}

	// “The character data in a UTF-8 encoded string MUST be well-formed
	// UTF-8 as defined by the Unicode specification and restated in RFC
	// 3629.”
	if !utf8.ValidString(s) {
		return errUTF8
	}

	// “A UTF-8 encoded string MUST NOT include an encoding of the null
	// character U+0000.”
	// — MQTT Version 3.1.1, conformance statement MQTT-1.5.3-2
	if strings.IndexByte(s, 0) >= 0 {
		return errNull
	}

	// Characters 0x01–0x1F, 0x7F–0x9F, and the code points defined in the
	// Unicode specification to be non-characters are all tolerated, yet the
	// receiver may decide otherwise, and it may close the connection.
	return nil
}

// “All Topic Names and Topic Filters MUST be at least one character long.”
// — MQTT Version 3.1.1, conformance statement MQTT-4.7.3-1
func topicCheck(s string) error {
	if s == "" {
		return errZero
	}
	return stringCheck(s)
}

// IsDeny returns whether execution was rejected by the Client based on some
// validation constraint, like a size limitation or an illegal UTF-8 encoding.
// The rejection is permanent in such case. Another invocation with the same
// arguments will result in the same error again.
func IsDeny(err error) bool {
	for err != nil {
		switch err {
		case errPacketMax, errStringMax, errUTF8, errNull, errZero, errSubscribeNone, errUnsubscribeNone:
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

// ConnectReturn is the response code from CONNACK.
type connectReturn byte

// Connect return errors are predefined reasons for a broker to deny a connect
// request. IsConnectionRefused returns true for each of these.
const (
	accepted connectReturn = iota

	// ErrProtocolLevel means that the broker does not support the level of
	// the MQTT protocol requested by the Client.
	ErrProtocolLevel

	// ErrClientID means that the client identifier is correct UTF-8 but not
	// allowed by the broker.
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

// FileSystem stores values per file in a directory. Callers must ensure the
// availability, including write permission for the user. The empty string
// defaults to the working directory.
func FileSystem(dir string) Persistence {
	if dir != "" && dir[len(dir)-1] != os.PathSeparator {
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
	_, err = value.WriteTo(f)
	// ⚠️ inverse error checks
	if err == nil {
		err = f.Sync()
	}
	f.Close()
	if err == nil {
		err = os.Rename(f.Name(), dir.file(key))
	}
	if err == nil {
		return nil // OK
	}

	if removeErr := os.Remove(f.Name()); removeErr != nil {
		err = fmt.Errorf("%w, AND file leak: %w", err, removeErr)
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

// ruggedPersistence applies a sequence number plus integrity checks to a
// delegate.
type ruggedPersistence struct {
	Persistence // delegate

	// Content is ordered based on this sequence number. The clientIDKey
	// will have zero, as it is set [Save] only once as the first thing.
	seqNo atomic.Uint64
}

// Load implements the Persistence interface.
func (r *ruggedPersistence) Load(key uint) ([]byte, error) {
	value, err := r.Persistence.Load(key)
	switch {
	case err != nil:
		return nil, err
	case value == nil:
		return nil, nil
	default:
		value, _, err := decodeValue(value)
		if err != nil {
			return nil, fmt.Errorf("%w; record %#x unavailable", err, key)
		}
		return value, err
	}
}

// Save implements the Persistence interface.
func (r *ruggedPersistence) Save(key uint, value net.Buffers) error {
	return r.Persistence.Save(key, encodeValue(value, r.seqNo.Add(1)))
}

func encodeValue(packet net.Buffers, seqNo uint64) net.Buffers {
	digest := fnv.New32a()
	for _, buf := range packet {
		digest.Write(buf)
	}
	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:8], seqNo)
	digest.Write(buf[:8])
	binary.BigEndian.PutUint32(buf[8:], digest.Sum32())
	return append(packet, buf[:])
}

func decodeValue(buf []byte) (packet []byte, seqNo uint64, _ error) {
	if len(buf) < 12 {
		return nil, 0, errors.New("mqtt: persisted value truncated")
	}
	digest := fnv.New32a()
	digest.Write(buf[:len(buf)-4])
	if digest.Sum32() != binary.BigEndian.Uint32(buf[len(buf)-4:]) {
		return nil, 0, errors.New("mqtt: persisted value corrupt")
	}
	return buf[:len(buf)-12], binary.LittleEndian.Uint64(buf[len(buf)-12:]), nil
}
