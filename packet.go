package mqtt

import "sync"

// Fixed Packets
var (
	pingPacket    = &packet{[]byte{ping << 4, 0}}
	pongPacket    = &packet{[]byte{pong << 4, 0}}
	disconnPacket = &packet{[]byte{disconn << 4, 0}}
)

var packetPool = sync.Pool{New: func() interface{} { return new(packet) }}

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

func newConnReq(config *SessionConfig) *packet {
	size := 6 // variable header

	var flags uint
	if config.UserName != "" {
		size += 2 + len(config.UserName)
		flags |= 1 << 7
	}
	if config.Password != nil {
		size += 2 + len(config.Password)
		flags |= 1 << 6
	}
	if w := config.Will; w != nil {
		size += 2 + len(w.Topic)
		size += 2 + len(w.Message)
		if w.Retain {
			flags |= 1 << 5
		}
		flags |= uint(w.Deliver) << 3
		flags |= 1 << 2
	}
	if config.CleanSession {
		flags |= 1 << 1
	}
	size += 2 + len(config.ClientID)

	p := packetPool.Get().(*packet)

	// compose header
	p.buf = append(p.buf[:0], connReq<<4)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf[:0], byte(size))

	p.buf = append(p.buf, 0, 4, 'M', 'Q', 'T', 'T', 4, byte(flags))

	// append payload
	p.addString(config.ClientID)
	if w := config.Will; w != nil {
		p.addString(w.Topic)
		p.addBytes(w.Message)
	}
	if config.UserName != "" {
		p.addString(config.UserName)
	}
	if config.Password != nil {
		p.addBytes(config.Password)
	}

	return p
}

func newConnAck(code connectReturn, sessionPresent bool) *packet {
	var flags byte
	if sessionPresent {
		flags = 1
	}

	p := packetPool.Get().(*packet)
	p.buf = append(p.buf[:0], connAck<<4, 2, flags, byte(code))
	return p
}

func newPubMsg(topic string, message []byte) *packet {
	p := packetPool.Get().(*packet)
	p.buf = append(p.buf[:0], pubMsg<<4)

	size := 2 + len(topic) + len(message)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf, byte(size))

	p.addString(topic)
	p.buf = append(p.buf, message...)
	return p
}

func newPubMsgWithID(id uint, topic string, message []byte, deliver QoS) *packet {
	p := packetPool.Get().(*packet)
	p.buf = append(p.buf[:0], pubMsg<<4|byte(deliver)<<1)

	size := 4 + len(topic) + len(message)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf, byte(size))

	p.addString(topic)
	p.buf = append(p.buf, byte(id>>8), byte(id))
	p.buf = append(p.buf, message...)
	return p
}

func newPubAck(id uint) *packet {
	p := packetPool.Get().(*packet)
	p.buf = append(p.buf[:0], pubAck<<4, 2, byte(id>>8), byte(id))
	return p
}

func newPubReceived(id uint) *packet {
	p := packetPool.Get().(*packet)
	p.buf = append(p.buf[:0], pubReceived<<4, 2, byte(id>>8), byte(id))
	return p
}

func newPubRelease(id uint) *packet {
	p := packetPool.Get().(*packet)
	p.buf = append(p.buf[:0], pubRelease<<4, 2, byte(id>>8), byte(id))
	return p
}

func newPubComplete(id uint) *packet {
	p := packetPool.Get().(*packet)
	p.buf = append(p.buf[:0], pubComplete<<4, 2, byte(id>>8), byte(id))
	return p
}
