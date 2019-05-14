package mqtt

// Fixed Packets
var (
	pingPacket    = []byte{ping << 4, 0}
	pongPacket    = []byte{pong << 4, 0}
	disconnPacket = []byte{disconn << 4, 0}
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

func (p *packet) connReq(attr *Attributes) {
	size := 6 // variable header

	var flags uint
	if attr.UserName != "" {
		size += 2 + len(attr.UserName)
		flags |= 1 << 7
	}
	if attr.Password != nil {
		size += 2 + len(attr.Password)
		flags |= 1 << 6
	}
	if w := attr.Will; w != nil {
		size += 2 + len(w.Topic)
		size += 2 + len(w.Message)
		if w.Retain {
			flags |= 1 << 5
		}
		flags |= uint(w.Deliver) << 3
		flags |= 1 << 2
	}
	if attr.CleanSession {
		flags |= 1 << 1
	}
	size += 2 + len(attr.ClientID)

	// compose header
	p.buf = append(p.buf[:0], connReq<<4)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf[:0], byte(size))

	p.buf = append(p.buf, 0, 4, 'M', 'Q', 'T', 'T', 4, byte(flags))

	// append payload
	p.addString(attr.ClientID)
	if w := attr.Will; w != nil {
		p.addString(w.Topic)
		p.addBytes(w.Message)
	}
	if attr.UserName != "" {
		p.addString(attr.UserName)
	}
	if attr.Password != nil {
		p.addBytes(attr.Password)
	}
}

func (p *packet) connAck(code connectReturn, sessionPresent bool) {
	var flags byte
	if sessionPresent {
		flags = 1
	}

	p.buf = append(p.buf[:0], connAck<<4, 2, flags, byte(code))
}

func (p *packet) pub(id uint, topic string, message []byte, deliver QoS) {
	size := len(message)
	if deliver != AtMostOnce {
		size += 2 // packet ID
	}
	size += 2 + len(topic)

	p.buf = append(p.buf[:0], pubReq<<4|byte(deliver)<<1)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf[:0], byte(size))
	p.addString(topic)
	if deliver != AtMostOnce {
		p.buf = append(p.buf, byte(id>>8), byte(id))
	}
	p.buf = append(p.buf, message...)
}

func (p *packet) pubAck(id uint) {
	p.buf = append(p.buf[:0], pubAck<<4, 2, byte(id>>8), byte(id))
}

func (p *packet) pubReceived(id uint) {
	p.buf = append(p.buf[:0], pubReceived<<4, 2, byte(id>>8), byte(id))
}

func (p *packet) pubRelease(id uint) {
	p.buf = append(p.buf[:0], pubRelease<<4, 2, byte(id>>8), byte(id))
}

func (p *packet) pubComplete(id uint) {
	p.buf = append(p.buf[:0], pubComplete<<4, 2, byte(id>>8), byte(id))
}

// TODO: batch
func (p *packet) subReq(id uint, topicFilter string, max QoS) {
	size := 3 + len(topicFilter)

	p.buf = append(p.buf[:0], subReq<<4)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf[:0], byte(size))
	p.addString(topicFilter)
	p.buf = append(p.buf, byte(max))
}

// TODO: batch
func (p *packet) subAck(id uint, returnCode byte) {
	p.buf = append(p.buf[:0], subAck<<4, 3, byte(id>>8), byte(id), returnCode)
}

// TODO: batch
func (p *packet) unsubReq(id uint, topicFilter string) {
	size := 2 + len(topicFilter)

	p.buf = append(p.buf[:0], unsubReq<<4)
	for size > 127 {
		p.buf = append(p.buf, byte(size|128))
		size >>= 7
	}
	p.buf = append(p.buf[:0], byte(size))
	p.buf = append(p.buf, byte(id>>8), byte(id))
	p.addString(topicFilter)
}

// TODO: batch
func (p *packet) unsubAck(id uint) {
	p.buf = append(p.buf[:0], unsubAck<<4, 2, byte(id>>8), byte(id))
}
