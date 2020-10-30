package mqtt

import (
	"bytes"
	"testing"
)

func TestAppendConnReq(t *testing.T) {
	c := new(SessionConfig)

	want := []byte{0x10, 12, 0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, 0, 0}
	if got := c.appendConnReq(nil); !bytes.Equal(got, want) {
		t.Errorf("empty session config got %#x, want %#x", got, want)
	}

	c.ClientID = "#🤖"
	c.UserName = "me"
	c.Password = []byte{'?'}
	c.CleanSession = true
	c.Will.Topic = "☯️"
	c.Will.Message = []byte("☠")
	c.Will.Retain = true
	c.Will.AtLeastOnce = true
	c.Will.ExactlyOnce = true
	c.KeepAlive = 3600

	want = []byte{0x10, 37, 0, 4, 'M', 'Q', 'T', 'T', 4, 0b1111_0110, 0x0e, 0x10,
		0, 5, '#', 0xF0, 0x9F, 0xA4, 0x96,
		0, 6, 0xe2, 0x98, 0xaf, 0xef, 0xb8, 0x8f,
		0, 3, 0xe2, 0x98, 0xa0,
		0, 2, 'm', 'e',
		0, 1, '?'}
	if got := c.appendConnReq(nil); !bytes.Equal(got, want) {
		t.Errorf("full session config got %#x, want %#x", got, want)
	}
}
