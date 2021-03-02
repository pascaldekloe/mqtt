package mqtt

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sort"
	"testing"
)

func TestConstants(t *testing.T) {
	if want := 268_435_455; packetMax != want {
		t.Errorf("got packetMax %d, want %d", packetMax, want)
	}
	if want := 65_535; stringMax != want {
		t.Errorf("got stringMax %d, want %d", stringMax, want)
	}
}

func TestNewCONNREQ(t *testing.T) {
	c := &Config{
		Dialer: func(context.Context) (net.Conn, error) {
			return nil, errors.New("dialer call not allowed for test")
		},
		UserName:     "me",
		Password:     []byte{'?'},
		CleanSession: true,
		KeepAlive:    3600,
	}
	c.Will.Topic = "☯️"
	c.Will.Message = []byte("☠")
	c.Will.Retain = true
	c.Will.AtLeastOnce = true
	c.Will.ExactlyOnce = true

	got := c.newCONNREQ([]byte("#🤖"))
	want := []byte{0x10, 37, 0, 4, 'M', 'Q', 'T', 'T', 4, 0b1111_0110, 0x0e, 0x10,
		0, 5, '#', 0xF0, 0x9F, 0xA4, 0x96,
		0, 6, 0xe2, 0x98, 0xaf, 0xef, 0xb8, 0x8f,
		0, 3, 0xe2, 0x98, 0xa0,
		0, 2, 'm', 'e',
		0, 1, '?'}
	if !bytes.Equal(got, want) {
		t.Errorf("full session config got %#x, want %#x", got, want)
	}
}

func TestPesistenceEmpty(t *testing.T) {
	t.Run("volatile", func(t *testing.T) {
		testPersistenceEmpty(t, newVolatile())
	})
	t.Run("fileSystem", func(t *testing.T) {
		testPersistenceEmpty(t, FileSystem(t.TempDir()))
	})
}

func testPersistenceEmpty(t *testing.T, p Persistence) {
	if data, err := p.Load(42); err != nil {
		t.Error("Load got error:", err)
	} else if data != nil {
		t.Errorf("Load got %#x, want nil", data)
	}

	if err := p.Delete(42); err != nil {
		t.Error("Delete got error:", err)
	}

	if keys, err := p.List(); err != nil {
		t.Error("List got error:", err)
	} else if len(keys) != 0 {
		t.Errorf("List got keys %d", keys)
	}
}

func TestPersistence(t *testing.T) {
	t.Run("volatile", func(t *testing.T) {
		testPersistence(t, newVolatile())
	})
	t.Run("fileSystem", func(t *testing.T) {
		testPersistence(t, FileSystem(t.TempDir()))
	})
}

func testPersistence(t *testing.T, p Persistence) {
	for i := 0; i < 3; i++ {
		bufs := make(net.Buffers, i+1)
		for j := range bufs {
			bufs[j] = make([]byte, j+1)
			for k := range bufs[j] {
				bufs[j][k] = byte('a' + k)
			}
		}

		err := p.Save(uint(i), bufs)
		if err != nil {
			t.Errorf("Save %d got error: %s", i, err)
		}
	}

	if keys, err := p.List(); err != nil {
		t.Error("List got error:", err)
	} else {
		// order undefined
		ints := make([]int, len(keys))
		for i := range keys {
			ints[i] = int(keys[i])
		}
		sort.Ints(ints)
		if len(ints) != 3 || ints[0] != 0 || ints[1] != 1 || ints[2] != 2 {
			t.Errorf("List got %d, want %d", ints, []int{0, 1, 2})
		}
	}

	if data, err := p.Load(0); err != nil {
		t.Error("Load 0 got error:", err)
	} else if want := "a"; string(data) != want {
		t.Errorf("Load 0 got %q, want %q", data, want)
	}
	if data, err := p.Load(1); err != nil {
		t.Error("Load 1 got error:", err)
	} else if want := "aab"; string(data) != want {
		t.Errorf("Load 1 got %q, want %q", data, want)
	}
	if data, err := p.Load(2); err != nil {
		t.Error("Load 2 got error:", err)
	} else if want := "aababc"; string(data) != want {
		t.Errorf("Load 2 got %q, want %q", data, want)
	}
}

func TestPersistenceUpdate(t *testing.T) {
	t.Run("volatile", func(t *testing.T) {
		testPersistenceUpdate(t, newVolatile())
	})
	t.Run("fileSystem", func(t *testing.T) {
		testPersistenceUpdate(t, FileSystem(t.TempDir()))
	})
}

func testPersistenceUpdate(t *testing.T, p Persistence) {
	err := p.Save(0, net.Buffers{[]byte("ab"), []byte("cd")})
	if err != nil {
		t.Fatal("Save new 0 got error:", err)
	}
	err = p.Save(42, net.Buffers{[]byte("ef")})
	if err != nil {
		t.Fatal("Save new 42 got error:", err)
	}
	err = p.Save(0, net.Buffers{[]byte("12")})
	if err != nil {
		t.Fatal("Save update 0 got error:", err)
	}
	err = p.Save(42, net.Buffers{[]byte("34"), []byte("56")})
	if err != nil {
		t.Fatal("Save update 42 got error:", err)
	}

	if data, err := p.Load(0); err != nil {
		t.Error("Load 0 got error:", err)
	} else if want := "12"; string(data) != want {
		t.Errorf("Load 0 got %#v, want %#v", data, want)
	}
	if data, err := p.Load(42); err != nil {
		t.Error("Load 42 got error:", err)
	} else if want := "3456"; string(data) != want {
		t.Errorf("Load 42 got %#v, want %#v", data, want)
	}
}

func TestPersistenceDelete(t *testing.T) {
	t.Run("volatile", func(t *testing.T) {
		testPersistenceDelete(t, newVolatile())
	})
	t.Run("fileSystem", func(t *testing.T) {
		testPersistenceDelete(t, FileSystem(t.TempDir()))
	})
}

func testPersistenceDelete(t *testing.T, p Persistence) {
	err := p.Save(0, net.Buffers{[]byte("ab"), []byte("cd")})
	if err != nil {
		t.Fatal("Save new 0 got error:", err)
	}
	err = p.Save(42, net.Buffers{[]byte("ef")})
	if err != nil {
		t.Fatal("Save new 42 got error:", err)
	}
	err = p.Save(42, net.Buffers{[]byte("gh")})
	if err != nil {
		t.Fatal("Save update 42 got error:", err)
	}
	err = p.Save(99, net.Buffers{[]byte("ij")})
	if err != nil {
		t.Fatal("Save new 99 got error:", err)
	}

	if err := p.Delete(42); err != nil {
		t.Error("Delete 42 got error:", err)
	}
	if err := p.Delete(0); err != nil {
		t.Error("Delete 0 got error:", err)
	}
	if keys, err := p.List(); err != nil {
		t.Error("List got error:", err)
	} else if len(keys) != 1 || keys[0] != 99 {
		t.Errorf("List got %d, want %d", keys, []uint{99})
	}
}
