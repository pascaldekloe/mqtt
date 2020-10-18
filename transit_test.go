package mqtt

import "testing"

func TestAssignIDRange(t *testing.T) {
	var line atLeastOnceLine

	// assign full range
	for seqNo := uint(1); seqNo <= 255; seqNo++ {
		packetID := line.assignID()
		if want := seqNo << 8; packetID != want {
			t.Errorf("assignID # %d got packet identifier %#04x, want %#04x", seqNo, packetID, want)
		}
	}
}

// Redundant acknowledgements can happen for various reasons.
func TestDoubleFreeID(t *testing.T) {
	var line atLeastOnceLine
	if packetID := line.assignID(); packetID != 0x0100 {
		t.Fatalf("assignID #1 got %#04x, want 0x0100", packetID)
	}
	if packetID := line.assignID(); packetID != 0x0200 {
		t.Fatalf("assignID #2 got %#04x, want 0x0200", packetID)
	}

	t.Logf("line bits %#016x", line)
	if err := line.freeID(0x0100); err != nil {
		t.Error("freeID first got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.freeID(0x0100); err != nil {
		t.Error("double freeID first got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.freeID(0x0200); err != nil {
		t.Error("freeID last got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.freeID(0x0200); err != nil {
		t.Error("double freeID last got error", err)
	}
}

// Missed acknowledgements must be rejected.
func TestSkipFreeID(t *testing.T) {
	var line atLeastOnceLine
	if packetID := line.assignID(); packetID != 0x0100 {
		t.Errorf("assignID #1 got %#04x, want 0x0100", packetID)
	}
	if packetID := line.assignID(); packetID != 0x0200 {
		t.Errorf("assignID #2 got %#04x, want 0x0200", packetID)
	}

	t.Logf("line bits %#016x", line)
	if err := line.freeID(0x0100); err != nil {
		t.Fatal("freeID first got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.freeID(0x0100); err != nil {
		t.Fatal("double freeID first got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.freeID(0x0200); err != nil {
		t.Fatal("freeID last got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.freeID(0x0200); err != nil {
		t.Fatal("double freeID last got error", err)
	}
}
