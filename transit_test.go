package mqtt

import "testing"

func TestAssignIDRange(t *testing.T) {
	var line atLeastOnceLine

	// assign full range
	for seqNo := uint(1); seqNo <= 255; seqNo++ {
		packetID := line.AssignID()
		if want := seqNo << 8; packetID != want {
			t.Errorf("AssignID # %d got packet identifier %#04x, want %#04x", seqNo, packetID, want)
		}
	}
}

// Redundant acknowledgements can happen for various reasons.
func TestDoubleFreeID(t *testing.T) {
	var line atLeastOnceLine
	if packetID := line.AssignID(); packetID != 0x0100 {
		t.Fatalf("AssignID #1 got %#04x, want 0x0100", packetID)
	}
	if packetID := line.AssignID(); packetID != 0x0200 {
		t.Fatalf("AssignID #2 got %#04x, want 0x0200", packetID)
	}

	t.Logf("line bits %#016x", line)
	if err := line.FreeID(0x0100); err != nil {
		t.Error("FreeID first got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.FreeID(0x0100); err != nil {
		t.Error("double FreeID first got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.FreeID(0x0200); err != nil {
		t.Error("FreeID last got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.FreeID(0x0200); err != nil {
		t.Error("double FreeID last got error", err)
	}
}

// Missed acknowledgements must be rejected.
func TestSkipFreeID(t *testing.T) {
	var line atLeastOnceLine
	if packetID := line.AssignID(); packetID != 0x0100 {
		t.Errorf("AssignID #1 got %#04x, want 0x0100", packetID)
	}
	if packetID := line.AssignID(); packetID != 0x0200 {
		t.Errorf("AssignID #2 got %#04x, want 0x0200", packetID)
	}

	t.Logf("line bits %#016x", line)
	if err := line.FreeID(0x0100); err != nil {
		t.Fatal("FreeID first got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.FreeID(0x0100); err != nil {
		t.Fatal("double FreeID first got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.FreeID(0x0200); err != nil {
		t.Fatal("FreeID last got error", err)
	}
	t.Logf("line bits %#016x", line)
	if err := line.FreeID(0x0200); err != nil {
		t.Fatal("double FreeID last got error", err)
	}
}
