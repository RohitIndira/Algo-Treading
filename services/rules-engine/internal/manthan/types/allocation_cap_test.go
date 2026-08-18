package types

import "testing"

// The Manthan caps are HARD ceilings on the complete book: sector ≤ 25% and
// mcap bucket ≤ 50% of max_positions. Floor semantics — the old ceiling math
// allowed 7/25 (28%) and 13/25 (52%).
func TestCapCheck_FloorSemantics(t *testing.T) {
	c := NewCapCheck(25, nil)
	if c.MaxPerSector != 6 {
		t.Errorf("MaxPerSector = %d, want 6 (25%% of 25, floored; 7 would be 28%%)", c.MaxPerSector)
	}
	if c.MaxPerBucket != 12 {
		t.Errorf("MaxPerBucket = %d, want 12 (50%% of 25, floored; 13 would be 52%%)", c.MaxPerBucket)
	}

	// 6 sector adds pass, the 7th is blocked.
	for i := 0; i < 6; i++ {
		if ok, why := c.CanAdd("Auto", "SMALL"); !ok {
			t.Fatalf("add %d rejected early: %s", i+1, why)
		}
		c.Add("Auto", "SMALL")
	}
	if ok, _ := c.CanAdd("Auto", "SMALL"); ok {
		t.Error("7th same-sector position must be blocked (would be 28%)")
	}
	// A different sector in the same bucket still passes (6 < 12).
	if ok, why := c.CanAdd("Pharma", "SMALL"); !ok {
		t.Errorf("different sector blocked: %s", why)
	}

	// Bucket cap: fill SMALL to 12 total, 13th blocked regardless of sector.
	for i := 6; i < 12; i++ {
		c.Add("S"+string(rune('A'+i)), "SMALL")
	}
	if ok, _ := c.CanAdd("FreshSector", "SMALL"); ok {
		t.Error("13th SMALL position must be blocked (would be 52%)")
	}

	// Tiny books never floor to zero.
	c2 := NewCapCheck(2, nil)
	if c2.MaxPerSector != 1 || c2.MaxPerBucket != 1 {
		t.Errorf("min-1 guard: sector=%d bucket=%d, want 1/1", c2.MaxPerSector, c2.MaxPerBucket)
	}
}
