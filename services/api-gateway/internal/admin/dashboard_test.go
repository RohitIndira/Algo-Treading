package admin

import (
	"net/http/httptest"
	"testing"
)

func TestDrawdownStats(t *testing.T) {
	// classic peak → trough → partial recovery
	vals := []float64{100, 110, 99, 104.5, 121, 121}
	maxDD, dd := drawdownStats(vals)
	if maxDD != -10 {
		t.Fatalf("maxDD = %v, want -10 (110→99)", maxDD)
	}
	if dd[0] != 0 || dd[1] != 0 {
		t.Fatalf("new highs must be 0 drawdown, got %v %v", dd[0], dd[1])
	}
	if dd[2] != -10 {
		t.Fatalf("dd at trough = %v, want -10", dd[2])
	}
	if dd[5] != 0 {
		t.Fatalf("recovered to new high must be 0, got %v", dd[5])
	}

	// empty + all-zero series must not panic or divide by zero
	if m, d := drawdownStats(nil); m != 0 || len(d) != 0 {
		t.Fatalf("nil series: got %v %v", m, d)
	}
	if m, _ := drawdownStats([]float64{0, 0}); m != 0 {
		t.Fatalf("zero series maxDD = %v, want 0", m)
	}
}

func TestPctAndRounding(t *testing.T) {
	if got := pct(50, 200); got != 25 {
		t.Fatalf("pct(50,200) = %v, want 25", got)
	}
	if got := pct(10, 0); got != 0 {
		t.Fatalf("pct with zero denominator = %v, want 0", got)
	}
	if got := pct(-3382, 100000); got != -3.38 {
		t.Fatalf("negative pct = %v, want -3.38", got)
	}
	if got := round2f(-3.665); got != -3.67 {
		t.Fatalf("round2f(-3.665) = %v, want -3.67 (round half away from zero)", got)
	}
}

func TestDaysParam(t *testing.T) {
	for _, tc := range []struct {
		q    string
		def  int
		want int
	}{
		{"", 90, 90},          // absent → default
		{"days=30", 90, 30},   // explicit
		{"days=abc", 30, 30},  // junk → default
		{"days=-5", 30, 30},   // negative → default
		{"days=9999", 90, 730}, // clamp
	} {
		r := httptest.NewRequest("GET", "/x?"+tc.q, nil)
		if got := daysParam(r, tc.def); got != tc.want {
			t.Fatalf("daysParam(%q, %d) = %d, want %d", tc.q, tc.def, got, tc.want)
		}
	}
}
