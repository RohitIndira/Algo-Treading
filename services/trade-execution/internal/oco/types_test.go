package oco

import "testing"

// TestCalculateSLTPFromExecutionPrice verifies SL/TP are computed directly from
// the execution price (no ÷1.005 buffer strip) for both BUY and SELL.
func TestCalculateSLTPFromExecutionPrice(t *testing.T) {
	cases := []struct {
		name   string
		side   string
		fill   float64
		slPct  float64
		tpPct  float64
		wantSL float64
		wantTP float64
	}{
		{
			// BLUSPRING IS19094: fill 90.34, SL 2%, TP 2% → SL 88.55, TP 92.15
			// (90.34*0.98=88.5332→88.55; 90.34*1.02=92.1468→92.15 at NSE 0.05 tick)
			name: "buy_2_2", side: "BUY", fill: 90.34, slPct: 2, tpPct: 2,
			wantSL: 88.55, wantTP: 92.15,
		},
		{
			// BLUSPRING L027: fill 90.34, SL 2%, TP 5% → SL 88.55, TP 94.85
			// (90.34*1.05=94.857→94.85)
			name: "buy_2_5", side: "BUY", fill: 90.34, slPct: 2, tpPct: 5,
			wantSL: 88.55, wantTP: 94.85,
		},
		{
			// SELL entry: SL above, TP below.
			name: "sell_2_3", side: "SELL", fill: 100, slPct: 2, tpPct: 3,
			wantSL: 102.00, wantTP: 97.00,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g := &OCOGroup{OrderSide: c.side, SLPercent: c.slPct, TPPercent: c.tpPct}
			if got := g.CalculateSLFromFill(c.fill); got != c.wantSL {
				t.Errorf("SL: got %.2f want %.2f", got, c.wantSL)
			}
			if got := g.CalculateTPFromFill(c.fill); got != c.wantTP {
				t.Errorf("TP: got %.2f want %.2f", got, c.wantTP)
			}
		})
	}
}

// TestStripEntryBuffer verifies the buffer strip applies only to BUY.
func TestStripEntryBuffer(t *testing.T) {
	// BUY: limit 90.85 (≈90.40*1.005) strips to ~90.40.
	if got := stripEntryBuffer(90.85, "BUY"); got <= 90.0 || got >= 90.9 {
		t.Errorf("BUY strip: got %.4f, want ~90.40", got)
	}
	// SELL: unchanged.
	if got := stripEntryBuffer(90.85, "SELL"); got != 90.85 {
		t.Errorf("SELL strip: got %.4f, want 90.85", got)
	}
}

// TestSLTPDirectionalSafety guards the invariant the leg-placement validation
// relies on: BUY SL below fill & TP above fill; SELL the reverse.
func TestSLTPDirectionalSafety(t *testing.T) {
	buy := &OCOGroup{OrderSide: "BUY", SLPercent: 2, TPPercent: 2}
	if sl := buy.CalculateSLFromFill(90.34); sl >= 90.34 {
		t.Errorf("BUY SL %.2f must be < fill 90.34", sl)
	}
	if tp := buy.CalculateTPFromFill(90.34); tp <= 90.34 {
		t.Errorf("BUY TP %.2f must be > fill 90.34", tp)
	}
	sell := &OCOGroup{OrderSide: "SELL", SLPercent: 2, TPPercent: 2}
	if sl := sell.CalculateSLFromFill(100); sl <= 100 {
		t.Errorf("SELL SL %.2f must be > fill 100", sl)
	}
	if tp := sell.CalculateTPFromFill(100); tp >= 100 {
		t.Errorf("SELL TP %.2f must be < fill 100", tp)
	}
}
