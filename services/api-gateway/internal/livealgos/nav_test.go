package livealgos

import (
	"database/sql"
	"testing"
	"time"
)

// ComputeNAVRow must equal the details page's math: realized (exited lots) +
// unrealized (active lots at live LTP) over deployed capital.
func TestComputeNAVRow_MatchesPageMath(t *testing.T) {
	d := Deployment{StrategyID: "s1", UserID: "u1", DeployedCapital: 1_000_000}
	positions := []PositionRow{
		{Status: "ACTIVE", Symbol: "AAA", Quantity: 10, EntryPrice: 100,
			ExchangeToken: sql.NullString{String: "1", Valid: true}},
		{Status: "EXITED", Symbol: "BBB", Quantity: 5, EntryPrice: 50,
			RealizedPnL: sql.NullFloat64{Float64: -3890.65, Valid: true}},
	}
	ltps := map[string]LTPQuote{"1": {LTP: 110, PrevClose: 100}}
	row := ComputeNAVRow(d, positions, ltps, time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))
	if row.RealizedAmount != -3890 {
		t.Errorf("realized = %d, want −3890", row.RealizedAmount)
	}
	if row.UnrealizedAmount != 100 { // (110−100)×10
		t.Errorf("unrealized = %d, want 100", row.UnrealizedAmount)
	}
	if row.NetPnLAmount != -3790 || row.OpenPositions != 1 {
		t.Errorf("net = %d open = %d", row.NetPnLAmount, row.OpenPositions)
	}
	if row.NetPnLPct != -0.38 { // −3790/1e6 ×100 rounded
		t.Errorf("pct = %v, want −0.38", row.NetPnLPct)
	}
}
