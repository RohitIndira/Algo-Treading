package portfolio

// Hermetic tests for Enricher. Uses stubs for TokenLookup + LTPSource so
// no Postgres / Redis is dialled.
//
// Key invariants under test:
//   - LTP HEALTHY + token+quote present → LTP/CurrentValue/UnrealizedPnL populated
//   - LTP UNAVAILABLE → per-row + summary LTP fields ALL nil (never 0)
//   - Missing token OR missing quote → that row alone lacks LTP; others enrich fine
//   - Empty positions list → HEALTHY status regardless of infra state

import (
	"context"
	"testing"

	pfpb "github.com/RohitIndira/Algo-Treading/api/proto/portfolio"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

// stubTokens returns a fixed map.
type stubTokens struct {
	byS map[string]string
	err error
}

func (s *stubTokens) TokensBySymbols(_ context.Context, _ []string) (map[string]string, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.byS, nil
}

// stubLTP returns fixed quotes + status.
type stubLTP struct {
	byToken map[string]livealgos.LTPQuote
	status  livealgos.Status
}

func (s *stubLTP) FetchByTokens(_ context.Context, _ []string) (map[string]livealgos.LTPQuote, livealgos.Status) {
	return s.byToken, s.status
}

// mkLot is a compact factory for the pfpb ActivePosition rows.
func mkLot(symbol string, qty int32, entry float64) *pfpb.ActivePosition {
	return &pfpb.ActivePosition{
		PositionId:     "pos-" + symbol,
		Origin:         "MANTHAN",
		Symbol:         symbol,
		Exchange:       "NSE",
		Quantity:       qty,
		EntryPrice:     entry,
		InvestedAmount: float64(qty) * entry,
	}
}

// -----------------------------------------------------------------------------

func TestEnrichPositions_HealthyPopulatesAllFields(t *testing.T) {
	lot := mkLot("SBI", 20, 500)
	tokens := &stubTokens{byS: map[string]string{"SBI": "3045"}}
	ltp := &stubLTP{
		byToken: map[string]livealgos.LTPQuote{"3045": {LTP: 520}},
		status:  livealgos.StatusHealthy,
	}
	e := NewEnricher(tokens, ltp)

	got, err := e.EnrichPositions(context.Background(), []*pfpb.ActivePosition{lot})
	if err != nil {
		t.Fatalf("EnrichPositions: %v", err)
	}
	if got.LTPStatus != string(livealgos.StatusHealthy) {
		t.Errorf("top-level status: got %q, want HEALTHY", got.LTPStatus)
	}
	if len(got.Positions) != 1 {
		t.Fatalf("rows: got %d, want 1", len(got.Positions))
	}
	r := got.Positions[0]
	if r.LTP == nil || *r.LTP != 520 {
		t.Errorf("row.LTP: got %v, want 520", r.LTP)
	}
	if r.CurrentValue == nil || *r.CurrentValue != 20*520 {
		t.Errorf("row.CurrentValue: got %v, want %v", r.CurrentValue, 20*520)
	}
	if r.UnrealizedPnL == nil || *r.UnrealizedPnL != (520-500)*20 {
		t.Errorf("row.UnrealizedPnL: got %v, want %v", r.UnrealizedPnL, (520-500)*20)
	}
}

func TestEnrichPositions_UnavailableLeavesLTPFieldsNil(t *testing.T) {
	lot := mkLot("SBI", 20, 500)
	tokens := &stubTokens{byS: map[string]string{"SBI": "3045"}}
	ltp := &stubLTP{status: livealgos.StatusUnavailable} // no quotes

	e := NewEnricher(tokens, ltp)
	got, err := e.EnrichPositions(context.Background(), []*pfpb.ActivePosition{lot})
	if err != nil {
		t.Fatalf("EnrichPositions: %v", err)
	}
	if got.LTPStatus != string(livealgos.StatusUnavailable) {
		t.Errorf("top-level status: got %q, want UNAVAILABLE", got.LTPStatus)
	}
	r := got.Positions[0]
	if r.LTP != nil || r.CurrentValue != nil || r.UnrealizedPnL != nil {
		t.Errorf("unavailable: LTP fields must be nil, got ltp=%v cur=%v unr=%v",
			r.LTP, r.CurrentValue, r.UnrealizedPnL)
	}
	if r.Symbol != "SBI" || r.Quantity != 20 {
		t.Errorf("non-LTP fields must still render, got %+v", r)
	}
}

func TestEnrichPositions_MissingTokenOrQuoteDoesntKillOtherRows(t *testing.T) {
	// SBI has a token AND a quote → enriched.
	// IDEA has a token but NO quote in Redis → not enriched.
	// UNKNOWN has NO token at all → not enriched.
	sbi := mkLot("SBI", 20, 500)
	idea := mkLot("IDEA", 100, 10)
	unknown := mkLot("UNKNOWN", 5, 300)

	tokens := &stubTokens{byS: map[string]string{
		"SBI":  "3045",
		"IDEA": "14366",
		// UNKNOWN: absent → no lookup match
	}}
	ltp := &stubLTP{
		byToken: map[string]livealgos.LTPQuote{"3045": {LTP: 520}}, // only SBI
		status:  livealgos.StatusHealthy,
	}

	e := NewEnricher(tokens, ltp)
	got, _ := e.EnrichPositions(context.Background(), []*pfpb.ActivePosition{sbi, idea, unknown})
	if len(got.Positions) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(got.Positions))
	}
	// SBI enriched
	if got.Positions[0].LTP == nil || *got.Positions[0].LTP != 520 {
		t.Errorf("SBI row must be enriched, got %+v", got.Positions[0])
	}
	// IDEA — token but no quote → nil LTP fields
	if got.Positions[1].LTP != nil {
		t.Errorf("IDEA row must have nil LTP (no quote), got %v", got.Positions[1].LTP)
	}
	// UNKNOWN — no token → nil LTP fields
	if got.Positions[2].LTP != nil {
		t.Errorf("UNKNOWN row must have nil LTP (no token), got %v", got.Positions[2].LTP)
	}
	// Top-level still HEALTHY (subsystem is healthy; individual rows just have no data)
	if got.LTPStatus != string(livealgos.StatusHealthy) {
		t.Errorf("subsystem is HEALTHY even if individual rows lack quotes, got %q", got.LTPStatus)
	}
}

func TestEnrichPositions_EmptyListIsHealthy(t *testing.T) {
	e := NewEnricher(&stubTokens{}, &stubLTP{status: livealgos.StatusUnavailable})
	got, err := e.EnrichPositions(context.Background(), nil)
	if err != nil {
		t.Fatalf("EnrichPositions: %v", err)
	}
	if got.LTPStatus != string(livealgos.StatusHealthy) {
		t.Errorf("empty list must be vacuously HEALTHY, got %q", got.LTPStatus)
	}
	if len(got.Positions) != 0 {
		t.Errorf("empty in → empty out, got %d rows", len(got.Positions))
	}
}

// -----------------------------------------------------------------------------

func TestEnrichSummary_HealthyPopulatesUnrealized(t *testing.T) {
	// 20 SBI @ 500 (invested 10k), current LTP 520 → currentValue 10400, unr +400.
	// 10 IDEA @ 15 (invested 150),  current LTP 12  → currentValue 120, unr -30.
	// Totals: invested=10150, current=10520, unrealized=+370.
	sum := &pfpb.Summary{
		TotalInvested:  10150,
		ActiveLotCount: 2,
	}
	lots := []*pfpb.ActivePosition{
		mkLot("SBI", 20, 500),
		mkLot("IDEA", 10, 15),
	}
	tokens := &stubTokens{byS: map[string]string{"SBI": "3045", "IDEA": "14366"}}
	ltp := &stubLTP{
		byToken: map[string]livealgos.LTPQuote{
			"3045":  {LTP: 520},
			"14366": {LTP: 12},
		},
		status: livealgos.StatusHealthy,
	}

	e := NewEnricher(tokens, ltp)
	got, err := e.EnrichSummary(context.Background(), sum, lots)
	if err != nil {
		t.Fatalf("EnrichSummary: %v", err)
	}
	if got.LTPStatus != string(livealgos.StatusHealthy) {
		t.Errorf("status: got %q, want HEALTHY", got.LTPStatus)
	}
	wantCur := float64(10520)
	wantUnr := float64(370)
	if got.CurrentValue == nil || *got.CurrentValue != wantCur {
		t.Errorf("CurrentValue: got %v, want %v", got.CurrentValue, wantCur)
	}
	if got.UnrealizedPnL == nil || *got.UnrealizedPnL != wantUnr {
		t.Errorf("UnrealizedPnL: got %v, want %v", got.UnrealizedPnL, wantUnr)
	}
}

func TestEnrichSummary_UnavailableLeavesUnrealizedNil(t *testing.T) {
	sum := &pfpb.Summary{TotalInvested: 10000}
	lots := []*pfpb.ActivePosition{mkLot("SBI", 20, 500)}
	tokens := &stubTokens{byS: map[string]string{"SBI": "3045"}}
	ltp := &stubLTP{status: livealgos.StatusUnavailable}

	e := NewEnricher(tokens, ltp)
	got, err := e.EnrichSummary(context.Background(), sum, lots)
	if err != nil {
		t.Fatalf("EnrichSummary: %v", err)
	}
	if got.LTPStatus != string(livealgos.StatusUnavailable) {
		t.Errorf("status: got %q, want UNAVAILABLE", got.LTPStatus)
	}
	if got.CurrentValue != nil || got.UnrealizedPnL != nil {
		t.Errorf("UNAVAILABLE: fields must be nil, got cur=%v unr=%v",
			got.CurrentValue, got.UnrealizedPnL)
	}
	// non-LTP totals still render
	if got.TotalInvested != 10000 {
		t.Errorf("TotalInvested must always render, got %v", got.TotalInvested)
	}
}
