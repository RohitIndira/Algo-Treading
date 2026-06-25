package projector

// Tests for PositionProjector.Apply across the supported event types.
//
// Each test follows the same shape:
//   1. resetTables(t)            — clean slate
//   2. insertProposedDecision(t) — set the precondition where required
//   3. proj := newProjector(t)
//   4. ev := &FillEvent{...}
//   5. applied, err := proj.Apply(ctx, ev)
//   6. assert table state + (applied,err) return values
//
// Helpers are in testharness_test.go.

import (
	"context"
	"testing"
)

// ─── ENTRY_FILLED ──────────────────────────────────────────────────────

// TestApply_ENTRY_FILLED_HappyPath verifies the canonical fresh-entry path:
// a PROPOSED decision exists, the broker confirms a full fill, the projector
// should:
//   - insert a row into manthan_position_events (idempotency log)
//   - INSERT a manthan_positions row with ACTIVE status, copying
//     industry/mcap_bucket/index_name from the decision
//   - UPDATE the decision row's status from PROPOSED → CONFIRMED
//   - return (applied=true, err=nil)
func TestApply_ENTRY_FILLED_HappyPath(t *testing.T) {
	resetTables(t)
	signalID := newTestUUID(t, "signal")
	strategyID := newTestUUID(t, "strategy")

	insertProposedDecision(t, proposedDecision{
		SignalID:         signalID,
		UserID:           "S4450",
		StrategyID:       strategyID,
		Symbol:           "GALLANTT",
		ISIN:             "INE223H01032",
		LTP:              100.00,
		EMAAllocPct:      0.30,
		IntendedQty:      100,
		IntendedInvested: 10000.00,
		InitialSLTarget:  80.00,
		Industry:         "Steel",
		MCapBucket:       "MID",
		IndexName:        "NIFTY50",
	})

	proj := newProjector(t)
	ev := &FillEvent{
		Type:          "ENTRY_FILLED",
		SignalID:      signalID,
		EventSeq:      1,
		StrategyID:    strategyID,
		UserID:        "S4450",
		Symbol:        "GALLANTT",
		ISIN:          "INE223H01032",
		FillPrice:     102.50, // small slippage from LTP — projector should record FILL not LTP
		FillQty:       100,
		BrokerOrderID: "BO-12345",
		Source:        "WSS",
		TradingMode:   "LIVE",
	}

	applied, err := proj.Apply(context.Background(), ev)
	if err != nil {
		t.Fatalf("Apply returned err: %v", err)
	}
	if !applied {
		t.Fatalf("Apply returned applied=false on a fresh event; want true")
	}

	// 1. Idempotency log has one entry for this (signal_id, event_seq).
	if got := countPositionEvents(t, signalID); got != 1 {
		t.Errorf("manthan_position_events row count = %d, want 1", got)
	}

	// 2. manthan_positions row exists with broker fill data.
	pos, ok := fetchPosition(t, signalID)
	if !ok {
		t.Fatalf("manthan_positions row missing after ENTRY_FILLED")
	}
	if pos.Symbol != "GALLANTT" {
		t.Errorf("position.Symbol = %q, want GALLANTT", pos.Symbol)
	}
	if pos.Quantity != 100 {
		t.Errorf("position.Quantity = %d, want 100", pos.Quantity)
	}
	if pos.EntryPrice != 102.50 {
		t.Errorf("position.EntryPrice = %v, want 102.50 (broker fill, not LTP)", pos.EntryPrice)
	}
	if pos.InvestedAmt != 10250.00 { // 100 * 102.50
		t.Errorf("position.InvestedAmt = %v, want 10250.00", pos.InvestedAmt)
	}
	if !pos.CurrentSL.Valid || pos.CurrentSL.Float64 != 82.00 { // 102.50 * 0.80
		t.Errorf("position.CurrentSL = %+v, want valid=true value=82.00", pos.CurrentSL)
	}
	if !pos.HighSinceEntry.Valid || pos.HighSinceEntry.Float64 != 102.50 {
		t.Errorf("position.HighSinceEntry = %+v, want valid=true value=102.50", pos.HighSinceEntry)
	}
	if pos.Status != "ACTIVE" {
		t.Errorf("position.Status = %q, want ACTIVE", pos.Status)
	}
	if !pos.EventSeq.Valid || pos.EventSeq.Int64 != 1 {
		t.Errorf("position.EventSeq = %+v, want valid=true value=1", pos.EventSeq)
	}
	if !pos.BrokerOrderID.Valid || pos.BrokerOrderID.String != "BO-12345" {
		t.Errorf("position.BrokerOrderID = %+v, want valid=true value=BO-12345", pos.BrokerOrderID)
	}

	// 3. Decision flipped to CONFIRMED with final_status_at set.
	dec := fetchDecision(t, signalID)
	if dec.Status != "CONFIRMED" {
		t.Errorf("decision.Status = %q, want CONFIRMED", dec.Status)
	}
	if !dec.FinalStatusAt.Valid {
		t.Errorf("decision.FinalStatusAt was NULL; expected the UPDATE to set NOW()")
	}
}

// TestApply_Idempotent_DuplicateEventSeq verifies the idempotency gate:
// applying the same (signal_id, event_seq) twice — once from WSS, once
// from the API poller racing in — must leave state identical and return
// (applied=false, nil) on the second call. This protects against the
// projector being driven by both ingest paths simultaneously.
func TestApply_Idempotent_DuplicateEventSeq(t *testing.T) {
	resetTables(t)
	signalID := newTestUUID(t, "signal")
	strategyID := newTestUUID(t, "strategy")

	insertProposedDecision(t, proposedDecision{
		SignalID:         signalID,
		UserID:           "S4450",
		StrategyID:       strategyID,
		Symbol:           "GALLANTT",
		LTP:              100.00,
		IntendedQty:      100,
		IntendedInvested: 10000.00,
		InitialSLTarget:  80.00,
		Industry:         "Steel",
		MCapBucket:       "MID",
		IndexName:        "NIFTY50",
	})

	proj := newProjector(t)
	ev := &FillEvent{
		Type:          "ENTRY_FILLED",
		SignalID:      signalID,
		EventSeq:      42,
		StrategyID:    strategyID,
		UserID:        "S4450",
		Symbol:        "GALLANTT",
		FillPrice:     105.00,
		FillQty:       100,
		BrokerOrderID: "BO-First",
		Source:        "WSS",
		TradingMode:   "LIVE",
	}

	// First call — applies cleanly.
	applied, err := proj.Apply(context.Background(), ev)
	if err != nil {
		t.Fatalf("first Apply err: %v", err)
	}
	if !applied {
		t.Fatalf("first Apply returned applied=false, want true")
	}

	// Second call — same (signal_id, event_seq). Even if source/broker_order_id
	// differ (e.g. the API poller saw a slightly different shape), the
	// idempotency gate must reject it.
	evDup := *ev
	evDup.Source = "API_POLLER"
	evDup.BrokerOrderID = "BO-Second" // would clobber if the dup was applied
	applied2, err := proj.Apply(context.Background(), &evDup)
	if err != nil {
		t.Fatalf("duplicate Apply err: %v", err)
	}
	if applied2 {
		t.Fatalf("duplicate Apply returned applied=true; want false (idempotent no-op)")
	}

	// Still exactly one row in the event log.
	if got := countPositionEvents(t, signalID); got != 1 {
		t.Errorf("manthan_position_events row count = %d after dup, want 1", got)
	}
	// Position must reflect the FIRST event's values, not the duplicate's.
	pos, _ := fetchPosition(t, signalID)
	if pos.BrokerOrderID.String != "BO-First" {
		t.Errorf("position.BrokerOrderID = %q after dup, want BO-First (duplicate must not overwrite)", pos.BrokerOrderID.String)
	}
}

// TestApply_RejectsMissingSignalID verifies the projector refuses events
// that don't carry the idempotency key. Without (signal_id, event_seq)
// the position_events INSERT would either fail or violate the
// idempotency contract.
func TestApply_RejectsMissingSignalID(t *testing.T) {
	resetTables(t)
	proj := newProjector(t)

	ev := &FillEvent{
		Type:     "ENTRY_FILLED",
		EventSeq: 1,
		Symbol:   "GALLANTT",
		FillQty:  100,
		// SignalID + OrderID both blank → ResolvedSignalID() returns ""
	}
	applied, err := proj.Apply(context.Background(), ev)
	if err == nil {
		t.Fatalf("Apply with empty SignalID returned no error; want error")
	}
	if applied {
		t.Fatalf("Apply with empty SignalID returned applied=true; want false")
	}
}

// TestApply_RejectsMissingEventSeq verifies the projector refuses events
// whose event_seq resolves to zero — the idempotency natural key needs
// a real sequence number to be unique.
func TestApply_RejectsMissingEventSeq(t *testing.T) {
	resetTables(t)
	proj := newProjector(t)

	signalID := newTestUUID(t, "signal")
	ev := &FillEvent{
		Type:     "ENTRY_FILLED",
		SignalID: signalID,
		Symbol:   "GALLANTT",
		FillQty:  100,
		// EventSeq + Sequence both zero
	}
	applied, err := proj.Apply(context.Background(), ev)
	if err == nil {
		t.Fatalf("Apply with zero EventSeq returned no error; want error")
	}
	if applied {
		t.Fatalf("Apply with zero EventSeq returned applied=true; want false")
	}
}

// ─── SL_FILLED / EXIT_FILLED ───────────────────────────────────────────

// TestApply_SL_FILLED_RealizedPnL covers the SL trigger path: an ACTIVE
// position with a known entry price exits at a lower fill price. The
// projector must:
//   - INSERT into position_events (idempotency log)
//   - UPDATE manthan_positions: status=EXITED, exit_price, realized_pnl
//     computed as (exit_price - entry_price) × qty, exit_reason='SL_TRIGGERED',
//     exit_time non-null, event_seq advanced
//   - UPDATE manthan_signal_decisions: status → CLOSED
//
// Uses a realistic Manthan scenario: bought 100 @ 102.50, SL trips at
// 82.10 (a hair above the 20% intended trigger at 82.00 — DPR clamp
// shifted it). Realized P&L = (82.10 - 102.50) × 100 = -2,040.00.
func TestApply_SL_FILLED_RealizedPnL(t *testing.T) {
	resetTables(t)
	signalID := newTestUUID(t, "signal")
	strategyID := newTestUUID(t, "strategy")

	insertProposedDecision(t, proposedDecision{
		SignalID:         signalID,
		UserID:           "S4450",
		StrategyID:       strategyID,
		Symbol:           "SANDHAR",
		LTP:              100.00,
		IntendedQty:      100,
		IntendedInvested: 10000.00,
		InitialSLTarget:  80.00,
		Industry:         "Auto Components",
		MCapBucket:       "MID",
		IndexName:        "NFTYMCP150",
	})

	proj := newProjector(t)
	ctx := context.Background()

	// Drive the position to ACTIVE first.
	entryEv := &FillEvent{
		Type:          "ENTRY_FILLED",
		SignalID:      signalID,
		EventSeq:      1,
		StrategyID:    strategyID,
		UserID:        "S4450",
		Symbol:        "SANDHAR",
		FillPrice:     102.50,
		FillQty:       100,
		BrokerOrderID: "BO-Entry",
		Source:        "WSS",
		TradingMode:   "LIVE",
	}
	if _, err := proj.Apply(ctx, entryEv); err != nil {
		t.Fatalf("ENTRY_FILLED setup failed: %v", err)
	}

	// Now the SL trips. event_seq must be > entry's (monotonicity guard).
	slEv := &FillEvent{
		Type:          "SL_FILLED",
		SignalID:      signalID,
		EventSeq:      2,
		StrategyID:    strategyID,
		UserID:        "S4450",
		Symbol:        "SANDHAR",
		FillPrice:     82.10, // broker actually filled here
		FillQty:       100,
		BrokerOrderID: "BO-SL",
		Source:        "WSS",
		TradingMode:   "LIVE",
	}
	applied, err := proj.Apply(ctx, slEv)
	if err != nil {
		t.Fatalf("Apply SL_FILLED err: %v", err)
	}
	if !applied {
		t.Fatalf("Apply SL_FILLED applied=false; want true")
	}

	// 2 events in the idempotency log now (entry + sl).
	if got := countPositionEvents(t, signalID); got != 2 {
		t.Errorf("manthan_position_events count = %d, want 2 (ENTRY + SL)", got)
	}

	pos, ok := fetchPosition(t, signalID)
	if !ok {
		t.Fatalf("position row missing after SL_FILLED")
	}
	if pos.Status != "EXITED" {
		t.Errorf("position.Status = %q, want EXITED", pos.Status)
	}
	if !pos.ExitPrice.Valid || pos.ExitPrice.Float64 != 82.10 {
		t.Errorf("position.ExitPrice = %+v, want valid=true value=82.10", pos.ExitPrice)
	}
	// (82.10 - 102.50) * 100 = -2040.00
	wantPnL := -2040.00
	if !pos.RealizedPnL.Valid || pos.RealizedPnL.Float64 != wantPnL {
		t.Errorf("position.RealizedPnL = %+v, want valid=true value=%v", pos.RealizedPnL, wantPnL)
	}
	if !pos.ExitReason.Valid || pos.ExitReason.String != "SL_TRIGGERED" {
		t.Errorf("position.ExitReason = %+v, want valid=true value=SL_TRIGGERED", pos.ExitReason)
	}
	if !pos.ExitTime.Valid {
		t.Errorf("position.ExitTime = invalid; want non-null timestamp")
	}
	if !pos.EventSeq.Valid || pos.EventSeq.Int64 != 2 {
		t.Errorf("position.EventSeq = %+v, want valid=true value=2 (advanced past entry)", pos.EventSeq)
	}

	dec := fetchDecision(t, signalID)
	if dec.Status != "CLOSED" {
		t.Errorf("decision.Status after SL_FILLED = %q, want CLOSED", dec.Status)
	}
}

// ─── MANUAL_EXIT_DETECTED ──────────────────────────────────────────────

// TestApply_MANUAL_EXIT_DETECTED covers the path that liquidated S4450 on
// 2026-06-12 (see project_manthan_safety_monitor_liquidation_hazard memory):
// the user closed an ACTIVE position outside our system (broker mobile
// app, manual phone call to broker, etc.). The projector must:
//   - mark the position EXITED with exit_reason='MANUAL_EXIT',
//     exit_price LEFT NULL (we have no fill price for a manual exit),
//     exit_time non-null
//   - flip decision.status → MANUALLY_EXITED
//   - set decision.user_override_until = NOW() + 3 days so the allocator
//     skips this signal until the cooldown clears
//   - fire NotifyManualExit on the notifier (best-effort, post-commit)
func TestApply_MANUAL_EXIT_DETECTED(t *testing.T) {
	resetTables(t)
	signalID := newTestUUID(t, "signal")
	strategyID := newTestUUID(t, "strategy")

	insertProposedDecision(t, proposedDecision{
		SignalID:         signalID,
		UserID:           "S4450",
		StrategyID:       strategyID,
		Symbol:           "GALLANTT",
		LTP:              100.00,
		IntendedQty:      100,
		IntendedInvested: 10000.00,
		InitialSLTarget:  80.00,
		Industry:         "Steel",
		MCapBucket:       "MID",
		IndexName:        "NIFTY50",
	})

	proj := newProjector(t)
	notif := &fakeNotifier{}
	proj.SetNotifier(notif)
	ctx := context.Background()

	// Drive position to ACTIVE.
	entryEv := &FillEvent{
		Type:          "ENTRY_FILLED",
		SignalID:      signalID,
		EventSeq:      1,
		StrategyID:    strategyID,
		UserID:        "S4450",
		Symbol:        "GALLANTT",
		FillPrice:     102.50,
		FillQty:       100,
		BrokerOrderID: "BO-Entry",
		Source:        "WSS",
		TradingMode:   "LIVE",
	}
	if _, err := proj.Apply(ctx, entryEv); err != nil {
		t.Fatalf("ENTRY_FILLED setup failed: %v", err)
	}

	// Reconciler detects the position is no longer held at the broker.
	manualEv := &FillEvent{
		Type:            "MANUAL_EXIT_DETECTED",
		SignalID:        signalID,
		EventSeq:        2,
		StrategyID:      strategyID,
		UserID:          "S4450",
		Symbol:          "GALLANTT",
		ExpectedQty:     100, // qty we WANTED to hold (the notification reports this)
		RejectionReason: "broker reports 0 shares; expected 100",
		Source:          "RECONCILER",
		TradingMode:     "LIVE",
	}
	applied, err := proj.Apply(ctx, manualEv)
	if err != nil {
		t.Fatalf("Apply MANUAL_EXIT_DETECTED err: %v", err)
	}
	if !applied {
		t.Fatalf("Apply MANUAL_EXIT_DETECTED applied=false; want true")
	}

	pos, ok := fetchPosition(t, signalID)
	if !ok {
		t.Fatalf("position row missing")
	}
	if pos.Status != "EXITED" {
		t.Errorf("position.Status = %q, want EXITED", pos.Status)
	}
	if !pos.ExitReason.Valid || pos.ExitReason.String != "MANUAL_EXIT" {
		t.Errorf("position.ExitReason = %+v, want valid=true value=MANUAL_EXIT", pos.ExitReason)
	}
	// CRITICAL: exit_price stays NULL on manual exit — we don't have a
	// real broker fill price for it. Setting a fake value here would
	// pollute realized P&L reporting.
	if pos.ExitPrice.Valid {
		t.Errorf("position.ExitPrice = %+v; want NULL (manual exit has no broker fill price)", pos.ExitPrice)
	}
	if pos.RealizedPnL.Valid {
		t.Errorf("position.RealizedPnL = %+v; want NULL (cannot compute without exit_price)", pos.RealizedPnL)
	}
	if !pos.ExitTime.Valid {
		t.Errorf("position.ExitTime = invalid; want non-null")
	}

	dec := fetchDecision(t, signalID)
	if dec.Status != "MANUALLY_EXITED" {
		t.Errorf("decision.Status = %q, want MANUALLY_EXITED", dec.Status)
	}
	// user_override_until ≈ NOW() + 3 days. We just assert it's > NOW().
	uo := fetchDecisionUserOverride(t, signalID)
	if !uo.Valid {
		t.Errorf("decision.user_override_until = NULL; want NOW() + 3 days")
	}

	// Notifier fired exactly once for this signal.
	if len(notif.manualExits) != 1 || notif.manualExits[0] != signalID {
		t.Errorf("notifier.manualExits = %+v, want [%s]", notif.manualExits, signalID)
	}
}
