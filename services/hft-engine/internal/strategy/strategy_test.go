// Unit tests for the chunk-aware state machine.
//
// Each test drives synthetic ticks + fills into a Runner that's wired to
// a PaperBroker. We assert on the published Snapshot — never read r.live
// directly, mirroring how real callers see state.
//
// Coverage:
//   - happy path: IDLE → place → fill → IDLE → place → ... → HALT(max_reached)
//   - partial fill: place chunk → 5 of 10 fills → MODIFY (NOT a new place) → 5 more fill → next chunk
//   - price-band halt: tick above buy_limit → cancel + HALT(price_band)
//   - never-place-2nd-chunk invariant: PaperBroker.OnPlace counts and
//     asserts no place during CHUNK_OPEN/PARTIAL
package strategy

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/audit"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/broker"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// ─────────────────────────────────────────────────────────────────────────
// Test fixture
// ─────────────────────────────────────────────────────────────────────────

// fixture wires a Runner with a PaperBroker we control, in PAPER mode,
// no trade-window restriction, and a no-op audit writer (audit.New with
// a nil repo would panic on flush — we use a separate stub).
type fixture struct {
	t       *testing.T
	runner  *Runner
	broker  *broker.PaperBroker
	placeCh chan placedOrder
	cancel  context.CancelFunc
}

type placedOrder struct {
	orderID string
	side    state.Side
	qty     int
	price   float64
}

func newFixture(t *testing.T, cfg state.Config) *fixture {
	t.Helper()
	logger := zaptest.NewLogger(t)
	pb := broker.NewPaperBroker(logger)

	// Tests need to know what got placed so they can drive synthetic fills.
	// PaperBroker.OnPlace forwards every placement into a buffered channel.
	placeCh := make(chan placedOrder, 32)
	pb.OnPlace = func(id string, sym broker.SymbolSpec, side state.Side, qty int, price float64) {
		placeCh <- placedOrder{orderID: id, side: side, qty: qty, price: price}
	}

	// audit writer with nil repo — we set BatchSize huge and never flush
	// during the test so InsertAuditOrder never gets called.
	aw := audit.New(nil, logger, audit.Config{
		ChannelSize:   1000,
		BatchSize:     1_000_000,
		FlushInterval: 1 * time.Hour,
	})
	aw.Start(context.Background()) // worker idles waiting for flush

	auth := &broker.AuthContext{UserID: cfg.UserID}
	sym := broker.SymbolSpec{
		Symbol:    cfg.Symbol,
		Exchange:  cfg.Exchange,
		TickSize:  cfg.TickSize,
	}
	r := NewRunner(cfg, auth, sym, pb, aw, logger)

	ctx, cancel := context.WithCancel(context.Background())
	go r.Run(ctx)
	// Give Run a moment to enter its select.
	time.Sleep(20 * time.Millisecond)

	t.Cleanup(func() {
		cancel()
		<-r.Done()
		aw.Stop()
	})

	return &fixture{t: t, runner: r, broker: pb, placeCh: placeCh, cancel: cancel}
}

// nextPlace blocks until PaperBroker receives a PlaceLimit call, or fails
// the test after 500ms.
func (f *fixture) nextPlace() placedOrder {
	f.t.Helper()
	select {
	case p := <-f.placeCh:
		return p
	case <-time.After(500 * time.Millisecond):
		f.t.Fatal("expected a PlaceLimit call within 500ms — none received")
		return placedOrder{}
	}
}

// noPlace asserts that no PlaceLimit occurs in the next 100ms.
// Used to prove the chunk-completion invariant.
func (f *fixture) noPlace() {
	f.t.Helper()
	select {
	case p := <-f.placeCh:
		f.t.Fatalf("unexpected PlaceLimit: side=%s qty=%d price=%.2f", p.side, p.qty, p.price)
	case <-time.After(100 * time.Millisecond):
	}
}

// waitSnap polls Snapshot() until the predicate is true or timeout.
// Cheaper than sleeping a fixed duration.
func (f *fixture) waitSnap(pred func(*state.Strategy) bool, timeout time.Duration) *state.Strategy {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s := f.runner.Snapshot()
		if pred(s) {
			return s
		}
		time.Sleep(5 * time.Millisecond)
	}
	f.t.Fatalf("snapshot predicate never satisfied within %v", timeout)
	return nil
}

// baseCfg returns a typical BUY-only HFT config for tests.
func baseCfg() state.Config {
	return state.Config{
		StrategyID:          "00000000-0000-0000-0000-000000000001",
		UserID:              "TEST",
		Symbol:              "IDEA",
		Exchange:            "NSE",
		Side:                state.SideBuy, // BUY only — sell side never fires
		MaxBuyQty:           30,
		MaxSellQty:          0,
		SingleBuyQty:        10,
		SingleSellQty:       0,
		BuyLimitPrice:       13.00,
		SellLimitPrice:      0,
		TickSize:            0.01,
		WindowStart:         "", // disabled
		WindowEnd:           "",
		ModifyOnPriceChange: true,
		Mode:                state.ModePaper,
	}
}

// tick is the helper to push a synthetic bid/ask into the runner.
func tick(r *Runner, bid, ask float64) {
	r.SendTick(state.MarketData{
		Symbol: "IDEA",
		Bid:    bid,
		Ask:    ask,
		At:     time.Now(),
	})
}

// fill is the helper to push a synthetic FILL into the runner.
func fillFor(r *Runner, brokerOrderID string, qty int, price float64) {
	r.SendFill(state.FillEvent{
		BrokerOrderID: brokerOrderID,
		EventType:     "FILL",
		FillQty:       qty,
		FillPrice:     price,
		At:            time.Now(),
	})
}

// ─────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────

// TestHappyPath_FullFillsHitMax exercises 3 chunks of 10, each fully
// filled in one go, until max_buy_qty=30 is reached and the side
// HALTs with reason max_reached.
func TestHappyPath_FullFillsHitMax(t *testing.T) {
	cfg := baseCfg()
	f := newFixture(t, cfg)

	for i := 1; i <= 3; i++ {
		// Send a tick within band → expect a PLACE.
		tick(f.runner, 12.49, 12.50)
		p := f.nextPlace()

		if p.side != state.SideBuy || p.qty != 10 {
			t.Fatalf("chunk %d: expected BUY qty=10, got side=%s qty=%d", i, p.side, p.qty)
		}

		// Synthesize full fill.
		fillFor(f.runner, p.orderID, 10, p.price)

		// After fill, side should be IDLE again with Position = 10*i.
		// On the last iteration the cap (30) is hit and side.Done=true.
		expected := 10 * i
		f.waitSnap(func(s *state.Strategy) bool {
			return s.Buy.Position == expected
		}, 500*time.Millisecond)
	}

	// Final tick — side is Done, should NOT place a 4th chunk.
	tick(f.runner, 12.49, 12.50)
	f.noPlace()

	s := f.runner.Snapshot()
	if !s.Buy.Done {
		t.Fatalf("expected Buy.Done=true after hitting max, got false")
	}
	if s.Buy.HaltReason != state.HaltMaxReached {
		t.Fatalf("expected HaltReason=max_reached, got %q", s.Buy.HaltReason)
	}
	if s.Buy.Position != 30 {
		t.Fatalf("expected Position=30, got %d", s.Buy.Position)
	}
	if len(s.Buy.History) != 3 {
		t.Fatalf("expected 3 chunks in history, got %d", len(s.Buy.History))
	}
}

// TestPartialFill_ChunkCompletionInvariant is THE key test for the
// chunk-aware state machine. We place a chunk of 10, fill only 5,
// send several MORE ticks at different prices, and assert that
// NO new chunk is placed — only modifies. The next chunk only
// appears after the broker confirms the remaining 5 filled.
func TestPartialFill_ChunkCompletionInvariant(t *testing.T) {
	cfg := baseCfg()
	cfg.MaxBuyQty = 20 // 2 chunks of 10
	f := newFixture(t, cfg)

	modifyCount := atomic.Int32{}
	f.broker.OnModify = func(orderID string, qty int, newPrice float64) {
		modifyCount.Add(1)
	}

	// First tick — place chunk #1
	tick(f.runner, 12.49, 12.50)
	p1 := f.nextPlace()
	if p1.qty != 10 {
		t.Fatalf("expected chunk qty=10, got %d", p1.qty)
	}

	// Partial fill — 5 of 10.
	fillFor(f.runner, p1.orderID, 5, p1.price)
	f.waitSnap(func(s *state.Strategy) bool {
		return s.Buy.Position == 5 && s.Buy.Current != nil &&
			s.Buy.Current.Status == state.ChunkPartial
	}, 500*time.Millisecond)

	// Send 5 more ticks at different prices. Each should MODIFY, NOT place
	// a new chunk — the invariant from your point #3.
	for i := 0; i < 5; i++ {
		tick(f.runner, 12.50+float64(i)*0.01, 12.51+float64(i)*0.01)
		f.noPlace() // ← THE assertion: no new chunk while one is partial
	}
	if modifyCount.Load() < 1 {
		t.Fatalf("expected at least 1 MODIFY during partial fill, got %d", modifyCount.Load())
	}

	// Now complete chunk #1 — broker fills the remaining 5.
	fillFor(f.runner, p1.orderID, 5, p1.price)
	f.waitSnap(func(s *state.Strategy) bool {
		return s.Buy.Position == 10 && s.Buy.Current == nil
	}, 500*time.Millisecond)

	// Side is back in IDLE — next tick should place chunk #2.
	tick(f.runner, 12.52, 12.53)
	p2 := f.nextPlace()
	if p2.orderID == p1.orderID {
		t.Fatal("chunk #2 should have a different broker_order_id than chunk #1")
	}
	if p2.qty != 10 {
		t.Fatalf("expected chunk #2 qty=10, got %d", p2.qty)
	}

	// Full-fill chunk #2 → Position hits max → HALT.
	fillFor(f.runner, p2.orderID, 10, p2.price)
	f.waitSnap(func(s *state.Strategy) bool {
		return s.Buy.Done && s.Buy.HaltReason == state.HaltMaxReached
	}, 500*time.Millisecond)
}

// TestPriceBandHalt_CancelsRestingChunk: a tick above buy_limit while
// a chunk is open → broker.Cancel is called, side enters HALTED("price_band").
func TestPriceBandHalt_CancelsRestingChunk(t *testing.T) {
	cfg := baseCfg()
	f := newFixture(t, cfg)

	cancelCount := atomic.Int32{}
	f.broker.OnCancel = func(orderID string) { cancelCount.Add(1) }

	// Place a chunk at ask=12.50 (within band of 13.00).
	tick(f.runner, 12.49, 12.50)
	f.nextPlace()

	// Now price escapes the band.
	tick(f.runner, 13.04, 13.05)

	f.waitSnap(func(s *state.Strategy) bool {
		return s.Buy.Done && s.Buy.HaltReason == state.HaltPriceBand
	}, 500*time.Millisecond)
	if cancelCount.Load() != 1 {
		t.Fatalf("expected exactly 1 Cancel on band breach, got %d", cancelCount.Load())
	}

	// Further ticks should be no-ops (terminal state).
	tick(f.runner, 12.40, 12.41)
	f.noPlace()
}

// TestPriceBand_NoOrderResting: tick out of band when IDLE (no chunk).
// Should still HALT, but NOT call Cancel (nothing to cancel).
func TestPriceBand_NoOrderResting(t *testing.T) {
	cfg := baseCfg()
	f := newFixture(t, cfg)

	cancelCount := atomic.Int32{}
	f.broker.OnCancel = func(orderID string) { cancelCount.Add(1) }

	// First (and only) tick is already out of band.
	tick(f.runner, 13.04, 13.05)
	f.waitSnap(func(s *state.Strategy) bool {
		return s.Buy.Done && s.Buy.HaltReason == state.HaltPriceBand
	}, 500*time.Millisecond)
	if cancelCount.Load() != 0 {
		t.Fatalf("expected 0 Cancels when no chunk resting, got %d", cancelCount.Load())
	}
}
