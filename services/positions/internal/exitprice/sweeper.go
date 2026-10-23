// Package exitprice re-books Manthan exits at their broker-authoritative fill.
//
// Why (2026-10-23): a stop-loss order's first FILLED event on order.events
// comes from the REST order book, which has no traded price for an SL-L, so
// the state machine fell back to the order's LIMIT leg (trigger − 2 %). All
// ten stop-outs that day were booked 1–2 % below their real average fills
// (FILATEX 89.30 booked vs 91.08 filled), and a later priced event — if one
// arrives at all — is dropped because the lot is no longer ACTIVE. The true
// average lives in trade-execution's ledger (manthan_orders.avg_fill_price,
// from the broker's trade book, back-filled by trade-execution when the trade
// book lagged the poll) and is reachable through LookupOrderMeta.
//
// The sweeper re-reads that value for every recent exit and corrects the
// lot (exit_price, realized_pnl), audits EXIT_PRICE_CORRECTED and publishes
// POSITION_EXIT_CORRECTED so the rules-engine corrects trading_db too.
// It never changes a lot's status or quantity and never touches the broker.
package exitprice

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

const (
	// priceEpsilon — differences below half a paisa are rounding, not a correction.
	priceEpsilon = 0.005
	// callTimeout bounds every DB / gRPC call so a hung dependency cannot
	// stall the sweep loop (ticks are dropped while a sweep blocks).
	callTimeout = 5 * time.Second
	// republishWindow — corrections younger than this are re-published every
	// sweep: the DB write and the Kafka publish are not atomic, so a dropped
	// publish would otherwise leave trading_db diverged for good. The
	// rules-engine applies the same values idempotently.
	republishWindow = 10 * time.Minute
	// notFoundMemo — an exit whose broker order id is not a Manthan order
	// (a user's own sell that closed the lot) is skipped for this long
	// instead of being asked of trade-execution every minute.
	notFoundMemo = 24 * time.Hour
)

// Store is the slice of the positions store the sweeper uses.
type Store interface {
	ListManthanExitsSince(ctx context.Context, since time.Time) ([]*store.Position, error)
	ListRecentlyCorrectedExits(ctx context.Context, since time.Time) ([]*store.Position, error)
	CorrectExitPriceWithEvent(ctx context.Context, positionID uuid.UUID, exitPrice, realizedPnL float64, ev *store.PositionEvent) (bool, error)
}

// Lookup is the uncached meta lookup (tradeexec.Client.LookupOrderMetaFresh).
type Lookup interface {
	LookupOrderMetaFresh(ctx context.Context, brokerOrderID string) (tradeexec.OrderMeta, error)
}

// Publisher is the position.events fan-out.
type Publisher interface {
	Publish(ctx context.Context, ev *publisher.PositionEvent)
}

// Sweeper periodically corrects recent exits.
type Sweeper struct {
	store    Store
	lookup   Lookup
	pub      Publisher
	logger   *zap.Logger
	lookback time.Duration

	mu       sync.Mutex
	notFound map[string]time.Time // exit broker order id → when trade-execution said "not ours"

	done chan struct{}
}

// New builds a sweeper. lookback bounds how far back exits are re-checked
// (exit_time ≥ now − lookback); 0 → 24 h, so a deploy later in the day still
// corrects that morning's stop-outs.
func New(st Store, lookup Lookup, pub Publisher, lookback time.Duration, logger *zap.Logger) *Sweeper {
	if lookback <= 0 {
		lookback = 24 * time.Hour
	}
	return &Sweeper{
		store: st, lookup: lookup, pub: pub, logger: logger, lookback: lookback,
		notFound: map[string]time.Time{},
		done:     make(chan struct{}),
	}
}

// RunTicker sweeps once immediately, then every interval until ctx ends. The
// in-flight sweep finishes before it returns; Wait reports that.
func (s *Sweeper) RunTicker(ctx context.Context, interval time.Duration) {
	defer close(s.done)
	if interval <= 0 {
		interval = time.Minute
	}
	s.logger.Info("exit-price sweeper started", zap.Duration("interval", interval), zap.Duration("lookback", s.lookback))
	s.SweepOnce(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.logger.Info("exit-price sweeper stopped")
			return
		case <-t.C:
			s.SweepOnce(ctx)
		}
	}
}

// Wait blocks until RunTicker has returned or the timeout passes — lets a
// shutdown finish the in-flight sweep (DB write + publish) before the Kafka
// writer and the gRPC client are closed.
func (s *Sweeper) Wait(timeout time.Duration) {
	select {
	case <-s.done:
	case <-time.After(timeout):
		s.logger.Warn("exit-price sweeper did not stop within the shutdown timeout")
	}
}

// SweepOnce runs one pass. Returns the number of lots corrected.
func (s *Sweeper) SweepOnce(ctx context.Context) int {
	lctx, cancel := context.WithTimeout(ctx, callTimeout)
	exits, err := s.store.ListManthanExitsSince(lctx, time.Now().Add(-s.lookback))
	cancel()
	if err != nil {
		s.logger.Warn("exit-price sweep: list failed", zap.Error(err))
		return 0
	}
	corrected, lookupFailed := 0, 0
	var lastLookupErr error
	for _, pos := range exits {
		if ctx.Err() != nil {
			return corrected
		}
		if s.skipNotFound(pos.ExitBrokerOrderID) {
			continue
		}
		mctx, mcancel := context.WithTimeout(ctx, callTimeout)
		meta, err := s.lookup.LookupOrderMetaFresh(mctx, pos.ExitBrokerOrderID)
		mcancel()
		if err != nil {
			lookupFailed++
			lastLookupErr = err
			continue
		}
		if !meta.Found {
			s.rememberNotFound(pos.ExitBrokerOrderID)
			continue
		}
		newPrice, newPnL, ok := Correction(pos, meta)
		if !ok {
			continue
		}
		ev := correctionEvent(pos, meta, newPrice, newPnL)
		wctx, wcancel := context.WithTimeout(ctx, callTimeout)
		changed, err := s.store.CorrectExitPriceWithEvent(wctx, pos.PositionID, newPrice, newPnL, ev)
		wcancel()
		if err != nil {
			s.logger.Warn("exit-price sweep: correction write failed",
				zap.String("symbol", pos.Symbol), zap.String("position_id", pos.PositionID.String()), zap.Error(err))
			continue
		}
		if !changed {
			continue
		}
		corrected++
		s.logger.Info("exit price corrected to the broker fill",
			zap.String("user_id", pos.UserID), zap.String("symbol", pos.Symbol),
			zap.String("position_id", pos.PositionID.String()), zap.String("exit_broker_order_id", pos.ExitBrokerOrderID),
			zap.Float64("booked", pos.ExitPrice), zap.Float64("fill", newPrice),
			zap.Float64("pnl_before", pos.RealizedPnL), zap.Float64("pnl_after", newPnL))
		pos.ExitPrice, pos.RealizedPnL = newPrice, newPnL
		s.publish(ctx, pos)
	}
	if lookupFailed > 0 {
		// One line per sweep, at Warn: a dead gRPC path to trade-execution
		// must be visible — every stop-out would otherwise stay at its limit leg.
		s.logger.Warn("exit-price sweep: meta lookups failed this pass",
			zap.Int("failed", lookupFailed), zap.Int("lots", len(exits)), zap.NamedError("last_error", lastLookupErr))
	}
	s.republishRecent(ctx)
	return corrected
}

// republishRecent re-sends POSITION_EXIT_CORRECTED for lots corrected inside
// republishWindow (the rules-engine write is idempotent), so a publish lost to
// a Kafka hiccup or a restart between the DB write and the send still lands.
func (s *Sweeper) republishRecent(ctx context.Context) {
	if s.pub == nil {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, callTimeout)
	recent, err := s.store.ListRecentlyCorrectedExits(rctx, time.Now().Add(-republishWindow))
	cancel()
	if err != nil {
		s.logger.Warn("exit-price sweep: recent-corrections list failed", zap.Error(err))
		return
	}
	for _, pos := range recent {
		s.publish(ctx, pos)
	}
}

func (s *Sweeper) publish(ctx context.Context, pos *store.Position) {
	if s.pub == nil {
		return
	}
	s.pub.Publish(ctx, &publisher.PositionEvent{
		EventType:     publisher.EventPositionExitCorrected,
		PositionID:    pos.PositionID.String(),
		Origin:        pos.Origin,
		UserID:        pos.UserID,
		StrategyID:    pos.StrategyID,
		SignalID:      pos.SignalID,
		Symbol:        pos.Symbol,
		Action:        publisher.ActionExit,
		Price:         pos.ExitPrice,
		Quantity:      pos.Quantity,
		ExitReason:    pos.ExitReason,
		RealizedPnL:   pos.RealizedPnL,
		BrokerOrderID: pos.ExitBrokerOrderID,
	})
}

func (s *Sweeper) skipNotFound(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	at, ok := s.notFound[id]
	if !ok {
		return false
	}
	if time.Since(at) > notFoundMemo {
		delete(s.notFound, id)
		return false
	}
	return true
}

func (s *Sweeper) rememberNotFound(id string) {
	s.mu.Lock()
	s.notFound[id] = time.Now()
	s.mu.Unlock()
}

// correctionEvent builds the EXIT_PRICE_CORRECTED audit row. raw_source_event
// is a NOT NULL jsonb column: the "source event" here is the sweeper's own
// observation, written as a small JSON document (the first deploy on
// 2026-10-23 failed every write with "invalid input syntax for type json"
// because this was left empty).
func correctionEvent(pos *store.Position, meta tradeexec.OrderMeta, newPrice, newPnL float64) *store.PositionEvent {
	raw, _ := json.Marshal(map[string]any{
		"source":               "exit-price-sweeper",
		"exit_broker_order_id": pos.ExitBrokerOrderID,
		"booked_exit_price":    pos.ExitPrice,
		"ledger_avg_fill":      meta.AvgFillPrice,
		"booked_realized_pnl":  pos.RealizedPnL,
		"corrected_pnl":        newPnL,
		"meta_order_type":      meta.OrderType,
	})
	return &store.PositionEvent{
		EventType:        store.EventTypeExitPriceCorrected,
		BrokerOrderID:    pos.ExitBrokerOrderID,
		SignalID:         pos.SignalID,
		FillPrice:        newPrice,
		RealizedPnLDelta: newPnL - pos.RealizedPnL,
		Reason:           fmt.Sprintf("exit re-booked from %.2f to the ledger avg fill %.2f (trade-execution avg_fill_price)", pos.ExitPrice, newPrice),
		RawSourceEvent:   raw,
		SourceTopic:      "exit-price-sweeper",
		SourceEventID:    fmt.Sprintf("exitfix-%s-%.2f", pos.ExitBrokerOrderID, newPrice),
	}
}

// Correction decides whether a lot's booked exit must move to the ledger's
// average fill. ok=false when the ledger has no fill yet (0), the order is
// not a Manthan order, or the booked price already matches. The P&L moves by
// (new − booked) × quantity-at-close, so partial exits booked earlier on the
// same lot are preserved.
func Correction(pos *store.Position, meta tradeexec.OrderMeta) (newPrice, newPnL float64, ok bool) {
	if pos == nil || !meta.Found || meta.AvgFillPrice <= 0 || pos.Quantity <= 0 {
		return 0, 0, false
	}
	if math.Abs(meta.AvgFillPrice-pos.ExitPrice) < priceEpsilon {
		return 0, 0, false
	}
	newPrice = meta.AvgFillPrice
	newPnL = pos.RealizedPnL + (newPrice-pos.ExitPrice)*float64(pos.Quantity)
	return newPrice, newPnL, true
}
