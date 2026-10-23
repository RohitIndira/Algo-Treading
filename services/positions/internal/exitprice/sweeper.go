// Package exitprice re-books Manthan exits at their broker-authoritative fill.
//
// Why (2026-10-23): a stop-loss order's first FILLED event on order.events
// comes from the REST order book, which has no traded price for an SL-L, so
// the state machine fell back to the order's LIMIT leg (trigger − 2 %). All
// ten stop-outs that day were booked 1–2 % below their real average fills
// (FILATEX 89.30 booked vs 91.08 filled), and a later priced event — if one
// arrives at all — is dropped because the lot is no longer ACTIVE. The true
// average lives in trade-execution's ledger (manthan_orders.avg_fill_price,
// from the broker's trade book) and is reachable through LookupOrderMeta.
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
	"time"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// priceEpsilon — differences below half a paisa are rounding, not a correction.
const priceEpsilon = 0.005

// Store is the slice of the positions store the sweeper uses.
type Store interface {
	ListManthanExitsSince(ctx context.Context, since time.Time) ([]*store.Position, error)
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
}

// New builds a sweeper. lookback bounds how far back exits are re-checked
// (exit_time ≥ now − lookback); 0 → 24 h, so a deploy later in the day still
// corrects that morning's stop-outs.
func New(st Store, lookup Lookup, pub Publisher, lookback time.Duration, logger *zap.Logger) *Sweeper {
	if lookback <= 0 {
		lookback = 24 * time.Hour
	}
	return &Sweeper{store: st, lookup: lookup, pub: pub, logger: logger, lookback: lookback}
}

// RunTicker sweeps once immediately, then every interval until ctx ends.
func (s *Sweeper) RunTicker(ctx context.Context, interval time.Duration) {
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

// SweepOnce runs one pass. Returns the number of lots corrected.
func (s *Sweeper) SweepOnce(ctx context.Context) int {
	exits, err := s.store.ListManthanExitsSince(ctx, time.Now().Add(-s.lookback))
	if err != nil {
		s.logger.Warn("exit-price sweep: list failed", zap.Error(err))
		return 0
	}
	corrected := 0
	for _, pos := range exits {
		if ctx.Err() != nil {
			return corrected
		}
		meta, err := s.lookup.LookupOrderMetaFresh(ctx, pos.ExitBrokerOrderID)
		if err != nil {
			s.logger.Debug("exit-price sweep: meta lookup failed",
				zap.String("symbol", pos.Symbol), zap.String("exit_broker_order_id", pos.ExitBrokerOrderID), zap.Error(err))
			continue
		}
		newPrice, newPnL, ok := Correction(pos, meta)
		if !ok {
			continue
		}
		ev := correctionEvent(pos, meta, newPrice, newPnL)
		changed, err := s.store.CorrectExitPriceWithEvent(ctx, pos.PositionID, newPrice, newPnL, ev)
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
		if s.pub != nil {
			s.pub.Publish(ctx, &publisher.PositionEvent{
				EventType:     publisher.EventPositionExitCorrected,
				PositionID:    pos.PositionID.String(),
				Origin:        pos.Origin,
				UserID:        pos.UserID,
				StrategyID:    pos.StrategyID,
				SignalID:      pos.SignalID,
				Symbol:        pos.Symbol,
				Action:        publisher.ActionExit,
				Price:         newPrice,
				Quantity:      pos.Quantity,
				ExitReason:    pos.ExitReason,
				RealizedPnL:   newPnL,
				BrokerOrderID: pos.ExitBrokerOrderID,
			})
		}
	}
	return corrected
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
// not a Manthan order, or the booked price already matches.
func Correction(pos *store.Position, meta tradeexec.OrderMeta) (newPrice, newPnL float64, ok bool) {
	if pos == nil || !meta.Found || meta.AvgFillPrice <= 0 || pos.Quantity <= 0 {
		return 0, 0, false
	}
	if math.Abs(meta.AvgFillPrice-pos.ExitPrice) < priceEpsilon {
		return 0, 0, false
	}
	newPrice = meta.AvgFillPrice
	newPnL = (newPrice - pos.EntryPrice) * float64(pos.Quantity)
	return newPrice, newPnL, true
}
