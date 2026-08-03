package manthan

import (
	"context"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// FIX F — restart trail recovery.
//
// rules-engine computes the trailing-SL working state (HighSinceEntry,
// LastTrailLevel, CurrentSL) in memory and is the ONLY owner of it. Nothing
// else writes it. Before this fix, that state lived only in memory, so a
// rules-engine restart forgot every position's trail (rehydrate.go reads
// manthan_positions, which was empty) — the SL stopped advancing and the
// soft-exit stopped firing for the whole book.
//
// These methods mirror the in-memory portfolio into trading_db.manthan_positions
// at the SAME points the in-memory state changes, so rehydrate can restore the
// exact trail after a restart. This deliberately re-introduces a rules-engine
// writer of manthan_positions (the projector deleted 2026-07-10), scoped
// narrowly to trail-resume state.
//
// Persistence points (mirror memory exactly — entry-dispatch, not fill, because
// the in-memory model is optimistic and has no fill-confirmation source; see
// docs/known_issues or the FIX 3 pre-check):
//   - PersistPositionOpen : at AddPosition (entry-dispatch)
//   - PersistTrail         : at each SL_MODIFY (trail ratchet)
//   - PersistExit          : at SL exit
//
// All three are best-effort + logged, never fatal to the trading loop:
//   - A failed OPEN insert = same as today (nothing persisted) → restart just
//     can't restore that one position. No worse than the pre-fix behavior.
//   - A failed TRAIL update = memory advances, DB lags one ratchet. On restart
//     the slightly-lower trail is restored, and because SL only moves UP the
//     next tick re-ratchets it — it can NEVER restore a higher/looser stop.
//   - A failed EXIT update = an EXITED position may re-appear on restart; the
//     orphan scanner / next tick reconcile it (it's already gone at the broker).

// PersistPositionOpen inserts the ACTIVE position row that mirrors the in-memory
// AddPosition. Idempotent via ON CONFLICT (signal_id) — the deterministic entry
// OrderID is the replay key, so a Kafka replay / re-fire is a clean no-op.
func (p *ManthanPublisher) PersistPositionOpen(ctx context.Context, order ManthanOrder) error {
	if p.db == nil {
		return nil
	}
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO manthan_positions (
			strategy_id, user_id, symbol, isin, industry, mcap_bucket, index_name,
			entry_price, quantity, invested_amt, ema_alloc_pct,
			high_since_entry, current_sl, last_trail_level,
			status, signal_id
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'ACTIVE',$15)
		ON CONFLICT (signal_id) WHERE signal_id IS NOT NULL DO NOTHING`,
		order.StrategyID, order.UserID, order.Symbol, order.ISIN,
		order.Industry, order.MCapBucket, order.IndexName,
		order.EntryPrice, order.Quantity, order.InvestedAmt, order.EMAAllocPct/100,
		order.EntryPrice, order.StopLoss, order.EntryPrice, // high=entry, sl=initialSL, trail=entry
		order.OrderID,
	)
	if err != nil {
		p.logger.Warn("PersistPositionOpen failed — position not restart-recoverable (memory unaffected)",
			zap.String("symbol", order.Symbol),
			zap.String("signal_id", order.OrderID),
			zap.Error(err))
	}
	return err
}

// PersistTrail mirrors an in-memory trail ratchet to the DB. Best-effort:
// safe-if-stale because SL only moves up (see file docstring).
func (p *ManthanPublisher) PersistTrail(ctx context.Context, strategyID string, pos types.Position) error {
	if p.db == nil {
		return nil
	}
	_, err := p.db.ExecContext(ctx, `
		UPDATE manthan_positions
		SET current_sl = $1, high_since_entry = $2, last_trail_level = $3, updated_at = now()
		WHERE strategy_id = $4 AND symbol = $5 AND status = 'ACTIVE'`,
		pos.CurrentSL, pos.HighSinceEntry, pos.LastTrailLevel, strategyID, pos.Symbol)
	if err != nil {
		p.logger.Warn("PersistTrail failed — DB trail lags memory by one ratchet (safe: SL only moves up, next tick re-ratchets)",
			zap.String("symbol", pos.Symbol),
			zap.Float64("current_sl", pos.CurrentSL),
			zap.Error(err))
	}
	return err
}

// PersistExit marks the position EXITED so rehydrate does not restore a closed
// position after restart. Best-effort.
func (p *ManthanPublisher) PersistExit(ctx context.Context, strategyID, symbol string, exitPrice, pnl float64) error {
	if p.db == nil {
		return nil
	}
	_, err := p.db.ExecContext(ctx, `
		UPDATE manthan_positions
		SET status = 'EXITED', exit_price = $1, realized_pnl = $2,
		    exit_reason = 'TSL_HIT', exit_time = now(), updated_at = now()
		WHERE strategy_id = $3 AND symbol = $4 AND status = 'ACTIVE'`,
		exitPrice, pnl, strategyID, symbol)
	if err != nil {
		p.logger.Warn("PersistExit failed — EXITED position may re-appear on restart (reconciled by orphan scanner / next tick)",
			zap.String("symbol", symbol), zap.Error(err))
	}
	return err
}
