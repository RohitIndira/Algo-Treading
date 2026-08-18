package manthan

import (
	"time"
	"fmt"
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

// PersistPositionOpen inserts the position row at entry-DISPATCH as
// PENDING_ENTRY — NOT ACTIVE. A dispatch is optimistic: the entry can die at
// trade-execution (dead auth, upper circuit, DLQ) without ever filling, and
// persisting ACTIVE here created phantom rows that polluted the sector/mcap
// cap counters and were resurrected by every rehydrate (2026-08-18: 29
// phantoms had both users' SMALL bucket "full" and blocked all entries).
// Promotion to ACTIVE happens ONLY in PersistFillConfirmed, driven by the
// position.events fill confirmation. Idempotent via ON CONFLICT (signal_id).
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
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'PENDING_ENTRY',$15)
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

// PersistFillConfirmed promotes a PENDING_ENTRY row to ACTIVE with the
// broker-confirmed fill price/qty. Called from the position.events
// POSITION_OPENED consumer — the same confirmation that arms the in-memory
// re-entry guard — so the DB can never hold an ACTIVE row for an entry that
// did not really fill. Also idempotently upgrades legacy ACTIVE rows'
// price/qty when the confirmation arrives late.
func (p *ManthanPublisher) PersistFillConfirmed(ctx context.Context, strategyID, symbol string, fillPrice float64, qty int32) error {
	if p.db == nil {
		return nil
	}
	_, err := p.db.ExecContext(ctx, `
		UPDATE manthan_positions
		SET status = 'ACTIVE',
		    entry_price = $1,
		    quantity = $2,
		    invested_amt = $5,
		    high_since_entry = GREATEST(COALESCE(high_since_entry, 0), $1),
		    updated_at = now()
		WHERE strategy_id = $3 AND symbol = $4
		  AND status IN ('PENDING_ENTRY','ACTIVE')`,
		fillPrice, qty, strategyID, symbol, fillPrice*float64(qty))
	if err != nil {
		p.logger.Warn("PersistFillConfirmed failed — row stays PENDING_ENTRY until next confirmation/restart",
			zap.String("symbol", symbol), zap.Error(err))
	}
	return err
}

// PersistExit marks the position EXITED so rehydrate does not restore a closed
// position after restart. reason is the CONFIRMED exit reason from
// position.events (SL_TRIGGER → recorded as TSL_HIT for continuity). This is
// only called on a broker-confirmed exit — never at trail-cross (2026-08-18:
// trail-cross exits booked 4 positions as TSL_HIT that never sold; the SL_EXIT
// orders had died at trade-execution while the book showed them gone).
func (p *ManthanPublisher) PersistExit(ctx context.Context, strategyID, symbol string, exitPrice, pnl float64, reason string) error {
	if p.db == nil {
		return nil
	}
	if reason == "" || reason == "SL_TRIGGER" {
		reason = "TSL_HIT"
	}
	_, err := p.db.ExecContext(ctx, `
		UPDATE manthan_positions
		SET status = 'EXITED', exit_price = $1, realized_pnl = $2,
		    exit_reason = $5, exit_time = now(), updated_at = now()
		WHERE strategy_id = $3 AND symbol = $4 AND status IN ('ACTIVE','PENDING_ENTRY')`,
		exitPrice, pnl, strategyID, symbol, reason)
	if err != nil {
		p.logger.Warn("PersistExit failed — EXITED position may re-appear on restart (reconciled by orphan scanner / next tick)",
			zap.String("symbol", symbol), zap.Error(err))
	}
	return err
}

// ExpireStalePendingEntries EXPIREs PENDING_ENTRY rows older than maxAge —
// dispatches whose fills never confirmed (DLQ'd, rejected, upper-circuit
// holds that died at close). Called by the orphan scanner so failed
// dispatches cannot accumulate as clutter. Returns rows expired.
func (p *ManthanPublisher) ExpireStalePendingEntries(ctx context.Context, maxAge time.Duration) (int64, error) {
	if p.db == nil {
		return 0, nil
	}
	res, err := p.db.ExecContext(ctx, `
		UPDATE manthan_positions
		SET status = 'EXPIRED', exit_reason = 'ENTRY_NEVER_FILLED', updated_at = now()
		WHERE status = 'PENDING_ENTRY' AND entry_time < now() - $1::interval`,
		fmt.Sprintf("%d seconds", int(maxAge.Seconds())))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
