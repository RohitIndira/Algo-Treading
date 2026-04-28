package manthan

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// PositionProjector applies broker events to the position tables in a single
// atomic Postgres transaction:
//
//  1. INSERT manthan_position_events ON CONFLICT (signal_id, event_seq)
//     DO NOTHING — idempotency root. WSS, API poller and reconciler all
//     compute the same event_seq from broker exchTime, so the first writer
//     wins and others become no-ops.
//  2. UPSERT manthan_positions guarded by event_seq monotonicity — a stale
//     event arriving after a newer one cannot regress the row.
//  3. UPDATE manthan_signal_decisions status to reflect the broker outcome.
//
// All three writes run in ONE BEGIN/COMMIT. Either the whole projection
// succeeds or none of it does — manthan_positions can never disagree with
// the event log it was projected from.
//
// Active only when DecisionLogEnabled=true (paired with the ManthanPublisher
// flag that switches the entry-write path to manthan_signal_decisions).
type PositionProjector struct {
	db       *sql.DB
	notifier *NotificationPublisher // optional — nil-safe; emits user-facing events
	logger   *zap.Logger
}

func NewPositionProjector(db *sql.DB, logger *zap.Logger) *PositionProjector {
	return &PositionProjector{db: db, logger: logger}
}

// SetNotifier attaches a NotificationPublisher so MANUAL_* projections emit
// user-facing notifications to manthan.notifications. Called once during
// startup wiring; nil-safe to leave unset (no notifications fire).
func (p *PositionProjector) SetNotifier(n *NotificationPublisher) {
	if p == nil {
		return
	}
	p.notifier = n
}

// Apply projects one broker event onto manthan_positions / decisions.
// Returns true if the event was newly applied; false if it was a duplicate
// (already in manthan_position_events).
func (p *PositionProjector) Apply(ctx context.Context, ev *FillEvent) (bool, error) {
	if p == nil || p.db == nil {
		return false, fmt.Errorf("projector db is nil")
	}
	signalID := ev.ResolvedSignalID()
	eventSeq := ev.ResolvedEventSeq()
	if signalID == "" || eventSeq == 0 {
		return false, fmt.Errorf("event missing signal_id or event_seq")
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 1. Idempotency gate — INSERT into manthan_position_events.
	//    ON CONFLICT DO NOTHING means a duplicate (same signal_id+event_seq
	//    from a different source like WSS-vs-poller race) is silently
	//    swallowed. We use RETURNING id to detect whether the row was
	//    actually inserted — if not, this whole apply is a no-op.
	rawPayload, _ := json.Marshal(ev)
	var insertedID sql.NullInt64
	err = tx.QueryRowContext(ctx, `
		INSERT INTO manthan_position_events (
			signal_id, event_seq, event_type,
			broker_order_id, fill_price, fill_qty, rejection_reason,
			raw_payload, source
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
		ON CONFLICT (signal_id, event_seq) DO NOTHING
		RETURNING id`,
		signalID, eventSeq, ev.Type,
		ev.BrokerOrderID, ev.ResolvedFillPrice(), ev.ResolvedFillQty(),
		ev.RejectionReason,
		string(rawPayload),
		nonEmptySource(ev.Source),
	).Scan(&insertedID)
	if err == sql.ErrNoRows {
		// Duplicate — already applied. Commit empty tx and return.
		if commitErr := tx.Commit(); commitErr != nil {
			return false, fmt.Errorf("commit empty tx: %w", commitErr)
		}
		committed = true
		return false, nil
	}
	if err != nil {
		// Constraint violation on event_type: we got a Type value not in our
		// CHECK list (e.g. some legacy source). Log + skip rather than
		// poisoning the consumer; the event_log entry just won't exist for
		// that one event.
		//
		// IMPORTANT: a Postgres TX that hit a constraint violation is in
		// "aborted" state — Commit() on it returns "Could not complete
		// operation in a failed transaction". We must Rollback() instead.
		// The deferred rollback in Apply() handles this if we just return,
		// but be explicit: leave committed=false so defer triggers.
		if isCheckConstraintErr(err) {
			p.logger.Warn("Skipping event with unknown type",
				zap.String("type", ev.Type),
				zap.String("signal_id", signalID))
			return false, nil
		}
		return false, fmt.Errorf("insert position_event: %w", err)
	}

	// 2. Project the event onto manthan_positions / manthan_signal_decisions.
	//    Branch by event type — each one knows how to mutate the projection.
	//    Returns an optional post-commit callback for side effects (e.g.
	//    user notifications) that must NOT run inside the TX — a Kafka
	//    publish failure must not roll back the DB projection.
	postCommit, err := p.applyToProjection(ctx, tx, ev, signalID, eventSeq)
	if err != nil {
		return false, fmt.Errorf("project event: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit projection: %w", err)
	}
	committed = true

	// Run side effects only after the DB commit has succeeded. Failures here
	// are logged inside the callback; they don't roll back the projection.
	if postCommit != nil {
		postCommit()
	}
	return true, nil
}

// applyToProjection runs inside the projector transaction. It dispatches on
// event type and issues the appropriate UPSERT into manthan_positions and
// UPDATE on manthan_signal_decisions. The (signal_id, event_seq) pair has
// already been recorded in manthan_position_events by the caller.
//
// Returns a post-commit callback (or nil) that the caller MUST invoke after
// the TX has been committed. Used for side effects that don't belong in a
// DB transaction — e.g., Kafka notifications. Returning a nil func is the
// common case.
func (p *PositionProjector) applyToProjection(ctx context.Context, tx *sql.Tx, ev *FillEvent, signalID string, eventSeq int64) (func(), error) {
	switch strings.ToUpper(strings.TrimSpace(ev.Type)) {

	case "ENTRY_FILLED", "FILL_CONFIRMED":
		// TOP-UP path: when ParentSignalID is set, the broker fill belongs
		// to a top-up order — ADD onto the parent's manthan_positions row
		// (qty + invested grow; entry_price recomputed as weighted average;
		// current_sl + last_trail_level untouched — TSL governs those).
		// The top-up's OWN decision row also flips to CONFIRMED for audit.
		if ev.ParentSignalID != "" {
			addInvested := float64(ev.ResolvedFillQty()) * ev.ResolvedFillPrice()
			_, err := tx.ExecContext(ctx, `
				UPDATE manthan_positions
				SET
				  quantity     = quantity + $1,
				  invested_amt = invested_amt + $2,
				  -- weighted-average new entry price, computed from the
				  -- post-update totals so future fills weighted-avg again
				  entry_price  = (invested_amt + $2) / NULLIF(quantity + $1, 0),
				  high_since_entry = GREATEST(high_since_entry, $3),
				  event_seq    = $4,
				  updated_at   = NOW()
				WHERE signal_id = $5 AND event_seq < $4`,
				ev.ResolvedFillQty(), addInvested, ev.ResolvedFillPrice(),
				eventSeq, ev.ParentSignalID)
			if err != nil {
				return nil, fmt.Errorf("topup add to parent manthan_positions: %w", err)
			}
			_, err = tx.ExecContext(ctx, `
				UPDATE manthan_signal_decisions
				SET status = 'CONFIRMED', final_status_at = NOW()
				WHERE signal_id = $1 AND status NOT IN ('CONFIRMED','CLOSED')`,
				signalID)
			return nil, err
		}

		// Normal (non-top-up) entry path:
		// Broker fully filled. Project the position row and flip the decision
		// to CONFIRMED. event_seq monotonicity prevents an older fill event
		// (e.g. partial that arrived late) from clobbering the newer state.
		//
		// industry / mcap_bucket / index_name are pulled from the decision
		// row — REQUIRED for the rebalancer's top-up phase to look up the
		// per-index EMA target (without these the position has no index and
		// top-up skips it with "EMA target = 0 for ").
		invested := float64(ev.ResolvedFillQty()) * ev.ResolvedFillPrice()
		// Initial SL = 20% below fill (Manthan default). Will be refined as
		// SL_PLACED / SL_MODIFIED events arrive.
		initialSL := ev.ResolvedFillPrice() * 0.80
		_, err := tx.ExecContext(ctx, `
			INSERT INTO manthan_positions (
				strategy_id, user_id, symbol, isin,
				industry, mcap_bucket, index_name,
				entry_price, quantity, invested_amt,
				high_since_entry, current_sl, last_trail_level,
				status, signal_id, event_seq, broker_order_id
			)
			SELECT
				d.strategy_id, d.user_id, d.symbol, d.isin,
				d.industry, d.mcap_bucket, d.index_name,
				$1, $2, $3,
				$1, $4, $1,
				'ACTIVE', d.signal_id, $5, $6
			FROM manthan_signal_decisions d
			WHERE d.signal_id = $7
			ON CONFLICT (signal_id) WHERE signal_id IS NOT NULL DO UPDATE SET
				industry         = EXCLUDED.industry,
				mcap_bucket      = EXCLUDED.mcap_bucket,
				index_name       = EXCLUDED.index_name,
				entry_price      = EXCLUDED.entry_price,
				quantity         = EXCLUDED.quantity,
				invested_amt     = EXCLUDED.invested_amt,
				high_since_entry = EXCLUDED.high_since_entry,
				current_sl       = EXCLUDED.current_sl,
				last_trail_level = EXCLUDED.last_trail_level,
				status           = 'ACTIVE',
				event_seq        = EXCLUDED.event_seq,
				broker_order_id  = EXCLUDED.broker_order_id,
				updated_at       = NOW()
			WHERE manthan_positions.event_seq < EXCLUDED.event_seq`,
			ev.ResolvedFillPrice(), ev.ResolvedFillQty(), invested,
			initialSL, eventSeq, ev.BrokerOrderID, signalID)
		if err != nil {
			return nil, fmt.Errorf("upsert manthan_positions on ENTRY_FILLED: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE manthan_signal_decisions
			SET status = 'CONFIRMED', final_status_at = NOW()
			WHERE signal_id = $1 AND status NOT IN ('CONFIRMED','CLOSED')`,
			signalID)
		return nil, err

	case "ENTRY_PARTIAL_FILL", "FILL_UPDATE":
		// Broker filled less than expected. Same projection but PARTIAL_ACTIVE
		// status on both tables. (No exit — the partial position is real.)
		invested := float64(ev.ResolvedFillQty()) * ev.ResolvedFillPrice()
		initialSL := ev.ResolvedFillPrice() * 0.80
		_, err := tx.ExecContext(ctx, `
			INSERT INTO manthan_positions (
				strategy_id, user_id, symbol, isin,
				industry, mcap_bucket, index_name,
				entry_price, quantity, invested_amt,
				high_since_entry, current_sl, last_trail_level,
				status, signal_id, event_seq, broker_order_id
			)
			SELECT
				d.strategy_id, d.user_id, d.symbol, d.isin,
				d.industry, d.mcap_bucket, d.index_name,
				$1, $2, $3,
				$1, $4, $1,
				'PARTIAL_ACTIVE', d.signal_id, $5, $6
			FROM manthan_signal_decisions d
			WHERE d.signal_id = $7
			ON CONFLICT (signal_id) WHERE signal_id IS NOT NULL DO UPDATE SET
				industry         = EXCLUDED.industry,
				mcap_bucket      = EXCLUDED.mcap_bucket,
				index_name       = EXCLUDED.index_name,
				entry_price      = EXCLUDED.entry_price,
				quantity         = EXCLUDED.quantity,
				invested_amt     = EXCLUDED.invested_amt,
				current_sl       = EXCLUDED.current_sl,
				status           = 'PARTIAL_ACTIVE',
				event_seq        = EXCLUDED.event_seq,
				broker_order_id  = EXCLUDED.broker_order_id,
				updated_at       = NOW()
			WHERE manthan_positions.event_seq < EXCLUDED.event_seq`,
			ev.ResolvedFillPrice(), ev.ResolvedFillQty(), invested,
			initialSL, eventSeq, ev.BrokerOrderID, signalID)
		if err != nil {
			return nil, fmt.Errorf("upsert manthan_positions on PARTIAL: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE manthan_signal_decisions
			SET status = 'PARTIAL', final_status_at = NOW()
			WHERE signal_id = $1 AND status NOT IN ('CONFIRMED','PARTIAL','CLOSED')`,
			signalID)
		return nil, err

	case "ENTRY_REJECTED":
		// Broker rejected. NO position row created. Decision flips to REJECTED.
		// This is the ghost-position fix in action.
		_, err := tx.ExecContext(ctx, `
			UPDATE manthan_signal_decisions
			SET status = 'REJECTED', final_status_at = NOW(), rejection_reason = $1
			WHERE signal_id = $2 AND status NOT IN ('CONFIRMED','CLOSED')`,
			ev.RejectionReason, signalID)
		return nil, err

	case "ENTRY_TIMED_OUT":
		_, err := tx.ExecContext(ctx, `
			UPDATE manthan_signal_decisions
			SET status = 'TIMED_OUT', final_status_at = NOW()
			WHERE signal_id = $1 AND status IN ('PROPOSED','DISPATCHED')`,
			signalID)
		return nil, err

	case "SL_PLACED":
		// Broker accepted initial SL. Update current_sl on the existing
		// position row (it must exist by this point — entry has filled).
		_, err := tx.ExecContext(ctx, `
			UPDATE manthan_positions
			SET current_sl = $1, event_seq = $2, updated_at = NOW()
			WHERE signal_id = $3 AND event_seq < $2`,
			ev.SLTrigger, eventSeq, signalID)
		return nil, err

	case "SL_MODIFIED":
		// Trailing SL successfully modified. Bump current_sl + last_trail_level.
		_, err := tx.ExecContext(ctx, `
			UPDATE manthan_positions
			SET current_sl       = $1,
			    last_trail_level = $1,
			    high_since_entry = GREATEST(high_since_entry, $1),
			    event_seq        = $2,
			    updated_at       = NOW()
			WHERE signal_id = $3 AND event_seq < $2`,
			ev.SLTrigger, eventSeq, signalID)
		return nil, err

	case "SL_REJECTED":
		// SL placement / modify rejected — log only. Position stays ACTIVE
		// (broker may still have an old SL or we've fallen back to MARKET
		// SELL via emergency path; both paths emit their own follow-up event).
		// No projection change beyond the position_events row already inserted.
		return nil, nil

	case "SL_FILLED", "EXIT_FILLED":
		// Position exited at broker. Mark EXITED with realized P&L computed
		// from the fill price vs entry_price already stored.
		_, err := tx.ExecContext(ctx, `
			UPDATE manthan_positions
			SET status        = 'EXITED',
			    exit_price    = $1,
			    realized_pnl  = ($1 - entry_price) * quantity,
			    exit_reason   = $2,
			    exit_time     = NOW(),
			    event_seq     = $3,
			    updated_at    = NOW()
			WHERE signal_id = $4 AND event_seq < $3 AND status IN ('ACTIVE','PARTIAL_ACTIVE')`,
			ev.ResolvedFillPrice(), exitReasonForType(ev.Type), eventSeq, signalID)
		if err != nil {
			return nil, fmt.Errorf("update manthan_positions on EXIT: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE manthan_signal_decisions
			SET status = 'CLOSED', final_status_at = NOW()
			WHERE signal_id = $1 AND status NOT IN ('CLOSED','REJECTED','TIMED_OUT')`,
			signalID)
		return nil, err

	case "RECONCILER_DRIFT_FIX":
		// Reconciler reported drift. Currently no automatic projection — the
		// detail field tells us what was fixed; the next normal event will
		// update state. Log-only for forensics.
		return nil, nil

	case "MANUAL_EXIT_DETECTED":
		// User exited the position outside our system (broker mobile app etc).
		// Position → EXITED with exit_reason='MANUAL_EXIT', decision →
		// MANUALLY_EXITED, and user_override_until is set 3 days out so the
		// allocator won't re-buy this signal until the cooldown clears.
		// We don't have an exact exit_price (broker just reports we no longer
		// hold these shares); leave exit_price NULL so the audit trail makes
		// it obvious we couldn't capture the fill price.
		_, err := tx.ExecContext(ctx, `
			UPDATE manthan_positions
			SET status      = 'EXITED',
			    exit_reason = 'MANUAL_EXIT',
			    exit_time   = NOW(),
			    event_seq   = $1,
			    updated_at  = NOW()
			WHERE signal_id = $2
			  AND event_seq < $1
			  AND status IN ('ACTIVE','PARTIAL_ACTIVE')`,
			eventSeq, signalID)
		if err != nil {
			return nil, fmt.Errorf("manual exit position update: %w", err)
		}
		_, err = tx.ExecContext(ctx, `
			UPDATE manthan_signal_decisions
			SET status              = 'MANUALLY_EXITED',
			    final_status_at     = NOW(),
			    user_override_until = NOW() + INTERVAL '3 days',
			    rejection_reason    = COALESCE(rejection_reason || ' | ', '') || $1
			WHERE signal_id = $2
			  AND status NOT IN ('MANUALLY_EXITED','REJECTED')`,
			ev.RejectionReason, signalID)
		if err != nil {
			return nil, err
		}
		// Best-effort user notification. Returned as a post-commit closure
		// so the Kafka publish only fires after the DB TX has committed —
		// a notification failure must NEVER roll back the projection.
		var post func()
		if p.notifier != nil {
			userID := ev.UserID
			strategyID := ev.StrategyID
			symbol := ev.Symbol
			qty := int(ev.ExpectedQty)
			post = func() {
				p.notifier.NotifyManualExit(context.Background(),
					userID, strategyID, signalID, symbol, qty)
			}
		}
		return post, nil

	case "MANUAL_PARTIAL_EXIT_DETECTED":
		// User sold SOME of our position. Position stays ACTIVE but quantity
		// shrinks to what broker still shows. We do NOT set user_override_until
		// — the user explicitly kept part of the position so the algo can
		// keep managing the rest. fill_qty in the event payload carries the
		// remaining broker quantity.
		newQty := ev.ResolvedFillQty()
		if newQty <= 0 {
			// Defensive — partial-exit with 0 broker qty is really a full
			// exit; project as MANUAL_EXIT instead.
			_, err := tx.ExecContext(ctx, `
				UPDATE manthan_positions
				SET status='EXITED', exit_reason='MANUAL_EXIT',
				    exit_time=NOW(), event_seq=$1, updated_at=NOW()
				WHERE signal_id=$2 AND event_seq<$1
				  AND status IN ('ACTIVE','PARTIAL_ACTIVE')`,
				eventSeq, signalID)
			return nil, err
		}
		// Capture old qty before UPDATE so the notification can show the
		// delta (sold X of Y). The UPDATE returns the new state regardless.
		var oldQty int
		_ = tx.QueryRowContext(ctx,
			`SELECT quantity FROM manthan_positions WHERE signal_id = $1`,
			signalID).Scan(&oldQty)
		_, err := tx.ExecContext(ctx, `
			UPDATE manthan_positions
			SET quantity     = $1,
			    invested_amt = entry_price * $1,
			    status       = 'PARTIAL_ACTIVE',
			    event_seq    = $2,
			    updated_at   = NOW()
			WHERE signal_id = $3
			  AND event_seq < $2
			  AND status IN ('ACTIVE','PARTIAL_ACTIVE')`,
			newQty, eventSeq, signalID)
		if err != nil {
			return nil, err
		}
		var post func()
		if p.notifier != nil && oldQty > int(newQty) {
			userID := ev.UserID
			strategyID := ev.StrategyID
			symbol := ev.Symbol
			oq, nq := oldQty, int(newQty)
			post = func() {
				p.notifier.NotifyManualPartialExit(context.Background(),
					userID, strategyID, signalID, symbol, oq, nq)
			}
		}
		return post, nil

	case "MANUAL_BUY_DETECTED":
		// User bought MORE of the same symbol outside our system. We do NOT
		// take ownership of those extra shares — algo book and manual book
		// stay separate. Just log for forensics; no projection change.
		return nil, nil

	case "MANUAL_SL_CANCELLED_DETECTED":
		// User cancelled our SL order without selling. The position is now
		// unprotected at broker. Safety monitor will re-place the SL on its
		// next tick (existing behaviour); we log only here. No state change
		// on manthan_positions — the SL re-placement will fire SL_PLACED
		// which will refresh current_sl normally. Notify the user so they
		// know the system handled it.
		var post func()
		if p.notifier != nil {
			userID := ev.UserID
			strategyID := ev.StrategyID
			symbol := ev.Symbol
			post = func() {
				p.notifier.NotifyManualSLCancelled(context.Background(),
					userID, strategyID, signalID, symbol)
			}
		}
		return post, nil

	case "EXTERNAL_QTY_MISMATCH":
		// Generic catch-all. Forensics-only — the specific MANUAL_*
		// detectors above handle the cases we know how to act on.
		return nil, nil

	default:
		// Unknown event type already passed our INSERT check (so it must be
		// one of the permitted ones); we just don't have a projection rule.
		// Skip rather than error.
		p.logger.Debug("Projector: no projection rule for event_type",
			zap.String("type", ev.Type),
			zap.String("signal_id", signalID))
		return nil, nil
	}
}

// MarkStaleDecisionsTimedOut flips PROPOSED/DISPATCHED decisions older than
// the cutoff to TIMED_OUT. Called by the reaper goroutine. Idempotent — re-
// running has no effect on already-flipped rows.
func (p *PositionProjector) MarkStaleDecisionsTimedOut(ctx context.Context, staleAfterSeconds int) (int64, error) {
	if p == nil || p.db == nil {
		return 0, nil
	}
	res, err := p.db.ExecContext(ctx, `
		UPDATE manthan_signal_decisions
		SET status = 'TIMED_OUT', final_status_at = NOW(),
		    rejection_reason = COALESCE(rejection_reason, 'reaper: no broker event within deadline')
		WHERE status IN ('PROPOSED','DISPATCHED')
		  AND decided_at < NOW() - ($1 || ' seconds')::interval`,
		fmt.Sprintf("%d", staleAfterSeconds))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// nonEmptySource ensures source is one of the CHECK-allowed values.
func nonEmptySource(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "WSS", "API_POLLER", "RECONCILER", "SAFETY_MONITOR", "TRADE_EXEC":
		return strings.ToUpper(s)
	}
	return "TRADE_EXEC"
}

func exitReasonForType(t string) string {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "SL_FILLED":
		return "SL_TRIGGERED"
	case "EXIT_FILLED":
		return "MANUAL_EXIT"
	}
	return ""
}

// isCheckConstraintErr matches Postgres "check_violation" SQLSTATE 23514.
// We treat unknown event_type / source values as soft-skip rather than crash.
func isCheckConstraintErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "23514") ||
		strings.Contains(err.Error(), "violates check constraint")
}
