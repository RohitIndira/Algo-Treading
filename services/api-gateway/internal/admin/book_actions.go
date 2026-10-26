package admin

// Book-vs-broker actions (2026-10-26). What the reconciler DOES with a verdict,
// through the same Actions bundle the manual console uses:
//
//	PersistVerdicts — the latest verdict lands on every positions_db row of
//	                  the lot (state, plain reason, evidence, checked_at,
//	                  source). Idempotent.
//	Pledge          — PLEDGED: the lot stays in the book (it IS held) but the
//	                  strategy must leave it alone. trading_db row → 'PLEDGED'
//	                  (rules-engine: no slot, no trail, no exit), execution_db
//	                  stop rows parked SL_DEFERRED_BAND/broker_status 'PLEDGED'
//	                  (trade-execution: no naked-scan stop, no 09:14 or EOD
//	                  arming, trail and emergency sell refuse), one durable
//	                  POSITION_PLEDGED event for the attention queue.
//	Unpledge        — HELD again: the reverse — trading_db row back to ACTIVE,
//	                  parked rows closed so protection is re-placed by the
//	                  next naked scan / planner / EOD pass, POSITION_UNPLEDGED
//	                  event ("Unpledged – protection resumed").
//
// NOT_HELD / SOLD_TODAY closures go through GhostPreviewFromRead → GhostHeal
// (interventions2.go) — the one evidence gate — with the exit reason set by
// the reconciler. Nothing in this file touches a broker order.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// uuidArray renders ids as a Postgres uuid[] literal for `= ANY($n::uuid[])`.
func uuidArray(ids []string) string {
	return "{" + strings.Join(ids, ",") + "}"
}

// PersistVerdicts writes the latest verdict onto the lot's positions_db rows.
// Ledger-only verdicts have no row and are skipped (the audit trail carries
// them). An UNVERIFIED verdict never overwrites a previously persisted TRUTH
// (HELD / PLEDGED / QTY_MISMATCH / NOT_HELD / SOLD_TODAY): the row keeps its
// last state, reason and checked_at and is flagged broker_state_stale with
// the unverified reason in the evidence — so /fleet, /attention and the
// client app do not flip on a dead session, and the hysteresis clock
// (checked_at) still points at the last truthful read. Returns rows updated.
func (a *Actions) PersistVerdicts(ctx context.Context, verdicts []Verdict, source string) (int64, error) {
	var total int64
	for _, v := range verdicts {
		if len(v.PositionIDs) == 0 {
			continue
		}
		ev, err := json.Marshal(v.Evidence)
		if err != nil {
			return total, fmt.Errorf("verdict evidence marshal: %w", err)
		}
		var res sql.Result
		if v.State == BrokerStateUnverified {
			res, err = a.fleet.posDB.ExecContext(ctx, `
				UPDATE positions
				   SET broker_state        = COALESCE(NULLIF(broker_state, 'UNVERIFIED'), $2::text),
				       broker_state_reason = CASE WHEN broker_state IS NULL OR broker_state = 'UNVERIFIED' THEN $3 ELSE broker_state_reason END,
				       broker_checked_at   = CASE WHEN broker_state IS NULL OR broker_state = 'UNVERIFIED' THEN $5 ELSE broker_checked_at END,
				       broker_state_stale  = (broker_state IS NOT NULL AND broker_state <> 'UNVERIFIED'),
				       broker_state_evidence = $4::jsonb || jsonb_build_object('unverified_reason', $3::text, 'unverified_at', $5::timestamptz),
				       broker_state_source = $6, updated_at = now()
				 WHERE position_id = ANY($1::uuid[]) AND status = 'ACTIVE'`,
				uuidArray(v.PositionIDs), v.State, v.Reason, string(ev), v.CheckedAt, source)
		} else {
			res, err = a.fleet.posDB.ExecContext(ctx, `
				UPDATE positions
				   SET broker_state = $2, broker_state_reason = $3, broker_state_evidence = $4::jsonb,
				       broker_checked_at = $5, broker_state_source = $6, broker_state_stale = false, updated_at = now()
				 WHERE position_id = ANY($1::uuid[]) AND status = 'ACTIVE'`,
				uuidArray(v.PositionIDs), v.State, v.Reason, string(ev), v.CheckedAt, source)
		}
		if err != nil {
			return total, fmt.Errorf("persist verdict %s/%s: %w", v.UserID, v.Symbol, err)
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}

// pledgeMarkerSignal is the synthetic stop row's signal_id (UNIQUE) for a lot
// that has no stop row to park: one per (user, strategy, symbol).
func pledgeMarkerSignal(userID, strategyID, symbol string) string {
	return fmt.Sprintf("pledged-%s-%s-%s", userID, shortID(strategyID), strings.ToUpper(symbol))
}

// Pledge applies the PLEDGED verdict across the three books. live is the set
// of broker order ids seen live in the order book of this run: a stop row
// whose broker order still stands is NEVER parked (it would hide a real
// order) — it is reported in stop_rows_left_live_at_broker instead. In the
// pledged case the broker has no free qty for a SELL, so such a row is a
// contradiction worth seeing, not acting on.
func (a *Actions) Pledge(ctx context.Context, v Verdict, live *liveBook) (map[string]any, error) {
	if v.State != BrokerStatePledged {
		return nil, fmt.Errorf("pledge called with verdict %s", v.State)
	}
	out := map[string]any{"user_id": v.UserID, "strategy_id": v.StrategyID, "symbol": v.Symbol}
	note := "PLEDGED at broker: " + v.Reason

	// 1) rules-engine book: slot released, no trail, no exit.
	var slotRows int64 = -1
	if v.StrategyID != "" {
		res, err := a.fleet.tradingDB.ExecContext(ctx, `
			UPDATE manthan_positions
			   SET status = 'PLEDGED', protective_last_status = 'PLEDGED',
			       protective_last_reason = $3, protective_last_attempt = now(), updated_at = now()
			 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND status = 'ACTIVE'`,
			v.StrategyID, v.Symbol, v.Reason)
		if err != nil {
			return out, fmt.Errorf("pledge trading_db: %w", err)
		}
		slotRows, _ = res.RowsAffected()
	}
	out["slot_rows_pledged"] = slotRows

	// 2) execution ledger: park the lot's stop rows.
	var parked int64
	var left []string
	var parkedID int64
	var oldStatus string
	if v.StrategyID != "" {
		rows, err := a.fleet.execDB.QueryContext(ctx, `
			SELECT id, COALESCE(broker_order_id, ''), status, qty
			  FROM manthan_orders
			 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND order_side = 'SELL'
			   AND order_type IN ('SL_SELL','SL_SELL_AMO')
			   AND status IN ('PENDING','SL_PLACED','SL_MODIFY_PENDING','SL_DEFERRED_BAND','AMO_PENDING')
			   AND COALESCE(broker_status, '') <> 'PLEDGED'
			 ORDER BY id`, v.StrategyID, v.Symbol)
		if err != nil {
			return out, fmt.Errorf("pledge ledger read: %w", err)
		}
		type stopRow struct {
			id       int64
			brokerID string
			status   string
			qty      int
		}
		var candidates []stopRow
		for rows.Next() {
			var r stopRow
			if rows.Scan(&r.id, &r.brokerID, &r.status, &r.qty) == nil {
				candidates = append(candidates, r)
			}
		}
		rows.Close()
		for _, c := range candidates {
			if live.has(c.brokerID, v.Symbol, c.qty) {
				left = append(left, c.brokerID)
				continue
			}
			res, uerr := a.fleet.execDB.ExecContext(ctx, `
				UPDATE manthan_orders
				   SET status = 'SL_DEFERRED_BAND', broker_status = 'PLEDGED', last_error = $2, updated_at = now()
				 WHERE id = $1`, c.id, note)
			if uerr != nil {
				return out, fmt.Errorf("pledge park row %d: %w", c.id, uerr)
			}
			n, _ := res.RowsAffected()
			parked += n
			if parkedID == 0 {
				parkedID, oldStatus = c.id, c.status
			}
		}

		// Already-parked PLEDGED rows (a re-run) count as the marker.
		if parkedID == 0 {
			_ = a.fleet.execDB.QueryRowContext(ctx, `
				SELECT id FROM manthan_orders
				 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND order_side = 'SELL'
				   AND order_type IN ('SL_SELL','SL_SELL_AMO')
				   AND status = 'SL_DEFERRED_BAND' AND broker_status = 'PLEDGED'
				 ORDER BY id DESC LIMIT 1`, v.StrategyID, v.Symbol).Scan(&parkedID)
		}

		// No parked row for the lot: insert a marker row so the naked scan,
		// the 09:14 planner and EOD Phase A all see the lot as covered
		// (SL_DEFERRED_BAND) and tagged PLEDGED — also when a stop row had to
		// be left alone because its broker order still stands (the planner
		// must still skip the lot). Carries the latest trailed trigger so
		// protection resumes at the trailed level.
		if parkedID == 0 {
			var trig, lim sql.NullFloat64
			var isin, tok, isym sql.NullString
			_ = a.fleet.execDB.QueryRowContext(ctx, `
				SELECT trigger_price, limit_price, isin, exchange_token, indira_symbol
				  FROM manthan_orders
				 WHERE strategy_id = $1 AND UPPER(symbol) = $2
				 ORDER BY (order_type IN ('SL_SELL','SL_SELL_AMO')) DESC, id DESC LIMIT 1`,
				v.StrategyID, v.Symbol).Scan(&trig, &lim, &isin, &tok, &isym)
			// The naked scan is coverage-aware (Σ qty of parked/active stop
			// rows vs the ledger net): the marker must cover the WHOLE ledger
			// net or the monitor still places a stop for the gap.
			markerQty := v.BookQty
			var net int
			if err := a.fleet.execDB.QueryRowContext(ctx, `
				SELECT COALESCE(SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
				                         WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
				                         ELSE 0 END), 0)
				  FROM manthan_orders WHERE strategy_id = $1 AND UPPER(symbol) = $2`,
				v.StrategyID, v.Symbol).Scan(&net); err == nil && net > markerQty {
				markerQty = net
			}
			err := a.fleet.execDB.QueryRowContext(ctx, `
				INSERT INTO manthan_orders
					(signal_id, strategy_id, user_id, symbol, isin, exchange, order_type, order_side,
					 qty, filled_qty, trigger_price, limit_price, exchange_token, indira_symbol,
					 status, broker_status, last_error, created_at, updated_at)
				VALUES ($1, $2, $3, $4, $5, 'NSE', 'SL_SELL', 'SELL',
				        $6, 0, $7, $8, $9, $10,
				        'SL_DEFERRED_BAND', 'PLEDGED', $11, now(), now())
				ON CONFLICT (signal_id) DO UPDATE
				   SET status = 'SL_DEFERRED_BAND', broker_status = 'PLEDGED', last_error = EXCLUDED.last_error,
				       qty = EXCLUDED.qty, updated_at = now()
				 WHERE manthan_orders.status <> 'SL_DEFERRED_BAND' OR manthan_orders.broker_status IS DISTINCT FROM 'PLEDGED'
				RETURNING id`,
				pledgeMarkerSignal(v.UserID, v.StrategyID, v.Symbol), v.StrategyID, v.UserID, v.Symbol,
				isin, markerQty, trig, lim, tok, isym, note+" (marker row: lot had no stop row to park)").Scan(&parkedID)
			if err == sql.ErrNoRows {
				// Marker already parked PLEDGED (re-run): nothing changed.
				_ = a.fleet.execDB.QueryRowContext(ctx, `SELECT id FROM manthan_orders WHERE signal_id = $1`,
					pledgeMarkerSignal(v.UserID, v.StrategyID, v.Symbol)).Scan(&parkedID)
			} else if err != nil {
				return out, fmt.Errorf("pledge marker row: %w", err)
			} else {
				oldStatus = ""
				out["marker_row"] = parkedID
			}
		}
	}
	out["stop_rows_parked"], out["stop_rows_left_live_at_broker"] = parked, left
	_, markerInserted := out["marker_row"]
	changed := slotRows > 0 || parked > 0 || markerInserted
	out["changed"] = changed

	// 3) Durable trail for /attention and the audit — once per real
	// transition (a re-run on an already-pledged lot writes nothing).
	if parkedID > 0 && changed {
		if _, err := a.fleet.execDB.ExecContext(ctx, `
			INSERT INTO manthan_order_events (order_id, event_type, old_status, new_status, broker_status, qty, detail)
			VALUES ($1, 'POSITION_PLEDGED', $2, 'SL_DEFERRED_BAND', 'PLEDGED', $3, $4)`,
			parkedID, oldStatus, v.BookQty, v.Reason); err == nil {
			out["event_written"] = true
		}
	}
	return out, nil
}

// Unpledge reverses Pledge once the broker shows the quantity free again.
// Protection is not placed from here — the parked rows are closed so the
// existing placers (naked scan, 09:14 planner, EOD Phase A) re-arm the lot
// on their next pass, exactly as for any newly naked position.
func (a *Actions) Unpledge(ctx context.Context, v Verdict) (map[string]any, error) {
	out := map[string]any{"user_id": v.UserID, "strategy_id": v.StrategyID, "symbol": v.Symbol}
	if v.StrategyID == "" {
		return out, nil
	}
	res, err := a.fleet.tradingDB.ExecContext(ctx, `
		UPDATE manthan_positions
		   SET status = 'ACTIVE', protective_last_status = 'UNPLEDGED',
		       protective_last_reason = $3, protective_last_attempt = now(), updated_at = now()
		 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND status = 'PLEDGED'`,
		v.StrategyID, v.Symbol, ReasonUnpledged)
	if err != nil {
		return out, fmt.Errorf("unpledge trading_db: %w", err)
	}
	slotRows, _ := res.RowsAffected()
	out["slot_rows_reactivated"] = slotRows

	var eventRow int64
	_ = a.fleet.execDB.QueryRowContext(ctx, `
		SELECT id FROM manthan_orders
		 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND order_side = 'SELL'
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status = 'SL_DEFERRED_BAND' AND broker_status = 'PLEDGED'
		 ORDER BY id DESC LIMIT 1`, v.StrategyID, v.Symbol).Scan(&eventRow)
	res, err = a.fleet.execDB.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET status = 'CANCELLED', broker_status = 'UNPLEDGED', cancelled_at = now(), updated_at = now(),
		       last_error = $3
		 WHERE strategy_id = $1 AND UPPER(symbol) = $2 AND order_side = 'SELL'
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status = 'SL_DEFERRED_BAND' AND broker_status = 'PLEDGED'`,
		v.StrategyID, v.Symbol, ReasonUnpledged+": parked row closed; the next protection pass re-arms the lot")
	if err != nil {
		return out, fmt.Errorf("unpledge ledger: %w", err)
	}
	unparked, _ := res.RowsAffected()
	out["stop_rows_unparked"] = unparked
	changed := slotRows > 0 || unparked > 0
	out["changed"] = changed
	if eventRow > 0 && changed {
		if _, err := a.fleet.execDB.ExecContext(ctx, `
			INSERT INTO manthan_order_events (order_id, event_type, old_status, new_status, broker_status, qty, detail)
			VALUES ($1, 'POSITION_UNPLEDGED', 'SL_DEFERRED_BAND', 'CANCELLED', 'UNPLEDGED', $2, $3)`,
			eventRow, v.BookQty, ReasonUnpledged+" — "+v.Reason); err == nil {
			out["event_written"] = true
		}
	}
	return out, nil
}
