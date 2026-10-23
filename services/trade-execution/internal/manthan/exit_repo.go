package manthan

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// ─── Exit sell bookkeeping (2026-10-22) ─────────────────────────────────────
//
// Every immediate exit (software stop, safety-monitor escalation, protective
// replay on a breached stop, admin square-off) is a LIMIT order that can rest,
// so it is recorded as a PLACED row and confirmed FILLED / CANCELLED from
// broker truth (SafetyMonitor.checkPendingExits every cycle, Reconciler every
// 5 min). Only a FILLED SELL drops the position's net quantity — nothing here
// pretends a sell happened before the exchange says so.

// ExitSellRow describes an exit sell just accepted by the broker.
type ExitSellRow struct {
	StrategyID    string
	UserID        string
	Symbol        string
	ISIN          string
	Info          *SymbolInfo
	OrderType     OrderType // OrderTypeMarketSell (LIMIT, DAY) or OrderTypeAMOSell (LIMIT, AMO)
	Qty           int
	LimitPrice    float64
	BrokerOrderID string
	ParentOrderID int64 // entry manthan_orders.id when known (0 → resolved from the book)
	Reason        string
	RetryCount    int // re-price lineage counter, written with the row (never a follow-up UPDATE)
	// SignalIDPrefix distinguishes WHY the exit was placed for downstream
	// consumers that only see the row's signal_id (positions svc books the
	// lot's exit_reason from it): "exit-" (default) = the strategy's stop
	// exit → SL_TRIGGER (cooldown applies); "squareoff-" = an admin
	// square-off → STRATEGY_EXIT. A user's own sell never has a row here.
	SignalIDPrefix string
}

// InsertExitSellPlaced records an exit sell as PLACED. signal_id
// "exit-<broker id>" is unique per placement, so a repeated call for the
// same broker order is a no-op (returns 0, nil).
func (r *Repository) InsertExitSellPlaced(ctx context.Context, row ExitSellRow) (int64, error) {
	if row.BrokerOrderID == "" {
		return 0, fmt.Errorf("exit sell row for %s: broker order id required", row.Symbol)
	}
	if row.OrderType == "" {
		row.OrderType = OrderTypeMarketSell
	}
	if row.SignalIDPrefix == "" {
		row.SignalIDPrefix = "exit-"
	}
	parent := row.ParentOrderID
	if parent == 0 {
		if _, entryID, has, err := r.NetHeldQty(ctx, row.StrategyID, row.Symbol); err == nil && has {
			parent = entryID
		}
	}
	info := row.Info
	if info == nil {
		info = &SymbolInfo{}
	}
	now := time.Now().In(istLocation())
	tradeDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, istLocation())

	var id int64
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO manthan_orders (
			signal_id, strategy_id, user_id, symbol, isin, exchange,
			order_type, order_side, product_type,
			qty, limit_price, trigger_price,
			indira_symbol, exchange_token, status, broker_order_id,
			placed_at, parent_order_id, trade_date, max_retries, retry_count
		) VALUES ($1,$2,$3,$4,$5,'NSE',$6,'SELL','DELIVERY',$7,$8,0,$9,$10,'PLACED',$11,
			now(), NULLIF($12,0), $13, 0, $14)
		ON CONFLICT (signal_id) DO NOTHING
		RETURNING id`,
		row.SignalIDPrefix+row.BrokerOrderID, row.StrategyID, row.UserID, row.Symbol, nullStr(row.ISIN),
		string(row.OrderType), row.Qty, row.LimitPrice,
		info.IndiraSymbol, info.ExchangeToken, row.BrokerOrderID, parent, tradeDate, row.RetryCount,
	).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil // already recorded
	}
	if err != nil {
		return 0, err
	}
	_ = r.InsertEvent(ctx, id, "EXIT_SELL_PLACED", "PENDING", "PLACED", "", row.LimitPrice, row.Qty,
		fmt.Sprintf("%s: %s LIMIT %.2f broker_id=%s", row.Reason, row.OrderType, row.LimitPrice, row.BrokerOrderID))
	return id, nil
}

// NetHeldQty is the order-derived holding for one (strategy, symbol):
// FILLED BUY − FILLED SELL, plus the reference entry row (root entry first).
// hasEntry=false means the book has no filled BUY at all (legacy lot) —
// callers must NOT read that as "nothing held".
func (r *Repository) NetHeldQty(ctx context.Context, strategyID, symbol string) (net int, entryOrderID int64, hasEntry bool, err error) {
	var entry sql.NullInt64
	err = r.db.QueryRowContext(ctx, `
		WITH net_qty AS (
		    SELECT COALESCE(SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
		                              WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
		                              ELSE 0 END), 0) AS net
		      FROM manthan_orders
		     WHERE strategy_id = $1 AND symbol = $2
		)
		SELECT n.net, e.id
		  FROM net_qty n
		  LEFT JOIN LATERAL (
		      SELECT id FROM manthan_orders
		       WHERE strategy_id = $1 AND symbol = $2
		         AND order_side = 'BUY' AND status = 'FILLED'
		       ORDER BY (parent_order_id IS NOT NULL), created_at ASC
		       LIMIT 1
		  ) e ON true`, strategyID, symbol).Scan(&net, &entry)
	if err != nil {
		return 0, 0, false, err
	}
	return net, entry.Int64, entry.Valid, nil
}

// HasOpenExitSell reports whether an exit sell of ours (immediate LIMIT or
// AMO LIMIT) is still open for the position, whatever its age — a PLACED exit
// row means the quantity is locked at the broker, so a second sell or a stop
// on top would only be rejected "Free Qty=0". Stale rows are closed out by
// SafetyMonitor.checkPendingExits (fill, loss, conversion), not by time.
func (r *Repository) HasOpenExitSell(ctx context.Context, strategyID, symbol string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM manthan_orders
		     WHERE strategy_id = $1 AND symbol = $2
		       AND order_side = 'SELL'
		       AND order_type IN ('MARKET_SELL','AMO_SELL')
		       AND status IN ('PENDING','PLACED')
		       AND COALESCE(broker_order_id,'') <> ''
		)`, strategyID, symbol).Scan(&exists)
	return exists, err
}

// ListOpenExitSells returns every exit sell still PLACED at the broker, for
// fill confirmation / price chasing. No age cut (2026-10-22 review): a PLACED
// row counts as cover everywhere, so it must stay under watch until the
// broker's truth closes it — an AMO exit queued Friday converts Monday.
func (r *Repository) ListOpenExitSells(ctx context.Context) ([]*ManthanOrder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, signal_id, strategy_id, user_id, symbol, isin, exchange,
		       order_type, order_side, product_type, qty, filled_qty,
		       limit_price, trigger_price, avg_fill_price,
		       broker_order_id, broker_status, indira_symbol, exchange_token,
		       status, retry_count, max_retries, last_error, parent_order_id,
		       created_at, placed_at, filled_at, cancelled_at, updated_at
		FROM manthan_orders
		WHERE order_side = 'SELL'
		  AND order_type IN ('MARKET_SELL','AMO_SELL')
		  AND status = 'PLACED'
		  AND broker_order_id != ''
		ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

// RequeueTodaysFailedExitSignals pulls every MANTHAN_SL_EXIT inbox row
// created this session that is FAILED (waiting out a backoff) or DLQ (gave
// up) back to "retry now". Run once at boot: a fixed binary must fire the
// exits that the previous one could not, immediately — not at the tail of a
// 5-minute backoff and never left in DLQ. Same-day only: yesterday's exit
// quantity/price context is stale and the rules-engine issues a fresh exit
// when the stop is still crossed. Returns the number of rows requeued.
func (r *Repository) RequeueTodaysFailedExitSignals(ctx context.Context, todayStart time.Time) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
		UPDATE signal_inbox
		   SET status='FAILED',
		       next_attempt_at=now(),
		       attempts = CASE WHEN status='DLQ' THEN 0 ELSE attempts END,
		       completed_at=NULL,
		       last_error = 'requeued at boot: ' || left(COALESCE(last_error,''), 200)
		 WHERE order_type='MANTHAN_SL_EXIT'
		   AND status IN ('FAILED','DLQ')
		   AND created_at >= $1`, todayStart)
	if err != nil {
		return 0, fmt.Errorf("requeue failed exit signals: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// UpdateOrderLimitAndParent stamps the limit actually sent to the broker on an
// exit row placed by the protective replay (priced after the row was inserted)
// and links it to its entry when the row has no parent yet.
func (r *Repository) UpdateOrderLimitAndParent(ctx context.Context, id int64, limit float64, parentID int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET limit_price     = CASE WHEN $2::double precision > 0 THEN $2 ELSE limit_price END,
		       parent_order_id = COALESCE(parent_order_id, NULLIF($3::bigint, 0)),
		       updated_at      = now()
		 WHERE id = $1`, id, limit, parentID)
	return err
}

// HasOwnExitSell reports whether this service itself SOLD the lot recently
// (a FILLED exit row of ours with that quantity; 48 h window). Used by the
// manual-exit ledger only when the position event carries no broker order id.
// A PLACED (unfilled) exit of ours does not count: if the user sold the lot
// from the app while our LIMIT rested, that sale is real and must be booked.
func (r *Repository) HasOwnExitSell(ctx context.Context, strategyID, symbol string, qty int) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM manthan_orders
		     WHERE strategy_id = $1 AND symbol = $2
		       AND order_side = 'SELL'
		       AND order_type IN ('MARKET_SELL','AMO_SELL')
		       AND COALESCE(broker_order_id,'') <> ''
		       AND status = 'FILLED'
		       AND filled_qty = $3
		       AND created_at >= now() - interval '2 days'
		)`, strategyID, symbol, qty).Scan(&exists)
	return exists, err
}

// HasOwnExitSellByBrokerID reports whether brokerOrderID is one of OUR sell
// orders — an exit sell OR a stop (SL_SELL / SL_SELL_AMO), any status. The
// definitive test for "this MANUAL_EXIT event is our own sell seen through the
// whole-book path"; a stop fill the positions service routed as manual must
// not be projected twice either (2026-10-22 review).
func (r *Repository) HasOwnExitSellByBrokerID(ctx context.Context, brokerOrderID string) (bool, error) {
	if brokerOrderID == "" {
		return false, nil
	}
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM manthan_orders
		     WHERE broker_order_id = $1
		       AND order_side = 'SELL'
		)`, brokerOrderID).Scan(&exists)
	return exists, err
}

// HasOpenExitSellOfType is HasOpenExitSell restricted to one order type —
// EOD Phase A counts only an AMO exit (next session) as overnight cover; a
// PLACED DAY exit after the close rests nowhere.
func (r *Repository) HasOpenExitSellOfType(ctx context.Context, strategyID, symbol string, ot OrderType) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM manthan_orders
		     WHERE strategy_id = $1 AND symbol = $2
		       AND order_side = 'SELL'
		       AND order_type = $3
		       AND status IN ('PENDING','PLACED')
		       AND COALESCE(broker_order_id,'') <> ''
		)`, strategyID, symbol, string(ot)).Scan(&exists)
	return exists, err
}

// UpdateSLIntendedOnly records a trailed stop that could NOT be sent to the
// broker (below today's band) on a live SL row — trigger_price/limit_price are
// the intended level, broker_* columns stay as the exchange holds them.
func (r *Repository) UpdateSLIntendedOnly(ctx context.Context, id int64, intendedTrigger, intendedLimit float64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET trigger_price=$1, limit_price=$2, updated_at=NOW()
		 WHERE id=$3 AND status IN ('SL_PLACED','SL_MODIFY_PENDING')`, intendedTrigger, intendedLimit, id)
	return err
}

// SumOpenExitQty returns the quantity already committed to OTHER open exit
// rows of the position (excluding excludeID) — a re-place must never sell
// more than net holding minus what is already resting at the broker.
func (r *Repository) SumOpenExitQty(ctx context.Context, strategyID, symbol string, excludeID int64) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(qty - COALESCE(filled_qty,0)), 0)
		  FROM manthan_orders
		 WHERE strategy_id = $1 AND symbol = $2 AND id <> $3
		   AND order_side = 'SELL'
		   AND order_type IN ('MARKET_SELL','AMO_SELL')
		   AND status IN ('PENDING','PLACED')
		   AND COALESCE(broker_order_id,'') <> ''`, strategyID, symbol, excludeID).Scan(&n)
	return n, err
}

// ExpireOrphanPendingExits closes exit rows that were inserted but never got
// a broker id (process died / ctx expired between INSERT and the broker
// call). Such a row would otherwise count as cover in the naked scan forever.
func (r *Repository) ExpireOrphanPendingExits(ctx context.Context, olderThan time.Duration) (int64, error) {
	res, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET status = 'CANCELLED', cancelled_at = now(), updated_at = now(),
		       last_error = 'orphan: no broker id ' || $1::text || ' after insert'
		 WHERE order_side = 'SELL'
		   AND order_type IN ('MARKET_SELL','AMO_SELL')
		   AND status = 'PENDING'
		   AND COALESCE(broker_order_id,'') = ''
		   AND created_at < now() - ($1::interval)`,
		fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ListOpenExitSellsForPosition returns every exit sell of ours still open
// for the position, any age — a last-evening AMO LIMIT that converted this
// morning (new broker id) still locks the quantity and must be found.
func (r *Repository) ListOpenExitSellsForPosition(ctx context.Context, strategyID, symbol string) ([]*ManthanOrder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, signal_id, strategy_id, user_id, symbol, isin, exchange,
		       order_type, order_side, product_type, qty, filled_qty,
		       limit_price, trigger_price, avg_fill_price,
		       broker_order_id, broker_status, indira_symbol, exchange_token,
		       status, retry_count, max_retries, last_error, parent_order_id,
		       created_at, placed_at, filled_at, cancelled_at, updated_at
		FROM manthan_orders
		WHERE strategy_id = $1 AND symbol = $2
		  AND order_side = 'SELL'
		  AND order_type IN ('MARKET_SELL','AMO_SELL')
		  AND status IN ('PENDING','PLACED')
		  AND COALESCE(broker_order_id,'') <> ''
		ORDER BY created_at`, strategyID, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

// AdoptConvertedExit re-points an AMO exit row at the broker id its order
// received at the 08:50 AMO conversion. From here on it is an ordinary DAY
// LIMIT (chaseable), so the type flips to MARKET_SELL and the chase timer
// restarts.
func (r *Repository) AdoptConvertedExit(ctx context.Context, id int64, newBrokerID string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET last_error      = 'adopted converted AMO exit (was ' || COALESCE(broker_order_id,'') || ')',
		       broker_order_id = $2,
		       order_type      = 'MARKET_SELL',
		       placed_at       = now(),
		       updated_at      = now()
		 WHERE id = $1`, id, newBrokerID)
	return err
}

// UpdateSymbolToken rewrites the exchange token / broker symbol on every row
// of a position after the ISIN key showed the token moved (metadata only —
// never touches status, quantities or prices). Returns rows changed.
func (r *Repository) UpdateSymbolToken(ctx context.Context, strategyID, symbol, token, indiraSymbol string) (int64, error) {
	if token == "" {
		return 0, nil
	}
	// Rows with an order still resting at the broker keep the symbol/token the
	// order was PLACED with — Cancel/Modify requests are built from the row and
	// the broker may validate them against the original (2026-10-22 review).
	// Live-price reads do not depend on this: FreshLTP/ExitLimitPrice resolve
	// the token from the ISIN key on every call.
	res, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET exchange_token = $3, indira_symbol = $4, updated_at = now()
		 WHERE strategy_id = $1 AND symbol = $2
		   AND COALESCE(exchange_token,'') <> $3
		   AND (COALESCE(broker_order_id,'') = ''
		        OR status NOT IN ('PENDING','PLACED','SL_PLACED','SL_MODIFY_PENDING','AMO_PENDING','PARTIAL'))`,
		strategyID, symbol, token, indiraSymbol)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// CountOrderEventsSince counts events of one type for an order newer than
// `since`. Used as the safety monitor's per-row re-protect budget: each
// re-placement writes an SL_REPLACED event, so the count survives restarts.
func (r *Repository) CountOrderEventsSince(ctx context.Context, orderID int64, eventType string, since time.Time) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM manthan_order_events
		 WHERE order_id = $1 AND event_type = $2 AND created_at >= $3`,
		orderID, eventType, since).Scan(&n)
	return n, err
}

// ParkStopRow puts a stop row into SL_DEFERRED_BAND with a broker_status tag
// and a last_error note. A parked row still counts as cover for the naked
// scan (so no fresh stop is placed for the position every cycle) and is not
// visited by the monitor's CHECK 1 — it is a deliberate "hands off, human
// decides" state. Used for ghost suspects (broker holds nothing) and for rows
// whose re-protect budget is spent.
func (r *Repository) ParkStopRow(ctx context.Context, id int64, parentID int64, tag, note string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET status='SL_DEFERRED_BAND', broker_status=$1, last_error=$2,
		       parent_order_id=CASE WHEN $3 > 0 THEN $3 ELSE parent_order_id END,
		       updated_at=NOW()
		 WHERE id=$4`, tag, note, parentID, id)
	return err
}

// CloseOrphanStopRow closes a stop row that belongs to a position our ledger
// shows fully sold (net ≤ 0): status CANCELLED with a note, nothing at the
// broker is touched.
func (r *Repository) CloseOrphanStopRow(ctx context.Context, id int64, note string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET status='CANCELLED', broker_status='ORPHAN', last_error=$1,
		       cancelled_at=NOW(), updated_at=NOW()
		 WHERE id=$2`, note, id)
	return err
}

// ParkLatestStopRowForPosition parks the most recent stop row of a position
// (any status) as SL_DEFERRED_BAND with a tag — used when an exit was rejected
// "Set= 0" (the broker holds nothing): the parked row counts as cover so the
// naked scan stops placing stops for a position that does not exist at the
// broker. Returns the parked row id (0 when the position has no stop row).
func (r *Repository) ParkLatestStopRowForPosition(ctx context.Context, strategyID, symbol, tag, note string) (int64, error) {
	var id int64
	err := r.db.QueryRowContext(ctx, `
		UPDATE manthan_orders
		   SET status='SL_DEFERRED_BAND', broker_status=$3, last_error=$4, updated_at=NOW()
		 WHERE id = (SELECT id FROM manthan_orders
		              WHERE strategy_id=$1 AND symbol=$2
		                AND order_side='SELL' AND order_type IN ('SL_SELL','SL_SELL_AMO')
		                -- never a FILLED row (that is ledger truth) nor a live stop
		                -- (SL_PLACED/SL_MODIFY_PENDING — its fill must stay bookable)
		                AND status IN ('CANCELLED','REJECTED','EXPIRED','SL_DEFERRED_BAND')
		              ORDER BY id DESC LIMIT 1)
		 RETURNING id`, strategyID, symbol, tag, note).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

// HasGhostSuspectStop reports whether the position carries a stop row parked
// GHOST_SUSPECT (the broker's RMS said it holds nothing of the scrip). Any
// stop type — the trail skip and the exit gate must both see it.
func (r *Repository) HasGhostSuspectStop(ctx context.Context, strategyID, symbol string) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM manthan_orders
		     WHERE strategy_id = $1 AND symbol = $2
		       AND order_type IN ('SL_SELL','SL_SELL_AMO')
		       AND status = 'SL_DEFERRED_BAND' AND broker_status = 'GHOST_SUSPECT')`,
		strategyID, symbol).Scan(&exists)
	return exists, err
}

// ListRecentlyClosedStopsWithBroker returns the position's stop rows marked
// CANCELLED since `since` that still carry a broker id — rows the exit path
// itself closed ahead of a sell (SL_CANCELLED_FOR_EXIT / SL_ESCALATED). Their
// broker order may have (partly) EXECUTED before the cancel landed; that fill
// is ledger money and must be booked.
func (r *Repository) ListRecentlyClosedStopsWithBroker(ctx context.Context, strategyID, symbol string, since time.Time) ([]*ManthanOrder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, COALESCE(broker_order_id,''), qty, order_type, COALESCE(user_id,''), status
		  FROM manthan_orders
		 WHERE strategy_id = $1 AND symbol = $2
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status = 'CANCELLED'
		   AND COALESCE(broker_order_id,'') <> ''
		   AND updated_at >= $3
		 ORDER BY id`, strategyID, symbol, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ManthanOrder
	for rows.Next() {
		o := &ManthanOrder{Symbol: symbol, StrategyID: strategyID}
		if err := rows.Scan(&o.ID, &o.BrokerOrderID, &o.Qty, &o.OrderType, &o.UserID, &o.Status); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// ListFilledSellsForAvgCheck returns SELL rows FILLED since `since` whose
// average fill has not yet been verified against the broker's trade book
// (no AVG_FILL_VERIFIED event). The fill-time read falls back to the order's
// LIMIT price when the trade book lags the order book; the verifier
// replaces it with the true average once the trades show.
func (r *Repository) ListFilledSellsForAvgCheck(ctx context.Context, since time.Time) ([]*ManthanOrder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT o.id, COALESCE(o.user_id,''), o.symbol, COALESCE(o.broker_order_id,''),
		       COALESCE(o.filled_qty,0), COALESCE(o.avg_fill_price,0), COALESCE(o.limit_price,0), o.order_type
		  FROM manthan_orders o
		 WHERE o.order_side = 'SELL' AND o.status = 'FILLED'
		   AND COALESCE(o.broker_order_id,'') <> ''
		   AND o.filled_at >= $1
		   AND NOT EXISTS (SELECT 1 FROM manthan_order_events e
		                    WHERE e.order_id = o.id AND e.event_type = 'AVG_FILL_VERIFIED')
		 ORDER BY o.id`, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ManthanOrder
	for rows.Next() {
		o := &ManthanOrder{}
		if err := rows.Scan(&o.ID, &o.UserID, &o.Symbol, &o.BrokerOrderID, &o.FilledQty, &o.AvgFillPrice, &o.LimitPrice, &o.OrderType); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// UpdateAvgFillPrice replaces a FILLED row's average fill (status, filled_at
// and filled_qty untouched).
func (r *Repository) UpdateAvgFillPrice(ctx context.Context, id int64, avg float64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET avg_fill_price = $1, updated_at = NOW()
		 WHERE id = $2 AND status = 'FILLED'`, avg, id)
	return err
}
