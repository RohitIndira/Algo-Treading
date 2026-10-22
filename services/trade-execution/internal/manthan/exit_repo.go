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
			placed_at, parent_order_id, trade_date, max_retries
		) VALUES ($1,$2,$3,$4,$5,'NSE',$6,'SELL','DELIVERY',$7,$8,0,$9,$10,'PLACED',$11,
			now(), NULLIF($12,0), $13, 0)
		ON CONFLICT (signal_id) DO NOTHING
		RETURNING id`,
		"exit-"+row.BrokerOrderID, row.StrategyID, row.UserID, row.Symbol, nullStr(row.ISIN),
		string(row.OrderType), row.Qty, row.LimitPrice,
		info.IndiraSymbol, info.ExchangeToken, row.BrokerOrderID, parent, tradeDate,
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

// HasOwnExitSellByBrokerID reports whether brokerOrderID is one of OUR exit
// sells (any status). The definitive test for "this MANUAL_EXIT event is our
// own sell seen through the whole-book path".
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
		       AND order_type IN ('MARKET_SELL','AMO_SELL')
		)`, brokerOrderID).Scan(&exists)
	return exists, err
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
	res, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET exchange_token = $3, indira_symbol = $4, updated_at = now()
		 WHERE strategy_id = $1 AND symbol = $2
		   AND COALESCE(exchange_token,'') <> $3`,
		strategyID, symbol, token, indiraSymbol)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
