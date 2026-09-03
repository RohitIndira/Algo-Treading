package manthan

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/lib/pq"
)

// Repository handles manthan_orders + manthan_order_events persistence.
type Repository struct {
	db *sql.DB
}

// ErrDuplicateActiveEntry is returned by InsertOrder when the row would
// violate uq_manthan_orders_active_entry (migration 016) — i.e. another
// worker has ALREADY inserted an active entry order for this
// (strategy_id, symbol, order_type) tuple and it hasn't yet reached a
// terminal state.
//
// This is the DB-level race defense. In-process check-then-insert in
// entry_handler.go can lose to a concurrent worker; the partial UNIQUE
// index catches the loser atomically and this sentinel signals the
// caller to skip the broker call + gracefully exit without retry.
//
// Never treat this as a poisonous / permanent error — the SIBLING
// worker's insert IS being processed correctly; we just happened to
// arrive second. Inbox worker marks the row DONE.
var ErrDuplicateActiveEntry = errors.New("manthan: an active entry order already exists for (strategy_id, symbol, order_type)")

// pgUniqueViolation is the SQLSTATE code Postgres returns for a UNIQUE
// constraint or partial UNIQUE index conflict. See
// https://www.postgresql.org/docs/current/errcodes-appendix.html
const pgUniqueViolation = "23505"

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// InsertOrder creates a new manthan order record.
//
// Race-safe against concurrent duplicate placement:
//   - Migration 016 added a partial UNIQUE INDEX
//     (strategy_id, symbol, order_type) WHERE order_type IN
//     ('LIMIT_BUY','MARKET_BUY') AND status IN
//     ('PENDING','PLACED','PARTIAL').
//   - Two workers racing to place an entry order for the same
//     (strategy, symbol) both attempt the INSERT; whichever hits the
//     DB first wins. The loser gets a Postgres 23505 unique_violation
//     that we translate to ErrDuplicateActiveEntry, and the caller
//     (entry_handler.go) short-circuits WITHOUT touching the broker.
//   - Once the winning order reaches a terminal state (FILLED /
//     CANCELLED / REJECTED), the row leaves the partial index and a
//     subsequent entry for the same (strategy, symbol) is allowed
//     (legit re-entry after exit).
//
// Any other error (schema, connection, permission) surfaces unchanged.
func (r *Repository) InsertOrder(ctx context.Context, o *ManthanOrder) (int64, error) {
	var id int64
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO manthan_orders (
			signal_id, strategy_id, user_id, symbol, isin, exchange,
			order_type, order_side, product_type,
			qty, limit_price, trigger_price,
			indira_symbol, exchange_token, status, max_retries, trade_date
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
		RETURNING id`,
		nullStr(o.SignalID), o.StrategyID, o.UserID, o.Symbol, nullStr(o.ISIN), o.Exchange,
		o.OrderType, o.OrderSide, o.ProductType,
		o.Qty, o.LimitPrice, o.TriggerPrice,
		o.IndiraSymbol, o.ExchangeToken, o.Status, o.MaxRetries, o.TradeDate,
	).Scan(&id)
	if err != nil {
		// Detect the partial-UNIQUE conflict from migration 016 and
		// translate to a semantic sentinel. Callers use errors.Is to
		// decide "skip broker call, mark inbox DONE".
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && string(pqErr.Code) == pgUniqueViolation {
			return 0, fmt.Errorf("%w: strategy=%s symbol=%s order_type=%s: %s",
				ErrDuplicateActiveEntry, o.StrategyID, o.Symbol, o.OrderType, pqErr.Message)
		}
		return 0, err
	}
	return id, nil
}

// UpdateOrderParentID sets manthan_orders.parent_order_id for an existing
// row. Used by the market-topup flow in entry_handler.go: when a LIMIT BUY
// partially fills, we place a MARKET BUY for the remainder and link the
// new row to the parent LIMIT so downstream (positions svc via
// LookupOrderMeta, reconciler, mobile-app aggregation) can trace lineage.
func (r *Repository) UpdateOrderParentID(ctx context.Context, id, parentID int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET parent_order_id=$1, updated_at=NOW()
		 WHERE id=$2`, parentID, id)
	return err
}

// UpdateOrderPlaced marks an order as placed with broker.
func (r *Repository) UpdateOrderPlaced(ctx context.Context, id int64, brokerOrderID string) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET broker_order_id=$1, status='PLACED', placed_at=$2, updated_at=$2
		WHERE id=$3`, brokerOrderID, now, id)
	return err
}

// UpdateOrderFilled marks an order as filled.
func (r *Repository) UpdateOrderFilled(ctx context.Context, id int64, filledQty int, avgPrice float64) error {
	now := time.Now()
	status := StatusFilled
	if filledQty < 1 {
		return fmt.Errorf("filled qty must be > 0")
	}
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET status=$1, filled_qty=$2, avg_fill_price=$3, filled_at=$4, updated_at=$4
		WHERE id=$5`, status, filledQty, avgPrice, now, id)
	return err
}

// UpdateOrderPartial marks an order as partially filled.
func (r *Repository) UpdateOrderPartial(ctx context.Context, id int64, filledQty int, avgPrice float64) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET status='PARTIAL', filled_qty=$1, avg_fill_price=$2, updated_at=$3
		WHERE id=$4`, filledQty, avgPrice, now, id)
	return err
}

// UpdateOrderRejected marks an order as rejected.
func (r *Repository) UpdateOrderRejected(ctx context.Context, id int64, reason string) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET status='REJECTED', last_error=$1, updated_at=$2
		WHERE id=$3`, reason, now, id)
	return err
}

// UpdateOrderCancelled marks an order as cancelled.
func (r *Repository) UpdateOrderCancelled(ctx context.Context, id int64) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET status='CANCELLED', cancelled_at=$1, updated_at=$1
		WHERE id=$2`, now, id)
	return err
}

// UpdateOrderStatus updates status + broker_status + retry_count.
func (r *Repository) UpdateOrderStatus(ctx context.Context, id int64, status OrderStatus, brokerStatus string, retryCount int, lastError string) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET status=$1, broker_status=$2, retry_count=$3, last_error=$4, updated_at=$5
		WHERE id=$6`, status, brokerStatus, retryCount, lastError, now, id)
	return err
}

// UpdateSLBrokerID stores the SL order's broker ID and links it to parent entry.
func (r *Repository) UpdateSLBrokerID(ctx context.Context, id int64, brokerOrderID string, parentID int64) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders SET broker_order_id=$1, status='SL_PLACED', parent_order_id=$2, placed_at=$3, updated_at=$3
		WHERE id=$4`, brokerOrderID, parentID, now, id)
	return err
}

// UpdateSLBrokerExec is UpdateSLBrokerID plus the broker-real trigger/limit the
// exchange actually accepted (post DPR/tick clamp). trigger_price is left as the
// intended (un-clamped) stop set at InsertOrder; only broker_trigger_price /
// broker_limit_price record the clamped reality. This is the SSOT split.
func (r *Repository) UpdateSLBrokerExec(ctx context.Context, id int64, brokerOrderID string, parentID int64, brokerTrigger, brokerLimit float64) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET broker_order_id=$1, status='SL_PLACED', parent_order_id=$2,
		       broker_trigger_price=$3, broker_limit_price=$4, placed_at=$5, updated_at=$5
		 WHERE id=$6`, brokerOrderID, parentID, brokerTrigger, brokerLimit, now, id)
	return err
}

// UpdateSLAfterModify records a successful trail modify: trigger_price/limit_price
// move to the new INTENDED stop (drives the next ratchet), while
// broker_trigger_price/broker_limit_price record what the exchange accepted.
// Fixes the latent bug where trail modifies updated the broker but never the DB row.
func (r *Repository) UpdateSLAfterModify(ctx context.Context, id int64, intendedTrigger, intendedLimit, brokerTrigger, brokerLimit float64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET trigger_price=$1, limit_price=$2,
		       broker_trigger_price=$3, broker_limit_price=$4, updated_at=NOW()
		 WHERE id=$5`, intendedTrigger, intendedLimit, brokerTrigger, brokerLimit, id)
	return err
}

// UpdateBrokerTrigger mirrors the broker's actual resting SL trigger/limit into
// the DB without touching the intended trigger_price. Called by the Reconciler
// each cycle so manthan_orders always reflects broker reality (SSOT).
func (r *Repository) UpdateBrokerTrigger(ctx context.Context, id int64, brokerTrigger, brokerLimit float64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET broker_trigger_price=$1, broker_limit_price=$2, updated_at=NOW()
		 WHERE id=$3`, brokerTrigger, brokerLimit, id)
	return err
}

// MarkSLDeferredBand flags an SL row as deferred (intended 20% below the DPR
// band — no broker order placed) and links it to the parent entry. trigger_price
// already carries the intended stop from InsertOrder.
func (r *Repository) MarkSLDeferredBand(ctx context.Context, id int64, parentID int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET status='SL_DEFERRED_BAND', broker_status='DEFERRED_BAND',
		       parent_order_id=$1, updated_at=NOW()
		 WHERE id=$2`, parentID, id)
	return err
}

// GetDeferredSLBySymbol returns the latest deferred SL row for a strategy+symbol
// (status SL_DEFERRED_BAND), or (nil,nil) if none. Used by the trail to keep a
// deferred SL's intended trigger current and to promote it once the band allows.
func (r *Repository) GetDeferredSLBySymbol(ctx context.Context, strategyID, symbol string) (*ManthanOrder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, signal_id, strategy_id, user_id, symbol, isin, exchange,
		       order_type, order_side, product_type, qty, filled_qty,
		       limit_price, trigger_price, avg_fill_price,
		       broker_order_id, broker_status, indira_symbol, exchange_token,
		       status, retry_count, max_retries, last_error, parent_order_id,
		       created_at, placed_at, filled_at, cancelled_at, updated_at
		FROM manthan_orders
		WHERE order_type='SL_SELL' AND status='SL_DEFERRED_BAND'
		  AND strategy_id=$1 AND symbol=$2
		ORDER BY created_at DESC LIMIT 1`, strategyID, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	orders, err := scanOrders(rows)
	if err != nil {
		return nil, err
	}
	if len(orders) == 0 {
		return nil, nil
	}
	return orders[0], nil
}

// UpdateDeferredIntended trails a deferred SL's intended trigger/limit upward as
// the high rises. Status stays SL_DEFERRED_BAND (still no broker order). Ensures
// the daily replay places at the correct trailed 20% once the band re-centers.
func (r *Repository) UpdateDeferredIntended(ctx context.Context, id int64, intendedTrigger, intendedLimit float64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET trigger_price=$1, limit_price=$2, updated_at=NOW()
		 WHERE id=$3 AND status='SL_DEFERRED_BAND'`, intendedTrigger, intendedLimit, id)
	return err
}

// UpdateSLAfterTopupMerge bumps an existing SL_SELL row's qty + trigger/limit
// after a top-up fill merged into the parent SL via ModifyOrder. Without this
// the next trail tick would call ModifySLOrder with the STALE pre-topup qty,
// which Indira interprets as a request to shrink the SL back down — silently
// dropping protection on the top-up shares.
func (r *Repository) UpdateSLAfterTopupMerge(ctx context.Context, id int64, newQty int, intendedTrigger, intendedLimit, brokerTrigger, brokerLimit float64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET qty=$1, trigger_price=$2, limit_price=$3,
		       broker_trigger_price=$4, broker_limit_price=$5, updated_at=NOW()
		 WHERE id=$6`, newQty, intendedTrigger, intendedLimit, brokerTrigger, brokerLimit, id)
	return err
}

// CheckDuplicate returns true if a signal_id already exists (idempotency).
// ReuseOrderForRetry resets a terminal-failed entry row (REJECTED /
// CANCELLED / EXPIRED) back to PENDING with freshly derived qty and limit
// price, so the retry-on-login flow can re-drive it through placement.
// Reuse — not a fresh insert — because manthan_orders_signal_id_key is an
// UNCONDITIONAL unique: the signal_id can only ever live on one row
// (2026-08-27 SHREEPUSHK). The status guard makes this a no-op on a row
// that is live or completed.
func (r *Repository) ReuseOrderForRetry(ctx context.Context, id int64, qty int, limitPrice float64) error {
	res, err := r.db.ExecContext(ctx, `
        UPDATE manthan_orders
           SET status='PENDING', qty=$2, limit_price=$3, filled_qty=0,
               broker_order_id='', broker_status='', last_error='',
               retry_count=retry_count+1, placed_at=NULL, cancelled_at=NULL,
               updated_at=now()
         WHERE id=$1 AND status IN ('REJECTED','CANCELLED','EXPIRED')`,
		id, qty, limitPrice)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("order %d not in a terminal-failed state — refusing reuse", id)
	}
	return nil
}

func (r *Repository) CheckDuplicate(ctx context.Context, signalID string) (bool, error) {
	// Terminal-failure rows (REJECTED / CANCELLED / EXPIRED) do not count as
	// duplicates: they are failed ATTEMPTS, and the retry-on-login flow
	// (2026-08-27, SHREEPUSHK) re-runs the same signal_id after the user's
	// session is restored. Counting them here DLQ'd the retry as
	// "pre-check: duplicate signal_id" — the fourth status-blind dedup
	// layer, one below the inbox's. Live or completed rows still dedup.
	var count int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM manthan_orders
		  WHERE signal_id=$1
		    AND status NOT IN ('REJECTED','CANCELLED','EXPIRED')`, signalID).Scan(&count)
	return count > 0, err
}

// GetOrderBySignalID returns the manthan_order row for a given upstream
// signal_id (the UUID the rules-engine put in ManthanOrder.OrderID). Used by
// the Kafka signal consumer for idempotency — if a row already exists for
// this signal_id, the Kafka message is a duplicate delivery and must be
// skipped to avoid placing the same broker order twice.
//
// Returns (nil, nil) if no row exists — caller should then proceed with
// handling. Any DB error is returned as-is.
func (r *Repository) GetOrderBySignalID(ctx context.Context, signalID string) (*ManthanOrder, error) {
	if signalID == "" {
		return nil, nil
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT id, signal_id, strategy_id, user_id, symbol, isin, exchange,
		       order_type, order_side, product_type, qty, filled_qty,
		       limit_price, trigger_price, avg_fill_price,
		       broker_order_id, broker_status, indira_symbol, exchange_token,
		       status, retry_count, max_retries, last_error, parent_order_id,
		       created_at, placed_at, filled_at, cancelled_at, updated_at
		FROM manthan_orders WHERE signal_id = $1
		ORDER BY id DESC LIMIT 1`, signalID)
	o, err := scanOrder(row)
	if err == sql.ErrNoRows {
		return nil, nil // duplicate-check miss: order hasn't been seen before
	}
	if err != nil {
		return nil, err
	}
	return o, nil
}

// GetEntrySignalIDByOrderID resolves an order id to its entry's signal_id.
// For an entry order it returns its own signal_id; for an SL/exit order
// (which has parent_order_id pointing to its entry) it walks up one level.
//
// Used by sl_handler / safety_monitor when publishing manthan.execution.events
// — every SL_*/EXIT_* event must carry the original entry signal_id so the
// rules-engine FillConsumer can attribute the broker activity to the right
// manthan_signal_decisions row.
func (r *Repository) GetEntrySignalIDByOrderID(ctx context.Context, orderID int64) (string, error) {
	var signalID sql.NullString
	var parentID sql.NullInt64
	err := r.db.QueryRowContext(ctx,
		`SELECT signal_id, parent_order_id FROM manthan_orders WHERE id = $1`,
		orderID).Scan(&signalID, &parentID)
	if err != nil {
		return "", err
	}
	// SL orders carry parent_order_id pointing to their entry; entry orders
	// have signal_id set directly.
	if parentID.Valid {
		var parentSignalID sql.NullString
		if err := r.db.QueryRowContext(ctx,
			`SELECT signal_id FROM manthan_orders WHERE id = $1`,
			parentID.Int64).Scan(&parentSignalID); err != nil {
			return "", err
		}
		if parentSignalID.Valid {
			return parentSignalID.String, nil
		}
	}
	if signalID.Valid {
		return signalID.String, nil
	}
	return "", fmt.Errorf("signal_id not set on order %d", orderID)
}

// ListUserIDsWithLiveOrders returns distinct user_ids that have at least one
// non-terminal order. Used by the reconciler to decide which users need a
// broker-book fetch this cycle (no live orders = nothing to reconcile).
func (r *Repository) ListUserIDsWithLiveOrders(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT DISTINCT user_id FROM manthan_orders
		WHERE status IN ('PENDING','PLACED','SL_PLACED','SL_MODIFY_PENDING','PARTIALLY_FILLED')
		  AND broker_order_id != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var uid string
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		ids = append(ids, uid)
	}
	return ids, rows.Err()
}

// GetLiveOrdersByUser returns every manthan_order for a user that is NOT in a
// terminal state (PENDING, PLACED, SL_PLACED, SL_MODIFY_PENDING, PARTIALLY_FILLED).
// Used by the reconciler to compare DB beliefs against broker reality.
// Terminal states (FILLED, CANCELLED, REJECTED, EXITED) are excluded — they
// don't need reconciliation.
func (r *Repository) GetLiveOrdersByUser(ctx context.Context, userID string) ([]*ManthanOrder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, signal_id, strategy_id, user_id, symbol, isin, exchange,
		       order_type, order_side, product_type, qty, filled_qty,
		       limit_price, trigger_price, avg_fill_price,
		       broker_order_id, broker_status, indira_symbol, exchange_token,
		       status, retry_count, max_retries, last_error, parent_order_id,
		       created_at, placed_at, filled_at, cancelled_at, updated_at
		FROM manthan_orders
		WHERE user_id = $1
		  AND status IN ('PENDING','PLACED','SL_PLACED','SL_MODIFY_PENDING','PARTIALLY_FILLED')
		  AND broker_order_id != ''
		ORDER BY created_at`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

// GetActiveSLOrders returns all SL orders that are currently on the exchange.
func (r *Repository) GetActiveSLOrders(ctx context.Context) ([]*ManthanOrder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, signal_id, strategy_id, user_id, symbol, isin, exchange,
		       order_type, order_side, product_type, qty, filled_qty,
		       limit_price, trigger_price, avg_fill_price,
		       broker_order_id, broker_status, indira_symbol, exchange_token,
		       status, retry_count, max_retries, last_error, parent_order_id,
		       created_at, placed_at, filled_at, cancelled_at, updated_at
		FROM manthan_orders
		WHERE order_type = 'SL_SELL' AND status IN ('SL_PLACED', 'SL_MODIFY_PENDING')
		ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanOrders(rows)
}

// GetActiveSLByEntrySignalID finds the active SL order whose parent entry has
// the given signal_id. Returns nil with no error if no active SL exists for
// that entry — common case after broker has auto-cancelled the SL because
// underlying shares were sold by the user.
//
// Used by ExternalActivityDetector to cancel "orphan" SL orders left dangling
// after a manual user exit.
func (r *Repository) GetActiveSLByEntrySignalID(ctx context.Context, entrySignalID string) (*ManthanOrder, error) {
	if entrySignalID == "" {
		return nil, nil
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT sl.id, sl.signal_id, sl.strategy_id, sl.user_id, sl.symbol, sl.isin, sl.exchange,
		       sl.order_type, sl.order_side, sl.product_type, sl.qty, sl.filled_qty,
		       sl.limit_price, sl.trigger_price, sl.avg_fill_price,
		       sl.broker_order_id, sl.broker_status, sl.indira_symbol, sl.exchange_token,
		       sl.status, sl.retry_count, sl.max_retries, sl.last_error, sl.parent_order_id,
		       sl.created_at, sl.placed_at, sl.filled_at, sl.cancelled_at, sl.updated_at
		FROM manthan_orders sl
		JOIN manthan_orders entry ON sl.parent_order_id = entry.id
		WHERE entry.signal_id = $1
		  AND sl.order_type = 'SL_SELL'
		  AND sl.status IN ('SL_PLACED','SL_MODIFY_PENDING')
		ORDER BY sl.created_at DESC
		LIMIT 1`, entrySignalID)
	o, err := scanOrder(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return o, err
}

// GetActiveEntryBySymbol returns the active entry order for a symbol+strategy.
func (r *Repository) GetActiveEntryBySymbol(ctx context.Context, strategyID, symbol string) (*ManthanOrder, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, signal_id, strategy_id, user_id, symbol, isin, exchange,
		       order_type, order_side, product_type, qty, filled_qty,
		       limit_price, trigger_price, avg_fill_price,
		       broker_order_id, broker_status, indira_symbol, exchange_token,
		       status, retry_count, max_retries, last_error, parent_order_id,
		       created_at, placed_at, filled_at, cancelled_at, updated_at
		FROM manthan_orders
		WHERE strategy_id=$1 AND symbol=$2 AND order_type='LIMIT_BUY' AND status='FILLED'
		ORDER BY created_at DESC LIMIT 1`, strategyID, symbol)
	return scanOrder(row)
}

// ListOurBrokerOrderIDsForUser returns the set of broker_order_id values
// we placed for this user (across all order types — entries, SLs, exits).
// Used by the ExternalActivityDetector to identify "third-party" SELL
// orders in the broker's order-book — anything NOT in this set was placed
// outside our system (mobile app, web app, another tool).
func (r *Repository) ListOurBrokerOrderIDsForUser(ctx context.Context, userID string) (map[string]bool, error) {
	out := map[string]bool{}
	if userID == "" {
		return out, nil
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT broker_order_id
		FROM manthan_orders
		WHERE user_id = $1
		  AND broker_order_id IS NOT NULL
		  AND broker_order_id != ''`, userID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var bid string
		if err := rows.Scan(&bid); err == nil && bid != "" {
			out[bid] = true
		}
	}
	return out, rows.Err()
}

// ListActiveManthanBrokerOrderIDs returns broker_order_ids for orders that
// are still "live" from the broker's WSS perspective — i.e. the broker may
// still emit status events for them and we want to route those to our
// WSSKafkaBridge fanout.
//
// Includes entries whose fill status is PLACED / FILLED (an SL may still
// fire), SL rows whose status is SL_PLACED / SL_DEFERRED_BAND (broker may
// modify or trigger) and AMO rows waiting for conversion. Excludes truly
// terminal states — CANCELLED, REJECTED, EXPIRED, SL_FILLED, or entries
// whose position has been fully exited via manthan_exits.
//
// Called once at boot from wssBridge recovery in manthan_init.go.
func (r *Repository) ListActiveManthanBrokerOrderIDs(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT broker_order_id
		  FROM manthan_orders
		 WHERE broker_order_id IS NOT NULL
		   AND broker_order_id != ''
		   AND status NOT IN ('CANCELLED','REJECTED','EXPIRED','SL_FILLED','AMO_REJECTED')
		   -- Keep the window bounded so long-uptime doesn't accumulate stale
		   -- rows. Broker rarely emits WSS updates for orders older than a
		   -- few sessions; a 7-day window covers weekends + one holiday.
		   AND created_at >= NOW() - INTERVAL '7 days'`)
	if err != nil {
		return nil, fmt.Errorf("ListActiveManthanBrokerOrderIDs query: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var bid string
		if err := rows.Scan(&bid); err != nil {
			return nil, fmt.Errorf("ListActiveManthanBrokerOrderIDs scan: %w", err)
		}
		if bid != "" {
			out = append(out, bid)
		}
	}
	return out, rows.Err()
}

// GetEntryFilledAt returns the broker fill timestamp of an entry order
// identified by its signal_id. Used by the detector to time-order broker
// SELL events relative to when we bought (only post-buy SELLs can be
// against our position).
func (r *Repository) GetEntryFilledAt(ctx context.Context, signalID string) (time.Time, error) {
	var t sql.NullTime
	err := r.db.QueryRowContext(ctx, `
		SELECT filled_at FROM manthan_orders
		WHERE signal_id = $1 AND order_side = 'BUY' AND status = 'FILLED'
		ORDER BY id DESC LIMIT 1`, signalID).Scan(&t)
	if err != nil {
		return time.Time{}, err
	}
	if !t.Valid {
		return time.Time{}, nil
	}
	return t.Time, nil
}

// GetLiveEntryForSymbol finds the most recent active BUY entry for a user
// + symbol that does not yet have a matching exit. Used by the WSS-side
// manual-exit detector to resolve which signal_id (and strategy_id) should
// receive the MANUAL_EXIT_DETECTED event.
//
// Returns nil/"" with no error when nothing matches — caller should treat
// that as "user sold something we never owned via the algo" (silent skip).
func (r *Repository) GetLiveEntryForSymbol(ctx context.Context, userID, symbol string) (signalID, strategyID string, err error) {
	if userID == "" || symbol == "" {
		return "", "", nil
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT e.signal_id, e.strategy_id
		FROM manthan_orders e
		WHERE e.user_id  = $1
		  AND e.symbol   = $2
		  AND e.order_side = 'BUY'
		  AND e.status   = 'FILLED'
		  AND e.signal_id IS NOT NULL
		  AND NOT EXISTS (
		      SELECT 1 FROM manthan_orders x
		      WHERE x.strategy_id = e.strategy_id
		        AND x.symbol      = e.symbol
		        AND x.order_side  = 'SELL'
		        AND x.status      = 'FILLED'
		        AND x.created_at  > e.filled_at
		  )
		ORDER BY e.created_at DESC
		LIMIT 1`, userID, symbol)
	var sigID, stratID sql.NullString
	if scanErr := row.Scan(&sigID, &stratID); scanErr != nil {
		if scanErr == sql.ErrNoRows {
			return "", "", nil
		}
		return "", "", scanErr
	}
	if !sigID.Valid {
		return "", "", nil
	}
	return sigID.String, stratID.String, nil
}

// LiveEntry is a thin view of a filled BUY entry that does not yet have a
// matching exit (no FILLED SELL/SL_FILLED for the same strategy+symbol).
// Used by the external-activity detector to enumerate "what we believe we
// hold for this user" before comparing against broker reality.
type LiveEntry struct {
	OrderID    int64
	SignalID   string
	StrategyID string
	UserID     string
	Symbol     string
	FilledQty  int
}

// GetLiveEntriesByUser returns every filled entry order for a user that has
// no terminal exit yet. The "no exit" check excludes entries whose strategy
// already has a FILLED SL/MARKET SELL — those are already exited at our end
// and don't need detector polling.
//
// We scope by user_id (not strategy_id) so a single broker position-book
// fetch can answer for all of that user's strategies at once.
func (r *Repository) GetLiveEntriesByUser(ctx context.Context, userID string) ([]LiveEntry, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT e.id, e.signal_id, e.strategy_id, e.user_id, e.symbol, e.filled_qty
		FROM manthan_orders e
		WHERE e.user_id = $1
		  AND e.order_side = 'BUY'
		  AND e.status = 'FILLED'
		  AND e.signal_id IS NOT NULL
		  -- Exclude entries that already have a terminal exit recorded.
		  AND NOT EXISTS (
		      SELECT 1 FROM manthan_orders x
		      WHERE x.strategy_id = e.strategy_id
		        AND x.symbol = e.symbol
		        AND x.order_side = 'SELL'
		        AND x.status = 'FILLED'
		        AND x.created_at > e.filled_at
		  )`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []LiveEntry
	for rows.Next() {
		var e LiveEntry
		var sigID sql.NullString
		if err := rows.Scan(&e.OrderID, &sigID, &e.StrategyID, &e.UserID, &e.Symbol, &e.FilledQty); err != nil {
			return nil, err
		}
		if sigID.Valid {
			e.SignalID = sigID.String
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ─────────────────────────────────────────────────────────────────────────────
// Protective replayer (custom GTC) — Phase A/B/C support
// ─────────────────────────────────────────────────────────────────────────────

// PositionNeedingProtection is one open Manthan position that requires an
// SL on the broker for tomorrow's session. Returned by
// ListPositionsNeedingProtection at 16:35 IST.
type PositionNeedingProtection struct {
	EntryOrderID   int64
	EntrySignalID  string
	StrategyID     string
	UserID         string
	Symbol         string
	ISIN           string
	IndiraSymbol   string
	ExchangeToken  string
	Exchange       string
	NetQty         int     // sum(filled BUY) − sum(filled SELL); always > 0 here
	LatestTrigger  float64 // most recent trail trigger from any prior SL row (0 if no prior SL)
	LatestLimit    float64
	EntryFillPrice float64 // avg_fill_price of the entry BUY order — used as SL fallback when LatestTrigger==0 (e.g. EOD Phase A on entry-same-day positions with no tick-handler trail yet)
}

// ListPositionsNeedingProtection enumerates every position that has a filled
// entry and no terminal exit. Used by the replayer's Phase A (16:35 IST) to
// know which positions need an AMO+SL for the next session. Aggregates qty
// across top-ups; carries the latest trigger price from the most recent SL
// row so the replayer can preserve TSL trail state.
func (r *Repository) ListPositionsNeedingProtection(ctx context.Context) ([]PositionNeedingProtection, error) {
	rows, err := r.db.QueryContext(ctx, `
		WITH live_buys AS (
		    -- FIX 4: anchor on the ORIGINAL entry, not the latest BUY. With a
		    -- MARKET top-up, the previous "created_at DESC" picked the top-up as
		    -- the entry and keyed the SL off it, so the SL placed on the original
		    -- entry was missed and Phase A used the wrong (fallback) trigger
		    -- (UNIPARTS got 8% instead of 20%). Prefer the root (parent_order_id
		    -- IS NULL), then the earliest — that is the strategy's reference entry.
		    SELECT DISTINCT ON (e.strategy_id, e.symbol)
		           e.id AS entry_order_id, e.signal_id, e.strategy_id, e.user_id,
		           e.symbol, e.isin, e.indira_symbol, e.exchange_token, e.exchange,
		           COALESCE(e.avg_fill_price, 0) AS entry_fill_price
		    FROM manthan_orders e
		    WHERE e.order_side = 'BUY'
		      AND e.status = 'FILLED'
		      AND e.signal_id IS NOT NULL
		      AND NOT EXISTS (
		          SELECT 1 FROM manthan_orders x
		          WHERE x.strategy_id = e.strategy_id
		            AND x.symbol = e.symbol
		            AND x.order_side = 'SELL'
		            AND x.status = 'FILLED'
		            AND x.created_at > e.filled_at
		      )
		    ORDER BY e.strategy_id, e.symbol,
		             (e.parent_order_id IS NOT NULL), e.created_at ASC
		),
		net_qty AS (
		    SELECT strategy_id, symbol,
		           COALESCE(SUM(CASE WHEN order_side='BUY' AND status='FILLED'  THEN filled_qty
		                              WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
		                              ELSE 0 END), 0) AS net
		    FROM manthan_orders
		    GROUP BY strategy_id, symbol
		),
		latest_sl AS (
		    -- FIX 4: the position's most recent SL across the WHOLE lineage
		    -- (original entry + any top-up SLs), keyed by (strategy,symbol) — not
		    -- parent_order_id — so a top-up's SL or the original's SL both count.
		    SELECT DISTINCT ON (sl.strategy_id, sl.symbol)
		           sl.strategy_id, sl.symbol,
		           sl.trigger_price,
		           sl.limit_price
		    FROM manthan_orders sl
		    WHERE sl.order_type IN ('SL_SELL','SL_SELL_AMO')
		    ORDER BY sl.strategy_id, sl.symbol, sl.created_at DESC
		)
		SELECT b.entry_order_id, b.signal_id, b.strategy_id, b.user_id,
		       b.symbol, COALESCE(b.isin,''), COALESCE(b.indira_symbol,''),
		       COALESCE(b.exchange_token,''), COALESCE(b.exchange,'NSE'),
		       n.net,
		       COALESCE(l.trigger_price, 0), COALESCE(l.limit_price, 0),
		       b.entry_fill_price
		FROM   live_buys b
		JOIN   net_qty   n ON n.strategy_id = b.strategy_id AND n.symbol = b.symbol
		LEFT JOIN latest_sl l ON l.strategy_id = b.strategy_id AND l.symbol = b.symbol
		WHERE  n.net > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PositionNeedingProtection
	for rows.Next() {
		var p PositionNeedingProtection
		if err := rows.Scan(&p.EntryOrderID, &p.EntrySignalID, &p.StrategyID, &p.UserID,
			&p.Symbol, &p.ISIN, &p.IndiraSymbol, &p.ExchangeToken, &p.Exchange,
			&p.NetQty, &p.LatestTrigger, &p.LatestLimit, &p.EntryFillPrice); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// InsertEmergencySellFilled records the safety-monitor's emergency MARKET SELL
// as a FILLED SELL row in manthan_orders (FIX 5). Without this, an emergency
// exit only wrote a position-event, so the net-qty view (naked scan, Phase A,
// positions svc) still saw the position as OPEN — a state divergence from
// broker reality. The FILLED SELL nets the (strategy,symbol) position to 0.
// Idempotent via signal_id = 'emsell-'+brokerID (ON CONFLICT DO NOTHING).
// avg_fill_price is left NULL — the true fill price arrives later via WSS/
// tradebook and is reconciled; the qty is what closes the lot.
func (r *Repository) InsertEmergencySellFilled(ctx context.Context, sl *ManthanOrder, brokerID string, qty int) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO manthan_orders
			(signal_id, strategy_id, user_id, symbol, isin, order_type, order_side,
			 qty, filled_qty, status, broker_order_id,
			 indira_symbol, exchange_token, exchange, filled_at)
		VALUES ($1,$2,$3,$4,$5,'MARKET_SELL','SELL',$6,$6,'FILLED',$7,$8,$9,'NSE',now())
		ON CONFLICT (signal_id) DO NOTHING`,
		"emsell-"+brokerID, sl.StrategyID, sl.UserID, sl.Symbol, sl.ISIN,
		qty, brokerID, sl.IndiraSymbol, sl.ExchangeToken)
	return err
}

// WithPositionLock runs fn while holding a Postgres advisory lock keyed by
// (strategy_id, symbol), serializing concurrent operations for the SAME position
// across every inbox-worker goroutine AND every trade-execution replica (the lock
// lives in Postgres, not process memory). Blocks until the lock is free.
//
// FIX 1 (SL-regress race): two SL_MODIFY signals for one position could be
// processed concurrently by two inbox-worker goroutines (ClaimDueInboxRows has no
// per-position grouping). Both read the pre-update broker trigger, both pass the
// ratchet guard (sl_handler.go), and the later broker modify wins → the SL moves
// DOWN. Serializing the read→guard→broker-modify→update sequence per position
// makes the SECOND goroutine block until the first commits its new trigger; it
// then re-reads the higher trigger and the existing guard refuses the lower modify.
//
// The lock is session-scoped on a dedicated pooled connection and released in
// defer (and again by conn.Close as a backstop), so a panic or early return can
// never leak it. fn's own DB writes go through the pool as usual — the lock is a
// pure mutex, it does not need to share fn's connection. Liquidation-hazard-safe:
// it changes ordering only, never cancels or places anything itself.
func (r *Repository) WithPositionLock(ctx context.Context, strategyID, symbol string, fn func(context.Context) error) error {
	key := strategyID + ":" + symbol
	conn, err := r.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("WithPositionLock: acquire conn for %s: %w", key, err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, key); err != nil {
		return fmt.Errorf("WithPositionLock: acquire lock for %s: %w", key, err)
	}
	// Release on the SAME connection. Use a background context so the unlock still
	// runs even if ctx was cancelled mid-fn. conn.Close() is the final backstop —
	// closing a connection releases all its session advisory locks.
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, key)
	}()

	return fn(ctx)
}

// ListNakedOpenBuyPositions returns every position that violates the FIX 3
// invariant "every OPEN filled BUY position has exactly one ACTIVE stop-loss":
//
//	order_side='BUY' AND status='FILLED'      (1,2: filled BUY)
//	AND net_qty(strategy,symbol) > 0          (3: still OPEN — not sold/exited)
//	AND broker_order_id NOT LIKE 'PAPER-%'    (LIVE only — paper needs no broker SL)
//	AND NO active SL row for the entry        (4: unprotected)
//
// "Active SL" is matched by the deterministic SL signal_id ('sl-'||entry.signal_id)
// with a non-terminal status (PENDING / SL_PLACED / SL_DEFERRED_BAND). A REJECTED
// SL has parent_order_id=NULL (the parent link is only set on successful
// placement), so signal_id matching — not parent_order_id — is the reliable key.
// SL_DEFERRED_BAND counts as protected (deliberate defer + Phase A backstop).
//
// The safety-monitor's naked scan drives PlaceInitialSL for each row every 2s
// until an active SL exists. Never cancels/exits — liquidation-hazard-safe.
func (r *Repository) ListNakedOpenBuyPositions(ctx context.Context) ([]PositionNeedingProtection, error) {
	// Coverage-aware naked detection. Two aggregates per (strategy, symbol):
	//   net       = filled BUY − filled SELL  (shares actually held)
	//   sl_cover  = Σ qty of ACTIVE SL orders  (shares already protected)
	// A position is naked only for (net − sl_cover) shares. Returning the FULL
	// net here was the 2026-08-11 PICCADIL bug: a partial LIMIT fill (7) got a
	// 7-qty SL, the MARKET top-up (17) tranche looked "naked", and the monitor
	// placed a SECOND SL for the whole net (24) → 7+24=31 SL qty on a 24-share
	// holding → broker rejected the over-commit, and the 15s loop re-placed it
	// forever. The protect qty is now LEAST(this tranche's own fill, remaining
	// uncovered gap) so total SL coverage can never exceed the holding, and a
	// partial exit can never leave us trying to protect more than we hold.
	rows, err := r.db.QueryContext(ctx, `
		WITH net_qty AS (
		    SELECT strategy_id, symbol,
		           COALESCE(SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
		                             WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
		                             ELSE 0 END), 0) AS net
		    FROM manthan_orders
		    GROUP BY strategy_id, symbol
		),
		sl_cover AS (
		    SELECT strategy_id, symbol, COALESCE(SUM(qty), 0) AS covered
		    FROM manthan_orders
		    WHERE order_side = 'SELL'
		      AND order_type LIKE '%SL%'
		      AND status IN ('PENDING','SL_PLACED','SL_DEFERRED_BAND')
		    GROUP BY strategy_id, symbol
		)
		SELECT e.id, COALESCE(e.signal_id,''), e.strategy_id, e.user_id, e.symbol,
		       COALESCE(e.isin,''), COALESCE(e.indira_symbol,''),
		       COALESCE(e.exchange_token,''), COALESCE(e.exchange,'NSE'),
		       LEAST(COALESCE(e.filled_qty, e.qty), n.net - COALESCE(s.covered, 0)) AS protect_qty,
		       COALESCE(e.avg_fill_price, 0)
		FROM   manthan_orders e
		JOIN   net_qty n ON n.strategy_id = e.strategy_id AND n.symbol = e.symbol
		LEFT JOIN sl_cover s ON s.strategy_id = e.strategy_id AND s.symbol = e.symbol
		WHERE  e.order_side = 'BUY'
		  AND  e.order_type IN ('LIMIT_BUY','MARKET_BUY')
		  AND  e.status = 'FILLED'
		  AND  e.signal_id IS NOT NULL
		  AND  COALESCE(e.broker_order_id,'') NOT LIKE 'PAPER-%'
		  AND  n.net > 0
		  -- position is UNDER-covered: uncovered shares still exist
		  AND  n.net > COALESCE(s.covered, 0)
		  -- ...and THIS tranche has no active SL of its own
		  AND  NOT EXISTS (
		        SELECT 1 FROM manthan_orders sl
		        WHERE sl.signal_id = 'sl-' || e.signal_id
		          AND sl.status IN ('PENDING','SL_PLACED','SL_DEFERRED_BAND'))`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []PositionNeedingProtection
	for rows.Next() {
		var p PositionNeedingProtection
		if err := rows.Scan(&p.EntryOrderID, &p.EntrySignalID, &p.StrategyID, &p.UserID,
			&p.Symbol, &p.ISIN, &p.IndiraSymbol, &p.ExchangeToken, &p.Exchange,
			&p.NetQty, &p.EntryFillPrice); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ResetTerminalSLToPending resets a terminally-failed SL row (REJECTED /
// CANCELLED / EXPIRED) back to PENDING so PlaceInitialSL can re-drive it in
// place — preserving the deterministic signal_id and avoiding a duplicate
// broker SL. Returns (id, true, nil) if a terminal row was reset, or
// (0, false, nil) if none matched (caller then treats the original insert
// error as a real failure). Enforces "exactly one SL row per entry".
func (r *Repository) ResetTerminalSLToPending(ctx context.Context, slSignalID string, trigger, limit float64) (int64, bool, error) {
	var id int64
	err := r.db.QueryRowContext(ctx, `
		UPDATE manthan_orders
		SET status='PENDING', broker_order_id=NULL, broker_status=NULL,
		    trigger_price=$2, limit_price=$3, updated_at=now()
		WHERE signal_id=$1
		  AND status IN ('REJECTED','CANCELLED','EXPIRED')
		RETURNING id`,
		slSignalID, trigger, limit).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// ArmRetryRow is one entry in the manthan_arm_retries queue (Layer 4).
type ArmRetryRow struct {
	ID           int64
	UserID       string
	EntryOrderID int64
	TradeDate    time.Time
	Reason       string
	Attempts     int
	LastError    string
}

// EnqueueArmRetry inserts a PENDING retry row, or no-ops if a PENDING row
// already exists for the same (entry_order_id, trade_date). Idempotent under
// the partial unique index from migration 013.
//
// Called by EOD Phase A's skip-block whenever a position can't be armed for
// a recoverable reason (no broker auth, JWT expired during PlaceAMOSLSell).
func (r *Repository) EnqueueArmRetry(
	ctx context.Context,
	userID string,
	entryOrderID int64,
	tradeDate time.Time,
	reason string,
) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO manthan_arm_retries
		    (user_id, entry_order_id, trade_date, reason)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT DO NOTHING`,
		userID, entryOrderID, tradeDate, reason)
	return err
}

// ListPendingArmRetries returns every PENDING retry row. When userID is
// non-empty, filter to that user (the on-login wake path); otherwise scan
// the full queue (the 5-minute ticker path).
func (r *Repository) ListPendingArmRetries(ctx context.Context, userID string) ([]ArmRetryRow, error) {
	q := `
		SELECT id, user_id, entry_order_id, trade_date, reason, attempts, COALESCE(last_error, '')
		FROM   manthan_arm_retries
		WHERE  status = 'PENDING'`
	args := []any{}
	if userID != "" {
		q += " AND user_id = $1"
		args = append(args, userID)
	}
	q += " ORDER BY created_at ASC LIMIT 500"

	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArmRetryRow
	for rows.Next() {
		var rec ArmRetryRow
		if err := rows.Scan(&rec.ID, &rec.UserID, &rec.EntryOrderID, &rec.TradeDate, &rec.Reason, &rec.Attempts, &rec.LastError); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// MarkArmRetryAttempted bumps attempts + records the last error from a failed
// re-attempt. Row stays PENDING for the next tick.
func (r *Repository) MarkArmRetryAttempted(ctx context.Context, id int64, errMsg string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_arm_retries
		SET    attempts = attempts + 1,
		       last_attempt_at = NOW(),
		       last_error = $2,
		       updated_at = NOW()
		WHERE  id = $1 AND status = 'PENDING'`,
		id, errMsg)
	return err
}

// MarkArmRetryDone marks the retry row terminal-success: the position was
// armed by the latest re-attempt. Verified separately via
// HasActiveProtectionForToday before this is called.
func (r *Repository) MarkArmRetryDone(ctx context.Context, id int64) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_arm_retries
		SET    status = 'DONE', updated_at = NOW()
		WHERE  id = $1 AND status = 'PENDING'`,
		id)
	return err
}

// MarkArmRetriesGivenUpBefore marks every PENDING row whose trade_date is
// strictly before cutoff as GIVEN_UP. Called by the worker's tick to bound
// the queue: once a row's intended trading day has opened (09:30+ IST), the
// AMO ship has sailed and the morning hot-SL cron is the only remaining path.
func (r *Repository) MarkArmRetriesGivenUpBefore(ctx context.Context, cutoffTradeDate time.Time) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_arm_retries
		SET    status = 'GIVEN_UP', updated_at = NOW()
		WHERE  status = 'PENDING' AND trade_date < $1`,
		cutoffTradeDate)
	return err
}

// HasActiveProtectionForToday returns true if the given entry order already
// has an SL row (either a live SL_SELL or an AMO_PENDING SL_SELL_AMO) that
// covers today's session. Used by the 09:14 morning cron to skip positions
// already protected by the prior EOD Phase A submission — avoids placing
// a duplicate SL on top of an AMO that converted at 09:00.
//
// "Active" here means: not cancelled, not rejected, not expired, not filled
// (a filled SL means the position is already exited; nothing to re-arm).
// Trade_date must match today's IST date so an AMO row stamped for an
// earlier session doesn't cause a false positive.
//
// A row only counts if it was CREATED after the close of the last session
// before tradeDate (armedWindowStart). Anything older cannot be a live order
// for that session — it was placed for an earlier session and either
// converted+swept or rejected — however its status column reads. 2026-08-18:
// ten SL_PLACED rows stamped 2026-08-19 but created 00:03 IST on the 18th
// (a mislabelled arm-retry cycle; the orders were live on the 18th and
// swept at 15:30) would otherwise have told BOTH the evening Phase A and
// the 09:14 cron "already protected" for the 19th, leaving ten positions
// with no broker-side stop.
func (r *Repository) HasActiveProtectionForToday(ctx context.Context, entryOrderID int64, tradeDate time.Time) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
		    SELECT 1 FROM manthan_orders
		    WHERE  parent_order_id = $1
		      AND  trade_date = $2
		      AND  order_type IN ('SL_SELL','SL_SELL_AMO')
		      AND  status NOT IN ('CANCELLED','REJECTED','EXPIRED','FILLED','SL_FILLED','SL_SELL_AMO_REJECTED','AMO_REJECTED')
		      AND  created_at >= $3
		)`, entryOrderID, tradeDate, armedWindowStart(tradeDate),
	).Scan(&exists)
	return exists, err
}

// armedWindowStart is the earliest instant at which a protective order for
// `tradeDate` can legitimately have been created: 15:30 IST on the last
// trading day strictly before tradeDate (the previous session's close, when
// the exchange sweeps DAY orders and brokers open the AMO queue). Used by
// HasActiveProtectionForToday and InsertAMOOrder so a stale row can never
// masquerade as live protection for a later session.
func armedWindowStart(tradeDate time.Time) time.Time {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	if ist == nil {
		ist = time.FixedZone("IST", 5*60*60+30*60)
	}
	t := tradeDate.In(ist)
	d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, ist).Add(-24 * time.Hour)
	for !indiraClient.IsTradingDay(d) {
		d = d.Add(-24 * time.Hour)
	}
	// 60 s of tolerance: an ArmRetryWorker cycle that lands at 15:30:00.x
	// stamps tomorrow from the app clock while created_at comes from the DB
	// clock; without slack a legit row could be judged pre-window.
	return d.Add(15*time.Hour + 30*time.Minute - time.Minute)
}

// InsertAMOOrder writes a new SL_SELL_AMO row for the given position+trade_date
// and returns its id. Returns (0, sql.ErrNoRows) if a row already exists for
// the same (parent_order_id, trade_date) — the partial UNIQUE index from
// migration 011 makes this crash-safe: the 16:35 cron can be re-run safely.
//
// alreadyExists==true means the caller should skip rather than treat the
// conflict as an error.
// CountAMOAttemptsToday returns how many AMO rows (any status) exist for this
// entry order and trade date within the current arming window. The EOD armer
// uses it as a give-up cap: a broker that has rejected the same protective
// order several times tonight will reject it again — re-placing forever just
// churns the audit trail and the broker API (2026-08-26 incident: ghost
// positions whose exits never wrote a SELL row were re-armed every 5 minutes
// all evening).
func (r *Repository) CountAMOAttemptsToday(ctx context.Context, parentEntryOrderID int64, tradeDate time.Time) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM manthan_orders
		WHERE  parent_order_id = $1
		  AND  trade_date = $2
		  AND  order_type = 'SL_SELL_AMO'
		  AND  created_at >= $3`,
		parentEntryOrderID, tradeDate, armedWindowStart(tradeDate)).Scan(&n)
	return n, err
}

// LastAMOAttemptAt returns the newest overnight attempt's creation time
// for one (entry, target-date) — the spacing gate's clock. ok=false when
// no attempt exists yet.
func (r *Repository) LastAMOAttemptAt(ctx context.Context, parentEntryOrderID int64, tradeDate time.Time) (time.Time, bool, error) {
	var ts sql.NullTime
	err := r.db.QueryRowContext(ctx, `
		SELECT MAX(created_at) FROM manthan_orders
		WHERE  parent_order_id = $1
		  AND  trade_date = $2
		  AND  order_type = 'SL_SELL_AMO'
		  AND  created_at >= $3`,
		parentEntryOrderID, tradeDate, armedWindowStart(tradeDate)).Scan(&ts)
	if err != nil || !ts.Valid {
		return time.Time{}, false, err
	}
	return ts.Time, true, nil
}

// AnnotateOrderError appends the broker's stated reason to a row that a
// sync just closed — 'Rejected' without the why cost us the freeQty-release
// diagnosis for a week.
func (r *Repository) AnnotateOrderError(ctx context.Context, id int64, reason string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET last_error = COALESCE(NULLIF(last_error,'') || ' | ', '') || $2, updated_at = now()
		 WHERE id = $1`, id, reason)
	return err
}

func (r *Repository) InsertAMOOrder(
	ctx context.Context,
	parentEntryOrderID int64,
	p PositionNeedingProtection,
	tradeDate time.Time,
	trigger, limit float64,
) (id int64, alreadyExists bool, err error) {
	// Pre-check existence under the partial unique index's predicate. We do
	// this manually rather than ON CONFLICT because the index is a *partial*
	// unique index, which postgres can't reference by name in ON CONFLICT
	// (it requires a real CONSTRAINT, not just an INDEX). The check + insert
	// run inside a single transaction so the SELECT result is consistent
	// with the INSERT below.
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Same window rule as HasActiveProtectionForToday: a row created before
	// the previous session's close cannot be live protection for tradeDate,
	// whatever its status says (mislabelled arm-retry rows, 2026-08-18).
	var existingID int64
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM manthan_orders
		WHERE  parent_order_id = $1
		  AND  trade_date = $2
		  AND  order_type IN ('SL_SELL','SL_SELL_AMO')
		  AND  status NOT IN ('CANCELLED','REJECTED','FILLED','EXPIRED','SL_SELL_AMO_REJECTED')
		  AND  created_at >= $3
		LIMIT 1`,
		parentEntryOrderID, tradeDate, armedWindowStart(tradeDate),
	).Scan(&existingID)
	if err == nil {
		_ = tx.Commit()
		return 0, true, nil
	}
	if err != sql.ErrNoRows {
		return 0, false, fmt.Errorf("check existing: %w", err)
	}

	// Retire any PRE-WINDOW non-terminal row for this (entry, date) so the
	// partial UNIQUE index uniq_active_sl_per_day lets the real re-arm in.
	// Such a row was created before the previous session's close: whatever
	// its status column says, its broker order (if any) belonged to an
	// earlier session and was swept/rejected — it is not, and cannot become,
	// live protection for tradeDate. This is a DB-only status change; no
	// broker call, nothing cancelled. (2026-08-18: ten such rows.)
	if _, err = tx.ExecContext(ctx, `
		UPDATE manthan_orders
		SET    status = 'EXPIRED',
		       last_error = COALESCE(NULLIF(last_error, ''), '') ||
		                    ' | superseded: created before previous session close — cannot be live for trade_date',
		       updated_at = now()
		WHERE  parent_order_id = $1
		  AND  trade_date = $2
		  AND  order_type IN ('SL_SELL','SL_SELL_AMO')
		  AND  status NOT IN ('CANCELLED','REJECTED','FILLED','EXPIRED','SL_SELL_AMO_REJECTED')
		  AND  created_at < $3`,
		parentEntryOrderID, tradeDate, armedWindowStart(tradeDate),
	); err != nil {
		return 0, false, fmt.Errorf("retire stale rows: %w", err)
	}

	// signal_id must be unique per ATTEMPT, not per (entry, date): the table
	// has UNIQUE(signal_id), so a REJECTED/CANCELLED sibling for the same
	// date (dead token at 16:35, exchange sweep, mislabel) would otherwise
	// block every later re-arm for that session with "duplicate key". That
	// is exactly how 2026-08-17's ten correctly-dated evening attempts died
	// every 5 min until midnight. First attempt keeps the historical
	// "<entry>-amo-<date>" id; retries append "-r<n>".
	// Count by signal_id prefix (not order_type): a converted AMO is
	// promoted to SL_SELL but keeps its "<entry>-amo-<date>" id, and a later
	// re-arm for the same date must not collide with it.
	// Deliberately NOT filtered by trade_date: the admin cap-reset lever
	// (M7.6) clears the counter by NULLing trade_date on burned attempts —
	// those rows still hold their signal_ids, and excluding them from this
	// count made a freed retry reuse the base id and die on the
	// UNIQUE(signal_id) forever (2026-09-01, failed:8 every 5 minutes).
	// The prefix already encodes the date, so cross-date collisions are
	// impossible either way.
	var priorAttempts int
	if err = tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM manthan_orders
		WHERE  parent_order_id = $1
		  AND  signal_id LIKE $2`,
		parentEntryOrderID, p.EntrySignalID+"-amo-"+tradeDate.Format("20060102")+"%",
	).Scan(&priorAttempts); err != nil {
		return 0, false, fmt.Errorf("count prior attempts: %w", err)
	}
	amoSignalID := p.EntrySignalID + "-amo-" + tradeDate.Format("20060102")
	if priorAttempts > 0 {
		amoSignalID = fmt.Sprintf("%s-r%d", amoSignalID, priorAttempts)
	}

	err = tx.QueryRowContext(ctx, `
		INSERT INTO manthan_orders (
		    signal_id, strategy_id, user_id, symbol, isin, exchange,
		    order_type, order_side, product_type,
		    qty, limit_price, trigger_price,
		    indira_symbol, exchange_token, status,
		    parent_order_id, trade_date, max_retries
		)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'SELL','CNC',$8,$9,$10,$11,$12,$13,$14,$15,3)
		RETURNING id`,
		nullStr(amoSignalID), p.StrategyID, p.UserID,
		p.Symbol, nullStr(p.ISIN), p.Exchange,
		string(OrderTypeSLSellAMO), p.NetQty, limit, trigger,
		p.IndiraSymbol, p.ExchangeToken, string(StatusAMOPending),
		parentEntryOrderID, tradeDate,
	).Scan(&id)
	if err != nil {
		return 0, false, fmt.Errorf("insert: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("commit: %w", err)
	}
	return id, false, nil
}

// CountSignalIDPrefix returns how many rows already carry a signal_id that
// starts with prefix (used to derive "-rN" retry ids without colliding with
// UNIQUE(signal_id)).
func (r *Repository) CountSignalIDPrefix(ctx context.Context, prefix string) (int, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM manthan_orders WHERE signal_id LIKE $1`, prefix+"%").Scan(&n)
	return n, err
}

// PromoteAMOToLiveSL records a successful 08:50 IST AMO→live conversion:
// the broker assigns a NEW ordId at conversion, so the row is re-keyed to it
// and promoted to a plain SL_SELL so the trail (SL_MODIFY), the reconciler
// and HasActiveProtectionForToday all treat it as the live stop it now is.
// DB-only; the broker order is untouched.
func (r *Repository) PromoteAMOToLiveSL(ctx context.Context, id int64, newBrokerID, brokerStatus string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		SET    order_type = 'SL_SELL', broker_order_id = $2, broker_status = $3, updated_at = now()
		WHERE  id = $1 AND order_type = 'SL_SELL_AMO' AND status = 'SL_PLACED'`,
		id, newBrokerID, brokerStatus)
	return err
}

// MarkAMOConversionRejected records that the exchange REJECTED the AMO at
// 08:50 IST conversion (typically "Order entered has invalid data." — the
// trigger fell outside the new day's circuit band). Keeps the new broker id
// for traceability. Once REJECTED the row no longer counts as protection, so
// the 09:14 cron / ArmRetryWorker re-plan the position from scratch.
func (r *Repository) MarkAMOConversionRejected(ctx context.Context, id int64, newBrokerID, reason string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		SET    status = 'REJECTED', broker_order_id = COALESCE(NULLIF($2,''), broker_order_id),
		       broker_status = 'Rejected', last_error = $3, updated_at = now()
		WHERE  id = $1 AND status = 'SL_PLACED'`,
		id, newBrokerID, reason)
	return err
}

// ExpireStaleAMORow retires an SL_SELL_AMO row whose session has passed
// (created before the current armed window) and that never got reconciled
// — it cannot be a live order any more, whatever its status says. DB-only.
func (r *Repository) ExpireStaleAMORow(ctx context.Context, id int64, reason string) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		SET    status = 'EXPIRED', last_error = $2, updated_at = now()
		WHERE  id = $1 AND order_type = 'SL_SELL_AMO' AND status = 'SL_PLACED'`,
		id, reason)
	return err
}

// AMOReplayRow is a thin view of a pending SL_SELL_AMO row used by Phase B/C.
type AMOReplayRow struct {
	ID            int64
	EntryOrderID  int64
	EntrySignalID string
	StrategyID    string
	UserID        string
	Symbol        string
	IndiraSymbol  string
	ExchangeToken string
	Exchange      string
	Qty           int
	TriggerPrice  float64
	LimitPrice    float64
	BrokerOrderID string
	TradeDate     time.Time
}

// ListPendingAMOForDate returns every SL_SELL_AMO row with the given trade_date
// that is still in flight. Used by Phase B (09:14 IST re-validate) and Phase C
// (09:15:30 IST reconcile).
func (r *Repository) ListPendingAMOForDate(ctx context.Context, tradeDate time.Time) ([]*AMOReplayRow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT m.id, m.parent_order_id,
		       COALESCE(e.signal_id, ''),
		       m.strategy_id, m.user_id, m.symbol,
		       COALESCE(m.indira_symbol, ''), COALESCE(m.exchange_token, ''),
		       COALESCE(m.exchange, 'NSE'),
		       m.qty, m.trigger_price, m.limit_price,
		       COALESCE(m.broker_order_id, ''), m.trade_date
		FROM   manthan_orders m
		LEFT JOIN manthan_orders e ON e.id = m.parent_order_id
		WHERE  m.order_type = 'SL_SELL_AMO'
		  AND  m.trade_date = $1
		  AND  m.status NOT IN ('CANCELLED','REJECTED','FILLED','SL_SELL_AMO_REJECTED','AMO_REJECTED')`,
		tradeDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []*AMOReplayRow
	for rows.Next() {
		var (
			row AMOReplayRow
			pid sql.NullInt64
		)
		if err := rows.Scan(&row.ID, &pid, &row.EntrySignalID, &row.StrategyID, &row.UserID,
			&row.Symbol, &row.IndiraSymbol, &row.ExchangeToken, &row.Exchange,
			&row.Qty, &row.TriggerPrice, &row.LimitPrice, &row.BrokerOrderID,
			&row.TradeDate); err != nil {
			return nil, err
		}
		if pid.Valid {
			row.EntryOrderID = pid.Int64
		}
		out = append(out, &row)
	}
	return out, rows.Err()
}

// PromoteAMOToActiveSL records a successful AMO→live conversion. The pending
// SL_SELL_AMO row is promoted to a regular SL_SELL row in StatusSLPlaced with
// the fresh broker_order_id assigned at 08:50 conversion. After this call the
// existing SafetyMonitor / Reconciler treat it like any other live SL.
func (r *Repository) PromoteAMOToActiveSL(ctx context.Context, id int64, newBrokerID string) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		SET    order_type = $1,
		       status = $2,
		       broker_order_id = $3,
		       placed_at = $4,
		       updated_at = $4
		WHERE  id = $5`,
		string(OrderTypeSLSell), string(StatusSLPlaced), newBrokerID, now, id)
	return err
}

// MarkAMORejected marks an AMO row terminal after the broker rejected its
// conversion to a live order (typically DPR breach). The replayer's Phase C
// then hot-places a fresh SL with currently-valid DPR.
func (r *Repository) MarkAMORejected(ctx context.Context, id int64, reason string) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		SET    status = $1, last_error = $2, updated_at = $3
		WHERE  id = $4`,
		string(StatusAMORejected), reason, now, id)
	return err
}

// UpdateAMOTrigger rewrites trigger/limit on a pending AMO row. Used by
// Phase B when fresh DPR makes the original trigger invalid: we cancel the
// old AMO at the broker, re-place with corrected trigger, update this row
// to point at the new broker_order_id.
func (r *Repository) UpdateAMOTrigger(ctx context.Context, id int64, newTrigger, newLimit float64, newBrokerOrderID string) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE manthan_orders
		SET    trigger_price = $1,
		       limit_price = $2,
		       broker_order_id = $3,
		       placed_at = $4,
		       updated_at = $4
		WHERE  id = $5`,
		newTrigger, newLimit, newBrokerOrderID, now, id)
	return err
}

// HasActiveSLForPositionToday returns true if the position already has an
// active in-session SL row (order_type=SL_SELL, status SL_PLACED) created
// today. Phase A skips AMO submission for these — there's already a live
// SL on the broker that will be auto-cancelled at 15:30 EOD, but the AMO
// for tomorrow doesn't conflict because uniq_active_sl_per_day is keyed on
// trade_date. This check is purely informational so we can log "skipped:
// position already protected today" cleanly.
func (r *Repository) HasActiveSLForPositionToday(ctx context.Context, parentEntryOrderID int64) (bool, error) {
	var n int
	err := r.db.QueryRowContext(ctx, `
		SELECT count(*) FROM manthan_orders
		WHERE  parent_order_id = $1
		  AND  order_type = 'SL_SELL'
		  AND  status IN ('SL_PLACED','SL_MODIFY_PENDING')
		  AND  trade_date = CURRENT_DATE`, parentEntryOrderID).Scan(&n)
	return n > 0, err
}

// ─────────────────────────────────────────────────────────────────────────────

// InsertEvent records an order state change in the audit trail.
func (r *Repository) InsertEvent(ctx context.Context, orderID int64, eventType, oldStatus, newStatus, brokerStatus string, price float64, qty int, detail string) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO manthan_order_events (order_id, event_type, old_status, new_status, broker_status, price, qty, detail)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		orderID, eventType, oldStatus, newStatus, brokerStatus, price, qty, detail)
	return err
}

func scanOrders(rows *sql.Rows) ([]*ManthanOrder, error) {
	var out []*ManthanOrder
	for rows.Next() {
		o, err := scanOrderRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func scanOrder(row *sql.Row) (*ManthanOrder, error) {
	// Delegate to the NULL-safe scanner. The old direct-Scan version put
	// nullable columns (broker_status, last_error, …) straight into string
	// fields — any NULL made Scan error, and callers that swallowed the error
	// treated it as "row not found". That is how the 2026-08-05 double-buy
	// slipped past GetActiveEntryBySymbol: 7 of 8 held entries had NULL
	// broker_status/last_error, so the duplicate-guard lookup "found nothing"
	// and re-entries were placed with real money. *sql.Row satisfies the
	// scannable interface, so the multi-row scanner works here unchanged.
	return scanOrderRow(row)
}

type scannable interface {
	Scan(dest ...any) error
}

func scanOrderRow(row scannable) (*ManthanOrder, error) {
	o := &ManthanOrder{}
	var (
		signalID      sql.NullString
		isin          sql.NullString
		limitPrice    sql.NullFloat64
		triggerPrice  sql.NullFloat64
		avgFillPrice  sql.NullFloat64
		brokerOrderID sql.NullString
		brokerStatus  sql.NullString
		indiraSymbol  sql.NullString
		exchangeToken sql.NullString
		lastError     sql.NullString
	)
	err := row.Scan(
		&o.ID, &signalID, &o.StrategyID, &o.UserID, &o.Symbol, &isin, &o.Exchange,
		&o.OrderType, &o.OrderSide, &o.ProductType, &o.Qty, &o.FilledQty,
		&limitPrice, &triggerPrice, &avgFillPrice,
		&brokerOrderID, &brokerStatus, &indiraSymbol, &exchangeToken,
		&o.Status, &o.RetryCount, &o.MaxRetries, &lastError, &o.ParentOrderID,
		&o.CreatedAt, &o.PlacedAt, &o.FilledAt, &o.CancelledAt, &o.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	o.SignalID = signalID.String
	o.ISIN = isin.String
	o.LimitPrice = limitPrice.Float64
	o.TriggerPrice = triggerPrice.Float64
	o.AvgFillPrice = avgFillPrice.Float64
	o.BrokerOrderID = brokerOrderID.String
	o.BrokerStatus = brokerStatus.String
	o.IndiraSymbol = indiraSymbol.String
	o.ExchangeToken = exchangeToken.String
	o.LastError = lastError.String
	return o, nil
}

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// OrderMeta is the payload returned by LookupOrderMeta. Mirrors the
// LookupOrderMetaResponse proto shape so the gRPC handler is a pure mapper.
//
// For ENTRY orders: EntrySignalID == SignalID and EntryBrokerOrderID
// equals the row's own broker_order_id. For SL_SELL / EXIT orders those
// point at the parent ENTRY row.
type OrderMeta struct {
	Found              bool
	SignalID           string
	OrderType          string
	StrategyID         string
	UserID             string
	EntrySignalID      string
	EntryBrokerOrderID string

	// AvgFillPrice — this row's real avg fill price from manthan_orders.
	// 0 when the fill hasn't arrived (or the order didn't fill). Positions
	// svc uses this to avoid entry_price = limit fallback when a
	// REST_ORDERBOOK-sourced event was the first to arrive on order.events.
	AvgFillPrice float64

	// SLTriggerPrice / SLBrokerOrderID — for ENTRY rows only, the SL
	// currently placed at the broker for this entry (via manthan_orders
	// parent_order_id self-FK). Positions svc's BUY handler uses these to
	// pull an SL that arrived on Kafka BEFORE the parent BUY event (which
	// would otherwise be dropped by the state machine as "no parent").
	// Both are 0 / "" when no SL row exists.
	SLTriggerPrice  float64
	SLBrokerOrderID string
}

// LookupOrderMeta resolves a broker_order_id to its Manthan lineage.
// Consumed by positions svc via gRPC to enrich order.events (which only
// carry broker_order_id) with signal_id + order_type + entry lineage.
//
// Semantics:
//
//	(meta{Found:true},  nil)  — row found; all fields populated
//	(meta{Found:false}, nil)  — row not found in manthan_orders (probably a
//	                             user manual buy/sell via broker app)
//	(nil,               err)  — DB error; caller decides whether to retry
//
// Uses manthan_orders.parent_order_id (self-FK) to climb one hop for
// non-ENTRY orders. For ENTRY orders the same row's fields are returned.
func (r *Repository) LookupOrderMeta(ctx context.Context, brokerOrderID string) (*OrderMeta, error) {
	if brokerOrderID == "" {
		return nil, fmt.Errorf("broker_order_id is required")
	}

	// Three-way join:
	//   mo   → this row (always present)
	//   ent  → the ENTRY row via mo.parent_order_id (NULL when mo IS the
	//          entry — .Valid false and we fall back to mo.* below)
	//   sl   → the SL row whose parent_order_id points at THIS row
	//          (only meaningful when mo is the entry). LATERAL + LIMIT 1
	//          so a duplicated SL row can't multiply the outer result set.
	// Everything in one round-trip so positions svc's BUY handler doesn't
	// pay a second network hop per fill.
	var (
		signalID           string
		orderType          string
		strategyID         sql.NullString
		userID             string
		entrySignalID      sql.NullString
		entryBrokerOrderID sql.NullString
		avgFillPrice       sql.NullFloat64
		slTriggerPrice     sql.NullFloat64
		slBrokerOrderID    sql.NullString
	)
	err := r.db.QueryRowContext(ctx, `
		SELECT
		  mo.signal_id,
		  mo.order_type,
		  mo.strategy_id,
		  mo.user_id,
		  ent.signal_id        AS entry_signal_id,
		  ent.broker_order_id  AS entry_broker_order_id,
		  mo.avg_fill_price,
		  sl.trigger_price     AS sl_trigger_price,
		  sl.broker_order_id   AS sl_broker_order_id
		FROM manthan_orders mo
		LEFT JOIN manthan_orders ent ON ent.id = mo.parent_order_id
		LEFT JOIN LATERAL (
		  SELECT trigger_price, broker_order_id
		    FROM manthan_orders x
		   WHERE x.parent_order_id = mo.id
		     AND x.order_type LIKE 'SL%'
		   ORDER BY x.created_at DESC
		   LIMIT 1
		) sl ON true
		WHERE mo.broker_order_id = $1
		LIMIT 1`,
		brokerOrderID,
	).Scan(
		&signalID, &orderType, &strategyID, &userID,
		&entrySignalID, &entryBrokerOrderID,
		&avgFillPrice, &slTriggerPrice, &slBrokerOrderID,
	)
	if err == sql.ErrNoRows {
		return &OrderMeta{Found: false}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("LookupOrderMeta select: %w", err)
	}

	// Fall back: this IS the entry order (parent_order_id was NULL). The
	// entry lineage points to itself.
	esID := entrySignalID.String
	ebID := entryBrokerOrderID.String
	if !entrySignalID.Valid {
		esID = signalID
	}
	if !entryBrokerOrderID.Valid {
		ebID = brokerOrderID
	}

	return &OrderMeta{
		Found:              true,
		SignalID:           signalID,
		OrderType:          orderType,
		StrategyID:         strategyID.String,
		UserID:             userID,
		EntrySignalID:      esID,
		EntryBrokerOrderID: ebID,
		AvgFillPrice:       avgFillPrice.Float64,   // 0 when NULL
		SLTriggerPrice:     slTriggerPrice.Float64, // 0 when no SL row exists
		SLBrokerOrderID:    slBrokerOrderID.String, // "" when no SL row exists
	}, nil
}

// OrderContext is the minimal per-order snapshot the WSS→Kafka bridge
// needs to build a positions-svc-compatible `order.events` message. Kept
// deliberately narrow (no full ManthanOrder round-trip) — this method
// is on the WSS hot path and runs on every fill.
type OrderContext struct {
	UserID       string
	IndiraSymbol string // wire form: STK_AADHARHFC_EQ_NSE_23729
	Exchange     string
	OrderSide    string    // BUY / SELL
	OrderType    OrderType // LIMIT_BUY / SL_SELL etc.
	Qty          int
}

// GetOrderContext returns the minimal identity/routing snapshot for a
// broker_order_id. Used by the WSS→Kafka fill publisher (wss_bridge) —
// see PublishWSSFill for the payload shape.
//
// Returns (nil, nil) if broker_order_id isn't in manthan_orders — a
// user's manual order that isn't Manthan's concern.
func (r *Repository) GetOrderContext(ctx context.Context, brokerOrderID string) (*OrderContext, error) {
	if brokerOrderID == "" {
		return nil, fmt.Errorf("broker_order_id is required")
	}
	var oc OrderContext
	err := r.db.QueryRowContext(ctx, `
		SELECT user_id, indira_symbol, exchange, order_side, order_type, qty
		  FROM manthan_orders
		 WHERE broker_order_id = $1
		 LIMIT 1`, brokerOrderID,
	).Scan(&oc.UserID, &oc.IndiraSymbol, &oc.Exchange, &oc.OrderSide, &oc.OrderType, &oc.Qty)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("GetOrderContext scan: %w", err)
	}
	return &oc, nil
}

// InsertManualExitLedgerSell records the synthetic FILLED SELL that projects
// a positions-svc-confirmed manual exit (POSITION_EXITED / MANUAL_EXIT) into
// this service's ledger, so ListPositionsNeedingProtection stops arming SLs
// for shares the user already sold (2026-09-03 ghost-churn formation fix).
//
// Idempotent via UNIQUE(signal_id): signal_id is "manualexit-<position_id>",
// stable per closed lot. Returns (false, nil) on replay. Same column set as
// the admin ghost-heal FIX-5 insert — the net-qty CTE only needs order_side,
// status and filled_qty.
func (r *Repository) InsertManualExitLedgerSell(ctx context.Context, e ManualExitLedgerRow) (bool, error) {
	res, err := r.db.ExecContext(ctx, `
		INSERT INTO manthan_orders
			(signal_id, strategy_id, user_id, symbol, order_type, order_side,
			 qty, filled_qty, status, avg_fill_price, exchange, filled_at, last_error)
		VALUES ($1, $2, $3, $4, 'MARKET_SELL', 'SELL', $5, $5, 'FILLED', NULLIF($6, 0), 'NSE', now(),
		        'manual-exit ledger projection: user sold via broker app (event '||$7||', broker order '||COALESCE(NULLIF($8,''),'?')||')')
		ON CONFLICT (signal_id) DO NOTHING`,
		e.SignalID, e.StrategyID, e.UserID, e.Symbol, e.Qty, e.ExitPrice,
		e.SourceEventID, e.BrokerOrderID)
	if err != nil {
		return false, fmt.Errorf("InsertManualExitLedgerSell: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}
