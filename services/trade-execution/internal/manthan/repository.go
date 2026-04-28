package manthan

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Repository handles manthan_orders + manthan_order_events persistence.
type Repository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *Repository {
	return &Repository{db: db}
}

// InsertOrder creates a new manthan order record.
func (r *Repository) InsertOrder(ctx context.Context, o *ManthanOrder) (int64, error) {
	var id int64
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO manthan_orders (
			signal_id, strategy_id, user_id, symbol, isin, exchange,
			order_type, order_side, product_type,
			qty, limit_price, trigger_price,
			indira_symbol, exchange_token, status, max_retries
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		RETURNING id`,
		nullStr(o.SignalID), o.StrategyID, o.UserID, o.Symbol, nullStr(o.ISIN), o.Exchange,
		o.OrderType, o.OrderSide, o.ProductType,
		o.Qty, o.LimitPrice, o.TriggerPrice,
		o.IndiraSymbol, o.ExchangeToken, o.Status, o.MaxRetries,
	).Scan(&id)
	return id, err
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

// CheckDuplicate returns true if a signal_id already exists (idempotency).
func (r *Repository) CheckDuplicate(ctx context.Context, signalID string) (bool, error) {
	var count int
	err := r.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM manthan_orders WHERE signal_id=$1`, signalID).Scan(&count)
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
	o := &ManthanOrder{}
	err := row.Scan(
		&o.ID, &o.SignalID, &o.StrategyID, &o.UserID, &o.Symbol, &o.ISIN, &o.Exchange,
		&o.OrderType, &o.OrderSide, &o.ProductType, &o.Qty, &o.FilledQty,
		&o.LimitPrice, &o.TriggerPrice, &o.AvgFillPrice,
		&o.BrokerOrderID, &o.BrokerStatus, &o.IndiraSymbol, &o.ExchangeToken,
		&o.Status, &o.RetryCount, &o.MaxRetries, &o.LastError, &o.ParentOrderID,
		&o.CreatedAt, &o.PlacedAt, &o.FilledAt, &o.CancelledAt, &o.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return o, nil
}

type scannable interface {
	Scan(dest ...any) error
}

func scanOrderRow(row scannable) (*ManthanOrder, error) {
	o := &ManthanOrder{}
	var (
		signalID      sql.NullString
		isin          sql.NullString
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
		&o.LimitPrice, &o.TriggerPrice, &avgFillPrice,
		&brokerOrderID, &brokerStatus, &indiraSymbol, &exchangeToken,
		&o.Status, &o.RetryCount, &o.MaxRetries, &lastError, &o.ParentOrderID,
		&o.CreatedAt, &o.PlacedAt, &o.FilledAt, &o.CancelledAt, &o.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	o.SignalID = signalID.String
	o.ISIN = isin.String
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
