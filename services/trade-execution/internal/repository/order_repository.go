package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// ErrDuplicateOrder is returned by Create when the order_id already exists.
// Callers should treat this as a successful no-op (idempotent replay).
var ErrDuplicateOrder = errors.New("duplicate order")

// DashboardStats holds aggregated trading stats for a user.
type DashboardStats struct {
	Mode             string  `json:"mode"`              // "PAPER" or "LIVE"
	TotalInvested    float64 `json:"total_invested"`    // sum of filled_price * qty for open + closed orders
	CurrentInvested  float64 `json:"current_invested"`  // sum of filled_price * qty for open orders only
	RealizedPnL      float64 `json:"realized_pnl"`      // sum of closed pnl
	OpenCount        int     `json:"open_count"`
	ClosedCount      int     `json:"closed_count"`
}

// OrderRepository defines database operations for orders
type OrderRepository interface {
	Create(ctx context.Context, order *models.Order) error
	Update(ctx context.Context, order *models.Order) error
	ExistsByID(ctx context.Context, orderID uuid.UUID) (bool, error)
	GetByID(ctx context.Context, orderID uuid.UUID) (*models.Order, error)
	GetByOdinOrderID(ctx context.Context, odinOrderID string) (*models.Order, error)
	GetByIndiraOrderID(ctx context.Context, indiraOrderID string) (*models.Order, error)
	GetUserOrders(ctx context.Context, userID string, limit, offset int) ([]*models.Order, error)
	GetOrdersByStatus(ctx context.Context, status models.OrderStatus, limit int) ([]*models.Order, error)
	UpdateStatus(ctx context.Context, orderID uuid.UUID, status models.OrderStatus) error
	RecordExecutionEvent(ctx context.Context, orderID uuid.UUID, eventType string, eventData map[string]interface{}) error
	GetOpenOrders(ctx context.Context) ([]*models.Order, error)
	GetTrailingStopLossOrders(ctx context.Context) ([]*models.Order, error)
	// Paper trading
	GetAllActivePaperOrders(ctx context.Context) ([]*models.Order, error)
	GetFilledPaperOrdersBySymbol(ctx context.Context, symbol string) ([]*models.Order, error)
	GetFilledPaperOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	GetClosedPaperOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	UpdatePaperTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error
	// UpdatePaperExitPrice sets paper_exit_price and paper_pnl without changing order status.
	// Used during strategy deactivation so CancelAllOrdersByStrategy still sets rejection_reason.
	UpdatePaperExitPrice(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error
	// CreatePaperPartialExit inserts a partial exit (square-off) record for an ML level
	// trigger. Unlike Create, it writes filled_quantity, filled_price, paper_exit_price,
	// paper_pnl, is_square_off_order, and indira_order_id which the generic Create omits.
	CreatePaperPartialExit(ctx context.Context, order *models.Order) error
	// UpdatePaperPositionFilledQty reduces the entry order's filled_quantity after a partial
	// ML exit so the open positions view shows the remaining quantity, not the original.
	UpdatePaperPositionFilledQty(ctx context.Context, orderID uuid.UUID, remainingQty int32) error
	// UpdateTrailingSL persists the new trailing stop-loss price to the orders table.
	// Called each time advanceTrailingSL advances the SL so that REST fetches and
	// service-restart recovery both see the latest SL, not the original entry value.
	UpdateTrailingSL(ctx context.Context, orderID uuid.UUID, newSL float64) error
	// Live trading
	GetLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	GetAllTodayLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	// GetOpenLivePositionAttributionOrders returns non-paper orders whose broker
	// position could still be open today — used solely by enrichPositions to map
	// broker positions back to the strategy that placed them. Widens the today-only
	// lookback so positions opened on a prior session aren't orphaned as broker-direct.
	GetOpenLivePositionAttributionOrders(ctx context.Context, userID string) ([]*models.Order, error)
	GetClosedLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	// UpdateLiveTradeExit marks a live order as closed, recording the exact exit
	// price, P&L, exit timestamp, and exit reason (STOP_LOSS, TAKE_PROFIT,
	// FORCE_EXIT, SQUARE_OFF, MANUAL). Idempotent — first writer wins.
	UpdateLiveTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64, exitTime time.Time, reason string) error
	// GetAllActiveLivePositions returns every open live position across all users:
	// filled live entry orders that haven't been exited yet (live_exit_price IS NULL).
	// Used by the LiveOrderMonitor to subscribe market data and broadcast live P&L.
	GetAllActiveLivePositions(ctx context.Context) ([]*models.Order, error)
	// GetOpenLiveEntriesByUserSymbol returns filled live entry orders for one user
	// on one symbol that have not been exited yet, oldest first (FIFO). Used by
	// statusservice to record an exit on the matching entry when an external
	// (manual / broker-side) close event arrives on the order WebSocket.
	GetOpenLiveEntriesByUserSymbol(ctx context.Context, userID, symbol string) ([]*models.Order, error)
	CancelAllLiveOrdersByUser(ctx context.Context, userID string) error
	// Strategy-level cancellation (used on deactivate/delete)
	GetActiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error)
	CancelAllOrdersByStrategy(ctx context.Context, strategyID, userID string) error
	// Strategy-level exit: filled live orders that haven't been exited yet
	GetFilledLiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error)
	// GetExitablePaperOrdersByStrategy returns paper orders for a strategy that were filled
	// (have a filled_price) but haven't been exited yet — including CANCELLED orders
	// (e.g. after strategy deletion) so force-exit still works.
	GetExitablePaperOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error)
	// GetExitableLiveOrdersByStrategy is like GetFilledLiveOrdersByStrategy but also
	// includes CANCELLED orders that had been filled, so force-exit works after deletion.
	GetExitableLiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error)
	// GetExitableLiveOrdersByUser returns today's filled live entry orders that haven't been
	// exited yet (live_exit_price IS NULL). Excludes square-off orders and already-exited
	// orders. Safe to call multiple times — idempotent by design.
	GetExitableLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	// Price monitor: pending STOP_LOSS orders waiting for price trigger
	GetPendingMonitorOrders(ctx context.Context) ([]*models.Order, error)
	// Dashboard stats
	GetDashboardStats(ctx context.Context, userID string, isPaper bool) (*DashboardStats, error)
	// GetDistinctActiveUserIDs returns unique user IDs that have non-terminal live orders.
	// Used on startup to pre-warm the credentials cache.
	GetDistinctActiveUserIDs(ctx context.Context) ([]string, error)
	// GetUsersWithLiveExposure returns user IDs whose broker positions may still be
	// open right now — used on startup to eagerly re-subscribe their broker WS so
	// fill/cancel/reject events that happened during downtime aren't missed.
	// Includes non-terminal in-flight orders AND filled live entries whose
	// live_exit_price is NULL.
	GetUsersWithLiveExposure(ctx context.Context) ([]string, error)
	// GetReconciliationCandidates returns non-paper orders that have an
	// indira_order_id but are still in a non-terminal status — they may have
	// progressed at the broker while we were down. Bounded by maxAgeHours so
	// the reconciliation pass is O(recent orders), not O(history). Orders
	// younger than minAgeSeconds are excluded to avoid racing live placement.
	GetReconciliationCandidates(ctx context.Context, maxAgeHours int, minAgeSeconds int) ([]*models.Order, error)
	// GetActiveMLEntries returns entry orders whose multi-level exit groups
	// are still in progress (at least one level still PENDING or ACTIVE) so
	// the multilevel.Manager can rebuild its in-memory state on startup.
	// Filters: entry is FILLED, not paper, not square-off, live_exit_price IS NULL.
	GetActiveMLEntries(ctx context.Context) ([]*models.Order, error)
	// OCO (One-Cancels-the-Other) queries
	GetActiveOCOOrders(ctx context.Context) ([]*models.Order, error)
	GetOCOGroupOrders(ctx context.Context, groupID uuid.UUID) ([]*models.Order, error)
	// GetStrategyNamesByIDs returns a map of strategy_id → strategy_name from the orders table.
	// Looks across all orders (not just today's) to find names for strategies that may have been deleted.
	GetStrategyNamesByIDs(ctx context.Context, strategyIDs []string) (map[string]string, error)
	// GetUsersWithAutoSquareOffAtTime returns distinct user IDs whose square_off_time
	// matches timeStr ("HH:MM" IST) in user_square_off_config. Used by the scheduler
	// to trigger per-user square-offs at user-configured times.
	GetUsersWithAutoSquareOffAtTime(ctx context.Context, timeStr string) ([]string, error)
	// GetUsersWithExpiredAutoSquareOff returns user IDs whose enabled custom square_off_time
	// is <= beforeTime ("HH:MM" IST). Used on startup to catch-up positions for users whose
	// configured time already passed while the service was down.
	GetUsersWithExpiredAutoSquareOff(ctx context.Context, beforeTime string) ([]string, error)
	// GetOpenOrdersByUser returns FILLED/PARTIALLY_FILLED INTRADAY live orders for a single
	// user today that haven't been square-offed yet. Used for per-user live square-off.
	GetOpenOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	// GetDistinctActiveUsersToday returns the distinct set of user IDs that have at least
	// one live, filled, un-exited order created today through a strategy.
	// Used by position-book square-off to know which users to query the broker for.
	GetDistinctActiveUsersToday(ctx context.Context) ([]string, error)

	// ── Auto square-off config ────────────────────────────────────────────────
	// UpsertUserSquareOffConfig stores or updates the auto square-off config for a user.
	UpsertUserSquareOffConfig(ctx context.Context, userID, squareOffTime string, enabled bool) error
	// GetUserSquareOffConfig retrieves the auto square-off config for a user.
	// Returns ("", false, nil) when no config exists.
	GetUserSquareOffConfig(ctx context.Context, userID string) (squareOffTime string, enabled bool, err error)
	// BackfillTodaySquareOffConfig copies auto_square_off_time from today's orders into
	// user_square_off_config for any user who doesn't already have a row there.
	// Run once on startup so orders placed before the fix was deployed still fire correctly.
	BackfillTodaySquareOffConfig(ctx context.Context) (int, error)

	// ── Multi-level exit level operations ─────────────────────────────────────
	UpsertMultiLevelExitLevel(ctx context.Context, rec *models.MultiLevelExitRecord) error
	UpdateMultiLevelLevelStatus(ctx context.Context, entryOrderID uuid.UUID, exitType string, levelNum int, status string, exitPrice float64) error
	// SupersedeMultiLevelLevels marks every still-pending (PENDING/ACTIVE) level for an
	// entry order as SUPERSEDED — used when the position is fully closed by an external
	// exit (monitor SL/TP, square-off, force-exit) so the un-fired levels are not later
	// mislabelled CANCELLED. Already-terminal (TRIGGERED/CANCELLED) levels are untouched.
	SupersedeMultiLevelLevels(ctx context.Context, entryOrderID uuid.UUID) error
	UpdateMultiLevelLevelBrokerID(ctx context.Context, entryOrderID uuid.UUID, exitType string, levelNum int, brokerOrderID string, exitOrderID uuid.UUID) error
	GetMultiLevelExitLevels(ctx context.Context, entryOrderID uuid.UUID) ([]*models.MultiLevelExitRecord, error)
	// GetMultiLevelExitLevelsBatch fetches ML levels for multiple entry orders in one query.
	// Returns a map of entryOrderID → levels slice.
	GetMultiLevelExitLevelsBatch(ctx context.Context, entryOrderIDs []uuid.UUID) (map[uuid.UUID][]*models.MultiLevelExitRecord, error)
	// UpdateMLLevelRebalancedQty records a qty reduction caused by the opposite side firing.
	// original_exit_qty is written only on the first rebalance (subsequent calls preserve it).
	// rebalance_reason example: "SL_L1_TRIGGERED", "TP_L2_TRIGGERED".
	UpdateMLLevelRebalancedQty(ctx context.Context, entryOrderID uuid.UUID, exitType string, levelNum int, originalQty, newQty int32, reason string) error
	// UpdateOCOTag sets the oco_group_id and oco_role columns on an already-existing order.
	// Used by AdoptOrder to tag an entry order that was placed before OCO adoption.
	// The generic Update() method omits these columns, so a targeted query is required.
	UpdateOCOTag(ctx context.Context, orderID uuid.UUID, groupID uuid.UUID, role string) error
}

type orderRepository struct {
	db *sqlx.DB
}

// NewOrderRepository creates a new order repository
func NewOrderRepository(db *sqlx.DB) OrderRepository {
	return &orderRepository{db: db}
}

// Create inserts a new order into the database
func (r *orderRepository) Create(ctx context.Context, order *models.Order) error {
	// ON CONFLICT DO NOTHING combines the idempotency check and insert into a
	// single roundtrip. If the order_id already exists (duplicate Kafka delivery),
	// 0 rows are affected and ErrDuplicateOrder is returned so the caller can skip.
	query := `
		INSERT INTO orders (
			order_id, user_id, strategy_id, strategy_name, event_id,
			stock_code, exchange, symbol,
			order_type, order_side, quantity, price,
			stop_loss, take_profit, stop_loss_type, trailing_sl_pct, validity, product_type,
			status, risk_approved, risk_score,
			is_paper_trade, trading_mode,
			is_square_off_order,
			retry_count, created_at, updated_at,
			oco_group_id, oco_role, parent_order_id,
			bearer_token, app_id, source
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
			$13, $14, $15, $16, $17, $18, $19, $20, $21,
			$22, $23,
			$24,
			$25, $26, $27,
			$28, $29, $30,
			$31, $32, $33
		) ON CONFLICT (order_id) DO NOTHING
	`

	result, err := r.db.ExecContext(ctx, query,
		order.OrderID, order.UserID, order.StrategyID, order.StrategyName, order.EventID,
		order.StockCode, order.Exchange, order.Symbol,
		order.OrderType, order.OrderSide, order.Quantity, order.Price,
		order.StopLoss, order.TakeProfit, order.StopLossType, order.TrailingSLPct, order.Validity, order.ProductType,
		order.Status, order.RiskApproved, order.RiskScore,
		order.IsPaperTrade, order.TradingMode,
		order.IsSquareOffOrder,
		order.RetryCount, order.CreatedAt, order.UpdatedAt,
		order.OCOGroupID, order.OCORole, order.ParentOrderID,
		order.BearerToken, order.AppId, order.Source,
	)
	if err != nil {
		return fmt.Errorf("failed to create order: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if rows == 0 {
		return ErrDuplicateOrder
	}
	return nil
}

// CreatePaperPartialExit inserts a partial exit (square-off) record produced by an ML
// level trigger. It writes every field that the generic Create omits: filled_quantity,
// filled_price, paper_exit_price, paper_pnl, is_square_off_order, and indira_order_id.
func (r *orderRepository) CreatePaperPartialExit(ctx context.Context, order *models.Order) error {
	query := `
		INSERT INTO orders (
			order_id, user_id, strategy_id, strategy_name, event_id,
			stock_code, exchange, symbol,
			order_type, order_side, quantity, price,
			validity, product_type,
			status, risk_approved,
			is_paper_trade, trading_mode,
			filled_quantity, filled_price,
			paper_exit_price, paper_pnl,
			is_square_off_order, indira_order_id,
			submitted_at, executed_at, created_at, updated_at
		) VALUES (
			$1,  $2,  $3,  $4,  $5,
			$6,  $7,  $8,
			$9,  $10, $11, $12,
			$13, $14,
			$15, $16,
			$17, $18,
			$19, $20,
			$21, $22,
			$23, $24,
			$25, $26, $27, $28
		)
	`
	_, err := r.db.ExecContext(ctx, query,
		order.OrderID, order.UserID, order.StrategyID, order.StrategyName, order.EventID,
		order.StockCode, order.Exchange, order.Symbol,
		order.OrderType, order.OrderSide, order.Quantity, order.Price,
		order.Validity, order.ProductType,
		order.Status, order.RiskApproved,
		order.IsPaperTrade, order.TradingMode,
		order.FilledQuantity, order.FilledPrice,
		order.PaperExitPrice, order.PaperPnL,
		order.IsSquareOffOrder, order.IndiraOrderID,
		order.SubmittedAt, order.ExecutedAt, order.CreatedAt, order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create paper partial exit: %w", err)
	}
	return nil
}

// Update updates an existing order
func (r *orderRepository) Update(ctx context.Context, order *models.Order) error {
	query := `
		UPDATE orders SET
			status = $1, indira_order_id = $2, indira_response = $3,
			filled_quantity = $4, filled_price = $5, commission = $6,
			total_cost = $7, submitted_at = $8, executed_at = $9,
			error_message = $10, rejection_reason = $11, retry_count = $12,
			is_paper_trade = $13, trading_mode = $14,
			broker_status = $15, broker_ws_data = $16, exchange_order_number = $17,
			updated_at = $18
		WHERE order_id = $19
	`

	result, err := r.db.ExecContext(ctx, query,
		order.Status, order.IndiraOrderID, order.IndiraResponse,
		order.FilledQuantity, order.FilledPrice, order.Commission,
		order.TotalCost, order.SubmittedAt, order.ExecutedAt,
		order.ErrorMessage, order.RejectionReason, order.RetryCount,
		order.IsPaperTrade, order.TradingMode,
		order.BrokerStatus, order.BrokerWSData, order.ExchangeOrderNumber,
		time.Now(), order.OrderID,
	)

	if err != nil {
		return fmt.Errorf("failed to update order: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("order not found: %s", order.OrderID)
	}

	return nil
}

// UpdateOCOTag sets oco_group_id and oco_role on an already-existing order.
// Used by AdoptOrder to tag an entry order placed before OCO adoption.
func (r *orderRepository) UpdateOCOTag(ctx context.Context, orderID uuid.UUID, groupID uuid.UUID, role string) error {
	result, err := r.db.ExecContext(ctx,
		`UPDATE orders SET oco_group_id = $1, oco_role = $2, updated_at = $3 WHERE order_id = $4`,
		groupID, role, time.Now(), orderID,
	)
	if err != nil {
		return fmt.Errorf("failed to tag order with OCO group: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("order not found: %s", orderID)
	}
	return nil
}

// ExistsByID checks if an order exists without loading the full row.
func (r *orderRepository) ExistsByID(ctx context.Context, orderID uuid.UUID) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM orders WHERE order_id = $1)`, orderID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check order existence: %w", err)
	}
	return exists, nil
}

// GetByID retrieves an order by ID
func (r *orderRepository) GetByID(ctx context.Context, orderID uuid.UUID) (*models.Order, error) {
	var order models.Order
	query := `SELECT * FROM orders WHERE order_id = $1`

	err := r.db.GetContext(ctx, &order, query, orderID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("order not found: %s", orderID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get order: %w", err)
	}

	return &order, nil
}

// GetByOdinOrderID retrieves an order by Odin order ID
func (r *orderRepository) GetByOdinOrderID(ctx context.Context, odinOrderID string) (*models.Order, error) {
	return r.GetByIndiraOrderID(ctx, odinOrderID)
}

// GetByIndiraOrderID retrieves an order by Indira order ID, Odin order ID, or exchange order number.
func (r *orderRepository) GetByIndiraOrderID(ctx context.Context, indiraOrderID string) (*models.Order, error) {
	var order models.Order
	query := `SELECT * FROM orders WHERE indira_order_id = $1 OR odin_order_id = $1 OR exchange_order_number = $1`

	err := r.db.GetContext(ctx, &order, query, indiraOrderID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("order not found with indira_order_id: %s", indiraOrderID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get order by indira_order_id: %w", err)
	}

	return &order, nil
}

// GetUserOrders retrieves orders for a user with pagination
func (r *orderRepository) GetUserOrders(ctx context.Context, userID string, limit, offset int) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	err := r.db.SelectContext(ctx, &orders, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get user orders: %w", err)
	}

	return orders, nil
}

// GetOrdersByStatus retrieves orders by status
func (r *orderRepository) GetOrdersByStatus(ctx context.Context, status models.OrderStatus, limit int) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2
	`

	err := r.db.SelectContext(ctx, &orders, query, status, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get orders by status: %w", err)
	}

	return orders, nil
}

// UpdateStatus updates the status of an order
func (r *orderRepository) UpdateStatus(ctx context.Context, orderID uuid.UUID, status models.OrderStatus) error {
	query := `UPDATE orders SET status = $1, updated_at = $2 WHERE order_id = $3`

	result, err := r.db.ExecContext(ctx, query, status, time.Now(), orderID)
	if err != nil {
		return fmt.Errorf("failed to update order status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("order not found: %s", orderID)
	}

	return nil
}

// RecordExecutionEvent records an execution event for an order
func (r *orderRepository) RecordExecutionEvent(ctx context.Context, orderID uuid.UUID, eventType string, eventData map[string]interface{}) error {
	query := `
		INSERT INTO execution_events (order_id, event_type, event_data, created_at)
		VALUES ($1, $2, $3, $4)
	`

	eventDataJSON, err := json.Marshal(eventData)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	_, err = r.db.ExecContext(ctx, query, orderID, eventType, eventDataJSON, time.Now())
	if err != nil {
		return fmt.Errorf("failed to record execution event: %w", err)
	}

	return nil
}

// GetOpenOrders retrieves all FILLED or PARTIALLY_FILLED INTRADAY live (non-paper) orders
// that have not already been squared off today. Excludes:
//   - Orders with live_exit_price set (force-exited by user)
//   - Orders for which a square-off order already exists today (prevents double square-off
//     on service restart — the earlier run placed an exit order the DB still marks as open)
func (r *orderRepository) GetOpenOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE status IN ('FILLED', 'PARTIALLY_FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
		AND product_type = 'INTRADAY'
		AND is_square_off_order = false
		AND is_paper_trade = false
		AND strategy_id != ''
		AND live_exit_price IS NULL
		AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		AND NOT EXISTS (
			SELECT 1 FROM orders sq
			WHERE sq.user_id = orders.user_id
			AND sq.symbol = orders.symbol
			AND sq.strategy_id = orders.strategy_id
			AND sq.is_square_off_order = true
			AND sq.is_paper_trade = false
			AND DATE(sq.created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		)
		ORDER BY created_at ASC
	`

	err := r.db.SelectContext(ctx, &orders, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get open orders: %w", err)
	}

	return orders, nil
}

// GetTrailingStopLossOrders retrieves all live (non-paper) orders with trailing stop loss enabled
func (r *orderRepository) GetTrailingStopLossOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE status IN ('FILLED', 'PARTIALLY_FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
		AND stop_loss_type = 'TRAILING'
		AND trailing_sl_pct > 0
		AND is_paper_trade = false
		ORDER BY created_at ASC
	`

	err := r.db.SelectContext(ctx, &orders, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get trailing stop loss orders: %w", err)
	}

	return orders, nil
}

// GetAllActivePaperOrders retrieves all FILLED paper orders (no exit price yet)
func (r *orderRepository) GetAllActivePaperOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE is_paper_trade = true
		AND is_square_off_order = false
		AND status IN ('FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY_FILLED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
		AND paper_exit_price IS NULL
		AND filled_quantity > 0
		ORDER BY created_at ASC
	`
	err := r.db.SelectContext(ctx, &orders, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get active paper orders: %w", err)
	}
	return orders, nil
}

// GetFilledPaperOrdersBySymbol retrieves active paper orders for a given symbol
func (r *orderRepository) GetFilledPaperOrdersBySymbol(ctx context.Context, symbol string) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE is_paper_trade = true
		AND status IN ('FILLED', 'EXECUTED', 'TRADED')
		AND paper_exit_price IS NULL
		AND symbol = $1
		ORDER BY created_at ASC
	`
	err := r.db.SelectContext(ctx, &orders, query, symbol)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get paper orders by symbol: %w", err)
	}
	return orders, nil
}

// GetFilledPaperOrdersByUser retrieves active paper trading positions for a user.
// Only returns FILLED entry orders that have NOT been exited yet (paper_exit_price IS NULL).
// RECEIVED/PENDING/SUBMITTED orders are excluded — they have no fill data and must not
// appear as open positions in the UI.
// Square-off reverse orders (audit trail for exits/partial-exits) are excluded.
func (r *orderRepository) GetFilledPaperOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = true
		AND is_square_off_order = false
		AND status IN ('FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY_FILLED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
		AND filled_quantity > 0
		AND filled_price IS NOT NULL
		AND paper_exit_price IS NULL
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get user paper positions: %w", err)
	}
	return orders, nil
}

// GetLiveOrdersByUser retrieves today's live (non-paper) orders for a user (IST date boundary).
// CANCELLED orders are excluded — they are surfaced separately via GetClosedLiveOrdersByUser.
func (r *orderRepository) GetLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = false
		AND status IN ('FILLED', 'PARTIALLY_FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED', 'RECEIVED', 'PENDING', 'SUBMITTED')
		AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get user live orders: %w", err)
	}
	return orders, nil
}

// GetAllTodayLiveOrdersByUser retrieves ALL of today's live (non-paper) orders regardless of status.
// Used for position enrichment — matching broker positions to algo orders requires seeing
// CANCELLED orders too (orders become CANCELLED after square-off/exit).
func (r *orderRepository) GetAllTodayLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = false
		AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get all today's live orders: %w", err)
	}
	return orders, nil
}

// GetOpenLivePositionAttributionOrders returns non-paper orders that could still
// correspond to an open broker position, so enrichPositions can attribute the
// position to the strategy that placed it. Includes:
//   - any order from the last 7 IST days (covers carry-over positions opened
//     before today that the today-only filter would orphan as broker-direct), and
//   - any older order that has NOT been marked exited (live_exit_price IS NULL)
//     and is not paper, as a long-tail safety net.
//
// Result is read-only and never used for status mutation, so widening the window
// here has no effect on order-state transitions, OCO logic, or the closed-orders
// endpoints — those continue to use GetAllTodayLiveOrdersByUser /
// GetClosedLiveOrdersByUser unchanged.
func (r *orderRepository) GetOpenLivePositionAttributionOrders(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = false
		AND (
			created_at AT TIME ZONE 'Asia/Kolkata'
				>= (DATE(NOW() AT TIME ZONE 'Asia/Kolkata') - INTERVAL '7 days')
			OR live_exit_price IS NULL
		)
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get open live position attribution orders: %w", err)
	}
	return orders, nil
}

// CancelAllLiveOrdersByUser cancels all active live entry orders for a user.
// Excludes square-off orders so freshly placed exit orders are not cancelled.
func (r *orderRepository) CancelAllLiveOrdersByUser(ctx context.Context, userID string) error {
	query := `
		UPDATE orders SET
			status = 'CANCELLED',
			rejection_reason = 'Force exit by user',
			updated_at = $1
		WHERE user_id = $2
		AND is_paper_trade = false
		AND is_square_off_order = false
		AND status IN ('FILLED', 'PARTIALLY_FILLED', 'RECEIVED', 'PENDING', 'SUBMITTED')
	`
	_, err := r.db.ExecContext(ctx, query, time.Now(), userID)
	if err != nil {
		return fmt.Errorf("failed to cancel live orders for user %s: %w", userID, err)
	}
	return nil
}

// GetClosedPaperOrdersByUser retrieves closed paper positions for a user.
// Includes three record types:
//  1. Fully closed entry orders (paper_exit_price IS NOT NULL, is_square_off_order = false)
//  2. Partial exit records (is_square_off_order = true) — each represents one ML level
//     that fired; they have paper_exit_price set by recordPaperPartialExit so
//     the Closed tab can show per-level closes with correct qty and P&L.
//  3. Partially-closed entry orders (paper_exit_price IS NULL but has TRIGGERED ML levels) —
//     included so the frontend always has the entry order's executed_at as the correct
//     entry time anchor for the ML group, even while some levels are still open.
func (r *orderRepository) GetClosedPaperOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = true
		AND (
			paper_exit_price IS NOT NULL
			OR (
				is_square_off_order = false
				AND EXISTS (
					SELECT 1 FROM multi_level_exit_levels ml
					WHERE ml.entry_order_id = order_id
					AND ml.status = 'TRIGGERED'
				)
			)
		)
		ORDER BY updated_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get closed paper orders: %w", err)
	}
	return orders, nil
}

// GetClosedLiveOrdersByUser retrieves today's closed live orders (IST date boundary).
// Covers force-exited orders (live_exit_price IS NOT NULL), CANCELLED, REJECTED, and FAILED orders.
func (r *orderRepository) GetClosedLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = false
		AND (
			live_exit_price IS NOT NULL
			OR status IN ('CANCELLED', 'REJECTED', 'FAILED')
		)
		AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		ORDER BY updated_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get closed live orders: %w", err)
	}
	return orders, nil
}

// UpdateLiveTradeExit marks a live order as closed with the exact exit price, PnL,
// exit timestamp, and exit reason. exitTime should be the broker fill time of the
// closing order when available, so it reflects when the position actually closed.
// Guards on live_exit_price IS NULL so concurrent calls are idempotent — the first
// writer wins (e.g. an OCO SL fill racing the EOD square-off sweep).
func (r *orderRepository) UpdateLiveTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64, exitTime time.Time, reason string) error {
	if exitTime.IsZero() {
		exitTime = time.Now()
	}
	if reason == "" {
		reason = "FORCE_EXIT"
	}
	query := `
		UPDATE orders SET
			status = 'CANCELLED',
			live_exit_price = $1,
			live_pnl = $2,
			live_exit_time = $3,
			live_exit_reason = $4,
			updated_at = $3
		WHERE order_id = $5
		AND live_exit_price IS NULL
	`
	result, err := r.db.ExecContext(ctx, query, exitPrice, pnl, exitTime, reason, orderID)
	if err != nil {
		return fmt.Errorf("failed to update live trade exit: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		// Either already exited (concurrent call) or not found — both are safe no-ops.
		return nil
	}
	return nil
}

// GetAllActiveLivePositions returns every open live position across all users:
// filled live entry orders that have not been exited yet. The LiveOrderMonitor
// loads these on startup to subscribe market data and broadcast live P&L.
//
// Filters mirror GetExitableLiveOrdersByUser but span all users and are not
// date-bounded — a position opened on a prior session is still open until
// live_exit_price is set.
func (r *orderRepository) GetAllActiveLivePositions(ctx context.Context) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE is_paper_trade = false
		AND is_square_off_order = false
		AND filled_price IS NOT NULL
		AND filled_quantity > 0
		AND live_exit_price IS NULL
		AND status IN ('FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY_FILLED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get all active live positions: %w", err)
	}
	return orders, nil
}

// GetPendingMonitorOrders retrieves all PENDING orders with STOP_LOSS type and BRACKET product
// that are waiting for the price monitor to trigger them. Used for restart recovery.
func (r *orderRepository) GetPendingMonitorOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE status IN ('RECEIVED', 'PENDING')
		AND order_type = 'STOP_LOSS'
		AND product_type IN ('BRACKET', 'BRACKET_ORDER', 'BO')
		AND is_paper_trade = false
		AND indira_order_id IS NULL
		ORDER BY created_at ASC
	`
	err := r.db.SelectContext(ctx, &orders, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get pending monitor orders: %w", err)
	}
	return orders, nil
}

// GetDashboardStats returns aggregated stats for a user in the given mode.
func (r *orderRepository) GetDashboardStats(ctx context.Context, userID string, isPaper bool) (*DashboardStats, error) {
	mode := "LIVE"
	if isPaper {
		mode = "PAPER"
	}
	stats := &DashboardStats{Mode: mode}

	if isPaper {
		// Use same status conditions as GetFilledPaperOrdersByUser so counts stay in sync.
		// total_invested = all orders (open + closed); current_invested = open only.
		err := r.db.QueryRowContext(ctx, `
			SELECT
				COALESCE(SUM(
					COALESCE(filled_price, price, 0) *
					COALESCE(NULLIF(filled_quantity, 0), quantity, 0)
				), 0) AS total_invested,
				COALESCE(SUM(
					COALESCE(filled_price, price, 0) *
					COALESCE(NULLIF(filled_quantity, 0), quantity, 0)
				) FILTER (WHERE paper_exit_price IS NULL), 0) AS current_invested,
				COALESCE(SUM(paper_pnl) FILTER (WHERE paper_exit_price IS NOT NULL), 0) AS realized_pnl,
				COUNT(*) FILTER (WHERE paper_exit_price IS NULL) AS open_count,
				COUNT(*) FILTER (WHERE paper_exit_price IS NOT NULL) AS closed_count
			FROM orders
			WHERE user_id = $1
			  AND is_paper_trade = true
			  AND status IN ('FILLED', 'RECEIVED', 'PENDING', 'SUBMITTED', 'CANCELLED')
		`, userID).Scan(&stats.TotalInvested, &stats.CurrentInvested, &stats.RealizedPnL, &stats.OpenCount, &stats.ClosedCount)
		if err != nil {
			return nil, fmt.Errorf("dashboard stats paper: %w", err)
		}
	} else {
		// Live orders: total_invested = all orders (open + closed); current_invested = open only.
		// live_exit_price column is added by migration 005; guard with a try.
		err := r.db.QueryRowContext(ctx, `
			SELECT
				COALESCE(SUM(
					COALESCE(filled_price, price, 0) *
					COALESCE(NULLIF(filled_quantity, 0), quantity, 0)
				), 0) AS total_invested,
				COALESCE(SUM(
					COALESCE(filled_price, price, 0) *
					COALESCE(NULLIF(filled_quantity, 0), quantity, 0)
				) FILTER (WHERE live_exit_price IS NULL AND status != 'CANCELLED'), 0) AS current_invested,
				COALESCE(SUM(live_pnl) FILTER (WHERE live_exit_price IS NOT NULL), 0) AS realized_pnl,
				COUNT(*) FILTER (WHERE live_exit_price IS NULL AND status != 'CANCELLED') AS open_count,
				COUNT(*) FILTER (WHERE live_exit_price IS NOT NULL OR status = 'CANCELLED') AS closed_count
			FROM orders
			WHERE user_id = $1
			  AND is_paper_trade = false
			  AND status IN ('FILLED', 'PARTIALLY_FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED', 'RECEIVED', 'PENDING', 'SUBMITTED', 'CANCELLED')
		`, userID).Scan(&stats.TotalInvested, &stats.CurrentInvested, &stats.RealizedPnL, &stats.OpenCount, &stats.ClosedCount)
		if err != nil {
			// live_exit_price column may not exist if migration 005 hasn't been applied yet;
			// fall back to a simpler query without that column.
			fallbackErr := r.db.QueryRowContext(ctx, `
				SELECT
					COALESCE(SUM(
						COALESCE(filled_price, price, 0) *
						COALESCE(NULLIF(filled_quantity, 0), quantity, 0)
					), 0) AS total_invested,
					COALESCE(SUM(
						COALESCE(filled_price, price, 0) *
						COALESCE(NULLIF(filled_quantity, 0), quantity, 0)
					) FILTER (WHERE status != 'CANCELLED'), 0) AS current_invested,
					0::numeric AS realized_pnl,
					COUNT(*) FILTER (WHERE status != 'CANCELLED') AS open_count,
					COUNT(*) FILTER (WHERE status = 'CANCELLED') AS closed_count
				FROM orders
				WHERE user_id = $1
				  AND is_paper_trade = false
				  AND status IN ('FILLED', 'PARTIALLY_FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED', 'RECEIVED', 'PENDING', 'SUBMITTED', 'CANCELLED')
			`, userID).Scan(&stats.TotalInvested, &stats.CurrentInvested, &stats.RealizedPnL, &stats.OpenCount, &stats.ClosedCount)
			if fallbackErr != nil {
				return nil, fmt.Errorf("dashboard stats live: %w", err)
			}
		}
	}

	return stats, nil
}

// GetActiveOrdersByStrategy returns all non-terminal orders for a given strategy.
func (r *orderRepository) GetActiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE strategy_id = $1
		AND user_id = $2
		AND status NOT IN ('CANCELLED', 'REJECTED', 'A.REJECTED')
		ORDER BY created_at ASC
	`
	err := r.db.SelectContext(ctx, &orders, query, strategyID, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get active orders for strategy %s: %w", strategyID, err)
	}
	return orders, nil
}

// CancelAllOrdersByStrategy cancels all active orders (both paper and live) for a given strategy.
// Used when a strategy is deactivated or deleted to ensure no positions remain open.
// For filled paper orders, paper_exit_price is set to filled_price so they appear in the
// closed positions view (which requires paper_exit_price IS NOT NULL).
// Also cancels any PENDING/ACTIVE multi_level_exit_levels rows for those orders so they
// are not left orphaned in the DB after the in-memory manager group is torn down.
func (r *orderRepository) CancelAllOrdersByStrategy(ctx context.Context, strategyID, userID string) error {
	now := time.Now()

	ordersQuery := `
		UPDATE orders SET
			status = 'CANCELLED',
			rejection_reason = 'Strategy deactivated or deleted',
			paper_exit_price = CASE
				WHEN is_paper_trade = true AND filled_price IS NOT NULL AND paper_exit_price IS NULL
				THEN filled_price
				ELSE paper_exit_price
			END,
			paper_pnl = CASE
				WHEN is_paper_trade = true AND filled_price IS NOT NULL AND paper_pnl IS NULL
				THEN 0
				ELSE paper_pnl
			END,
			paper_exit_time = CASE
				WHEN is_paper_trade = true AND filled_price IS NOT NULL AND paper_exit_time IS NULL
				THEN $1
				ELSE paper_exit_time
			END,
			updated_at = $1
		WHERE strategy_id = $2
		AND user_id = $3
		AND status NOT IN ('CANCELLED', 'REJECTED', 'A.REJECTED')
		-- Never cancel a FILLED live position that is still open: it must stay
		-- FILLED so the square-off path (deactivation step 1.5 or the EOD
		-- AutoSquareOffScheduler) can still find it via GetOpenOrders, close it,
		-- and record the real exit price/P&L. Flipping it to CANCELLED here would
		-- orphan the position — closed in our books but never squared off.
		AND NOT (
			is_paper_trade = false
			AND is_square_off_order = false
			AND filled_quantity > 0
			AND live_exit_price IS NULL
		)
	`
	if _, err := r.db.ExecContext(ctx, ordersQuery, now, strategyID, userID); err != nil {
		return fmt.Errorf("failed to cancel orders for strategy %s user %s: %w", strategyID, userID, err)
	}

	mlQuery := `
		UPDATE multi_level_exit_levels
		SET status     = 'CANCELLED',
		    updated_at = $1
		WHERE entry_order_id IN (
			SELECT order_id FROM orders
			WHERE strategy_id = $2 AND user_id = $3
		)
		AND status IN ('PENDING', 'ACTIVE')
	`
	if _, err := r.db.ExecContext(ctx, mlQuery, now, strategyID, userID); err != nil {
		return fmt.Errorf("failed to cancel ml levels for strategy %s user %s: %w", strategyID, userID, err)
	}

	return nil
}

// GetFilledLiveOrdersByStrategy returns filled live orders for a strategy that haven't been exited yet.
// Used by strategy-level force-exit to place reverse limit orders at the broker.
func (r *orderRepository) GetFilledLiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND strategy_id = $2
		AND is_paper_trade = false
		AND status IN ('FILLED', 'EXECUTED', 'TRADED')
		AND live_exit_price IS NULL
		AND is_square_off_order = false
		AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID, strategyID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get filled live orders for strategy %s: %w", strategyID, err)
	}
	return orders, nil
}

// GetExitablePaperOrdersByStrategy returns paper orders for a strategy that were filled
// but not yet exited — including CANCELLED orders so force-exit works after strategy deletion.
func (r *orderRepository) GetExitablePaperOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND strategy_id = $2
		AND is_paper_trade = true
		AND filled_price IS NOT NULL
		AND paper_exit_price IS NULL
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID, strategyID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get exitable paper orders for strategy %s: %w", strategyID, err)
	}
	return orders, nil
}

// GetExitableLiveOrdersByStrategy returns filled live orders for a strategy that haven't
// been exited — including CANCELLED orders so force-exit works after strategy deletion.
func (r *orderRepository) GetExitableLiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND strategy_id = $2
		AND is_paper_trade = false
		AND filled_price IS NOT NULL
		AND live_exit_price IS NULL
		AND is_square_off_order = false
		AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID, strategyID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get exitable live orders for strategy %s: %w", strategyID, err)
	}
	return orders, nil
}

// GetExitableLiveOrdersByUser returns today's filled live entry orders for a user that
// haven't been exited yet. Excludes square-off orders and already-exited orders so
// force-exit-all is safe to retry and never double-exits a position.
func (r *orderRepository) GetExitableLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = false
		AND is_square_off_order = false
		AND filled_price IS NOT NULL
		AND filled_quantity > 0
		AND live_exit_price IS NULL
		AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get exitable live orders for user %s: %w", userID, err)
	}
	return orders, nil
}

// GetOpenLiveEntriesByUserSymbol returns filled live entry orders for one user
// on one symbol that have not been exited yet, oldest first (FIFO). Used by
// statusservice to record an exit on the matching entry order when an external
// close event (manual broker-terminal sell, broker EOD MIS auto-square-off)
// arrives on the order WebSocket. Symbol is matched case-insensitively.
func (r *orderRepository) GetOpenLiveEntriesByUserSymbol(ctx context.Context, userID, symbol string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = false
		AND is_square_off_order = false
		AND filled_price IS NOT NULL
		AND filled_quantity > 0
		AND live_exit_price IS NULL
		AND UPPER(symbol) = UPPER($2)
		ORDER BY executed_at ASC NULLS LAST, created_at ASC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID, symbol)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get open live entries for user %s symbol %s: %w", userID, symbol, err)
	}
	return orders, nil
}

// UpdatePaperTradeExit marks a paper order as exited with the given price and PnL.
// Covers all exit scenarios: SL hit, TP hit, trailing SL, force exit, auto square-off.
// The WHERE clause guards paper_exit_price IS NULL so that if a position was already
// closed (e.g. SL fired right as auto square-off ran), the first exit wins and the
// exit time is never overwritten by a second call.
func (r *orderRepository) UpdatePaperTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error {
	now := time.Now()
	query := `
		UPDATE orders SET
			status = 'CANCELLED',
			paper_exit_price = $1,
			paper_pnl = $2,
			paper_exit_time = $3,
			updated_at = $4
		WHERE order_id = $5
		AND paper_exit_price IS NULL
	`
	result, err := r.db.ExecContext(ctx, query, exitPrice, pnl, now, now, orderID)
	if err != nil {
		return fmt.Errorf("failed to update paper trade exit: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		// Either the order does not exist or it was already exited — both are safe no-ops.
		return nil
	}
	return nil
}

// UpdatePaperExitPrice sets paper_exit_price, paper_pnl, and paper_exit_time WITHOUT changing status.
// Used during strategy deactivation/deletion so that CancelAllOrdersByStrategy can still
// set rejection_reason and status=CANCELLED in the same bulk pass.
func (r *orderRepository) UpdatePaperExitPrice(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error {
	now := time.Now()
	query := `
		UPDATE orders SET
			paper_exit_price = $1,
			paper_pnl = $2,
			paper_exit_time = $3,
			updated_at = $4
		WHERE order_id = $5
		AND is_paper_trade = true
		AND paper_exit_price IS NULL
	`
	_, err := r.db.ExecContext(ctx, query, exitPrice, pnl, now, now, orderID)
	if err != nil {
		return fmt.Errorf("failed to update paper exit price for order %s: %w", orderID, err)
	}
	return nil
}

// UpdatePaperPositionFilledQty reduces the entry order's filled_quantity after a partial
// ML exit so the open positions view shows remaining quantity instead of the original.
func (r *orderRepository) UpdatePaperPositionFilledQty(ctx context.Context, orderID uuid.UUID, remainingQty int32) error {
	query := `
		UPDATE orders SET
			filled_quantity = $1,
			updated_at = $2
		WHERE order_id = $3
		AND is_paper_trade = true
		AND is_square_off_order = false
	`
	result, err := r.db.ExecContext(ctx, query, remainingQty, time.Now(), orderID)
	if err != nil {
		return fmt.Errorf("failed to update paper position filled_quantity: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("paper entry order not found: %s", orderID)
	}
	return nil
}

// UpdateTrailingSL persists the advanced trailing stop-loss price to the orders table.
func (r *orderRepository) UpdateTrailingSL(ctx context.Context, orderID uuid.UUID, newSL float64) error {
	query := `
		UPDATE orders SET
			stop_loss = $1,
			updated_at = $2
		WHERE order_id = $3
	`
	_, err := r.db.ExecContext(ctx, query, newSL, time.Now(), orderID)
	if err != nil {
		return fmt.Errorf("failed to update trailing SL: %w", err)
	}
	return nil
}

// GetDistinctActiveUserIDs returns unique user IDs that have non-terminal live orders.
func (r *orderRepository) GetDistinctActiveUserIDs(ctx context.Context) ([]string, error) {
	query := `
		SELECT DISTINCT user_id FROM orders
		WHERE is_paper_trade = false
		AND status NOT IN ('CANCELLED', 'REJECTED', 'A.REJECTED', 'FAILED')
	`
	var userIDs []string
	if err := r.db.SelectContext(ctx, &userIDs, query); err != nil {
		return nil, fmt.Errorf("failed to get distinct active user IDs: %w", err)
	}
	return userIDs, nil
}

// GetActiveMLEntries returns FILLED non-paper entry orders that still have at
// least one PENDING or ACTIVE multi-level exit level. Used by the multilevel
// manager on startup to reconstruct in-memory group state so price ticks and
// broker WS events continue to route correctly after a restart. The query is
// intentionally restrictive — it does not return groups whose entry was
// force-exited or whose levels have all triggered/cancelled.
func (r *orderRepository) GetActiveMLEntries(ctx context.Context) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT o.* FROM orders o
		WHERE o.is_paper_trade = false
		AND o.is_square_off_order = false
		AND o.status IN ('FILLED', 'EXECUTED', 'TRADED')
		AND o.live_exit_price IS NULL
		AND EXISTS (
			SELECT 1 FROM multi_level_exit_levels ml
			WHERE ml.entry_order_id = o.order_id
			AND ml.status IN ('PENDING', 'ACTIVE')
		)
		ORDER BY o.created_at DESC
	`
	if err := r.db.SelectContext(ctx, &orders, query); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get active ML entries: %w", err)
	}
	return orders, nil
}

// GetReconciliationCandidates returns non-paper orders that are still in a
// non-terminal status but have an indira_order_id, so a reconciliation pass can
// fetch their authoritative state from the broker. The pass is bounded:
//   - by maxAgeHours so the query is cheap and only covers recent activity, and
//   - excludes orders younger than minAgeSeconds so the normal placement →
//     broker-WS-update path is not raced.
func (r *orderRepository) GetReconciliationCandidates(ctx context.Context, maxAgeHours int, minAgeSeconds int) ([]*models.Order, error) {
	if maxAgeHours <= 0 {
		maxAgeHours = 24
	}
	if minAgeSeconds <= 0 {
		minAgeSeconds = 30
	}
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE is_paper_trade = false
		AND indira_order_id IS NOT NULL
		AND indira_order_id <> ''
		AND status IN ('RECEIVED', 'PENDING', 'SUBMITTED', 'PARTIALLY_FILLED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
		AND created_at >= NOW() - ($1::int * INTERVAL '1 hour')
		AND updated_at <= NOW() - ($2::int * INTERVAL '1 second')
		ORDER BY created_at DESC
	`
	if err := r.db.SelectContext(ctx, &orders, query, maxAgeHours, minAgeSeconds); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get reconciliation candidates: %w", err)
	}
	return orders, nil
}

// GetUsersWithLiveExposure returns user IDs whose broker positions may still be
// open right now. Used on startup to eagerly re-subscribe their broker WS.
// A user qualifies if any of:
//   - they have a non-terminal live order in flight (still awaiting fill/cancel), or
//   - they have a filled live entry order whose position has not been exited
//     (live_exit_price IS NULL, not a square-off order).
// The 7-day cap bounds the query — positions older than that are stale and
// should be reconciled manually, not via auto-resubscribe.
func (r *orderRepository) GetUsersWithLiveExposure(ctx context.Context) ([]string, error) {
	query := `
		SELECT DISTINCT user_id FROM orders
		WHERE is_paper_trade = false
		AND created_at AT TIME ZONE 'Asia/Kolkata'
			>= (DATE(NOW() AT TIME ZONE 'Asia/Kolkata') - INTERVAL '7 days')
		AND (
			status IN ('RECEIVED', 'PENDING', 'SUBMITTED', 'PARTIALLY_FILLED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
			OR (
				status IN ('FILLED', 'EXECUTED', 'TRADED')
				AND live_exit_price IS NULL
				AND is_square_off_order = false
			)
		)
	`
	var userIDs []string
	if err := r.db.SelectContext(ctx, &userIDs, query); err != nil {
		return nil, fmt.Errorf("failed to get users with live exposure: %w", err)
	}
	return userIDs, nil
}

// ════════════════════════════════════════════════════════════════════════════
// OCO (One-Cancels-the-Other) queries
// ════════════════════════════════════════════════════════════════════════════

// GetActiveOCOOrders returns all non-terminal orders that belong to an OCO group.
// Used on startup to reconstruct in-memory OCO state.
func (r *orderRepository) GetActiveOCOOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE oco_group_id IS NOT NULL
		AND status NOT IN ('CANCELLED', 'REJECTED', 'A.REJECTED', 'FAILED')
		AND is_paper_trade = false
		ORDER BY created_at ASC
	`
	err := r.db.SelectContext(ctx, &orders, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get active OCO orders: %w", err)
	}
	return orders, nil
}

// GetOCOGroupOrders returns all orders belonging to a specific OCO group.
func (r *orderRepository) GetOCOGroupOrders(ctx context.Context, groupID uuid.UUID) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE oco_group_id = $1
		ORDER BY created_at ASC
	`
	err := r.db.SelectContext(ctx, &orders, query, groupID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get OCO group orders: %w", err)
	}
	return orders, nil
}

// GetStrategyNamesByIDs returns a map of strategy_id → strategy_name from the orders table.
// Scans all orders (not just today's) so deleted strategies still resolve to a name.
func (r *orderRepository) GetStrategyNamesByIDs(ctx context.Context, strategyIDs []string) (map[string]string, error) {
	result := make(map[string]string, len(strategyIDs))
	if len(strategyIDs) == 0 {
		return result, nil
	}

	query := `
		SELECT DISTINCT ON (strategy_id) strategy_id, strategy_name
		FROM orders
		WHERE strategy_id = ANY($1)
		AND strategy_name != ''
		ORDER BY strategy_id, created_at DESC
	`
	type row struct {
		StrategyID   string `db:"strategy_id"`
		StrategyName string `db:"strategy_name"`
	}
	var rows []row
	err := r.db.SelectContext(ctx, &rows, query, pq.Array(strategyIDs))
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get strategy names: %w", err)
	}
	for _, r := range rows {
		result[r.StrategyID] = r.StrategyName
	}
	return result, nil
}

// ════════════════════════════════════════════════════════════════════════════
// Multi-level exit level operations
// ════════════════════════════════════════════════════════════════════════════

// UpsertMultiLevelExitLevel inserts a new level row or updates it if (entry_order_id, exit_type, level_num) exists.
// On INSERT: original_exit_qty is set equal to exit_qty (first-computed value, never overwritten).
// On UPDATE: original_exit_qty is preserved via COALESCE so it always reflects the original.
func (r *orderRepository) UpsertMultiLevelExitLevel(ctx context.Context, rec *models.MultiLevelExitRecord) error {
	query := `
		INSERT INTO multi_level_exit_levels
			(entry_order_id, exit_type, level_num, price_pct, qty_pct,
			 trigger_price, exit_qty, original_exit_qty, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8)
		ON CONFLICT (entry_order_id, exit_type, level_num) DO UPDATE
		SET trigger_price     = EXCLUDED.trigger_price,
		    exit_qty          = EXCLUDED.exit_qty,
		    original_exit_qty = COALESCE(multi_level_exit_levels.original_exit_qty, EXCLUDED.exit_qty),
		    status            = EXCLUDED.status,
		    updated_at        = NOW()
	`
	_, err := r.db.ExecContext(ctx, query,
		rec.EntryOrderID, rec.ExitType, rec.LevelNum,
		rec.PricePct, rec.QtyPct, rec.TriggerPrice, rec.ExitQty, rec.Status,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert multi-level exit level: %w", err)
	}
	return nil
}

// UpdateMultiLevelLevelStatus marks a level as TRIGGERED or CANCELLED.
func (r *orderRepository) UpdateMultiLevelLevelStatus(
	ctx context.Context,
	entryOrderID uuid.UUID,
	exitType string,
	levelNum int,
	status string,
	exitPrice float64,
) error {
	var triggeredAt *time.Time
	var exitPricePtr *float64
	if status == models.MLStatusTriggered {
		now := time.Now()
		triggeredAt = &now
		exitPricePtr = &exitPrice
	}
	query := `
		UPDATE multi_level_exit_levels
		SET status       = $1,
		    triggered_at = $2,
		    exit_price   = $3,
		    updated_at   = NOW()
		WHERE entry_order_id = $4 AND exit_type = $5 AND level_num = $6
	`
	_, err := r.db.ExecContext(ctx, query,
		status, triggeredAt, exitPricePtr, entryOrderID, exitType, levelNum)
	if err != nil {
		return fmt.Errorf("failed to update multi-level level status: %w", err)
	}
	return nil
}

// SupersedeMultiLevelLevels marks all still-pending (PENDING/ACTIVE) levels for an
// entry order as SUPERSEDED. Called when the position is fully closed by an external
// exit path so the un-fired ladder levels are not later mislabelled as CANCELLED.
// Already-terminal levels (TRIGGERED/CANCELLED/SUPERSEDED) are left unchanged.
func (r *orderRepository) SupersedeMultiLevelLevels(ctx context.Context, entryOrderID uuid.UUID) error {
	query := `
		UPDATE multi_level_exit_levels
		SET status     = $1,
		    updated_at = NOW()
		WHERE entry_order_id = $2 AND status IN ($3, $4)
	`
	_, err := r.db.ExecContext(ctx, query,
		models.MLStatusSuperseded, entryOrderID, models.MLStatusPending, models.MLStatusActive)
	if err != nil {
		return fmt.Errorf("failed to supersede multi-level levels: %w", err)
	}
	return nil
}

// UpdateMultiLevelLevelBrokerID stores the broker order ID for a live TP level.
func (r *orderRepository) UpdateMultiLevelLevelBrokerID(
	ctx context.Context,
	entryOrderID uuid.UUID,
	exitType string,
	levelNum int,
	brokerOrderID string,
	exitOrderID uuid.UUID,
) error {
	query := `
		UPDATE multi_level_exit_levels
		SET broker_order_id = $1,
		    exit_order_id   = $2,
		    status          = 'ACTIVE',
		    updated_at      = NOW()
		WHERE entry_order_id = $3 AND exit_type = $4 AND level_num = $5
	`
	_, err := r.db.ExecContext(ctx, query, brokerOrderID, exitOrderID, entryOrderID, exitType, levelNum)
	if err != nil {
		return fmt.Errorf("failed to update multi-level broker ID: %w", err)
	}
	return nil
}

// UpdateMLLevelRebalancedQty records a qty reduction caused by the opposite side firing.
// original_exit_qty is written only on the first rebalance (i.e. when currently NULL)
// so subsequent rebalances don't overwrite the original first-computed qty.
func (r *orderRepository) UpdateMLLevelRebalancedQty(
	ctx context.Context,
	entryOrderID uuid.UUID,
	exitType string,
	levelNum int,
	originalQty, newQty int32,
	reason string,
) error {
	query := `
		UPDATE multi_level_exit_levels
		SET exit_qty          = $1,
		    original_exit_qty = COALESCE(original_exit_qty, $2),
		    rebalanced_at     = NOW(),
		    rebalance_reason  = $3,
		    updated_at        = NOW()
		WHERE entry_order_id = $4 AND exit_type = $5 AND level_num = $6
	`
	_, err := r.db.ExecContext(ctx, query,
		newQty, originalQty, reason, entryOrderID, exitType, levelNum)
	if err != nil {
		return fmt.Errorf("failed to update ml level rebalanced qty: %w", err)
	}
	return nil
}

// GetMultiLevelExitLevels returns all level rows for a given entry order.
func (r *orderRepository) GetMultiLevelExitLevels(ctx context.Context, entryOrderID uuid.UUID) ([]*models.MultiLevelExitRecord, error) {
	var recs []*models.MultiLevelExitRecord
	query := `
		SELECT id, entry_order_id, exit_type, level_num, price_pct, qty_pct,
		       trigger_price, exit_qty, status, exit_order_id, broker_order_id,
		       triggered_at, exit_price, created_at, updated_at
		FROM multi_level_exit_levels
		WHERE entry_order_id = $1
		ORDER BY exit_type, level_num
	`
	err := r.db.SelectContext(ctx, &recs, query, entryOrderID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get multi-level exit levels: %w", err)
	}
	return recs, nil
}

// GetMultiLevelExitLevelsBatch fetches ML levels for multiple entry orders in a single query.
// Returns a map of entryOrderID → levels slice.
func (r *orderRepository) GetMultiLevelExitLevelsBatch(ctx context.Context, entryOrderIDs []uuid.UUID) (map[uuid.UUID][]*models.MultiLevelExitRecord, error) {
	result := make(map[uuid.UUID][]*models.MultiLevelExitRecord, len(entryOrderIDs))
	if len(entryOrderIDs) == 0 {
		return result, nil
	}

	// Build $1,$2,... placeholders
	placeholders := make([]string, len(entryOrderIDs))
	args := make([]interface{}, len(entryOrderIDs))
	for i, id := range entryOrderIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(`
		SELECT id, entry_order_id, exit_type, level_num, price_pct, qty_pct,
		       trigger_price, exit_qty, status, exit_order_id, broker_order_id,
		       triggered_at, exit_price, created_at, updated_at
		FROM multi_level_exit_levels
		WHERE entry_order_id IN (%s)
		ORDER BY entry_order_id, exit_type, level_num
	`, strings.Join(placeholders, ","))

	var recs []*models.MultiLevelExitRecord
	if err := r.db.SelectContext(ctx, &recs, query, args...); err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to batch-get multi-level exit levels: %w", err)
	}

	for _, rec := range recs {
		result[rec.EntryOrderID] = append(result[rec.EntryOrderID], rec)
	}
	return result, nil
}

// ════════════════════════════════════════════════════════════════════════════
// Per-user auto square-off queries
// ════════════════════════════════════════════════════════════════════════════

// GetUsersWithAutoSquareOffAtTime returns user IDs whose enabled square_off_time
// in user_square_off_config matches timeStr ("HH:MM" IST).
func (r *orderRepository) GetUsersWithAutoSquareOffAtTime(ctx context.Context, timeStr string) ([]string, error) {
	query := `
		SELECT user_id FROM user_square_off_config
		WHERE square_off_time = $1
		AND enabled = true
	`
	var userIDs []string
	if err := r.db.SelectContext(ctx, &userIDs, query, timeStr); err != nil && err != sql.ErrNoRows {
		// If the table doesn't exist yet (migration 016 not applied), return empty
		// instead of spamming error logs every minute. Run migration 016 to enable
		// per-user auto square-off config.
		if isTableNotFoundErr(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get users with auto sq-off at %s: %w", timeStr, err)
	}
	return userIDs, nil
}

// isTableNotFoundErr returns true when err is a PostgreSQL "relation does not exist" error.
func isTableNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	// pq error code 42P01 = undefined_table
	if pqErr, ok := err.(*pq.Error); ok {
		return pqErr.Code == "42P01"
	}
	return strings.Contains(err.Error(), "does not exist")
}

// GetUsersWithExpiredAutoSquareOff returns user IDs whose enabled custom square_off_time
// is <= beforeTime ("HH:MM" IST). Used on startup to catch-up missed per-user square-offs.
func (r *orderRepository) GetUsersWithExpiredAutoSquareOff(ctx context.Context, beforeTime string) ([]string, error) {
	query := `
		SELECT user_id FROM user_square_off_config
		WHERE enabled = true
		AND square_off_time <= $1
	`
	var userIDs []string
	if err := r.db.SelectContext(ctx, &userIDs, query, beforeTime); err != nil && err != sql.ErrNoRows {
		if isTableNotFoundErr(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get users with expired sq-off before %s: %w", beforeTime, err)
	}
	return userIDs, nil
}

// GetOpenOrdersByUser returns FILLED/PARTIALLY_FILLED INTRADAY live orders for a single
// user placed today that have not been square-offed yet. Used for per-user live square-off.
func (r *orderRepository) GetOpenOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND status IN ('FILLED', 'PARTIALLY_FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
		AND product_type = 'INTRADAY'
		AND is_square_off_order = false
		AND is_paper_trade = false
		AND strategy_id != ''
		AND live_exit_price IS NULL
		AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		AND NOT EXISTS (
			SELECT 1 FROM orders sq
			WHERE sq.user_id = orders.user_id
			AND sq.symbol = orders.symbol
			AND sq.strategy_id = orders.strategy_id
			AND sq.is_square_off_order = true
			AND sq.is_paper_trade = false
			AND DATE(sq.created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		)
		ORDER BY created_at ASC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get open orders for user %s: %w", userID, err)
	}
	return orders, nil
}

// GetDistinctActiveUsersToday returns the distinct set of user IDs that have
// at least one live, filled, un-exited order placed today through a strategy.
// Intentionally broad — no NOT EXISTS, no product_type filter — so that
// position-book square-off can ask the broker for every user who may have an
// open intraday position, regardless of how their bracket/exit legs are stored.
func (r *orderRepository) GetDistinctActiveUsersToday(ctx context.Context) ([]string, error) {
	var userIDs []string
	query := `
		SELECT DISTINCT user_id
		FROM orders
		WHERE is_paper_trade       = false
		AND   is_square_off_order  = false
		AND   filled_quantity      > 0
		AND   live_exit_price      IS NULL
		AND   DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		ORDER BY user_id
	`
	err := r.db.SelectContext(ctx, &userIDs, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get distinct active users today: %w", err)
	}
	return userIDs, nil
}

// ════════════════════════════════════════════════════════════════════════════
// Auto square-off config
// ════════════════════════════════════════════════════════════════════════════

// UpsertUserSquareOffConfig stores or updates the auto square-off config for a user.
func (r *orderRepository) UpsertUserSquareOffConfig(ctx context.Context, userID, squareOffTime string, enabled bool) error {
	query := `
		INSERT INTO user_square_off_config (user_id, square_off_time, enabled, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (user_id) DO UPDATE
		SET square_off_time = EXCLUDED.square_off_time,
		    enabled         = EXCLUDED.enabled,
		    updated_at      = NOW()
	`
	if _, err := r.db.ExecContext(ctx, query, userID, squareOffTime, enabled); err != nil {
		return fmt.Errorf("failed to upsert square-off config for user %s: %w", userID, err)
	}
	return nil
}

// GetUserSquareOffConfig retrieves the auto square-off config for a user.
// Returns ("", false, nil) when no config row exists.
func (r *orderRepository) GetUserSquareOffConfig(ctx context.Context, userID string) (string, bool, error) {
	var squareOffTime string
	var enabled bool
	err := r.db.QueryRowContext(ctx, `
		SELECT square_off_time, enabled
		FROM user_square_off_config
		WHERE user_id = $1
	`, userID).Scan(&squareOffTime, &enabled)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("failed to get square-off config for user %s: %w", userID, err)
	}
	return squareOffTime, enabled, nil
}

// BackfillTodaySquareOffConfig copies auto_square_off_time from today's orders into
// user_square_off_config for any user who doesn't already have a row there.
// Only considers orders placed today (IST) with a non-empty auto_square_off_time.
// Safe to call on every startup — ON CONFLICT skips users already configured.
func (r *orderRepository) BackfillTodaySquareOffConfig(ctx context.Context) (int, error) {
	query := `
		INSERT INTO user_square_off_config (user_id, square_off_time, enabled, updated_at)
		SELECT DISTINCT ON (user_id)
			user_id,
			auto_square_off_time,
			true,
			NOW()
		FROM orders
		WHERE auto_square_off_time IS NOT NULL
		  AND auto_square_off_time != ''
		  AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')
		ORDER BY user_id, created_at DESC
		ON CONFLICT (user_id) DO NOTHING
	`
	result, err := r.db.ExecContext(ctx, query)
	if err != nil {
		if isTableNotFoundErr(err) {
			return 0, nil // migration 016 not applied yet — skip silently
		}
		return 0, fmt.Errorf("failed to backfill square-off config: %w", err)
	}
	n, _ := result.RowsAffected()
	return int(n), nil
}
