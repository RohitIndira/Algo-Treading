package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

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
	// Live trading
	GetLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	GetClosedLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error)
	UpdateLiveTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error
	CancelAllLiveOrdersByUser(ctx context.Context, userID string) error
	// Strategy-level cancellation (used on deactivate/delete)
	GetActiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error)
	CancelAllOrdersByStrategy(ctx context.Context, strategyID, userID string) error
	// Price monitor: pending STOP_LOSS orders waiting for price trigger
	GetPendingMonitorOrders(ctx context.Context) ([]*models.Order, error)
	// Dashboard stats
	GetDashboardStats(ctx context.Context, userID string, isPaper bool) (*DashboardStats, error)
	// GetDistinctActiveUserIDs returns unique user IDs that have non-terminal live orders.
	// Used on startup to pre-warm the credentials cache.
	GetDistinctActiveUserIDs(ctx context.Context) ([]string, error)
	// OCO (One-Cancels-the-Other) queries
	GetActiveOCOOrders(ctx context.Context) ([]*models.Order, error)
	GetOCOGroupOrders(ctx context.Context, groupID uuid.UUID) ([]*models.Order, error)
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
	query := `
		INSERT INTO orders (
			order_id, user_id, strategy_id, event_id,
			stock_code, exchange, symbol,
			order_type, order_side, quantity, price,
			stop_loss, take_profit, validity, product_type,
			status, risk_approved, risk_score,
			is_paper_trade, trading_mode,
			retry_count, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20,
			$21, $22, $23
		)
	`

	_, err := r.db.ExecContext(ctx, query,
		order.OrderID, order.UserID, order.StrategyID, order.EventID,
		order.StockCode, order.Exchange, order.Symbol,
		order.OrderType, order.OrderSide, order.Quantity, order.Price,
		order.StopLoss, order.TakeProfit, order.Validity, order.ProductType,
		order.Status, order.RiskApproved, order.RiskScore,
		order.IsPaperTrade, order.TradingMode,
		order.RetryCount, order.CreatedAt, order.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("failed to create order: %w", err)
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
func (r *orderRepository) GetOpenOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders
		WHERE status IN ('FILLED', 'PARTIALLY_FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
		AND product_type = 'INTRADAY'
		AND is_square_off_order = false
		AND is_paper_trade = false
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
		AND status IN ('FILLED', 'EXECUTED', 'TRADED')
		AND paper_exit_price IS NULL
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
// Only returns orders that have NOT been exited yet (paper_exit_price IS NULL).
func (r *orderRepository) GetFilledPaperOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = true
		AND status IN ('FILLED', 'EXECUTED', 'TRADED', 'RECEIVED', 'PENDING', 'SUBMITTED')
		AND paper_exit_price IS NULL
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get user paper positions: %w", err)
	}
	return orders, nil
}

// GetLiveOrdersByUser retrieves live (non-paper) orders for a user.
// Includes CANCELLED so exited positions still appear in the Indira positions view.
func (r *orderRepository) GetLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = false
		AND status IN ('FILLED', 'PARTIALLY_FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED', 'RECEIVED', 'PENDING', 'SUBMITTED', 'CANCELLED')
		ORDER BY created_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get user live orders: %w", err)
	}
	return orders, nil
}

// CancelAllLiveOrdersByUser cancels all active live (non-paper) orders for a user
func (r *orderRepository) CancelAllLiveOrdersByUser(ctx context.Context, userID string) error {
	query := `
		UPDATE orders SET
			status = 'CANCELLED',
			rejection_reason = 'Force exit by user',
			updated_at = $1
		WHERE user_id = $2
		AND is_paper_trade = false
		AND status IN ('FILLED', 'PARTIALLY_FILLED', 'RECEIVED', 'PENDING', 'SUBMITTED')
	`
	_, err := r.db.ExecContext(ctx, query, time.Now(), userID)
	if err != nil {
		return fmt.Errorf("failed to cancel live orders for user %s: %w", userID, err)
	}
	return nil
}

// GetClosedPaperOrdersByUser retrieves paper orders that have been exited (paper_exit_price IS NOT NULL)
func (r *orderRepository) GetClosedPaperOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = true
		AND paper_exit_price IS NOT NULL
		ORDER BY updated_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get closed paper orders: %w", err)
	}
	return orders, nil
}

// GetClosedLiveOrdersByUser retrieves live orders that have been force-exited (live_exit_price IS NOT NULL)
func (r *orderRepository) GetClosedLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	orders := make([]*models.Order, 0)
	query := `
		SELECT * FROM orders
		WHERE user_id = $1
		AND is_paper_trade = false
		AND live_exit_price IS NOT NULL
		ORDER BY updated_at DESC
	`
	err := r.db.SelectContext(ctx, &orders, query, userID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get closed live orders: %w", err)
	}
	return orders, nil
}

// UpdateLiveTradeExit marks a live order as force-exited with exit price and PnL
func (r *orderRepository) UpdateLiveTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error {
	query := `
		UPDATE orders SET
			status = 'CANCELLED',
			live_exit_price = $1,
			live_pnl = $2,
			rejection_reason = 'Force exit by user',
			updated_at = $3
		WHERE order_id = $4
	`
	result, err := r.db.ExecContext(ctx, query, exitPrice, pnl, time.Now(), orderID)
	if err != nil {
		return fmt.Errorf("failed to update live trade exit: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("live order not found: %s", orderID)
	}
	return nil
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
				) FILTER (WHERE live_exit_price IS NULL), 0) AS current_invested,
				COALESCE(SUM(live_pnl) FILTER (WHERE live_exit_price IS NOT NULL), 0) AS realized_pnl,
				COUNT(*) FILTER (WHERE live_exit_price IS NULL) AS open_count,
				COUNT(*) FILTER (WHERE live_exit_price IS NOT NULL) AS closed_count
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
					), 0) AS current_invested,
					0::numeric AS realized_pnl,
					COUNT(*) AS open_count,
					0 AS closed_count
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
func (r *orderRepository) CancelAllOrdersByStrategy(ctx context.Context, strategyID, userID string) error {
	query := `
		UPDATE orders SET
			status = 'CANCELLED',
			rejection_reason = 'Strategy deactivated or deleted',
			updated_at = $1
		WHERE strategy_id = $2
		AND user_id = $3
		AND status NOT IN ('CANCELLED', 'REJECTED', 'A.REJECTED')
	`
	_, err := r.db.ExecContext(ctx, query, time.Now(), strategyID, userID)
	if err != nil {
		return fmt.Errorf("failed to cancel orders for strategy %s user %s: %w", strategyID, userID, err)
	}
	return nil
}

// UpdatePaperTradeExit marks a paper order as exited with the given price and PnL
func (r *orderRepository) UpdatePaperTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error {
	query := `
		UPDATE orders SET
			status = 'CANCELLED',
			paper_exit_price = $1,
			paper_pnl = $2,
			updated_at = $3
		WHERE order_id = $4
	`
	result, err := r.db.ExecContext(ctx, query, exitPrice, pnl, time.Now(), orderID)
	if err != nil {
		return fmt.Errorf("failed to update paper trade exit: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("paper order not found: %s", orderID)
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
