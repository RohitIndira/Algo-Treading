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

// OrderRepository defines database operations for orders
type OrderRepository interface {
	Create(ctx context.Context, order *models.Order) error
	Update(ctx context.Context, order *models.Order) error
	GetByID(ctx context.Context, orderID uuid.UUID) (*models.Order, error)
	GetByIndiraOrderID(ctx context.Context, indiraOrderID string) (*models.Order, error)
	GetUserOrders(ctx context.Context, userID string, limit, offset int) ([]*models.Order, error)
	GetOrdersByStatus(ctx context.Context, status models.OrderStatus, limit int) ([]*models.Order, error)
	UpdateStatus(ctx context.Context, orderID uuid.UUID, status models.OrderStatus) error
	RecordExecutionEvent(ctx context.Context, orderID uuid.UUID, eventType string, eventData map[string]interface{}) error
	GetOpenOrders(ctx context.Context) ([]*models.Order, error)
	GetTrailingStopLossOrders(ctx context.Context) ([]*models.Order, error)
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
			bearer_token, app_id, source,
			stop_loss_type, trailing_sl_pct, highest_price, target_price,
			is_square_off_order,
			retry_count, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
			$22, $23, $24, $25, $26, $27, $28, $29
		)
	`

	_, err := r.db.ExecContext(ctx, query,
		order.OrderID, order.UserID, order.StrategyID, order.EventID,
		order.StockCode, order.Exchange, order.Symbol,
		order.OrderType, order.OrderSide, order.Quantity, order.Price,
		order.StopLoss, order.TakeProfit, order.Validity, order.ProductType,
		order.Status, order.RiskApproved, order.RiskScore,
		order.BearerToken, order.AppId, order.Source,
		order.StopLossType, order.TrailingSLPct, order.HighestPrice, order.TargetPrice,
		order.IsSquareOffOrder,
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
			updated_at = $13
		WHERE order_id = $14
	`

	result, err := r.db.ExecContext(ctx, query,
		order.Status, order.IndiraOrderID, order.IndiraResponse,
		order.FilledQuantity, order.FilledPrice, order.Commission,
		order.TotalCost, order.SubmittedAt, order.ExecutedAt,
		order.ErrorMessage, order.RejectionReason, order.RetryCount,
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

// GetByIndiraOrderID retrieves an order by Indira order ID
func (r *orderRepository) GetByIndiraOrderID(ctx context.Context, indiraOrderID string) (*models.Order, error) {
	var order models.Order
	query := `SELECT * FROM orders WHERE indira_order_id = $1`

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

// GetOpenOrders retrieves all FILLED or PARTIALLY_FILLED INTRADAY orders
func (r *orderRepository) GetOpenOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders 
		WHERE status IN ('FILLED', 'PARTIALLY_FILLED')
		AND product_type = 'INTRADAY'
		AND is_square_off_order = false
		ORDER BY created_at ASC
	`

	err := r.db.SelectContext(ctx, &orders, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get open orders: %w", err)
	}

	return orders, nil
}

// GetTrailingStopLossOrders retrieves all orders with trailing stop loss enabled
func (r *orderRepository) GetTrailingStopLossOrders(ctx context.Context) ([]*models.Order, error) {
	var orders []*models.Order
	query := `
		SELECT * FROM orders 
		WHERE status IN ('FILLED', 'PARTIALLY_FILLED')
		AND stop_loss_type = 'TRAILING'
		AND trailing_sl_pct > 0
		ORDER BY created_at ASC
	`

	err := r.db.SelectContext(ctx, &orders, query)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get trailing stop loss orders: %w", err)
	}

	return orders, nil
}
