package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	models "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type OrderRepository struct {
	db *gorm.DB
}

func NewOrderRepository(db *gorm.DB) *OrderRepository {
	return &OrderRepository{db: db}
}

// ────────────────────────────────────────────────────────────────────────────
// CRUD
// ────────────────────────────────────────────────────────────────────────────

func (r *OrderRepository) Create(ctx context.Context, order *models.Order) error {
	return r.db.WithContext(ctx).Create(order).Error
}

func (r *OrderRepository) Update(ctx context.Context, order *models.Order) error {
	return r.db.WithContext(ctx).Save(order).Error
}

func (r *OrderRepository) GetByID(ctx context.Context, orderID uuid.UUID) (*models.Order, error) {
	var order models.Order
	err := r.db.WithContext(ctx).
		Preload("Instrument").
		Preload("StopLoss").
		First(&order, "order_id = ?", orderID).Error
	if err != nil {
		return nil, fmt.Errorf("order not found: %s", orderID)
	}
	return &order, nil
}

func (r *OrderRepository) GetBySignalID(ctx context.Context, signalID uuid.UUID) (*models.Order, error) {
	var order models.Order
	err := r.db.WithContext(ctx).First(&order, "signal_id = ?", signalID).Error
	if err != nil {
		return nil, err
	}
	return &order, nil
}

func (r *OrderRepository) ExistsBySignalID(ctx context.Context, signalID uuid.UUID) (bool, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(&models.Order{}).
		Where("signal_id = ?", signalID).Count(&count).Error
	return count > 0, err
}

// ────────────────────────────────────────────────────────────────────────────
// Status Management (always writes to history ledger)
// ────────────────────────────────────────────────────────────────────────────

func (r *OrderRepository) TransitionStatus(ctx context.Context, orderID uuid.UUID, from, to, source, reason string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Update order status
		result := tx.Model(&models.Order{}).
			Where("order_id = ? AND status = ?", orderID, from).
			Updates(map[string]interface{}{
				"status":     to,
				"updated_at": time.Now(),
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("order %s not in expected status %s", orderID, from)
		}

		// Append to history ledger
		history := models.StatusHistory{
			OrderID:    orderID,
			FromStatus: &from,
			ToStatus:   to,
			Source:     source,
			Reason:     &reason,
			CreatedAt:  time.Now(),
		}
		return tx.Create(&history).Error
	})
}

func (r *OrderRepository) TransitionStatusWithBrokerData(ctx context.Context, orderID uuid.UUID, from, to, reason string, brokerData json.RawMessage) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Model(&models.Order{}).
			Where("order_id = ?", orderID).
			Updates(map[string]interface{}{
				"status":     to,
				"updated_at": time.Now(),
			})
		if result.Error != nil {
			return result.Error
		}

		history := models.NewBrokerStatusTransition(orderID, &from, to, reason, brokerData)
		return tx.Create(&history).Error
	})
}

func (r *OrderRepository) SetSubmitted(ctx context.Context, orderID uuid.UUID, brokerOrderID string) error {
	now := time.Now()
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Model(&models.Order{}).
			Where("order_id = ?", orderID).
			Updates(map[string]interface{}{
				"status":       models.StatusSubmitted,
				"submitted_at": now,
			}).Error
		if err != nil {
			return err
		}

		pending := models.StatusPending
		history := models.StatusHistory{
			OrderID:    orderID,
			FromStatus: &pending,
			ToStatus:   models.StatusSubmitted,
			Source:     models.SourceSystem,
			Reason:     strPtr(fmt.Sprintf("Placed via Codifi API, broker_order_id=%s", brokerOrderID)),
			CreatedAt:  now,
		}
		return tx.Create(&history).Error
	})
}

// ────────────────────────────────────────────────────────────────────────────
// Query Methods
// ────────────────────────────────────────────────────────────────────────────

func (r *OrderRepository) GetUserOrders(ctx context.Context, userID string, limit, offset int) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("user_id = ?", userID).
		Order("created_at DESC").
		Limit(limit).Offset(offset).
		Find(&orders).Error
	return orders, err
}

func (r *OrderRepository) GetOrdersByStatus(ctx context.Context, status string, limit int) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("status = ?", status).
		Order("created_at ASC").
		Limit(limit).
		Find(&orders).Error
	return orders, err
}

func (r *OrderRepository) GetOpenLiveOrders(ctx context.Context) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("status IN ? AND product_type = 'INTRADAY' AND trading_mode = 'LIVE'",
			[]string{"FILLED", "PARTIALLY_FILLED", "EXECUTED", "TRADED"}).
		Order("created_at ASC").
		Find(&orders).Error
	return orders, err
}

func (r *OrderRepository) GetLiveOrdersByUser(ctx context.Context, userID string) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND trading_mode = 'LIVE' AND status IN ? AND DATE(created_at AT TIME ZONE 'Asia/Kolkata') = DATE(NOW() AT TIME ZONE 'Asia/Kolkata')",
			userID,
			[]string{"FILLED", "PARTIALLY_FILLED", "EXECUTED", "TRADED", "RECEIVED", "PENDING", "SUBMITTED"}).
		Order("created_at DESC").
		Find(&orders).Error
	return orders, err
}

func (r *OrderRepository) GetPaperOrdersByUser(ctx context.Context, userID string) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND trading_mode = 'PAPER' AND status IN ?",
			userID,
			[]string{"FILLED", "EXECUTED", "TRADED", "RECEIVED", "PENDING", "SUBMITTED"}).
		Order("created_at DESC").
		Find(&orders).Error
	return orders, err
}

func (r *OrderRepository) GetActiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]models.Order, error) {
	var orders []models.Order
	err := r.db.WithContext(ctx).
		Where("strategy_id = ? AND user_id = ? AND status NOT IN ?",
			strategyID, userID,
			[]string{"CANCELLED", "REJECTED", "A.REJECTED", "FAILED"}).
		Order("created_at ASC").
		Find(&orders).Error
	return orders, err
}

func (r *OrderRepository) CancelAllOrdersByStrategy(ctx context.Context, strategyID, userID string) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Get order IDs first
		var orderIDs []uuid.UUID
		err := tx.Model(&models.Order{}).
			Where("strategy_id = ? AND user_id = ? AND status NOT IN ?",
				strategyID, userID,
				[]string{"CANCELLED", "REJECTED", "A.REJECTED", "FAILED"}).
			Pluck("order_id", &orderIDs).Error
		if err != nil {
			return err
		}
		if len(orderIDs) == 0 {
			return nil
		}

		// Bulk update status
		now := time.Now()
		err = tx.Model(&models.Order{}).
			Where("order_id IN ?", orderIDs).
			Updates(map[string]interface{}{
				"status":     models.StatusCancelled,
				"updated_at": now,
			}).Error
		if err != nil {
			return err
		}

		// Create history entries for each
		reason := "Strategy deactivated or deleted"
		histories := make([]models.StatusHistory, len(orderIDs))
		for i, oid := range orderIDs {
			histories[i] = models.StatusHistory{
				OrderID:   oid,
				ToStatus:  models.StatusCancelled,
				Source:    models.SourceStrategy,
				Reason:    &reason,
				CreatedAt: now,
			}
		}
		return tx.Create(&histories).Error
	})
}

func (r *OrderRepository) GetDistinctActiveUserIDs(ctx context.Context) ([]string, error) {
	var userIDs []string
	err := r.db.WithContext(ctx).Model(&models.Order{}).
		Where("trading_mode = 'LIVE' AND status NOT IN ?",
			[]string{"CANCELLED", "REJECTED", "A.REJECTED", "FAILED"}).
		Distinct("user_id").
		Pluck("user_id", &userIDs).Error
	return userIDs, err
}

func (r *OrderRepository) GetByBrokerOrderID(ctx context.Context, brokerOrderID string) (*models.Order, error) {
	// Look up through broker_requests table
	var br models.BrokerRequest
	err := r.db.WithContext(ctx).
		Where("broker_order_id = ?", brokerOrderID).
		Order("created_at DESC").
		First(&br).Error
	if err != nil {
		return nil, fmt.Errorf("no order found for broker_order_id: %s", brokerOrderID)
	}
	return r.GetByID(ctx, br.OrderID)
}

func (r *OrderRepository) GetStatusHistory(ctx context.Context, orderID uuid.UUID) ([]models.StatusHistory, error) {
	var history []models.StatusHistory
	err := r.db.WithContext(ctx).
		Where("order_id = ?", orderID).
		Order("created_at ASC").
		Find(&history).Error
	return history, err
}

func strPtr(s string) *string { return &s }
