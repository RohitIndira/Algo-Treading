package v2

import (
	"context"
	"time"

	models "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type FillRepository struct {
	db *gorm.DB
}

func NewFillRepository(db *gorm.DB) *FillRepository {
	return &FillRepository{db: db}
}

// RecordFill inserts a fill and updates the order's denormalized filled_qty / avg_fill_price.
// Uses ON CONFLICT to handle duplicate WS events (idempotency via broker_order_id + exchange_trade_no).
func (r *FillRepository) RecordFill(ctx context.Context, fill *models.Fill) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Insert fill (skip if duplicate)
		result := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(fill)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil // Duplicate fill, skip
		}

		// Recompute order's denormalized fields from all fills
		return r.refreshOrderFillStats(tx, fill.OrderID)
	})
}

// refreshOrderFillStats recomputes filled_qty and avg_fill_price from the fills table.
func (r *FillRepository) refreshOrderFillStats(tx *gorm.DB, orderID uuid.UUID) error {
	var stats struct {
		TotalQty int32   `gorm:"column:total_qty"`
		AvgPrice float64 `gorm:"column:avg_price"`
	}

	err := tx.Model(&models.Fill{}).
		Select("COALESCE(SUM(fill_qty), 0) AS total_qty, CASE WHEN SUM(fill_qty) > 0 THEN SUM(fill_qty * fill_price) / SUM(fill_qty) ELSE 0 END AS avg_price").
		Where("order_id = ?", orderID).
		Scan(&stats).Error
	if err != nil {
		return err
	}

	updates := map[string]interface{}{
		"filled_qty":     stats.TotalQty,
		"avg_fill_price": stats.AvgPrice,
		"updated_at":     time.Now(),
	}

	return tx.Model(&models.Order{}).Where("order_id = ?", orderID).Updates(updates).Error
}

func (r *FillRepository) GetByOrderID(ctx context.Context, orderID uuid.UUID) ([]models.Fill, error) {
	var fills []models.Fill
	err := r.db.WithContext(ctx).
		Where("order_id = ?", orderID).
		Order("filled_at ASC").
		Find(&fills).Error
	return fills, err
}

func (r *FillRepository) GetAvgFillPrice(ctx context.Context, orderID uuid.UUID) (float64, int32, error) {
	var result struct {
		AvgPrice float64 `gorm:"column:avg_price"`
		TotalQty int32   `gorm:"column:total_qty"`
	}
	err := r.db.WithContext(ctx).Model(&models.Fill{}).
		Select("CASE WHEN SUM(fill_qty) > 0 THEN SUM(fill_qty * fill_price) / SUM(fill_qty) ELSE 0 END AS avg_price, COALESCE(SUM(fill_qty), 0) AS total_qty").
		Where("order_id = ?", orderID).
		Scan(&result).Error
	return result.AvgPrice, result.TotalQty, err
}
