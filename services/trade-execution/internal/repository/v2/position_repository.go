package v2

import (
	"context"
	"fmt"
	"time"

	models "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type PositionRepository struct {
	db *gorm.DB
}

func NewPositionRepository(db *gorm.DB) *PositionRepository {
	return &PositionRepository{db: db}
}

// ────────────────────────────────────────────────────────────────────────────
// Open / Close
// ────────────────────────────────────────────────────────────────────────────

// OpenPosition creates a new OPEN position and links entry fills.
func (r *PositionRepository) OpenPosition(ctx context.Context, pos *models.Position, entryFillIDs []uuid.UUID, orderID uuid.UUID) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pos.Status = models.PositionOpen
		pos.OpenedAt = time.Now()
		if err := tx.Create(pos).Error; err != nil {
			return err
		}

		for _, fid := range entryFillIDs {
			pf := models.PositionFill{
				PositionID: pos.PositionID,
				FillID:     fid,
				OrderID:    orderID,
				Direction:  "ENTRY",
			}
			if err := tx.Create(&pf).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// ClosePosition marks a position as closed and links exit fills.
func (r *PositionRepository) ClosePosition(ctx context.Context, positionID uuid.UUID, exitFillIDs []uuid.UUID, exitOrderID uuid.UUID, exitReason string, exitPrice, pnl float64) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now()
		err := tx.Model(&models.Position{}).
			Where("position_id = ? AND status = 'OPEN'", positionID).
			Updates(map[string]interface{}{
				"status":         models.PositionClosed,
				"open_qty":       0,
				"avg_exit_price": exitPrice,
				"realized_pnl":   pnl,
				"net_pnl":        pnl, // TODO: subtract total_charges
				"exit_reason":    exitReason,
				"closed_at":      now,
				"updated_at":     now,
			}).Error
		if err != nil {
			return err
		}

		for _, fid := range exitFillIDs {
			pf := models.PositionFill{
				PositionID: positionID,
				FillID:     fid,
				OrderID:    exitOrderID,
				Direction:  "EXIT",
			}
			if err := tx.Create(&pf).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

// ────────────────────────────────────────────────────────────────────────────
// Queries
// ────────────────────────────────────────────────────────────────────────────

func (r *PositionRepository) GetByID(ctx context.Context, id uuid.UUID) (*models.Position, error) {
	var pos models.Position
	err := r.db.WithContext(ctx).
		Preload("Instrument").
		Preload("PositionFills.Fill").
		First(&pos, "position_id = ?", id).Error
	if err != nil {
		return nil, fmt.Errorf("position not found: %s", id)
	}
	return &pos, nil
}

func (r *PositionRepository) GetOpenByUser(ctx context.Context, userID, tradingMode string) ([]models.Position, error) {
	var positions []models.Position
	err := r.db.WithContext(ctx).
		Preload("Instrument").
		Where("user_id = ? AND trading_mode = ? AND status = 'OPEN'", userID, tradingMode).
		Order("opened_at DESC").
		Find(&positions).Error
	return positions, err
}

func (r *PositionRepository) GetClosedByUser(ctx context.Context, userID, tradingMode string) ([]models.Position, error) {
	var positions []models.Position
	err := r.db.WithContext(ctx).
		Preload("Instrument").
		Where("user_id = ? AND trading_mode = ? AND status = 'CLOSED'", userID, tradingMode).
		Order("closed_at DESC").
		Find(&positions).Error
	return positions, err
}

func (r *PositionRepository) GetOpenByStrategy(ctx context.Context, strategyID, userID string) ([]models.Position, error) {
	var positions []models.Position
	err := r.db.WithContext(ctx).
		Where("strategy_id = ? AND user_id = ? AND status = 'OPEN'", strategyID, userID).
		Find(&positions).Error
	return positions, err
}

func (r *PositionRepository) GetAllOpenPositions(ctx context.Context, tradingMode string) ([]models.Position, error) {
	var positions []models.Position
	err := r.db.WithContext(ctx).
		Preload("Instrument").
		Where("status = 'OPEN' AND trading_mode = ?", tradingMode).
		Find(&positions).Error
	return positions, err
}

// ────────────────────────────────────────────────────────────────────────────
// Dashboard Stats
// ────────────────────────────────────────────────────────────────────────────

type DashboardStats struct {
	Mode            string  `json:"mode"`
	TotalInvested   float64 `json:"total_invested"`
	CurrentInvested float64 `json:"current_invested"`
	RealizedPnL     float64 `json:"realized_pnl"`
	OpenCount       int     `json:"open_count"`
	ClosedCount     int     `json:"closed_count"`
}

func (r *PositionRepository) GetDashboardStats(ctx context.Context, userID, tradingMode string) (*DashboardStats, error) {
	stats := &DashboardStats{Mode: tradingMode}

	err := r.db.WithContext(ctx).Model(&models.Position{}).
		Select(`
			COALESCE(SUM(COALESCE(avg_entry_price, 0) * COALESCE(open_qty, 0)) FILTER (WHERE status = 'OPEN'), 0) AS current_invested,
			COALESCE(SUM(COALESCE(realized_pnl, 0)) FILTER (WHERE status = 'CLOSED'), 0) AS realized_pnl,
			COUNT(*) FILTER (WHERE status = 'OPEN') AS open_count,
			COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_count
		`).
		Where("user_id = ? AND trading_mode = ?", userID, tradingMode).
		Scan(stats).Error
	if err != nil {
		return nil, err
	}

	stats.TotalInvested = stats.CurrentInvested + stats.RealizedPnL
	return stats, nil
}
