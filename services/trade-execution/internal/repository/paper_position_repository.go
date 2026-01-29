package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// PaperPositionRepository handles paper trading position persistence
type PaperPositionRepository interface {
	// Create creates a new paper position
	Create(ctx context.Context, position *models.PaperPosition) error

	// Get retrieves a position by ID
	Get(ctx context.Context, positionID uuid.UUID) (*models.PaperPosition, error)

	// GetByToken retrieves a position by user, token and status
	GetByToken(ctx context.Context, userID string, token int64, status models.PaperPositionStatus) (*models.PaperPosition, error)

	// GetOpenPositions retrieves all open positions for a user
	GetOpenPositions(ctx context.Context, userID string, strategyID ...string) ([]*models.PaperPosition, error)

	// GetAllOpenPositions retrieves all open positions across all users
	GetAllOpenPositions(ctx context.Context) ([]*models.PaperPosition, error)

	// GetAllPositions retrieves all positions (open and closed) for a user
	GetAllPositions(ctx context.Context, userID string, limit, offset int) ([]*models.PaperPosition, error)

	// UpdatePrice updates the current price and PnL for a position
	UpdatePrice(ctx context.Context, positionID uuid.UUID, currentPrice float64) error

	// ClosePosition closes a position and records the exit
	ClosePosition(ctx context.Context, positionID uuid.UUID, exitPrice float64, exitReason models.ExitReason, exitOrderID uuid.UUID) error

	// GetUserDailyPnL retrieves daily PnL summary for a user
	GetUserDailyPnL(ctx context.Context, userID string, date time.Time) (*models.UserDailyPaperPnL, error)

	// GetTotalRealizedPnL gets total realized PnL for a user and strategy
	GetTotalRealizedPnL(ctx context.Context, userID, strategyID string, startDate, endDate time.Time) (float64, error)
}

type paperPositionRepository struct {
	db *sqlx.DB
}

// NewPaperPositionRepository creates a new paper position repository
func NewPaperPositionRepository(db *sqlx.DB) PaperPositionRepository {
	return &paperPositionRepository{db: db}
}

func (r *paperPositionRepository) Create(ctx context.Context, position *models.PaperPosition) error {
	query := `
		INSERT INTO paper_positions (
			position_id, user_id, strategy_id, stock_code, token, symbol, exchange,
			quantity, entry_price, current_price, stop_loss, take_profit,
			unrealized_pnl, unrealized_pnl_pct, status, entry_order_id,
			opened_at, last_updated
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
		)`

	_, err := r.db.ExecContext(ctx, query,
		position.PositionID, position.UserID, position.StrategyID,
		position.StockCode, position.Token, position.Symbol, position.Exchange,
		position.Quantity, position.EntryPrice, position.CurrentPrice,
		position.StopLoss, position.TakeProfit,
		position.UnrealizedPnL, position.UnrealizedPnLPct,
		position.Status, position.EntryOrderID,
		position.OpenedAt, position.LastUpdated,
	)

	if err != nil {
		return fmt.Errorf("failed to create paper position: %w", err)
	}

	return nil
}

func (r *paperPositionRepository) Get(ctx context.Context, positionID uuid.UUID) (*models.PaperPosition, error) {
	var position models.PaperPosition
	query := `SELECT * FROM paper_positions WHERE position_id = $1`

	err := r.db.GetContext(ctx, &position, query, positionID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("position not found")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get paper position: %w", err)
	}

	return &position, nil
}

func (r *paperPositionRepository) GetByToken(ctx context.Context, userID string, token int64, status models.PaperPositionStatus) (*models.PaperPosition, error) {
	var position models.PaperPosition
	query := `
		SELECT * FROM paper_positions 
		WHERE user_id = $1 AND token = $2 AND status = $3
		ORDER BY opened_at DESC
		LIMIT 1`

	err := r.db.GetContext(ctx, &position, query, userID, token, status)
	if err == sql.ErrNoRows {
		return nil, nil // Return nil without error if not found
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get paper position by token: %w", err)
	}

	return &position, nil
}

func (r *paperPositionRepository) GetOpenPositions(ctx context.Context, userID string, strategyID ...string) ([]*models.PaperPosition, error) {
	var positions []*models.PaperPosition
	var query string
	var args []interface{}

	if len(strategyID) > 0 && strategyID[0] != "" {
		query = `
			SELECT * FROM paper_positions 
			WHERE user_id = $1 AND strategy_id = $2 AND status = $3
			ORDER BY opened_at DESC`
		args = []interface{}{userID, strategyID[0], models.PositionStatusOpen}
	} else {
		query = `
			SELECT * FROM paper_positions 
			WHERE user_id = $1 AND status = $2
			ORDER BY opened_at DESC`
		args = []interface{}{userID, models.PositionStatusOpen}
	}

	err := r.db.SelectContext(ctx, &positions, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to get open paper positions: %w", err)
	}

	return positions, nil
}

func (r *paperPositionRepository) GetAllOpenPositions(ctx context.Context) ([]*models.PaperPosition, error) {
	var positions []*models.PaperPosition
	query := `
		SELECT * FROM paper_positions 
		WHERE status = $1
		ORDER BY opened_at DESC`

	err := r.db.SelectContext(ctx, &positions, query, models.PositionStatusOpen)
	if err != nil {
		return nil, fmt.Errorf("failed to get all open paper positions: %w", err)
	}

	return positions, nil
}

func (r *paperPositionRepository) GetAllPositions(ctx context.Context, userID string, limit, offset int) ([]*models.PaperPosition, error) {
	var positions []*models.PaperPosition
	query := `
		SELECT * FROM paper_positions 
		WHERE user_id = $1
		ORDER BY opened_at DESC
		LIMIT $2 OFFSET $3`

	err := r.db.SelectContext(ctx, &positions, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to get all paper positions: %w", err)
	}

	return positions, nil
}

func (r *paperPositionRepository) UpdatePrice(ctx context.Context, positionID uuid.UUID, currentPrice float64) error {
	// First get the position to calculate PnL
	position, err := r.Get(ctx, positionID)
	if err != nil {
		return err
	}

	// Calculate PnL
	position.CalculatePnL(currentPrice)

	query := `
		UPDATE paper_positions 
		SET current_price = $1, 
		    unrealized_pnl = $2, 
		    unrealized_pnl_pct = $3,
		    last_updated = $4
		WHERE position_id = $5`

	_, err = r.db.ExecContext(ctx, query,
		position.CurrentPrice,
		position.UnrealizedPnL,
		position.UnrealizedPnLPct,
		position.LastUpdated,
		positionID,
	)

	if err != nil {
		return fmt.Errorf("failed to update paper position price: %w", err)
	}

	return nil
}

func (r *paperPositionRepository) ClosePosition(ctx context.Context, positionID uuid.UUID, exitPrice float64, exitReason models.ExitReason, exitOrderID uuid.UUID) error {
	// Start transaction
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Get the position
	var position models.PaperPosition
	query := `SELECT * FROM paper_positions WHERE position_id = $1 FOR UPDATE`
	err = tx.GetContext(ctx, &position, query, positionID)
	if err != nil {
		return fmt.Errorf("failed to get paper position for closing: %w", err)
	}

	// Calculate realized PnL
	realizedPnL, realizedPnLPct := models.CalculateRealizedPnL(
		position.EntryPrice,
		exitPrice,
		position.Quantity,
	)

	// Determine status based on exit reason
	var status models.PaperPositionStatus
	switch exitReason {
	case models.ExitReasonStopLoss:
		status = models.PositionStatusClosedSL
	case models.ExitReasonTakeProfit:
		status = models.PositionStatusClosedTP
	default:
		status = models.PositionStatusClosedManual
	}

	now := time.Now()

	// Update position to closed
	updateQuery := `
		UPDATE paper_positions 
		SET status = $1, 
		    current_price = $2,
		    exit_order_id = $3,
		    closed_at = $4,
		    last_updated = $5
		WHERE position_id = $6`

	_, err = tx.ExecContext(ctx, updateQuery,
		status, exitPrice, exitOrderID, now, now, positionID,
	)
	if err != nil {
		return fmt.Errorf("failed to close paper position: %w", err)
	}

	// Insert into PnL history
	pnlQuery := `
		INSERT INTO paper_pnl_history (
			pnl_id, user_id, strategy_id, position_id, symbol, exchange,
			quantity, entry_price, exit_price, realized_pnl, realized_pnl_pct,
			exit_reason, entry_time, exit_time
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
		)`

	_, err = tx.ExecContext(ctx, pnlQuery,
		uuid.New(), position.UserID, position.StrategyID, position.PositionID,
		position.Symbol, position.Exchange, position.Quantity,
		position.EntryPrice, exitPrice, realizedPnL, realizedPnLPct,
		exitReason, position.OpenedAt, now,
	)
	if err != nil {
		return fmt.Errorf("failed to insert PnL history: %w", err)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (r *paperPositionRepository) GetUserDailyPnL(ctx context.Context, userID string, date time.Time) (*models.UserDailyPaperPnL, error) {
	var dailyPnL models.UserDailyPaperPnL
	query := `
		SELECT * FROM user_daily_paper_pnl 
		WHERE user_id = $1 AND trade_date = $2`

	err := r.db.GetContext(ctx, &dailyPnL, query, userID, date)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get daily PnL: %w", err)
	}

	return &dailyPnL, nil
}

func (r *paperPositionRepository) GetTotalRealizedPnL(ctx context.Context, userID, strategyID string, startDate, endDate time.Time) (float64, error) {
	var totalPnL sql.NullFloat64
	query := `
		SELECT COALESCE(SUM(realized_pnl), 0) as total_pnl
		FROM paper_pnl_history 
		WHERE user_id = $1 
		  AND strategy_id = $2
		  AND exit_time >= $3 
		  AND exit_time < $4`

	err := r.db.GetContext(ctx, &totalPnL, query, userID, strategyID, startDate, endDate)
	if err != nil {
		return 0, fmt.Errorf("failed to get total realized PnL: %w", err)
	}

	if !totalPnL.Valid {
		return 0, nil
	}

	return totalPnL.Float64, nil
}
