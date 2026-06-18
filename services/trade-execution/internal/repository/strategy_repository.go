package repository

import (
	"context"

	"github.com/jmoiron/sqlx"
)

// StrategyRepository queries the trading_db strategies table (separate DB from
// trading_execution). Used only for startup hydration of the active-user WS
// protection set; all runtime updates come via Kafka strategy events.
type StrategyRepository interface {
	// GetActiveLiveStrategyUserIDs returns distinct user IDs that have at least
	// one active, non-deleted LIVE strategy. These users' broker WebSocket
	// connections are protected from the idle sweep for the whole trading day.
	GetActiveLiveStrategyUserIDs(ctx context.Context) ([]string, error)
}

type strategyRepository struct {
	db *sqlx.DB
}

// NewStrategyRepository creates a StrategyRepository backed by the supplied DB.
// The caller is responsible for connecting to trading_db (not trading_execution).
func NewStrategyRepository(db *sqlx.DB) StrategyRepository {
	return &strategyRepository{db: db}
}

func (r *strategyRepository) GetActiveLiveStrategyUserIDs(ctx context.Context) ([]string, error) {
	var userIDs []string
	query := `
		SELECT DISTINCT user_id FROM strategies
		WHERE active = true
		  AND deleted_at IS NULL
		  AND trading_mode = 'LIVE'
	`
	if err := r.db.SelectContext(ctx, &userIDs, query); err != nil {
		return nil, err
	}
	return userIDs, nil
}
