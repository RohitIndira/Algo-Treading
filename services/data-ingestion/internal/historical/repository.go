package historical

import (
	"context"
	"database/sql"

	"go.uber.org/zap"
)

// Repository handles PostgreSQL operations against signals_db.
//
// Post-DB.7b surface (2026-07-13): only instrument-master reads remain.
// The daily_ohlcv + breakout_events code paths were removed alongside
// the tables they hit — see docs/db_ownership.md for the drop record
// and commit history for the surgical removal.
type Repository struct {
	db     *sql.DB
	logger *zap.Logger
}

// NewRepository creates a new repository backed by the given DB connection.
func NewRepository(db *sql.DB, logger *zap.Logger) *Repository {
	return &Repository{db: db, logger: logger}
}

// --- Instruments ---

// GetAllNSESymbols returns all active NSE instrument symbols for WebSocket subscription.
func (r *Repository) GetAllNSESymbols(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT DISTINCT symbol FROM instruments WHERE exchange = 'NSE' AND is_active = TRUE AND symbol != ''`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var symbols []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		symbols = append(symbols, s)
	}
	return symbols, rows.Err()
}

// GetInstrumentBySymbol looks up instrument by symbol+exchange.
func (r *Repository) GetInstrumentBySymbol(ctx context.Context, symbol, exchange string) (int, string, error) {
	var id int
	var token string
	err := r.db.QueryRowContext(ctx,
		`SELECT instrument_id, token FROM instruments WHERE symbol = $1 AND exchange = $2 LIMIT 1`,
		symbol, exchange).Scan(&id, &token)
	if err == sql.ErrNoRows {
		return 0, "", nil
	}
	return id, token, err
}
