package livealgos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrStrategyNotFound is returned by StrategyMeta when the strategy id
// doesn't exist in stockk_trading.strategies. Handler maps this to 404.
var ErrStrategyNotFound = errors.New("livealgos: strategy not found")

// PositionRow is one row of stockk_trading.manthan_positions.
// Fields nullable in the DB use sql.NullString/Float64 so the query
// can distinguish "value not set" from "value zero".
type PositionRow struct {
	ID              int64
	StrategyID      string
	UserID          string
	Symbol          string
	ISIN            sql.NullString
	Industry        sql.NullString
	MCapBucket      sql.NullString
	EntryPrice      float64
	Quantity        int
	InvestedAmt     float64
	HighSinceEntry  sql.NullFloat64
	CurrentSL       sql.NullFloat64
	Status          string // ACTIVE, EXITED, ...
	ExitPrice       sql.NullFloat64
	RealizedPnL     sql.NullFloat64
	ExitReason      sql.NullString
	EntryTime       time.Time
	ExitTime        sql.NullTime
	ExchangeToken   sql.NullString // joined from manthan_orders below
}

// OrderRow is one row of stockk_trading.manthan_orders — an individual
// order/fill. Used for the Stock P&L drilldown trades table.
type OrderRow struct {
	Symbol       string
	OrderSide    string  // BUY / SELL
	Qty          int
	FilledQty    sql.NullInt32
	AvgFillPrice sql.NullFloat64
	Status       string
	FilledAt     sql.NullTime
	ExchangeToken sql.NullString
}

// StrategyMetaRow is the small header data every Details endpoint needs:
// strategy name + trading_mode + active flag + total_capital.
type StrategyMetaRow struct {
	StrategyID     string
	UserID         string
	StrategyName   string
	StrategyType   string
	TradingMode    string
	Active         bool
	TotalCapital   float64
	MaxPositions   int
	PerStockAmount float64
	CreatedAt      time.Time
}

// Store is the DB-access surface for Live Algos endpoints. Backed by
// stockk_trading Postgres today; interface stays thin so it can be
// swapped for a caching wrapper or a mock in tests.
type Store interface {
	// StrategyMeta returns the header row + capital info for one strategy
	// owned by userID. ErrStrategyNotFound when no such (strategy, user)
	// pair exists — matched against BOTH so users can't peek at other
	// users' strategies just by knowing the UUID.
	StrategyMeta(ctx context.Context, strategyID, userID string) (*StrategyMetaRow, error)

	// Positions returns every position row for a strategy, in insert
	// order (entry_time ASC). Handler filters ACTIVE vs EXITED as needed.
	// Includes exchange_token (LEFT JOIN'd from an order that opened
	// the position) so LTP fetch is one hop away.
	Positions(ctx context.Context, strategyID, userID string) ([]PositionRow, error)

	// Orders returns every FILLED order for a strategy+symbol, most-recent
	// first — powers the trade-history table on the Stock P&L drilldown.
	OrdersForSymbol(ctx context.Context, strategyID, userID, symbol string) ([]OrderRow, error)
}

// PostgresStore implements Store against stockk_trading Postgres.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore wires the store to an open *sql.DB. Handler pool is
// opened once at boot in main.go.
func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

// StrategyMeta joins strategies + trade_configs to return everything a
// handler needs about a single deployment. See interface doc for the
// user-scoping guarantee.
func (s *PostgresStore) StrategyMeta(ctx context.Context, strategyID, userID string) (*StrategyMetaRow, error) {
	const q = `
		SELECT
			s.strategy_id::text,
			s.user_id,
			s.strategy_name,
			s.strategy_type,
			s.trading_mode,
			s.active,
			COALESCE(tc.total_capital, 0),
			COALESCE(tc.max_positions, 0),
			COALESCE(tc.per_stock_amount, 0),
			s.created_at
		FROM public.strategies s
		LEFT JOIN public.trade_configs tc ON tc.strategy_id = s.strategy_id
		WHERE s.strategy_id::text = $1
		  AND s.user_id = $2
		LIMIT 1`

	row := s.db.QueryRowContext(ctx, q, strategyID, userID)

	var r StrategyMetaRow
	err := row.Scan(
		&r.StrategyID, &r.UserID, &r.StrategyName, &r.StrategyType,
		&r.TradingMode, &r.Active,
		&r.TotalCapital, &r.MaxPositions, &r.PerStockAmount,
		&r.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrStrategyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("livealgos: StrategyMeta scan: %w", err)
	}
	return &r, nil
}

// Positions returns every position for (strategy, user). Also joins to
// manthan_orders to pick up exchange_token so the LTP fetch step doesn't
// need a second round-trip. DISTINCT ON keeps only one order per symbol
// (the earliest FILLED buy) since exchange_token is stable per symbol.
func (s *PostgresStore) Positions(ctx context.Context, strategyID, userID string) ([]PositionRow, error) {
	const q = `
		WITH tokens AS (
			SELECT DISTINCT ON (symbol) symbol, exchange_token
			  FROM public.manthan_orders
			 WHERE strategy_id::text = $1
			   AND user_id = $2
			   AND status = 'FILLED'
			   AND exchange_token IS NOT NULL
			 ORDER BY symbol, filled_at ASC
		)
		SELECT
			p.id,
			p.strategy_id::text,
			p.user_id,
			p.symbol,
			p.isin,
			p.industry,
			p.mcap_bucket,
			p.entry_price,
			p.quantity,
			p.invested_amt,
			p.high_since_entry,
			p.current_sl,
			p.status,
			p.exit_price,
			p.realized_pnl,
			p.exit_reason,
			p.entry_time,
			p.exit_time,
			t.exchange_token
		FROM public.manthan_positions p
		LEFT JOIN tokens t ON t.symbol = p.symbol
		WHERE p.strategy_id::text = $1
		  AND p.user_id = $2
		ORDER BY p.entry_time ASC`

	rows, err := s.db.QueryContext(ctx, q, strategyID, userID)
	if err != nil {
		return nil, fmt.Errorf("livealgos: Positions query: %w", err)
	}
	defer rows.Close()

	var out []PositionRow
	for rows.Next() {
		var p PositionRow
		if err := rows.Scan(
			&p.ID, &p.StrategyID, &p.UserID, &p.Symbol,
			&p.ISIN, &p.Industry, &p.MCapBucket,
			&p.EntryPrice, &p.Quantity, &p.InvestedAmt,
			&p.HighSinceEntry, &p.CurrentSL,
			&p.Status, &p.ExitPrice, &p.RealizedPnL, &p.ExitReason,
			&p.EntryTime, &p.ExitTime,
			&p.ExchangeToken,
		); err != nil {
			return nil, fmt.Errorf("livealgos: Positions scan: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// OrdersForSymbol returns filled order rows for one strategy+symbol,
// most-recent first — powers the Trades table on the Stock P&L
// drilldown (Screen 7). Only FILLED status; pending/rejected orders
// aren't user-facing history.
func (s *PostgresStore) OrdersForSymbol(ctx context.Context, strategyID, userID, symbol string) ([]OrderRow, error) {
	const q = `
		SELECT
			symbol,
			order_side,
			qty,
			filled_qty,
			avg_fill_price,
			status,
			filled_at,
			exchange_token
		FROM public.manthan_orders
		WHERE strategy_id::text = $1
		  AND user_id = $2
		  AND symbol = $3
		  AND status = 'FILLED'
		ORDER BY filled_at DESC`

	rows, err := s.db.QueryContext(ctx, q, strategyID, userID, symbol)
	if err != nil {
		return nil, fmt.Errorf("livealgos: OrdersForSymbol query: %w", err)
	}
	defer rows.Close()

	var out []OrderRow
	for rows.Next() {
		var o OrderRow
		if err := rows.Scan(
			&o.Symbol, &o.OrderSide,
			&o.Qty, &o.FilledQty, &o.AvgFillPrice,
			&o.Status, &o.FilledAt, &o.ExchangeToken,
		); err != nil {
			return nil, fmt.Errorf("livealgos: OrdersForSymbol scan: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// Compile-time assertion: *PostgresStore satisfies Store.
var _ Store = (*PostgresStore)(nil)
