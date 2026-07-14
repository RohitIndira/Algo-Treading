package livealgos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrStrategyNotFound is returned by StrategyMeta when the strategy id
// doesn't exist in the strategies table. Handler maps this to 404.
var ErrStrategyNotFound = errors.New("livealgos: strategy not found")

// PositionRow is one row of manthan_positions (owned by rules-engine in
// trading_db). Fields nullable in the DB use sql.NullString/Float64 so
// the query can distinguish "value not set" from "value zero".
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

// OrderRow is one row of manthan_orders (owned by trade-execution in
// execution_db) — an individual order/fill. Used for the Stock P&L
// drilldown trades table.
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
	// StoppedAt is set when the user hits STOP (terminal transition).
	// Non-nil → derive Status=STOPPED regardless of Active. See
	// aggregator_details.go statusFromMeta.
	StoppedAt      *time.Time
}

// Store is the DB-access surface for Live Algos endpoints. Reads from
// TWO Postgres databases because writers live in different services:
//
//   strategies + trade_configs + manthan_positions  → trading_db      (rules-engine)
//   manthan_orders                                  → execution_db (trade-execution)
//
// Previously this store hit a single `stockk_trading` DB that had both
// tables copied in — but that DB was a silent replica that drifted from
// the authoritative sources, causing UI vs main-handler inconsistencies.
// See docs/db_ownership.md for the current ownership map.
type Store interface {
	// StrategyMeta returns the header row + capital info for one strategy
	// owned by userID. ErrStrategyNotFound when no such (strategy, user)
	// pair exists — matched against BOTH so users can't peek at other
	// users' strategies just by knowing the UUID.
	StrategyMeta(ctx context.Context, strategyID, userID string) (*StrategyMetaRow, error)

	// Positions returns every position row for a strategy, in insert
	// order (entry_time ASC). Handler filters ACTIVE vs EXITED as needed.
	// Includes exchange_token (looked up from manthan_orders in a
	// separate query against execution_db) so the LTP fetch is
	// one hop away.
	Positions(ctx context.Context, strategyID, userID string) ([]PositionRow, error)

	// Orders returns every FILLED order for a strategy+symbol, most-recent
	// first — powers the trade-history table on the Stock P&L drilldown.
	OrdersForSymbol(ctx context.Context, strategyID, userID, symbol string) ([]OrderRow, error)
}

// PostgresStore implements Store using two DB pools.
//
// positionsDB holds strategies + trade_configs + manthan_positions (the
// rules-engine + user-config write side). ordersDB holds manthan_orders
// (trade-execution's write side). Callers own both pool lifecycles.
//
// Passing nil for either is legal — StrategyMeta needs positionsDB,
// Positions needs both (tokens degrade gracefully to nil if ordersDB
// is nil), OrdersForSymbol needs ordersDB. The router already
// nil-guards the whole details tier if any of these are missing.
type PostgresStore struct {
	positionsDB *sql.DB
	ordersDB    *sql.DB
}

// NewPostgresStore wires the store to two open *sql.DB pools:
// positionsDB → trading_db, ordersDB → execution_db.
//
// Both are opened once at boot in main.go and reused across handlers
// (ManthanHandler shares the same pools).
func NewPostgresStore(positionsDB, ordersDB *sql.DB) *PostgresStore {
	return &PostgresStore{positionsDB: positionsDB, ordersDB: ordersDB}
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
			s.created_at,
			s.stopped_at
		FROM public.strategies s
		LEFT JOIN public.trade_configs tc ON tc.strategy_id = s.strategy_id
		WHERE s.strategy_id::text = $1
		  AND s.user_id = $2
		LIMIT 1`

	row := s.positionsDB.QueryRowContext(ctx, q, strategyID, userID)

	var r StrategyMetaRow
	var stoppedAt sql.NullTime
	err := row.Scan(
		&r.StrategyID, &r.UserID, &r.StrategyName, &r.StrategyType,
		&r.TradingMode, &r.Active,
		&r.TotalCapital, &r.MaxPositions, &r.PerStockAmount,
		&r.CreatedAt,
		&stoppedAt,
	)
	if stoppedAt.Valid {
		r.StoppedAt = &stoppedAt.Time
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrStrategyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("livealgos: StrategyMeta scan: %w", err)
	}
	return &r, nil
}

// Positions returns every position for (strategy, user). Because
// manthan_positions (trading_db) and manthan_orders (execution_db)
// live in DIFFERENT Postgres databases now, we can't do the exchange_token
// enrichment as a single JOIN. Instead:
//
//	1. SELECT positions from positionsDB
//	2. SELECT distinct (symbol, exchange_token) from ordersDB
//	3. Merge in Go
//
// This costs one extra round-trip vs the old single-DB JOIN, but it's
// the price of not having a silently-drifting replica DB. Both queries
// are indexed on (strategy_id, user_id) so latency is a few ms each.
//
// ordersDB nil is legal — Positions() still returns the row list, just
// with every ExchangeToken=NULL. Caller's LTP fetch step will skip
// those rows (already behaves that way — see collectActiveTokens).
func (s *PostgresStore) Positions(ctx context.Context, strategyID, userID string) ([]PositionRow, error) {
	const positionsQ = `
		SELECT
			id,
			strategy_id::text,
			user_id,
			symbol,
			isin,
			industry,
			mcap_bucket,
			entry_price,
			quantity,
			invested_amt,
			high_since_entry,
			current_sl,
			status,
			exit_price,
			realized_pnl,
			exit_reason,
			entry_time,
			exit_time
		FROM public.manthan_positions
		WHERE strategy_id::text = $1
		  AND user_id = $2
		ORDER BY entry_time ASC`

	rows, err := s.positionsDB.QueryContext(ctx, positionsQ, strategyID, userID)
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
		); err != nil {
			return nil, fmt.Errorf("livealgos: Positions scan: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Second round-trip: symbol → exchange_token from the OTHER DB.
	// Skipped when ordersDB is nil or when the position list is empty.
	if s.ordersDB == nil || len(out) == 0 {
		return out, nil
	}

	tokens, err := s.tokensForStrategy(ctx, strategyID, userID)
	if err != nil {
		// Non-fatal: return positions without exchange_token rather than
		// killing the whole details page. LTP fetch degrades to no-LTP
		// downstream and the AG.LTP subsystem surfaces ltpStatus.
		return out, nil
	}
	for i := range out {
		if tok, ok := tokens[out[i].Symbol]; ok {
			out[i].ExchangeToken = sql.NullString{String: tok, Valid: true}
		}
	}
	return out, nil
}

// tokensForStrategy queries manthan_orders (ordersDB) for the
// (symbol → exchange_token) map, keyed to one strategy+user.
// exchange_token is stable per symbol so DISTINCT ON returns the
// earliest FILLED order's value.
func (s *PostgresStore) tokensForStrategy(ctx context.Context, strategyID, userID string) (map[string]string, error) {
	const q = `
		SELECT DISTINCT ON (symbol) symbol, exchange_token
		  FROM public.manthan_orders
		 WHERE strategy_id::text = $1
		   AND user_id = $2
		   AND status = 'FILLED'
		   AND exchange_token IS NOT NULL
		 ORDER BY symbol, filled_at ASC`

	rows, err := s.ordersDB.QueryContext(ctx, q, strategyID, userID)
	if err != nil {
		return nil, fmt.Errorf("livealgos: tokens query: %w", err)
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var sym, tok string
		if err := rows.Scan(&sym, &tok); err != nil {
			return nil, fmt.Errorf("livealgos: tokens scan: %w", err)
		}
		out[sym] = tok
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

	rows, err := s.ordersDB.QueryContext(ctx, q, strategyID, userID, symbol)
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
