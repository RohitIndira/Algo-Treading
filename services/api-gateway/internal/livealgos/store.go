package livealgos

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// ErrStrategyNotFound is returned by StrategyMeta when the strategy id
// doesn't exist in the strategies table. Handler maps this to 404.
var ErrStrategyNotFound = errors.New("livealgos: strategy not found")

// PositionRow is one row of positions_db.positions (owned by the
// positions service — the canonical SSOT since the CQRS split from
// rules-engine's manthan_positions). ISIN/Industry/MCapBucket are NOT
// stored in positions_db; they are looked up per-symbol from
// signals_db.manthan_signals (see Positions).
//
// Fields nullable in the DB use sql.NullString/Float64 so the query can
// distinguish "value not set" from "value zero".
type PositionRow struct {
	// ID is positions.position_id (uuid) — string form. Was int64 back
	// when we read from manthan_positions.id.
	ID              string
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
// FOUR Postgres databases because writers live in different services:
//
//   strategies + trade_configs        → trading_db    (user-config)
//   positions                         → positions_db  (positions svc — SSOT
//                                                      for lifecycle P&L)
//   manthan_orders                    → execution_db  (trade-execution)
//   manthan_signals (symbol metadata) → signals_db    (data-ingestion)
//
// positions_db replaced the legacy trading_db.manthan_positions read
// (2026-07 CQRS migration) because that table's realized_pnl writer
// was unreliable — e.g. HSCL entry=676.13 exit=548 qty=30 got stored
// as realized_pnl=0.00. positions_db is fed by the positions
// service reconciler which trusts broker fills, so its numbers match
// reality. signals_db.manthan_signals is joined per-symbol to supply
// isin/industry/mcap_bucket (positions_db doesn't carry them).
//
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

// PostgresStore implements Store using four DB pools.
//
//   strategiesDB → trading_db   — strategies + trade_configs (user-config).
//   positionsDB  → positions_db — positions.* (positions svc, SSOT).
//   ordersDB     → execution_db — manthan_orders (trade-execution).
//   signalsDB    → signals_db   — manthan_signals for symbol→(isin,
//                                  industry, mcap_bucket) enrichment.
//
// Any pool may be nil — the store degrades gracefully:
//   - StrategyMeta needs strategiesDB (returns ErrStrategyNotFound if nil).
//   - Positions needs positionsDB (returns [] if nil). ordersDB nil →
//     exchange_token stays NULL; signalsDB nil → industry/isin/mcap
//     stay empty on each row.
//   - OrdersForSymbol needs ordersDB.
//
// The router already nil-guards the whole details tier if any of these
// are missing at boot.
type PostgresStore struct {
	strategiesDB *sql.DB
	positionsDB  *sql.DB
	ordersDB     *sql.DB
	signalsDB    *sql.DB
}

// NewPostgresStore wires the store to four open *sql.DB pools:
//   strategiesDB → trading_db
//   positionsDB  → positions_db  (2026-07 CQRS SSOT; NOT trading_db anymore)
//   ordersDB     → execution_db
//   signalsDB    → signals_db    (for symbol metadata enrichment; nil-safe)
//
// All are opened once at boot in main.go and reused across handlers.
func NewPostgresStore(strategiesDB, positionsDB, ordersDB, signalsDB *sql.DB) *PostgresStore {
	return &PostgresStore{
		strategiesDB: strategiesDB,
		positionsDB:  positionsDB,
		ordersDB:     ordersDB,
		signalsDB:    signalsDB,
	}
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

	row := s.strategiesDB.QueryRowContext(ctx, q, strategyID, userID)

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

// Positions returns every MANTHAN-origin position for (strategy, user).
// Reads from positions_db.positions (the CQRS SSOT since 2026-07). Since
// positions_db doesn't store symbol metadata OR the exchange_token, we
// enrich in three round-trips:
//
//	1. SELECT positions from positionsDB (positions_db)
//	2. SELECT (symbol → isin, industry, mcap_bucket) from signalsDB
//	   (signals_db.manthan_signals — latest run_date per symbol)
//	3. SELECT distinct (symbol → exchange_token) from ordersDB
//	   (execution_db.manthan_orders)
//	4. Merge all three in Go
//
// All three secondary reads are nil-safe: signalsDB nil → industry/isin/
// mcap stay empty on every row (allocation charts degrade to "(empty)"
// buckets, ltpStatus still works). ordersDB nil → exchange_token stays
// NULL and LTP fetch skips the row. Both writers are indexed on the
// columns we hit, so latency is a few ms each even on a large book.
//
// Origin filter: `origin = 'MANTHAN'` deliberately excludes USER_MANUAL
// rows in positions_db — those don't belong on the live-algos details
// page (they're surfaced through /users/me/portfolio/*). If the caller
// ever needs both, add an `IncludeManual` field to the interface.
func (s *PostgresStore) Positions(ctx context.Context, strategyID, userID string) ([]PositionRow, error) {
	if s.positionsDB == nil {
		return nil, nil
	}
	const positionsQ = `
		SELECT
			position_id::text,
			strategy_id::text,
			user_id,
			symbol,
			entry_price,
			quantity,
			invested_amount,
			high_since_entry,
			current_sl,
			status,
			exit_price,
			realized_pnl,
			exit_reason,
			entry_time,
			exit_time
		FROM public.positions
		WHERE strategy_id::text = $1
		  AND user_id = $2
		  AND origin = 'MANTHAN'
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
	if len(out) == 0 {
		return out, nil
	}

	// Enrichment (2): symbol → isin/industry/mcap_bucket from signals_db.
	// Non-fatal on error — allocation charts degrade to "(empty)" buckets
	// but the money numbers stay accurate. Skipped when signalsDB is nil.
	if s.signalsDB != nil {
		if meta, err := s.symbolMetadata(ctx, collectSymbols(out)); err == nil {
			for i := range out {
				if m, ok := meta[out[i].Symbol]; ok {
					if m.ISIN != "" {
						out[i].ISIN = sql.NullString{String: m.ISIN, Valid: true}
					}
					if m.Industry != "" {
						out[i].Industry = sql.NullString{String: m.Industry, Valid: true}
					}
					if m.MCapBucket != "" {
						out[i].MCapBucket = sql.NullString{String: m.MCapBucket, Valid: true}
					}
				}
			}
		}
	}

	// Enrichment (3): symbol → exchange_token from execution_db.
	// Skipped when ordersDB is nil.
	if s.ordersDB != nil {
		if tokens, err := s.tokensForStrategy(ctx, strategyID, userID); err == nil {
			for i := range out {
				if tok, ok := tokens[out[i].Symbol]; ok {
					out[i].ExchangeToken = sql.NullString{String: tok, Valid: true}
				}
			}
		}
	}

	return out, nil
}

// symbolMeta is the enrichment payload from signals_db.manthan_signals.
// Fields default to "" when the signal row didn't set them.
type symbolMeta struct {
	ISIN       string
	Industry   string
	MCapBucket string
}

// symbolMetadata returns a symbol → symbolMeta map by hitting
// signals_db.manthan_signals. Uses DISTINCT ON (symbol) ORDER BY
// run_date DESC to grab the latest metadata row per symbol — a symbol
// re-appearing across days keeps its most-recent classification.
func (s *PostgresStore) symbolMetadata(ctx context.Context, symbols []string) (map[string]symbolMeta, error) {
	if len(symbols) == 0 {
		return nil, nil
	}
	const q = `
		SELECT DISTINCT ON (symbol)
			symbol,
			COALESCE(isin, ''),
			COALESCE(industry, ''),
			COALESCE(mcap_bucket, '')
		FROM public.manthan_signals
		WHERE symbol = ANY($1)
		ORDER BY symbol, run_date DESC`
	// pq.Array is required to marshal []string as PostgreSQL text[] for the
	// `symbol = ANY($1)` clause. Passing the slice raw silently returns
	// zero rows — the DETAILS response's "Uncategorised 100%" allocation
	// bug on 2026-07-17 traced to exactly this missing wrapper.
	rows, err := s.signalsDB.QueryContext(ctx, q, pq.Array(symbols))
	if err != nil {
		return nil, fmt.Errorf("livealgos: symbolMetadata query: %w", err)
	}
	defer rows.Close()

	out := map[string]symbolMeta{}
	for rows.Next() {
		var sym string
		var m symbolMeta
		if err := rows.Scan(&sym, &m.ISIN, &m.Industry, &m.MCapBucket); err != nil {
			return nil, fmt.Errorf("livealgos: symbolMetadata scan: %w", err)
		}
		out[sym] = m
	}
	return out, rows.Err()
}

// collectSymbols returns the distinct symbols in a PositionRow slice.
// Order preserved by first-occurrence for stable log/query traces.
func collectSymbols(rows []PositionRow) []string {
	seen := make(map[string]struct{}, len(rows))
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		if _, ok := seen[r.Symbol]; ok {
			continue
		}
		seen[r.Symbol] = struct{}{}
		out = append(out, r.Symbol)
	}
	return out
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
