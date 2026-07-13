// Package store — read-only accessors over positions_db for portfolio svc.
//
// All methods are single-query where possible; portfolio queries are the
// UI hot path so extra round-trips matter. §5.1-5.3 of
// docs/portfolio_service_design.md specifies the columns each response
// needs.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Store wraps the positions_db handle with query helpers.
type Store struct {
	db     *sql.DB
	logger *zap.Logger
}

// New returns a Store. The *sql.DB must already be Ping-verified —
// callers own the pool.
func New(db *sql.DB, logger *zap.Logger) *Store {
	return &Store{db: db, logger: logger}
}

// -----------------------------------------------------------------------------
// Types — the response shapes portfolio svc's gRPC handlers (PF.C) will
// project into the wire envelope. Keeping them here so tests can assert
// against a concrete struct.
// -----------------------------------------------------------------------------

// Summary is the one-shot portfolio rollup returned by SummaryFor.
// Unrealized P&L is deliberately NOT here — api-gateway computes it
// after LTP fetch (§3 of the design doc).
type Summary struct {
	TotalInvested            float64 // SUM(invested_amount) WHERE status='ACTIVE'
	TotalRealizedPnLLifetime float64 // SUM(realized_pnl)   WHERE status='EXITED'
	TodayRealizedPnL         float64 // SUM(realized_pnl)   WHERE exit_time >= IST today 00:00
	ActiveLotCount           int
	ClosedLotCount           int
	ManthanInvested          float64 // SUM(invested_amount) WHERE ACTIVE AND origin='MANTHAN'
	UserManualInvested       float64 // SUM(invested_amount) WHERE ACTIVE AND origin='USER_MANUAL'
}

// ActivePosition is one ACTIVE lot as displayed in the UI's positions grid.
type ActivePosition struct {
	PositionID     uuid.UUID
	Origin         string
	Symbol         string
	Exchange       string
	StrategyID     string    // "" for USER_MANUAL
	SignalID       string    // "" for USER_MANUAL
	EntryTime      time.Time
	EntryPrice     float64
	Quantity       int
	InvestedAmount float64
	CurrentSL      float64 // 0 = no SL set (Manthan-only feature; USER_MANUAL is always 0)
	HighSinceEntry float64
}

// ClosedPosition is one EXITED lot as displayed in the history page.
type ClosedPosition struct {
	PositionID     uuid.UUID
	Origin         string
	Symbol         string
	EntryTime      time.Time
	EntryPrice     float64
	Quantity       int
	ExitTime       time.Time
	ExitPrice      float64
	ExitReason     string
	RealizedPnL    float64
	InvestedAmount float64
}

// -----------------------------------------------------------------------------
// SummaryFor — one query, all aggregates + counts + origin split.
// -----------------------------------------------------------------------------

// SummaryFor returns the per-user portfolio rollup. "Today" boundary is
// the current IST calendar day — computed in Go so the SQL stays portable.
//
// Zero rows for a user is a valid state (new user, no trading yet). The
// returned Summary has all-zero fields and nil error.
func (s *Store) SummaryFor(ctx context.Context, userID string) (*Summary, error) {
	if userID == "" {
		return nil, fmt.Errorf("SummaryFor: userID is required")
	}

	todayStartIST := startOfTodayIST()

	// One-shot: FILTER clauses turn every branch into a single sequential
	// scan of the user's rows. No indexed sort needed.
	row := s.db.QueryRowContext(ctx, `
		SELECT
		  COALESCE(SUM(invested_amount) FILTER (WHERE status = 'ACTIVE'), 0)             AS total_invested,
		  COALESCE(SUM(realized_pnl)   FILTER (WHERE status = 'EXITED'), 0)              AS total_realized_pnl,
		  COALESCE(SUM(realized_pnl)   FILTER (WHERE status = 'EXITED'
		                                         AND exit_time >= $2::timestamptz), 0)  AS today_realized_pnl,
		  COUNT(*) FILTER (WHERE status = 'ACTIVE')                                       AS active_count,
		  COUNT(*) FILTER (WHERE status = 'EXITED')                                       AS closed_count,
		  COALESCE(SUM(invested_amount) FILTER (WHERE status = 'ACTIVE'
		                                          AND origin = 'MANTHAN'), 0)             AS manthan_invested,
		  COALESCE(SUM(invested_amount) FILTER (WHERE status = 'ACTIVE'
		                                          AND origin = 'USER_MANUAL'), 0)         AS user_manual_invested
		FROM positions
		WHERE user_id = $1`, userID, todayStartIST)

	var out Summary
	if err := row.Scan(
		&out.TotalInvested,
		&out.TotalRealizedPnLLifetime,
		&out.TodayRealizedPnL,
		&out.ActiveLotCount,
		&out.ClosedLotCount,
		&out.ManthanInvested,
		&out.UserManualInvested,
	); err != nil {
		return nil, fmt.Errorf("SummaryFor scan: %w", err)
	}
	return &out, nil
}

// -----------------------------------------------------------------------------
// ActiveLotsFor — every ACTIVE lot, entry_time ASC.
// -----------------------------------------------------------------------------

// ActiveLotsFor returns all ACTIVE lots (both MANTHAN and USER_MANUAL)
// for one user. Sorted entry_time ASC = FIFO display, matches the FIFO
// exit rule in §7.2 of positions_service_design.md.
//
// Nil user_id returns error; empty result is a valid state (all lots
// exited) → returns (nil, nil) after driving through the loop 0 times.
func (s *Store) ActiveLotsFor(ctx context.Context, userID string) ([]*ActivePosition, error) {
	if userID == "" {
		return nil, fmt.Errorf("ActiveLotsFor: userID is required")
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT position_id, origin, symbol, exchange,
		       strategy_id, signal_id,
		       entry_time, entry_price, quantity, invested_amount,
		       COALESCE(current_sl, 0), COALESCE(high_since_entry, 0)
		FROM positions
		WHERE user_id = $1 AND status = 'ACTIVE'
		ORDER BY entry_time ASC, position_id ASC`, userID)
	if err != nil {
		return nil, fmt.Errorf("ActiveLotsFor query: %w", err)
	}
	defer rows.Close()

	var out []*ActivePosition
	for rows.Next() {
		var (
			p          ActivePosition
			strategyID sql.NullString
			signalID   sql.NullString
		)
		if err := rows.Scan(
			&p.PositionID, &p.Origin, &p.Symbol, &p.Exchange,
			&strategyID, &signalID,
			&p.EntryTime, &p.EntryPrice, &p.Quantity, &p.InvestedAmount,
			&p.CurrentSL, &p.HighSinceEntry,
		); err != nil {
			return nil, fmt.Errorf("ActiveLotsFor scan: %w", err)
		}
		p.StrategyID = strategyID.String
		p.SignalID = signalID.String
		out = append(out, &p)
	}
	return out, rows.Err()
}

// -----------------------------------------------------------------------------
// ClosedLotsPaged — every EXITED lot, exit_time DESC, paginated.
// -----------------------------------------------------------------------------

// ClosedLotsPaged returns a page of EXITED lots sorted exit_time DESC.
// Returns (rows, totalCount, err) — totalCount is over the WHOLE user
// history so the UI can render "showing X of N".
//
// pageSize clamps to [1, 200]; page clamps to >=1. Weird inputs get
// coerced silently rather than erroring — UIs sometimes send stale
// pagination state.
func (s *Store) ClosedLotsPaged(ctx context.Context, userID string, page, pageSize int) ([]*ClosedPosition, int, error) {
	if userID == "" {
		return nil, 0, fmt.Errorf("ClosedLotsPaged: userID is required")
	}
	if pageSize < 1 {
		pageSize = 50
	}
	if pageSize > 200 {
		pageSize = 200
	}
	if page < 1 {
		page = 1
	}
	offset := (page - 1) * pageSize

	// Total count (used for UI paginator). One query, no joins, cheap.
	var total int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM positions
		WHERE user_id = $1 AND status = 'EXITED'`, userID).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("ClosedLotsPaged count: %w", err)
	}
	if total == 0 {
		return nil, 0, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT position_id, origin, symbol,
		       entry_time, entry_price, quantity,
		       exit_time, exit_price, COALESCE(exit_reason, ''),
		       COALESCE(realized_pnl, 0),
		       invested_amount
		FROM positions
		WHERE user_id = $1 AND status = 'EXITED'
		ORDER BY exit_time DESC, position_id DESC
		LIMIT $2 OFFSET $3`, userID, pageSize, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("ClosedLotsPaged query: %w", err)
	}
	defer rows.Close()

	var out []*ClosedPosition
	for rows.Next() {
		var (
			p        ClosedPosition
			exitTime sql.NullTime
		)
		if err := rows.Scan(
			&p.PositionID, &p.Origin, &p.Symbol,
			&p.EntryTime, &p.EntryPrice, &p.Quantity,
			&exitTime, &p.ExitPrice, &p.ExitReason,
			&p.RealizedPnL,
			&p.InvestedAmount,
		); err != nil {
			return nil, 0, fmt.Errorf("ClosedLotsPaged scan: %w", err)
		}
		if exitTime.Valid {
			p.ExitTime = exitTime.Time
		}
		out = append(out, &p)
	}
	return out, total, rows.Err()
}

// -----------------------------------------------------------------------------
// helpers
// -----------------------------------------------------------------------------

// startOfTodayIST returns 00:00:00 IST for the current calendar day,
// as a timestamptz. Used to bound the "today realized P&L" query.
//
// Falls back to UTC-based truncation if the tzdata for Asia/Kolkata
// isn't linked into the binary (shouldn't happen in prod; guard is
// belt-and-braces for Alpine minimal images).
func startOfTodayIST() time.Time {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		now := time.Now().UTC()
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	}
	now := time.Now().In(loc)
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, loc)
}
