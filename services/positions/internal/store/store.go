package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Store owns positions_db writes and reads for positions svc.
type Store struct {
	db     *sql.DB
	logger *zap.Logger
}

func New(db *sql.DB, logger *zap.Logger) *Store {
	return &Store{db: db, logger: logger}
}

// ------------------------------------------------------------------------------
// Reads
// ------------------------------------------------------------------------------

// FindManthanLotBySignalID returns the ACTIVE MANTHAN row whose signal_id
// matches. Used by Manthan-driven SELL processing to find the specific lot
// the SL/EXIT signal targets (per §7.2 of the design doc).
//
// Returns (nil, nil) when no matching lot exists — treated by the caller as
// "SL fired against a position we didn't record" (drift, but not an error).
func (s *Store) FindManthanLotBySignalID(ctx context.Context, signalID string) (*Position, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+positionColumns+`
		FROM positions
		WHERE signal_id = $1::uuid
		  AND origin = 'MANTHAN'
		  AND status = 'ACTIVE'
		LIMIT 1`, signalID)
	pos, err := scanPosition(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("FindManthanLotBySignalID: %w", err)
	}
	return pos, nil
}

// FindActiveLotsFIFO returns all ACTIVE lots for (user, symbol) ordered
// per §7.2 manual-sell rule: USER_MANUAL first (FIFO within origin), then
// MANTHAN (FIFO within origin). Callers iterate + decrement in returned order.
// FindActiveLotsFIFO returns ACTIVE lots for a (user, symbol, exchange) tuple
// in FIFO order for manual-SELL matching: USER_MANUAL first (entry_time ASC),
// then MANTHAN (entry_time ASC). §7.2 manual-sell branch of the design doc.
//
// The exchange filter matters: a stock like SONACOMS is listed on both NSE
// (STK_SONACOMS_EQ_NSE_4684) and BSE (STK_SONACOMS_EQ_BSE_543300). A user
// can own shares on BOTH exchanges simultaneously and they are NOT
// fungible — an NSE SELL cannot deliver from BSE holdings and vice versa.
// Broker holdings track per-exchange too; positions_db must match.
//
// If exchange is empty (legacy WSS callers where the wire enum was "1"/"2"
// numeric or unmapped), the filter degrades to just (user, symbol) so old
// callers still work — but any FRESH callers going through the state
// machine's normalizeSymbol path will always pass a real exchange.
func (s *Store) FindActiveLotsFIFO(ctx context.Context, userID, symbol, exchange string) ([]*Position, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if exchange == "" {
		rows, err = s.db.QueryContext(ctx, `
			SELECT `+positionColumns+`
			FROM positions
			WHERE user_id = $1
			  AND symbol  = $2
			  AND status  = 'ACTIVE'
			ORDER BY
			  CASE origin WHEN 'USER_MANUAL' THEN 0 ELSE 1 END,
			  entry_time ASC`, userID, symbol)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT `+positionColumns+`
			FROM positions
			WHERE user_id  = $1
			  AND symbol   = $2
			  AND exchange = $3
			  AND status   = 'ACTIVE'
			ORDER BY
			  CASE origin WHEN 'USER_MANUAL' THEN 0 ELSE 1 END,
			  entry_time ASC`, userID, symbol, exchange)
	}
	if err != nil {
		return nil, fmt.Errorf("FindActiveLotsFIFO query: %w", err)
	}
	defer rows.Close()

	var out []*Position
	for rows.Next() {
		pos, err := scanPosition(rows)
		if err != nil {
			return nil, fmt.Errorf("FindActiveLotsFIFO scan: %w", err)
		}
		out = append(out, pos)
	}
	return out, rows.Err()
}

// FindAllActiveLotsForUser returns every ACTIVE lot (both MANTHAN + USER_MANUAL)
// for one user. Used by the reconciler (Chunk P.G) to compare against broker
// holdings; the reconciler groups by symbol itself.
//
// Ordered by (symbol, entry_time) so callers walking output stay deterministic.
func (s *Store) FindAllActiveLotsForUser(ctx context.Context, userID string) ([]*Position, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+positionColumns+`
		FROM positions
		WHERE user_id = $1
		  AND status  = 'ACTIVE'
		ORDER BY symbol ASC, entry_time ASC`, userID)
	if err != nil {
		return nil, fmt.Errorf("FindAllActiveLotsForUser query: %w", err)
	}
	defer rows.Close()

	var out []*Position
	for rows.Next() {
		pos, err := scanPosition(rows)
		if err != nil {
			return nil, fmt.Errorf("FindAllActiveLotsForUser scan: %w", err)
		}
		out = append(out, pos)
	}
	return out, rows.Err()
}

// DistinctUsersWithActive returns the set of user_ids that have at least
// one ACTIVE lot in positions_db. Used by the reconciler ticker to decide
// whose broker holdings to fetch each sweep — users with nothing at stake
// don't need a round-trip.
//
// Order is stable (alphabetical) so log lines across sweeps stay comparable.
func (s *Store) DistinctUsersWithActive(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT user_id
		FROM positions
		WHERE status = 'ACTIVE'
		ORDER BY user_id ASC`)
	if err != nil {
		return nil, fmt.Errorf("DistinctUsersWithActive query: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, fmt.Errorf("DistinctUsersWithActive scan: %w", err)
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// FindPositionByEntryBrokerOrderID returns the ACTIVE lot whose BUY broker
// order created it. Returns (nil, nil) when no match — caller decides if
// that's a real drift or a legitimate "SL event for a position we don't
// track" case (e.g. USER_MANUAL SL that never had a Manthan BUY row).
//
// Used by the SL-tracking path in the state machine: an SL_MODIFY event
// arrives with the SL's own broker_order_id → gRPC LookupOrderMeta returns
// the parent BUY's broker_order_id → this lookup finds the position it
// belongs to → current_sl gets updated. See §7.5 (added 2026-07-16).
//
// Filter is `status='ACTIVE'` so an SL modification arriving after the
// position already exited (broker race) doesn't accidentally reopen it.
func (s *Store) FindPositionByEntryBrokerOrderID(ctx context.Context, entryBrokerOrderID string) (*Position, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT `+positionColumns+`
		FROM positions
		WHERE entry_broker_order_id = $1
		  AND status = 'ACTIVE'
		LIMIT 1`, entryBrokerOrderID)
	pos, err := scanPosition(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("FindPositionByEntryBrokerOrderID: %w", err)
	}
	return pos, nil
}

// PositionExistsForEntry returns true if we've already INSERTed a position
// row for this BUY broker_order_id — the idempotency check on the BUY path.
// Kafka may replay the same FILLED event; without this we'd double-insert.
func (s *Store) PositionExistsForEntry(ctx context.Context, entryBrokerOrderID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM positions
		WHERE entry_broker_order_id = $1`, entryBrokerOrderID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("PositionExistsForEntry: %w", err)
	}
	return n > 0, nil
}

// ------------------------------------------------------------------------------
// Writes (transactional)
// ------------------------------------------------------------------------------

// InsertEntryWithEvent atomically INSERTs a new positions row + a position_events
// audit row. Used on BUY fills (both MANTHAN and USER_MANUAL).
//
// Idempotency: caller has already verified via PositionExistsForEntry that no
// position exists for this entry_broker_order_id. The position_events UNIQUE
// index still catches raced double-inserts.
func (s *Store) InsertEntryWithEvent(ctx context.Context, pos *Position, ev *PositionEvent) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, insertPositionSQL, insertPositionArgs(pos)...); err != nil {
			return fmt.Errorf("insert position: %w", err)
		}
		ev.PositionID = pos.PositionID
		if err := insertEventTx(ctx, tx, ev); err != nil {
			return fmt.Errorf("insert position_event: %w", err)
		}
		return nil
	})
}

// UpdateExitWithEvent atomically marks a lot EXITED (full close) + audits.
// Used on Manthan-driven SELL fills where a specific position row is the
// target and it fully closes.
func (s *Store) UpdateExitWithEvent(ctx context.Context, positionID uuid.UUID, exitPrice float64, exitBrokerOrderID, exitReason string, realizedPnL float64, ev *PositionEvent) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE positions
			SET status                = 'EXITED',
			    exit_price            = $1,
			    exit_time             = NOW(),
			    exit_reason           = $2,
			    realized_pnl          = $3,
			    exit_broker_order_id  = $4,
			    updated_at            = NOW()
			WHERE position_id = $5
			  AND status = 'ACTIVE'`,
			exitPrice, exitReason, realizedPnL, nullStr(exitBrokerOrderID), positionID)
		if err != nil {
			return fmt.Errorf("update exit: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			// Position wasn't ACTIVE (already exited by a prior call or race).
			// This is a legitimate no-op — audit still records the observation.
			s.logger.Debug("update exit: position not ACTIVE (idempotent no-op)",
				zap.String("position_id", positionID.String()))
		}
		ev.PositionID = positionID
		return insertEventTx(ctx, tx, ev)
	})
}

// UpdateCurrentSLWithEvent atomically sets positions.current_sl to newSL +
// appends an SL_MODIFIED audit row. Used every time the state machine
// observes a broker SL PENDING/STATUS_CHANGED event with a real
// trigger_price — first observation OR any subsequent ratchet.
//
// Only touches ACTIVE positions (RowsAffected=0 is a legitimate no-op:
// SL observation arriving after the position already exited via SL fire).
//
// Idempotency: same SL event replayed via Kafka → position_events UNIQUE
// (position_id, source_event_id) rejects the audit INSERT → transaction
// rolls back → current_sl unchanged. Net effect: exactly-once semantics
// even with at-least-once Kafka.
func (s *Store) UpdateCurrentSLWithEvent(ctx context.Context, positionID uuid.UUID, newSL float64, ev *PositionEvent) error {
	if newSL <= 0 {
		return fmt.Errorf("UpdateCurrentSLWithEvent: newSL must be > 0 (got %f)", newSL)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE positions
			SET current_sl = $1,
			    updated_at = NOW()
			WHERE position_id = $2
			  AND status = 'ACTIVE'`,
			newSL, positionID)
		if err != nil {
			return fmt.Errorf("update current_sl: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			s.logger.Debug("update current_sl: position not ACTIVE (idempotent no-op)",
				zap.String("position_id", positionID.String()))
		}
		ev.PositionID = positionID
		return insertEventTx(ctx, tx, ev)
	})
}

// ClearCurrentSLWithEvent atomically sets positions.current_sl to NULL +
// appends an SL_CANCELLED audit row. Used when the broker publishes a
// CANCELLED event on an SL order with filled_qty=0 (broker rejected the
// SL, rules-engine cancelled it during strategy STOP, etc.).
//
// Position is left ACTIVE — the user still owns the shares; only the
// automated protection is gone. Mobile UI reads current_sl=NULL as
// "unprotected" and can prompt the user to re-arm or exit manually.
//
// Same idempotency + no-op semantics as UpdateCurrentSLWithEvent.
func (s *Store) ClearCurrentSLWithEvent(ctx context.Context, positionID uuid.UUID, ev *PositionEvent) error {
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE positions
			SET current_sl = NULL,
			    updated_at = NOW()
			WHERE position_id = $1
			  AND status = 'ACTIVE'`,
			positionID)
		if err != nil {
			return fmt.Errorf("clear current_sl: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			s.logger.Debug("clear current_sl: position not ACTIVE (idempotent no-op)",
				zap.String("position_id", positionID.String()))
		}
		ev.PositionID = positionID
		return insertEventTx(ctx, tx, ev)
	})
}

// UpdatePartialExitWithEvent decrements quantity by delta (keeps status
// ACTIVE) + audits. Used on manual SELL fills that partially consume a lot.
// If the caller's math would drive quantity ≤ 0, they must call
// UpdateExitWithEvent instead — this method does NOT flip status.
// CorrectEntryQuantityWithEvent downsizes an ACTIVE position's quantity +
// invested_amount to reflect the broker's actual final fill count. Used by
// handleBuyFill's partial-fill correction path (see handler.go:handleBuyFill)
// when a CANCELLED-with-fill event arrives for a position that was opened
// with the ORDER quantity (because the initial WSS FILLED event had
// filled_qty=null and the state machine fell back to ev.Quantity).
//
// Only downsizes — refuses to grow. If newQuantity >= current quantity the
// UPDATE is a no-op via the WHERE clause (safer than silently accepting a
// growth request that could paper over a real bug elsewhere).
//
// Emits the passed audit event atomically in the same transaction.
func (s *Store) CorrectEntryQuantityWithEvent(ctx context.Context, positionID uuid.UUID, newQuantity int, newInvested float64, ev *PositionEvent) error {
	if newQuantity <= 0 {
		return fmt.Errorf("CorrectEntryQuantityWithEvent: newQuantity must be > 0, got %d", newQuantity)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE positions
			SET quantity        = $1,
			    invested_amount = $2,
			    updated_at      = NOW()
			WHERE position_id = $3
			  AND status = 'ACTIVE'
			  AND quantity > $1`,
			newQuantity, newInvested, positionID)
		if err != nil {
			return fmt.Errorf("correct entry qty: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return fmt.Errorf("entry qty correction not applied — position not ACTIVE or quantity <= newQuantity")
		}
		ev.PositionID = positionID
		return insertEventTx(ctx, tx, ev)
	})
}

func (s *Store) UpdatePartialExitWithEvent(ctx context.Context, positionID uuid.UUID, deltaQty int, ev *PositionEvent) error {
	if deltaQty <= 0 {
		return fmt.Errorf("UpdatePartialExitWithEvent: deltaQty must be > 0, got %d", deltaQty)
	}
	return s.inTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `
			UPDATE positions
			SET quantity   = quantity - $1,
			    updated_at = NOW()
			WHERE position_id = $2
			  AND status = 'ACTIVE'
			  AND quantity > $1`,
			deltaQty, positionID)
		if err != nil {
			return fmt.Errorf("update partial: %w", err)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			// Either not ACTIVE anymore, or the SELL would fully close the lot
			// (quantity <= delta). The caller must have decided the wrong
			// method — surface as an error so state stays consistent.
			return fmt.Errorf("partial exit not applied — position not ACTIVE or quantity <= delta")
		}
		ev.PositionID = positionID
		return insertEventTx(ctx, tx, ev)
	})
}

// ------------------------------------------------------------------------------
// SQL bits (below the API layer)
// ------------------------------------------------------------------------------

const positionColumns = `
	position_id, origin, user_id, strategy_id, signal_id,
	symbol, exchange, status,
	entry_price, entry_time, quantity, invested_amount,
	exit_price, exit_time, exit_reason, realized_pnl,
	entry_broker_order_id, exit_broker_order_id,
	current_sl, high_since_entry, last_trail_level`

const insertPositionSQL = `
	INSERT INTO positions (
	  position_id, origin, user_id, strategy_id, signal_id,
	  symbol, exchange, status,
	  entry_price, entry_time, quantity, invested_amount,
	  entry_broker_order_id,
	  current_sl, high_since_entry, last_trail_level
	) VALUES (
	  $1,  $2,  $3,  $4::uuid, $5::uuid,
	  $6,  $7,  $8,
	  $9,  $10, $11, $12,
	  $13,
	  $14, $15, $16
	)`

func insertPositionArgs(p *Position) []interface{} {
	return []interface{}{
		p.PositionID,
		p.Origin,
		p.UserID,
		nullStr(p.StrategyID),
		nullStr(p.SignalID),
		p.Symbol,
		p.Exchange,
		p.Status,
		p.EntryPrice,
		p.EntryTime,
		p.Quantity,
		p.InvestedAmount,
		nullStr(p.EntryBrokerOrderID),
		nullFloat(p.CurrentSL),
		nullFloat(p.HighSinceEntry),
		nullFloat(p.LastTrailLevel),
	}
}

// scanPosition scans one row from positions into a *Position.
// Accepts either *sql.Row or *sql.Rows via the Scanner interface.
func scanPosition(sc interface{ Scan(dest ...interface{}) error }) (*Position, error) {
	var (
		p                   Position
		strategyID          sql.NullString
		signalID            sql.NullString
		exitPrice           sql.NullFloat64
		exitTime            sql.NullTime
		exitReason          sql.NullString
		realizedPnL         sql.NullFloat64
		entryBrokerOrderID  sql.NullString
		exitBrokerOrderID   sql.NullString
		currentSL           sql.NullFloat64
		highSinceEntry      sql.NullFloat64
		lastTrailLevel      sql.NullFloat64
	)
	err := sc.Scan(
		&p.PositionID, &p.Origin, &p.UserID, &strategyID, &signalID,
		&p.Symbol, &p.Exchange, &p.Status,
		&p.EntryPrice, &p.EntryTime, &p.Quantity, &p.InvestedAmount,
		&exitPrice, &exitTime, &exitReason, &realizedPnL,
		&entryBrokerOrderID, &exitBrokerOrderID,
		&currentSL, &highSinceEntry, &lastTrailLevel,
	)
	if err != nil {
		return nil, err
	}
	p.StrategyID = strategyID.String
	p.SignalID = signalID.String
	p.ExitPrice = exitPrice.Float64
	if exitTime.Valid {
		p.ExitTime = exitTime.Time
	}
	p.ExitReason = exitReason.String
	p.RealizedPnL = realizedPnL.Float64
	p.EntryBrokerOrderID = entryBrokerOrderID.String
	p.ExitBrokerOrderID = exitBrokerOrderID.String
	p.CurrentSL = currentSL.Float64
	p.HighSinceEntry = highSinceEntry.Float64
	p.LastTrailLevel = lastTrailLevel.Float64
	return &p, nil
}

// insertEventTx appends an audit row via the given transaction.
// Duplicate (position_id, source_event_id) → ON CONFLICT DO NOTHING = no-op.
func insertEventTx(ctx context.Context, tx *sql.Tx, ev *PositionEvent) error {
	if ev.SourceEventID == "" {
		return fmt.Errorf("PositionEvent.SourceEventID is required (idempotency)")
	}
	if ev.SourceTopic == "" {
		ev.SourceTopic = "order.events"
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO position_events (
		  position_id, event_type,
		  broker_order_id, signal_id, delta_qty,
		  fill_price, realized_pnl_delta, reason,
		  raw_source_event, source_topic, source_offset, source_event_id
		) VALUES (
		  $1, $2,
		  $3, $4::uuid, $5,
		  $6, $7, $8,
		  $9::jsonb, $10, $11, $12
		)
		ON CONFLICT (position_id, source_event_id) DO NOTHING`,
		ev.PositionID, ev.EventType,
		nullStr(ev.BrokerOrderID), nullStr(ev.SignalID), nullInt(ev.DeltaQty),
		nullFloat(ev.FillPrice), nullFloat(ev.RealizedPnLDelta), nullStr(ev.Reason),
		ev.RawSourceEvent, ev.SourceTopic, nullInt64(ev.SourceOffset), ev.SourceEventID,
	)
	return err
}

// inTx wraps fn in a BEGIN/COMMIT with automatic rollback on error.
func (s *Store) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()
	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			s.logger.Warn("tx rollback after error failed",
				zap.NamedError("original", err),
				zap.NamedError("rollback", rbErr))
		}
		return err
	}
	return tx.Commit()
}

// ------------------------------------------------------------------------------
// nullable helpers — same pattern as trade-execution's repository
// ------------------------------------------------------------------------------

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullFloat(v float64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

func nullInt(v int) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

func nullInt64(v int64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}

var _ = time.Now // keep time import in case future SQL uses it directly
