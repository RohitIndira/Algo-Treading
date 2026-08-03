// Package store owns the broker_events table writes for orderstatus svc.
//
// Append-only, idempotent. Every observation (WSS event, REST orderbook row,
// reconciler drift fix) becomes one INSERT here. The UNIQUE (broker_order_id,
// event_seq) constraint dedupes when the same broker event races us via
// multiple paths (WSS pushed first → REST poll observed same event 15s later
// → INSERT with same event_seq → ON CONFLICT DO NOTHING).
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
)

// Source enum — where an event was observed.
type Source string

const (
	SourceWSS            Source = "WSS"
	SourceRESTOrderbook  Source = "REST_ORDERBOOK"
	SourceRESTReconciler Source = "REST_RECONCILER"
	SourceRESTTradebook  Source = "REST_TRADEBOOK"
)

// EventType maps to `broker_events.event_type`. Matches the verified WSS
// state machine (docs/codify_wss_state_machine.md).
type EventType string

const (
	EventPlaced          EventType = "PLACED"
	EventStatusChanged   EventType = "STATUS_CHANGED"
	EventFilled          EventType = "FILLED"
	EventPartiallyFilled EventType = "PARTIALLY_FILLED"
	EventCancelled       EventType = "CANCELLED"
	EventRejected        EventType = "REJECTED"
	EventModified        EventType = "MODIFIED"
	EventTriggered       EventType = "TRIGGERED"
	EventExpired         EventType = "EXPIRED"
)

// Event is the payload orderstatus svc writes on every observation.
// Field names match broker_events columns 1:1.
type Event struct {
	BrokerOrderID   string    // Codify UniqueCode / nstOrdNo — the join key everywhere
	ExchangeOrderID string    // NSE OrderNumber ("0" pre-exchange)
	EventSeq        int64     // Broker exch time in microseconds — deterministic across paths
	Source          Source
	EventType       EventType
	Status          string // verified enum: PENDING/EXECUTED/CANCELLED/A.REJECTED/ORDER ERROR/ADMIN PENDING
	OMSStatusCode   int    // Codify OMSOrderStatus (8=admin pending, 10=cancel-of-dead, 15=fresh reject)

	UserID   string
	Symbol   string
	Exchange string
	BuySell  string // '1'=BUY '2'=SELL
	OrderType string // REGULAR LIMIT / REGULAR MARKET / SL LIMIT / etc.
	Product  string

	OrderPrice   float64
	TriggerPrice float64
	Quantity     int
	FilledQty    int
	TradedPrice  float64
	PendingQty   int
	Reason       string

	RawPayload []byte // full source event JSON — for forensics + reconstruction
	BrokerTsMs int64  // broker's own timestamp (millis); 0 for REST-only paths
}

// Writer owns broker_events INSERTs. Thin wrapper around *sql.DB — the
// idempotency guarantee lives in the SQL, not here.
type Writer struct {
	db     *sql.DB
	logger *zap.Logger
}

func NewWriter(db *sql.DB, logger *zap.Logger) *Writer {
	return &Writer{db: db, logger: logger}
}

// LatestObservedAt returns MAX(observed_at) from broker_events. Used by the
// reconciler at boot to build a cutoff: only Indira orders newer than this
// timestamp get re-published to Kafka. Prevents a fresh install / DB flush
// from re-publishing every historical order still in Indira's orderbook
// (2026-07-17 incident: flushing DBs then rebooting orderstatus svc caused
// the reconciler's first sweep to publish 42 morning IDEA test orders + all
// prior strategy runs as if they were fresh events, corrupting positions_db).
//
// Returns (zero time, nil) when the table is empty — caller uses time.Now()
// as the cutoff in that case.
func (w *Writer) LatestObservedAt(ctx context.Context) (sql.NullTime, error) {
	var t sql.NullTime
	err := w.db.QueryRowContext(ctx, `SELECT MAX(observed_at) FROM broker_events`).Scan(&t)
	return t, err
}

// Insert appends one event. Returns (id, inserted, error).
//
//	inserted = true  → new row (id is its primary key), publish to order.events
//	inserted = false → duplicate (WSS + REST race), id = 0; silently swallow —
//	                   the original writer already handled downstream fan-out
//
// Never UPDATE existing rows; new events always mean a new row keyed by
// (broker_order_id, event_seq). The returned id lets the caller stamp
// published_at after a successful inline publish; rows left unstamped are
// retried by publisher.RunWorker.
func (w *Writer) Insert(ctx context.Context, ev *Event) (int64, bool, error) {
	if ev.BrokerOrderID == "" {
		return 0, false, fmt.Errorf("broker_order_id is required")
	}
	if ev.EventSeq == 0 {
		return 0, false, fmt.Errorf("event_seq is required (deterministic dedup key)")
	}
	if ev.RawPayload == nil {
		// Empty JSON object is a valid payload — but let's not silently persist
		// nothing. Force callers to think about it.
		return 0, false, fmt.Errorf("raw_payload is required")
	}
	if !json.Valid(ev.RawPayload) {
		return 0, false, fmt.Errorf("raw_payload is not valid JSON")
	}

	var id int64
	err := w.db.QueryRowContext(ctx, `
		INSERT INTO broker_events (
			broker_order_id, exchange_order_id, event_seq,
			source, event_type, status, oms_status_code,
			user_id, symbol, exchange, buy_sell, order_type, product,
			order_price, trigger_price, quantity, filled_qty,
			traded_price, pending_qty, reason,
			raw_payload, broker_ts_ms
		) VALUES (
			$1,  $2,  $3,
			$4,  $5,  $6,  $7,
			$8,  $9,  $10, $11, $12, $13,
			$14, $15, $16, $17,
			$18, $19, $20,
			$21::jsonb, $22
		)
		ON CONFLICT (broker_order_id, event_seq) DO NOTHING
		RETURNING id`,
		ev.BrokerOrderID, nullStr(ev.ExchangeOrderID), ev.EventSeq,
		string(ev.Source), string(ev.EventType), nullStr(ev.Status), ev.OMSStatusCode,
		ev.UserID, nullStr(ev.Symbol), nullStr(ev.Exchange),
		nullStr(ev.BuySell), nullStr(ev.OrderType), nullStr(ev.Product),
		nullFloat(ev.OrderPrice), nullFloat(ev.TriggerPrice),
		nullInt(ev.Quantity), nullInt(ev.FilledQty),
		nullFloat(ev.TradedPrice), nullInt(ev.PendingQty), nullStr(ev.Reason),
		ev.RawPayload, ev.BrokerTsMs,
	).Scan(&id)
	if err == sql.ErrNoRows {
		// ON CONFLICT DO NOTHING produced no row → duplicate.
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("insert broker_events: %w", err)
	}
	return id, true, nil
}

// ConflictRow describes the pre-existing broker_events row that a would-be
// insert collided with on (broker_order_id, event_seq). Used only for the
// tradebook cross-source-collision WARN.
type ConflictRow struct {
	Source   string
	EventSeq int64
	EventTyp string
}

// LookupConflictSource returns the existing row at (brokerOrderID, eventSeq)
// so a caller whose Insert was deduped (inserted=false) can log which source
// already owned that key. Returns (nil, nil) if no such row (race — already
// gone). Read-only.
func (w *Writer) LookupConflictSource(ctx context.Context, brokerOrderID string, eventSeq int64) (*ConflictRow, error) {
	var c ConflictRow
	err := w.db.QueryRowContext(ctx,
		`SELECT source, event_seq, event_type FROM broker_events
		 WHERE broker_order_id = $1 AND event_seq = $2`,
		brokerOrderID, eventSeq).Scan(&c.Source, &c.EventSeq, &c.EventTyp)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup conflict row: %w", err)
	}
	return &c, nil
}

// MarkPublished stamps published_at=NOW() on a row after it has been produced
// to order.events. Called on the inline happy path and by RunWorker. Idempotent
// — re-stamping an already-published row is harmless.
func (w *Writer) MarkPublished(ctx context.Context, id int64) error {
	_, err := w.db.ExecContext(ctx,
		`UPDATE broker_events SET published_at = NOW() WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("mark published id=%d: %w", id, err)
	}
	return nil
}

// PersistedEvent is a broker_events row plus its primary key, used by the
// durability worker to republish and then stamp rows that never reached Kafka.
type PersistedEvent struct {
	ID int64
	Event
}

// FetchUnpublished returns up to `limit` rows that were never confirmed on
// order.events (published_at IS NULL) AND are older than graceSeconds, oldest
// first. Publishing in id order preserves per-broker_order_id FIFO.
//
// The graceSeconds floor is what makes the inline-publish path and the worker
// non-overlapping: fresh rows (younger than the grace period) are owned by the
// inline publisher, so the worker only ever republishes rows old enough that
// the inline publish must have genuinely failed. Pass 0 to disable (tests).
func (w *Writer) FetchUnpublished(ctx context.Context, limit int, graceSeconds float64) ([]*PersistedEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := w.db.QueryContext(ctx, `
		SELECT id, broker_order_id, exchange_order_id, event_seq,
		       source, event_type, status, oms_status_code,
		       user_id, symbol, exchange, buy_sell, order_type, product,
		       order_price, trigger_price, quantity, filled_qty,
		       traded_price, pending_qty, reason,
		       raw_payload, broker_ts_ms
		FROM broker_events
		WHERE published_at IS NULL
		  AND observed_at < NOW() - make_interval(secs => $2)
		ORDER BY id ASC
		LIMIT $1`, limit, graceSeconds)
	if err != nil {
		return nil, fmt.Errorf("fetch unpublished: %w", err)
	}
	defer rows.Close()

	var out []*PersistedEvent
	for rows.Next() {
		var (
			pe                                    PersistedEvent
			exchangeOrderID, status, symbol       sql.NullString
			exchange, buySell, orderType, product sql.NullString
			reason                                sql.NullString
			orderPrice, triggerPrice, tradedPrice sql.NullFloat64
			quantity, filledQty, pendingQty       sql.NullInt64
			omsStatusCode                         sql.NullInt64
			brokerTsMs                            sql.NullInt64
			source, eventType                     string
		)
		if err := rows.Scan(
			&pe.ID, &pe.BrokerOrderID, &exchangeOrderID, &pe.EventSeq,
			&source, &eventType, &status, &omsStatusCode,
			&pe.UserID, &symbol, &exchange, &buySell, &orderType, &product,
			&orderPrice, &triggerPrice, &quantity, &filledQty,
			&tradedPrice, &pendingQty, &reason,
			&pe.RawPayload, &brokerTsMs,
		); err != nil {
			return nil, fmt.Errorf("scan unpublished row: %w", err)
		}
		pe.Source = Source(source)
		pe.EventType = EventType(eventType)
		pe.ExchangeOrderID = exchangeOrderID.String
		pe.Status = status.String
		pe.OMSStatusCode = int(omsStatusCode.Int64)
		pe.Symbol = symbol.String
		pe.Exchange = exchange.String
		pe.BuySell = buySell.String
		pe.OrderType = orderType.String
		pe.Product = product.String
		pe.OrderPrice = orderPrice.Float64
		pe.TriggerPrice = triggerPrice.Float64
		pe.Quantity = int(quantity.Int64)
		pe.FilledQty = int(filledQty.Int64)
		pe.TradedPrice = tradedPrice.Float64
		pe.PendingQty = int(pendingQty.Int64)
		pe.Reason = reason.String
		pe.BrokerTsMs = brokerTsMs.Int64
		out = append(out, &pe)
	}
	return out, rows.Err()
}

// nullStr converts empty string to sql.NullString.
func nullStr(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// nullInt converts zero to sql.NullInt64 (Postgres NULL).
// Zero is treated as "unset" — traders don't place 0-qty orders.
func nullInt(v int) sql.NullInt64 {
	if v == 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: int64(v), Valid: true}
}

// nullFloat converts zero to sql.NullFloat64.
func nullFloat(v float64) sql.NullFloat64 {
	if v == 0 {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{Float64: v, Valid: true}
}
