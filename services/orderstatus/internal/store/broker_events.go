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

// Insert appends one event. Returns (inserted bool, error).
//
//	inserted = true  → new row, publish to order.events Kafka topic downstream
//	inserted = false → duplicate (WSS + REST race), silently swallow — the
//	                    original writer already handled downstream fan-out
//
// Never UPDATE existing rows; new events always mean a new row keyed by
// (broker_order_id, event_seq).
func (w *Writer) Insert(ctx context.Context, ev *Event) (bool, error) {
	if ev.BrokerOrderID == "" {
		return false, fmt.Errorf("broker_order_id is required")
	}
	if ev.EventSeq == 0 {
		return false, fmt.Errorf("event_seq is required (deterministic dedup key)")
	}
	if ev.RawPayload == nil {
		// Empty JSON object is a valid payload — but let's not silently persist
		// nothing. Force callers to think about it.
		return false, fmt.Errorf("raw_payload is required")
	}
	if !json.Valid(ev.RawPayload) {
		return false, fmt.Errorf("raw_payload is not valid JSON")
	}

	res, err := w.db.ExecContext(ctx, `
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
		ON CONFLICT (broker_order_id, event_seq) DO NOTHING`,
		ev.BrokerOrderID, nullStr(ev.ExchangeOrderID), ev.EventSeq,
		string(ev.Source), string(ev.EventType), nullStr(ev.Status), ev.OMSStatusCode,
		ev.UserID, nullStr(ev.Symbol), nullStr(ev.Exchange),
		nullStr(ev.BuySell), nullStr(ev.OrderType), nullStr(ev.Product),
		nullFloat(ev.OrderPrice), nullFloat(ev.TriggerPrice),
		nullInt(ev.Quantity), nullInt(ev.FilledQty),
		nullFloat(ev.TradedPrice), nullInt(ev.PendingQty), nullStr(ev.Reason),
		ev.RawPayload, ev.BrokerTsMs,
	)
	if err != nil {
		return false, fmt.Errorf("insert broker_events: %w", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		// Postgres always reports RowsAffected — if this fails, something is off.
		return false, fmt.Errorf("rows affected: %w", err)
	}
	return rows == 1, nil
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
