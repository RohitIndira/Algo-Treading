package projector

// FillEvent is the canonical broker fill-update event consumed from the
// manthan.execution.events Kafka topic.
//
// trade-execution emits a FillEvent for every broker-side state change —
// ENTRY_FILLED, ENTRY_PARTIAL_FILL, ENTRY_REJECTED, ENTRY_TIMED_OUT,
// SL_PLACED / SL_MODIFIED / SL_REJECTED, SL_FILLED / EXIT_FILLED, the
// MANUAL_* interference detections, RECONCILER_DRIFT_FIX, etc. The
// PositionProjector dispatches on Type and applies the appropriate
// mutation to manthan_positions / manthan_signal_decisions.
//
// Backwards compat: Type can hold either a legacy or a new event-type
// string; OrderID was renamed conceptually to SignalID but the JSON
// tag stays "order_id" for back-compat. SignalID/EventSeq/Source/
// RejectionReason fields are populated by the new ManthanEventPublisher
// in trade-execution; the Resolved* methods below fall back to the
// legacy fields for older messages still in flight.
//
// Moved from internal/manthan/models.go on 2026-06-25 (Finding #3).
type FillEvent struct {
	Type          string  `json:"type"`
	EventID       string  `json:"event_id"`
	OrderID       string  `json:"order_id"` // == SignalID; kept for back-compat
	StrategyID    string  `json:"strategy_id"`
	UserID        string  `json:"user_id"`
	Symbol        string  `json:"symbol"`
	ISIN          string  `json:"isin"`
	AvgFillPrice  float64 `json:"avg_fill_price"`
	FilledQty     int32   `json:"filled_qty"`
	ExpectedQty   int32   `json:"expected_qty"`
	BrokerOrderID string  `json:"broker_order_id"`
	SLTrigger     float64 `json:"sl_trigger"`
	SLLimit       float64 `json:"sl_limit"`
	SLBrokerID    string  `json:"sl_broker_id"`
	TradingMode   string  `json:"trading_mode"`
	Timestamp     string  `json:"timestamp"`
	Sequence      int64   `json:"sequence"`

	// New canonical fields (Step 3+).
	SignalID        string `json:"signal_id,omitempty"`
	EventSeq        int64  `json:"event_seq,omitempty"`
	Source          string `json:"source,omitempty"`
	FillPrice       float64 `json:"fill_price,omitempty"`
	FillQty         int32  `json:"fill_qty,omitempty"`
	RejectionReason string `json:"rejection_reason,omitempty"`
	// ParentSignalID — non-empty when this fill is from a TOP-UP order. The
	// projector adds the new fill onto the parent's manthan_positions row
	// instead of creating a new row.
	ParentSignalID string `json:"parent_signal_id,omitempty"`
}

// ResolvedSignalID returns SignalID if set, otherwise OrderID (legacy fallback).
func (e *FillEvent) ResolvedSignalID() string {
	if e.SignalID != "" {
		return e.SignalID
	}
	return e.OrderID
}

// ResolvedEventSeq returns EventSeq if set, otherwise the legacy Sequence.
func (e *FillEvent) ResolvedEventSeq() int64 {
	if e.EventSeq != 0 {
		return e.EventSeq
	}
	return e.Sequence
}

// ResolvedFillPrice returns FillPrice if set, else legacy AvgFillPrice.
func (e *FillEvent) ResolvedFillPrice() float64 {
	if e.FillPrice != 0 {
		return e.FillPrice
	}
	return e.AvgFillPrice
}

// ResolvedFillQty returns FillQty if set, else legacy FilledQty.
func (e *FillEvent) ResolvedFillQty() int32 {
	if e.FillQty != 0 {
		return e.FillQty
	}
	return e.FilledQty
}
