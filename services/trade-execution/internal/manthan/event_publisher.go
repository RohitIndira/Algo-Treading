package manthan

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ManthanEventPublisher is the single point through which trade-execution
// reports broker activity to rules-engine via the manthan.execution.events
// Kafka topic.
//
// Every event carries:
//   - signal_id : the rules-engine-generated UUID for the originating signal.
//                 Used by the FillConsumer to look up manthan_signal_decisions
//                 and project state into manthan_positions.
//   - event_seq : a monotonic per-signal sequence number used as the
//                 idempotency key. Derived from the broker's exchange
//                 timestamp in microseconds whenever possible — that way WSS,
//                 the API poller and the reconciler all generate the SAME
//                 event_seq for the same broker event, so ON CONFLICT DO
//                 NOTHING dedupes naturally regardless of which path arrived
//                 first.
//
// Event types mirror trading_db.manthan_position_events.event_type:
//
//	ENTRY_FILLED, ENTRY_PARTIAL_FILL, ENTRY_REJECTED, ENTRY_TIMED_OUT,
//	SL_PLACED, SL_MODIFIED, SL_REJECTED, SL_FILLED,
//	EXIT_FILLED, RECONCILER_DRIFT_FIX
//
// We also keep emitting legacy FILL_CONFIRMED / FILL_UPDATE alongside
// ENTRY_FILLED / ENTRY_PARTIAL_FILL so the existing rules-engine code path
// keeps working until the new FillConsumer takes over (Step 4).
type ManthanEventPublisher struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// NewManthanEventPublisher returns a NO-OP publisher as of 2026-07-10.
//
// The manthan.execution.events topic was consumed only by rules-engine's
// projector + fill_consumer, both deleted in the signal-engine-only refactor
// (see docs/rules_engine_refactor.md §4.5). The event surface is preserved
// here so callers (entry_handler / sl_handler / safety_monitor / reconciler)
// keep compiling and can be wired to the new order.events topic once
// orderstatus svc is built.
//
// Until then: publish() sees writer==nil and returns silently. All PublishXxx
// methods no-op. The struct + call sites stay so the transition to
// orderstatus svc is a wiring change, not a rewrite.
func NewManthanEventPublisher(brokers []string, logger *zap.Logger) *ManthanEventPublisher {
	logger.Info("ManthanEventPublisher: manthan.execution.events retired — publisher is a no-op until orderstatus svc lands")
	return &ManthanEventPublisher{writer: nil, logger: logger}
}

func (p *ManthanEventPublisher) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}
	return p.writer.Close()
}

// EventEnvelope is the JSON shape on the manthan.execution.events topic.
// Field naming kept compatible with the legacy FillEvent struct in rules-engine
// so existing consumers continue parsing — new fields are additive.
type EventEnvelope struct {
	// New canonical fields
	Type            string  `json:"type"`              // ENTRY_FILLED / ENTRY_REJECTED / SL_FILLED / ...
	SignalID        string  `json:"signal_id"`         // rules-engine UUID; the idempotency root
	EventSeq        int64   `json:"event_seq"`         // microseconds — broker exch time when available
	Source          string  `json:"source"`            // WSS | API_POLLER | RECONCILER | SAFETY_MONITOR | TRADE_EXEC
	StrategyID      string  `json:"strategy_id"`
	UserID          string  `json:"user_id"`
	Symbol          string  `json:"symbol"`
	ISIN            string  `json:"isin,omitempty"`
	BrokerOrderID   string  `json:"broker_order_id,omitempty"`
	FillPrice       float64 `json:"fill_price,omitempty"`
	FillQty         int32   `json:"fill_qty,omitempty"`
	ExpectedQty     int32   `json:"expected_qty,omitempty"`
	SLTrigger       float64 `json:"sl_trigger,omitempty"`
	SLLimit         float64 `json:"sl_limit,omitempty"`
	RejectionReason string  `json:"rejection_reason,omitempty"`
	TradingMode     string  `json:"trading_mode,omitempty"`
	Timestamp       string  `json:"timestamp"` // human-readable ISO-8601
	// ParentSignalID — non-empty on top-up fill events. The projector uses
	// this to ADD the new fill onto the parent's manthan_positions row
	// (qty += FillQty, invested_amt += FillQty × FillPrice, weighted-avg
	// entry_price recomputed) instead of inserting a new row.
	ParentSignalID  string  `json:"parent_signal_id,omitempty"`

	// Legacy fields kept for backwards-compatible consumers (FILL_CONFIRMED path)
	EventID      string  `json:"event_id,omitempty"`
	OrderID      string  `json:"order_id,omitempty"`       // == SignalID
	AvgFillPrice float64 `json:"avg_fill_price,omitempty"` // == FillPrice
	FilledQty    int32   `json:"filled_qty,omitempty"`     // == FillQty
	Sequence     int64   `json:"sequence,omitempty"`       // == EventSeq
}

// publish writes one envelope, keyed by signal_id for per-signal partition
// affinity. Returns the publish error so callers can decide whether to retry
// or to swallow it (FillConsumer is reachable via reconciler if Kafka is down).
func (p *ManthanEventPublisher) publish(ctx context.Context, env EventEnvelope) error {
	if p == nil || p.writer == nil {
		return nil
	}
	if env.SignalID == "" {
		return fmt.Errorf("signal_id is required")
	}
	if env.EventSeq == 0 {
		env.EventSeq = time.Now().UnixMicro()
	}
	if env.Timestamp == "" {
		env.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	}
	// Mirror new fields to legacy field names for older consumers.
	if env.OrderID == "" {
		env.OrderID = env.SignalID
	}
	if env.AvgFillPrice == 0 {
		env.AvgFillPrice = env.FillPrice
	}
	if env.FilledQty == 0 {
		env.FilledQty = env.FillQty
	}
	if env.Sequence == 0 {
		env.Sequence = env.EventSeq
	}
	if env.EventID == "" {
		env.EventID = fmt.Sprintf("%s-%d", env.SignalID, env.EventSeq)
	}

	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(env.SignalID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(env.Type)},
			{Key: "signal_id", Value: []byte(env.SignalID)},
			{Key: "strategy_id", Value: []byte(env.StrategyID)},
			{Key: "symbol", Value: []byte(env.Symbol)},
			{Key: "source", Value: []byte(env.Source)},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Warn("manthan.execution.events publish failed",
			zap.String("type", env.Type),
			zap.String("signal_id", env.SignalID),
			zap.String("symbol", env.Symbol),
			zap.Error(err))
		return err
	}
	p.logger.Info("manthan.execution.events published",
		zap.String("type", env.Type),
		zap.String("signal_id", env.SignalID),
		zap.String("symbol", env.Symbol),
		zap.Int64("event_seq", env.EventSeq))
	return nil
}

// PublishEntryFilled — broker confirmed full entry fill. Also emits the legacy
// FILL_CONFIRMED type so existing rules-engine FillConsumer keeps working.
func (p *ManthanEventPublisher) PublishEntryFilled(ctx context.Context, signal ManthanSignal, brokerOrderID string, fillPrice float64, filledQty int32, exchTimeMicros int64) {
	seq := exchTimeMicros
	if seq == 0 {
		seq = time.Now().UnixMicro()
	}
	// Single canonical event. Earlier versions also published a legacy
	// FILL_CONFIRMED for backwards compat, but rules-engine's FillConsumer
	// + projector now handle ENTRY_FILLED natively. Dual-publish was causing
	// (a) projector CHECK-constraint failures because FILL_CONFIRMED isn't in
	// manthan_position_events.event_type, and (b) double in-memory portfolio
	// updates (active_count++ twice). One canonical event is correct.
	//
	// signal.TopUpForSignalID flows through as ParentSignalID so the rules-
	// engine projector can ADD onto the parent's manthan_positions row
	// instead of creating a new row.
	_ = p.publish(ctx, EventEnvelope{
		Type:           "ENTRY_FILLED",
		SignalID:       signal.OrderID,
		EventSeq:       seq,
		Source:         "WSS",
		StrategyID:     signal.StrategyID,
		UserID:         signal.UserID,
		Symbol:         signal.Symbol,
		ISIN:           signal.ISIN,
		BrokerOrderID:  brokerOrderID,
		FillPrice:      fillPrice,
		FillQty:        filledQty,
		ExpectedQty:    signal.Quantity,
		TradingMode:    signal.TradingMode,
		ParentSignalID: signal.TopUpForSignalID,
	})
}

// PublishEntryPartialFill — broker filled less than expected quantity.
func (p *ManthanEventPublisher) PublishEntryPartialFill(ctx context.Context, signal ManthanSignal, brokerOrderID string, fillPrice float64, filledQty int32, exchTimeMicros int64) {
	seq := exchTimeMicros
	if seq == 0 {
		seq = time.Now().UnixMicro()
	}
	// See PublishEntryFilled — we no longer dual-publish FILL_UPDATE.
	_ = p.publish(ctx, EventEnvelope{
		Type:          "ENTRY_PARTIAL_FILL",
		SignalID:      signal.OrderID,
		EventSeq:      seq,
		Source:        "WSS",
		StrategyID:    signal.StrategyID,
		UserID:        signal.UserID,
		Symbol:        signal.Symbol,
		ISIN:          signal.ISIN,
		BrokerOrderID: brokerOrderID,
		FillPrice:     fillPrice,
		FillQty:       filledQty,
		ExpectedQty:   signal.Quantity,
		TradingMode:   signal.TradingMode,
	})
}

// PublishEntryRejected — broker (or pre-check) refused the entry. This is the
// event that prevents ghost positions: rules-engine flips the decision to
// REJECTED and never writes manthan_positions.
//
// Source can be TRADE_EXEC for our local pre-check failures (DPR, freeze,
// margin, missing token) or WSS when the broker rejects after acceptance.
func (p *ManthanEventPublisher) PublishEntryRejected(ctx context.Context, signal ManthanSignal, brokerOrderID, reason, source string) {
	if source == "" {
		source = "TRADE_EXEC"
	}
	_ = p.publish(ctx, EventEnvelope{
		Type:            "ENTRY_REJECTED",
		SignalID:        signal.OrderID,
		EventSeq:        time.Now().UnixMicro(),
		Source:          source,
		StrategyID:      signal.StrategyID,
		UserID:          signal.UserID,
		Symbol:          signal.Symbol,
		ISIN:            signal.ISIN,
		BrokerOrderID:   brokerOrderID,
		ExpectedQty:     signal.Quantity,
		RejectionReason: reason,
		TradingMode:     signal.TradingMode,
	})
}

// PublishSLPlaced — broker accepted initial SL placement.
func (p *ManthanEventPublisher) PublishSLPlaced(ctx context.Context, signal ManthanSignal, slBrokerID string, trigger, limit float64) {
	_ = p.publish(ctx, EventEnvelope{
		Type:          "SL_PLACED",
		SignalID:      signal.OrderID,
		EventSeq:      time.Now().UnixMicro(),
		Source:        "TRADE_EXEC",
		StrategyID:    signal.StrategyID,
		UserID:        signal.UserID,
		Symbol:        signal.Symbol,
		BrokerOrderID: slBrokerID,
		SLTrigger:     trigger,
		SLLimit:       limit,
		TradingMode:   signal.TradingMode,
	})
}

// PublishSLModified — broker confirmed SL trigger/limit modify.
func (p *ManthanEventPublisher) PublishSLModified(ctx context.Context, signal ManthanSignal, slBrokerID string, newTrigger, newLimit float64, exchTimeMicros int64) {
	seq := exchTimeMicros
	if seq == 0 {
		seq = time.Now().UnixMicro()
	}
	_ = p.publish(ctx, EventEnvelope{
		Type:          "SL_MODIFIED",
		SignalID:      signal.OrderID,
		EventSeq:      seq,
		Source:        "WSS",
		StrategyID:    signal.StrategyID,
		UserID:        signal.UserID,
		Symbol:        signal.Symbol,
		BrokerOrderID: slBrokerID,
		SLTrigger:     newTrigger,
		SLLimit:       newLimit,
		TradingMode:   signal.TradingMode,
	})
}

// PublishSLRejected — broker rejected SL place or modify.
func (p *ManthanEventPublisher) PublishSLRejected(ctx context.Context, signal ManthanSignal, slBrokerID, reason string) {
	_ = p.publish(ctx, EventEnvelope{
		Type:            "SL_REJECTED",
		SignalID:        signal.OrderID,
		EventSeq:        time.Now().UnixMicro(),
		Source:          "TRADE_EXEC",
		StrategyID:      signal.StrategyID,
		UserID:          signal.UserID,
		Symbol:          signal.Symbol,
		BrokerOrderID:   slBrokerID,
		RejectionReason: reason,
		TradingMode:     signal.TradingMode,
	})
}

// PublishSLFilled — broker triggered SL exit. Drives manthan_positions to EXITED.
func (p *ManthanEventPublisher) PublishSLFilled(ctx context.Context, signalID, strategyID, userID, symbol, slBrokerID string, exitPrice float64, exitQty int32, exchTimeMicros int64) {
	seq := exchTimeMicros
	if seq == 0 {
		seq = time.Now().UnixMicro()
	}
	_ = p.publish(ctx, EventEnvelope{
		Type:          "SL_FILLED",
		SignalID:      signalID,
		EventSeq:      seq,
		Source:        "SAFETY_MONITOR",
		StrategyID:    strategyID,
		UserID:        userID,
		Symbol:        symbol,
		BrokerOrderID: slBrokerID,
		FillPrice:     exitPrice,
		FillQty:       exitQty,
	})
}

// PublishExitFilled — manual exit / EOD square-off / emergency market sell.
func (p *ManthanEventPublisher) PublishExitFilled(ctx context.Context, signalID, strategyID, userID, symbol, brokerOrderID, source string, exitPrice float64, exitQty int32, exchTimeMicros int64) {
	seq := exchTimeMicros
	if seq == 0 {
		seq = time.Now().UnixMicro()
	}
	if source == "" {
		source = "TRADE_EXEC"
	}
	_ = p.publish(ctx, EventEnvelope{
		Type:          "EXIT_FILLED",
		SignalID:      signalID,
		EventSeq:      seq,
		Source:        source,
		StrategyID:    strategyID,
		UserID:        userID,
		Symbol:        symbol,
		BrokerOrderID: brokerOrderID,
		FillPrice:     exitPrice,
		FillQty:       exitQty,
	})
}

// PublishManualExitDetected — broker WSS reported an EXECUTED order we didn't
// place (user manually closed a position via mobile app / web). Real-time
// path; complements the 30-min ExternalActivityDetector poll. Rules-engine
// projector flips manthan_signal_decisions → MANUALLY_EXITED, marks
// manthan_positions EXITED, sets user_override_until = NOW() + 3 days, and
// emits a user-facing notification on manthan.notifications.
func (p *ManthanEventPublisher) PublishManualExitDetected(ctx context.Context, signalID, strategyID, userID, symbol, brokerOrderID, reason string, exchTimeMicros int64) {
	seq := exchTimeMicros
	if seq == 0 {
		seq = time.Now().UnixMicro()
	}
	_ = p.publish(ctx, EventEnvelope{
		Type:            "MANUAL_EXIT_DETECTED",
		SignalID:        signalID,
		EventSeq:        seq,
		Source:          "WSS",
		StrategyID:      strategyID,
		UserID:          userID,
		Symbol:          symbol,
		BrokerOrderID:   brokerOrderID,
		RejectionReason: reason,
	})
}

// PublishReconcilerDriftFix — reconciler detected DB ↔ broker drift and fixed
// it. Lets rules-engine know its in-memory state needs correction.
func (p *ManthanEventPublisher) PublishReconcilerDriftFix(ctx context.Context, signalID, strategyID, userID, symbol, brokerOrderID, brokerStatus, detail string) {
	_ = p.publish(ctx, EventEnvelope{
		Type:            "RECONCILER_DRIFT_FIX",
		SignalID:        signalID,
		EventSeq:        time.Now().UnixMicro(),
		Source:          "RECONCILER",
		StrategyID:      strategyID,
		UserID:          userID,
		Symbol:          symbol,
		BrokerOrderID:   brokerOrderID,
		RejectionReason: fmt.Sprintf("broker=%s detail=%s", brokerStatus, detail),
	})
}
