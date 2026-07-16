// Package statemachine turns order.events messages into positions state
// transitions per §7 of docs/positions_service_design.md. Replaces the
// P.B LoggingHandler stub.
//
// The state machine is the ONLY place `realized_pnl` gets computed —
// closing that gap is the original reason for the whole positions svc
// extraction.
package statemachine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/tradeexec"
)

// OrderMetaLookup is the surface the state machine needs from the
// trade-execution client. Small interface makes tests easy — production
// uses *tradeexec.Client, tests use an in-memory stub.
type OrderMetaLookup interface {
	LookupOrderMeta(ctx context.Context, brokerOrderID string) (tradeexec.OrderMeta, error)
}

// EventPublisher is the surface the state machine calls after every
// successful DB mutation. Prod uses *publisher.Publisher; tests pass nil
// (Publish is nil-safe) or a capturing stub for assertions.
type EventPublisher interface {
	Publish(ctx context.Context, ev *publisher.PositionEvent)
}

// Handler implements consumer.Handler. Injected into the OrderEventsConsumer
// in main.go — replaces LoggingHandler once P.C is wired.
type Handler struct {
	store  *store.Store
	lookup OrderMetaLookup
	pub    EventPublisher // may be nil — publish paths are nil-safe
	logger *zap.Logger
}

func New(s *store.Store, lookup OrderMetaLookup, pub EventPublisher, logger *zap.Logger) *Handler {
	return &Handler{store: s, lookup: lookup, pub: pub, logger: logger}
}

// Handle routes one parsed OrderEvent per §7 of the design doc.
//
// Events that drive state transitions today:
//
//	FILLED                        — full fill of BUY or SELL
//	CANCELLED   (filled_qty>0)    — partial-fill-then-cancelled (2026-07-16
//	                                CUB pattern: broker accepted qty=57 of
//	                                intended qty=90, cancelled the remaining
//	                                33 at 3:30 EOD). Treat filled_qty as
//	                                a legitimate fill event.
//	PARTIALLY_FILLED              — mid-flight partial (rare on Manthan
//	                                DELIVERY; broker usually rolls to CANCELLED
//	                                or FILLED). Same handling as CANCELLED
//	                                with filled_qty>0 — accept what filled.
//
// Other event types (STATUS_CHANGED, MODIFIED, REJECTED, TRIGGERED,
// EXPIRED) are logged + no-op'd. SL_MODIFY tracking (updating
// positions.current_sl on trailing SL ratchets) is Chunk P.E — not
// required for the position-open/exit lifecycle.
//
// buy_sell is normalized to handle the split wire format:
//
//	WSS source:            "1" (BUY) / "2" (SELL) — numeric enum from
//	                       Codifi's WSS envelope
//	REST_ORDERBOOK source: "BUY" / "SELL" — Indira's REST portfolio-services
//	                       API uses full strings
//
// Without normalization, REST events (which is what today's post-JWT-fix
// path emits — the WSS listener sometimes misses AMO-execute events) are
// silently skipped, leaving positions_db empty despite broker fills.
// Root-caused during 2026-07-16 end-to-end debug (AADHARHFC 38,
// IPCALAB 10, CUB 57 all filled at broker; 0 rows in positions_db).
//
// Returns an error only when the consumer should NOT commit the offset
// (message re-delivers). Idempotency is enforced at the DB layer via:
//
//	positions.entry_broker_order_id UNIQUE-ish idempotency for BUY inserts
//	position_events (position_id, source_event_id) UNIQUE for audit rows
func (h *Handler) Handle(ctx context.Context, ev *consumer.OrderEvent) error {
	if !isFillEvent(ev) {
		h.logger.Debug("state-machine: skip non-fill event",
			zap.String("event_id", ev.EventID),
			zap.String("event_type", ev.EventType),
			zap.Int("filled_qty", ev.FilledQty),
			zap.String("broker_order_id", ev.BrokerOrderID))
		return nil
	}

	switch normalizeBuySell(ev.BuySell) {
	case "BUY":
		return h.handleBuyFill(ctx, ev)
	case "SELL":
		return h.handleSellFill(ctx, ev)
	default:
		h.logger.Warn("state-machine: FILLED with unknown buy_sell — skipping",
			zap.String("event_id", ev.EventID),
			zap.String("buy_sell", ev.BuySell))
		return nil
	}
}

// isFillEvent returns true iff the event should trigger a position state
// transition. Accepts:
//   - event_type == "FILLED"                              — full fill
//   - event_type == "PARTIALLY_FILLED" AND filled_qty > 0 — mid-flight partial
//   - event_type == "CANCELLED"        AND filled_qty > 0 — partial-fill-then-
//                                                            cancelled
//
// Explicitly EXCLUDES event_type in {STATUS_CHANGED, MODIFIED, REJECTED,
// TRIGGERED, EXPIRED}. Also excludes CANCELLED events with filled_qty=0
// (the common case — user cancelled a resting order that never touched).
func isFillEvent(ev *consumer.OrderEvent) bool {
	switch ev.EventType {
	case "FILLED":
		return true
	case "PARTIALLY_FILLED", "CANCELLED":
		return ev.FilledQty > 0
	default:
		return false
	}
}

// normalizeBuySell maps the two wire formats to a single canonical value.
// Case-insensitive input; canonical output is uppercase "BUY" / "SELL".
// Empty / unknown returns "" so the Handle default branch fires.
func normalizeBuySell(v string) string {
	switch strings.ToUpper(strings.TrimSpace(v)) {
	case "1", "B", "BUY":
		return "BUY"
	case "2", "S", "SELL":
		return "SELL"
	default:
		return ""
	}
}

// ----------------------------------------------------------------------
// BUY fills — §7.1 of the design doc
// ----------------------------------------------------------------------

func (h *Handler) handleBuyFill(ctx context.Context, ev *consumer.OrderEvent) error {
	// Idempotency short-circuit: BUY event replayed → position row already exists.
	exists, err := h.store.PositionExistsForEntry(ctx, ev.BrokerOrderID)
	if err != nil {
		return fmt.Errorf("BUY idempotency check: %w", err)
	}
	if exists {
		h.logger.Debug("state-machine: BUY replay ignored (position row exists)",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("event_id", ev.EventID))
		return nil
	}

	// LookupOrderMeta enriches the event with signal_id + strategy_id when the
	// broker order came from Manthan. Found=false = user manual buy.
	meta, err := h.lookup.LookupOrderMeta(ctx, ev.BrokerOrderID)
	if err != nil {
		return fmt.Errorf("BUY LookupOrderMeta: %w", err)
	}

	entryPrice := ev.TradedPrice
	if entryPrice == 0 {
		entryPrice = ev.OrderPrice
	}
	qty := ev.FilledQty
	if qty == 0 {
		qty = ev.Quantity
	}
	if entryPrice == 0 || qty == 0 {
		h.logger.Warn("state-machine: BUY FILLED with 0 price or qty — skipping",
			zap.String("event_id", ev.EventID),
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.Float64("traded_price", ev.TradedPrice),
			zap.Int("filled_qty", ev.FilledQty))
		return nil
	}

	// Normalize the wire symbol (STK_AADHARHFC_EQ_NSE_23729) to the canonical
	// short form + separate exchange. positions_db.positions is the SSOT for
	// downstream (LTP enricher, mobile UI, portfolio API); all of them expect
	// short symbols. Unrecognized shapes pass through — see normalizeSymbol
	// docstring.
	//
	// If the wire form parsed → use its exchange. Otherwise fall back to
	// ev.Exchange (WSS emits numeric enum "1"/"2" — trade-execution's earlier
	// path stored these verbatim as "1"; not ideal but existing rows). Better
	// to write SOMETHING sensible than crash the pipeline.
	normSymbol, normExchange := normalizeSymbol(ev.Symbol)
	if normExchange == "" {
		normExchange = ev.Exchange
	}

	pos := &store.Position{
		PositionID:         uuid.New(),
		UserID:             ev.UserID,
		Symbol:             normSymbol,
		Exchange:           normExchange,
		Status:             store.StatusActive,
		EntryPrice:         entryPrice,
		EntryTime:          time.Now(),
		Quantity:           qty,
		InvestedAmount:     entryPrice * float64(qty),
		EntryBrokerOrderID: ev.BrokerOrderID,
	}

	eventType := store.EventTypeUserManualEntry
	if meta.Found {
		pos.Origin = store.OriginManthan
		pos.SignalID = meta.SignalID
		pos.StrategyID = meta.StrategyID
		eventType = store.EventTypeEntryFilled
	} else {
		pos.Origin = store.OriginUserManual
	}

	auditEvent := &store.PositionEvent{
		EventType:     eventType,
		BrokerOrderID: ev.BrokerOrderID,
		SignalID:      pos.SignalID,
		DeltaQty:      qty,
		FillPrice:     entryPrice,
		RawSourceEvent: ev.RawMessage,
		SourceEventID: ev.EventID,
	}

	if err := h.store.InsertEntryWithEvent(ctx, pos, auditEvent); err != nil {
		return fmt.Errorf("BUY insert: %w", err)
	}

	h.logger.Info("state-machine: BUY position opened",
		zap.String("position_id", pos.PositionID.String()),
		zap.String("origin", pos.Origin),
		zap.String("user_id", pos.UserID),
		zap.String("symbol", pos.Symbol),
		zap.Int("qty", pos.Quantity),
		zap.Float64("entry_price", pos.EntryPrice),
		zap.String("signal_id", pos.SignalID),
		zap.String("broker_order_id", ev.BrokerOrderID))

	if h.pub != nil {
		h.pub.Publish(ctx, &publisher.PositionEvent{
			EventType:     publisher.EventPositionOpened,
			PositionID:    pos.PositionID.String(),
			Origin:        pos.Origin,
			UserID:        pos.UserID,
			StrategyID:    pos.StrategyID,
			SignalID:      pos.SignalID,
			Symbol:        pos.Symbol,
			Action:        publisher.ActionEntry,
			Price:         pos.EntryPrice,
			Quantity:      pos.Quantity,
			BrokerOrderID: ev.BrokerOrderID,
		})
	}
	return nil
}

// ----------------------------------------------------------------------
// SELL fills — §7.2 of the design doc
// ----------------------------------------------------------------------

func (h *Handler) handleSellFill(ctx context.Context, ev *consumer.OrderEvent) error {
	meta, err := h.lookup.LookupOrderMeta(ctx, ev.BrokerOrderID)
	if err != nil {
		return fmt.Errorf("SELL LookupOrderMeta: %w", err)
	}

	if meta.Found && isSellOrderType(meta.OrderType) {
		return h.handleManthanSellFill(ctx, ev, meta)
	}
	return h.handleManualSellFill(ctx, ev)
}

// handleManthanSellFill — Manthan-driven exit. Look up the specific parent
// lot via entry_signal_id, close it, compute realized_pnl.
//
// Per §7.2 Manthan-driven branch: "only ONE lot exits per SL fire".
func (h *Handler) handleManthanSellFill(ctx context.Context, ev *consumer.OrderEvent, meta tradeexec.OrderMeta) error {
	entrySignalID := meta.EntrySignalID
	if entrySignalID == "" {
		// Shouldn't happen — trade-exec falls back to signal_id for entry rows.
		h.logger.Warn("state-machine: Manthan SELL with no entry_signal_id — skipping",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("meta_signal_id", meta.SignalID),
			zap.String("meta_order_type", meta.OrderType))
		return nil
	}

	pos, err := h.store.FindManthanLotBySignalID(ctx, entrySignalID)
	if err != nil {
		return fmt.Errorf("Manthan SELL find lot: %w", err)
	}
	if pos == nil {
		h.logger.Warn("state-machine: Manthan SELL — no ACTIVE lot with that signal_id (drift?)",
			zap.String("entry_signal_id", entrySignalID),
			zap.String("broker_order_id", ev.BrokerOrderID))
		return nil // don't block the offset commit; drift alert path handles it later
	}

	exitPrice := ev.TradedPrice
	if exitPrice == 0 {
		exitPrice = ev.OrderPrice
	}
	if exitPrice == 0 {
		h.logger.Warn("state-machine: Manthan SELL with 0 exit_price — skipping",
			zap.String("event_id", ev.EventID))
		return nil
	}

	// realized_pnl = (exit - entry) × qty_being_closed.
	// For the specific-lot Manthan branch, we close the WHOLE lot in one shot.
	qtyClosed := pos.Quantity
	realizedPnL := (exitPrice - pos.EntryPrice) * float64(qtyClosed)

	auditEvent := &store.PositionEvent{
		EventType:        store.EventTypeSLFilled,
		BrokerOrderID:    ev.BrokerOrderID,
		SignalID:         meta.SignalID,
		DeltaQty:         -qtyClosed,
		FillPrice:        exitPrice,
		RealizedPnLDelta: realizedPnL,
		RawSourceEvent:   ev.RawMessage,
		SourceEventID:    ev.EventID,
	}

	if err := h.store.UpdateExitWithEvent(ctx, pos.PositionID, exitPrice, ev.BrokerOrderID,
		store.ExitReasonSLTrigger, realizedPnL, auditEvent); err != nil {
		return fmt.Errorf("Manthan SELL update: %w", err)
	}

	h.logger.Info("state-machine: Manthan lot EXITED",
		zap.String("position_id", pos.PositionID.String()),
		zap.String("user_id", pos.UserID),
		zap.String("symbol", pos.Symbol),
		zap.Int("qty_closed", qtyClosed),
		zap.Float64("entry_price", pos.EntryPrice),
		zap.Float64("exit_price", exitPrice),
		zap.Float64("realized_pnl", realizedPnL))

	if h.pub != nil {
		h.pub.Publish(ctx, &publisher.PositionEvent{
			EventType:     publisher.EventPositionExited,
			PositionID:    pos.PositionID.String(),
			Origin:        pos.Origin,
			UserID:        pos.UserID,
			StrategyID:    pos.StrategyID,
			SignalID:      pos.SignalID,
			Symbol:        pos.Symbol,
			Action:        publisher.ActionExit,
			Price:         exitPrice,
			Quantity:      qtyClosed,
			ExitReason:    store.ExitReasonSLTrigger,
			RealizedPnL:   realizedPnL,
			BrokerOrderID: ev.BrokerOrderID,
		})
	}
	return nil
}

// handleManualSellFill — user sold via broker app. FIFO across ACTIVE lots
// per §7.2 manual-sell branch:
//
//	1. USER_MANUAL lots first (entry_time ASC)
//	2. MANTHAN lots next        (entry_time ASC)
//
// Each lot fully-consumed by the SELL flips EXITED. If a lot is only
// partially consumed (last remaining lot when qty runs out), quantity is
// decremented and status stays ACTIVE.
//
// Manthan lots consumed by manual SELL get exit_reason=MANUAL_EXIT — user
// reached into the strategy's shares. realized_pnl still computed vs the
// lot's original entry_price so per-strategy PnL stays honest.
func (h *Handler) handleManualSellFill(ctx context.Context, ev *consumer.OrderEvent) error {
	sellQty := ev.FilledQty
	if sellQty == 0 {
		sellQty = ev.Quantity
	}
	if sellQty <= 0 {
		h.logger.Warn("state-machine: manual SELL with 0 qty — skipping",
			zap.String("event_id", ev.EventID))
		return nil
	}
	exitPrice := ev.TradedPrice
	if exitPrice == 0 {
		exitPrice = ev.OrderPrice
	}
	if exitPrice == 0 {
		h.logger.Warn("state-machine: manual SELL with 0 exit_price — skipping",
			zap.String("event_id", ev.EventID))
		return nil
	}

	// Normalize the SELL event's symbol before FIFO lookup — positions_db.symbol
	// is stored canonically (e.g. "AADHARHFC") but the SELL event may arrive
	// with the raw wire form ("STK_AADHARHFC_EQ_NSE_23729"). Without this
	// normalize step every SELL from the REST_ORDERBOOK path would fail to
	// match its parent BUY position and log "no ACTIVE lots for user/symbol"
	// even though the position clearly exists (verified 2026-07-16).
	normSymbol, _ := normalizeSymbol(ev.Symbol)

	lots, err := h.store.FindActiveLotsFIFO(ctx, ev.UserID, normSymbol)
	if err != nil {
		return fmt.Errorf("manual SELL find lots: %w", err)
	}
	if len(lots) == 0 {
		h.logger.Warn("state-machine: manual SELL with no ACTIVE lots for user/symbol",
			zap.String("user_id", ev.UserID),
			zap.String("symbol_raw", ev.Symbol),
			zap.String("symbol_normalized", normSymbol),
			zap.String("event_id", ev.EventID))
		return nil // drift; positions.drift.detected covers this in P.G
	}

	remaining := sellQty
	touched := 0

	for _, lot := range lots {
		if remaining == 0 {
			break
		}

		// How much of THIS lot does the sell consume?
		delta := lot.Quantity
		if delta > remaining {
			delta = remaining
		}
		remaining -= delta

		// realized_pnl for the portion of THIS lot being closed.
		realizedPnL := (exitPrice - lot.EntryPrice) * float64(delta)

		// Suffix the source_event_id with the position_id so a single Kafka
		// event that touches N lots still yields N unique idempotency keys.
		// Without this the second UPDATE would collide on the position_events
		// UNIQUE index — first-writer-wins semantics we don't want here.
		perLotEventID := fmt.Sprintf("%s#%s", ev.EventID, lot.PositionID.String())
		auditEvent := &store.PositionEvent{
			EventType:        store.EventTypeManualSellApplied,
			BrokerOrderID:    ev.BrokerOrderID,
			SignalID:         lot.SignalID, // may be "" for USER_MANUAL
			DeltaQty:         -delta,
			FillPrice:        exitPrice,
			RealizedPnLDelta: realizedPnL,
			Reason:           fmt.Sprintf("manual SELL of %d from lot %s", delta, lot.Origin),
			RawSourceEvent:   ev.RawMessage,
			SourceEventID:    perLotEventID,
		}

		if delta == lot.Quantity {
			// Full close.
			if err := h.store.UpdateExitWithEvent(ctx, lot.PositionID, exitPrice,
				ev.BrokerOrderID, store.ExitReasonManualExit, realizedPnL, auditEvent); err != nil {
				return fmt.Errorf("manual SELL close lot: %w", err)
			}
			h.logger.Info("state-machine: manual SELL closed lot",
				zap.String("position_id", lot.PositionID.String()),
				zap.String("origin", lot.Origin),
				zap.Int("qty_closed", delta),
				zap.Float64("entry_price", lot.EntryPrice),
				zap.Float64("exit_price", exitPrice),
				zap.Float64("realized_pnl", realizedPnL))

			if h.pub != nil {
				h.pub.Publish(ctx, &publisher.PositionEvent{
					EventType:     publisher.EventPositionExited,
					PositionID:    lot.PositionID.String(),
					Origin:        lot.Origin,
					UserID:        lot.UserID,
					StrategyID:    lot.StrategyID,
					SignalID:      lot.SignalID,
					Symbol:        lot.Symbol,
					Action:        publisher.ActionExit,
					Price:         exitPrice,
					Quantity:      delta,
					ExitReason:    store.ExitReasonManualExit,
					RealizedPnL:   realizedPnL,
					BrokerOrderID: ev.BrokerOrderID,
				})
			}
		} else {
			// Partial — decrement qty, status stays ACTIVE.
			if err := h.store.UpdatePartialExitWithEvent(ctx, lot.PositionID, delta, auditEvent); err != nil {
				return fmt.Errorf("manual SELL partial: %w", err)
			}
			h.logger.Info("state-machine: manual SELL partial",
				zap.String("position_id", lot.PositionID.String()),
				zap.String("origin", lot.Origin),
				zap.Int("qty_reduced_by", delta),
				zap.Int("qty_remaining", lot.Quantity-delta),
				zap.Float64("realized_pnl_delta", realizedPnL))

			// Partial manual exits still publish POSITION_MODIFIED so downstream
			// consumers see the shrink. Consumers can distinguish full vs
			// partial by comparing action=EXIT with the position's current
			// quantity — POSITION_EXITED means status=EXITED at the DB.
			if h.pub != nil {
				h.pub.Publish(ctx, &publisher.PositionEvent{
					EventType:     publisher.EventManualInterrupt,
					PositionID:    lot.PositionID.String(),
					Origin:        lot.Origin,
					UserID:        lot.UserID,
					StrategyID:    lot.StrategyID,
					SignalID:      lot.SignalID,
					Symbol:        lot.Symbol,
					Action:        publisher.ActionExit,
					Price:         exitPrice,
					Quantity:      delta,
					RealizedPnL:   realizedPnL,
					BrokerOrderID: ev.BrokerOrderID,
				})
			}
		}
		touched++
	}

	if remaining > 0 {
		// User sold more than our books recorded — genuine drift.
		// Skip the offset commit? No — that would just replay the same
		// oversupply forever. Log it; the reconciler+drift topic (P.G)
		// is the right place to alert.
		h.logger.Warn("state-machine: manual SELL exceeded ACTIVE qty (drift)",
			zap.String("user_id", ev.UserID),
			zap.String("symbol", normSymbol),
			zap.Int("sell_qty", sellQty),
			zap.Int("overshoot", remaining),
			zap.Int("lots_touched", touched))
	}
	return nil
}

// isSellOrderType — trade-execution's order_type enum values that mean
// "a SELL made by rules-engine". Used to decide the Manthan-driven vs
// manual-sell branch in §7.2.
func isSellOrderType(t string) bool {
	switch strings.ToUpper(strings.TrimSpace(t)) {
	case "SL_SELL", "EXIT", "MANTHAN_SL_EXIT", "MANTHAN_EXIT":
		return true
	}
	return false
}
