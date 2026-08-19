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
	"regexp"
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
	// SL-tracking events (STATUS_CHANGED with an SL order type, or CANCELLED
	// on an SL order without a fill) update positions.current_sl. Checked
	// BEFORE the fill gate because these events have event_type=STATUS_CHANGED
	// (not FILLED), so isFillEvent would drop them.
	//
	// An SL that FIRES (broker-triggered exit) arrives as event_type=FILLED
	// with buy_sell=SELL — that goes through the fill path below and lands
	// in handleManthanSellFill via the meta lookup. current_sl becomes moot
	// because the position exits.
	if isSLTrackingEvent(ev) {
		return h.handleSLTrackingEvent(ctx, ev)
	}

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

// isSLTrackingEvent returns true iff the event should be routed to
// handleSLTrackingEvent (which updates positions.current_sl). We match:
//
//	order_type contains "SL"    — broker's SL order events (REST format
//	                              uses "SL"; WSS may use "SL-L", "SL_LIMIT")
//	AND (
//	  event carries a real trigger_price (SL_PLACED or SL_MODIFIED)
//	  OR event_type == CANCELLED with filled_qty == 0 (SL cancelled
//	     without firing — CANCELLED-with-fill is the fill path below)
//	)
//
// FILLED events on SL orders (SL triggered → position exit) are handled
// by the fill path via handleManthanSellFill, NOT here — position exits
// clear current_sl implicitly by leaving status=ACTIVE behind.
func isSLTrackingEvent(ev *consumer.OrderEvent) bool {
	if !isSLOrderTypeString(ev.OrderType) {
		return false
	}
	if ev.EventType == "CANCELLED" && ev.FilledQty == 0 {
		return true
	}
	if ev.TriggerPrice > 0 && ev.EventType != "FILLED" && ev.EventType != "PARTIALLY_FILLED" {
		return true
	}
	return false
}

// isSLOrderTypeString accepts the string forms broker actually publishes.
// REST_ORDERBOOK typically writes "SL"; WSS may write "SL-L" or with
// suffixes. Case-insensitive; empty string is safe (returns false).
func isSLOrderTypeString(ot string) bool {
	u := strings.ToUpper(strings.TrimSpace(ot))
	if u == "SL" {
		return true
	}
	// Match "SL_", "SL-" prefixes, and " SL " embedded (unlikely but safe).
	if strings.HasPrefix(u, "SL_") || strings.HasPrefix(u, "SL-") {
		return true
	}
	return false
}

// isFillEvent returns true iff the event should trigger a position state
// transition. Accepts:
//   - event_type == "FILLED"                              — full fill
//   - event_type == "PARTIALLY_FILLED" AND filled_qty > 0 — mid-flight partial
//   - event_type == "CANCELLED"        AND filled_qty > 0 — partial-fill-then-
//     cancelled
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
	// Paper-trading leak guard — paper-trading emits events with
	// broker_order_id="PAPER-*" onto the same Kafka topic as real broker
	// fills. Positions_db is the SSOT for LIVE holdings; paper events must
	// never land here. Verified drift 2026-07-17: a stale MANTHAN row for
	// SBI qty=15 with broker_order_id="PAPER-M2" was in positions_db while
	// the real broker had no such position.
	if strings.HasPrefix(ev.BrokerOrderID, "PAPER-") {
		h.logger.Info("state-machine: BUY event has PAPER-* broker_order_id — skipping (paper-trading events must not reach positions_db)",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("event_id", ev.EventID),
			zap.String("source", ev.Source))
		return nil
	}

	// Idempotency short-circuit: BUY event replayed → position row already exists.
	exists, err := h.store.PositionExistsForEntry(ctx, ev.BrokerOrderID)
	if err != nil {
		return fmt.Errorf("BUY idempotency check: %w", err)
	}
	if exists {
		// Partial-fill correction path — see docstring at the top of the
		// method. If this is a CANCELLED-with-fill (the broker's terminal
		// "I filled fq shares and cancelled the rest" event), and the
		// position we already stored has a bigger quantity than the real
		// broker fill, downsize. Verified 2026-07-17: AADHARHFC + CUB
		// opened with qty=38 / qty=90 from a premature WSS FILLED (fq=null,
		// qty=order_qty) and then the follow-up CANCELLED (fq=4) was
		// silently ignored by the plain "exists → skip" guard below.
		if ev.EventType == "CANCELLED" && ev.FilledQty > 0 {
			return h.correctPartialFill(ctx, ev)
		}
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

	// Entry price precedence:
	//   1. meta.AvgFillPrice — SSOT from manthan_orders (populated by
	//      trade-execution when it processed a WSS fill event with a
	//      real traded_price). This is the broker-authoritative value.
	//   2. ev.TradedPrice — the current Kafka event carries a real fill
	//      (i.e. this IS a WSS event and it beat trade-exec's writer).
	//   3. Neither → the event is a REST_ORDERBOOK-first arrival that
	//      has no fill-price information. SKIP rather than fall back
	//      to ev.OrderPrice (the LIMIT price), which produced the
	//      2026-07-16 incident where positions_db.entry_price = 518.15
	//      (limit) while manthan_orders.avg_fill_price = 518.05 (fill).
	//      A later WSS event on the same broker_order_id will retry
	//      and succeed; positions svc's ordering guarantee holds
	//      because idempotency is by broker_order_id, not event_id.
	entryPrice := meta.AvgFillPrice
	if entryPrice == 0 {
		entryPrice = ev.TradedPrice
	}
	qty := ev.FilledQty
	if qty == 0 {
		qty = ev.Quantity
	}
	if entryPrice == 0 || qty == 0 {
		h.logger.Info("state-machine: BUY event has no fill price yet — deferring (waiting for WSS event with traded_price)",
			zap.String("event_id", ev.EventID),
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("source", ev.Source),
			zap.Float64("meta_avg_fill", meta.AvgFillPrice),
			zap.Float64("ev_traded_price", ev.TradedPrice),
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

	// Top-up merge: a MANTHAN entry that partially filled on a LIMIT and was
	// completed with a separate MARKET order produces a SECOND fill on a
	// different broker_order_id but the SAME signal. If a lot already exists
	// for this signal, merge this fill into it (VWAP) rather than opening a
	// second row — positions_db must show ONE lot at the true combined qty
	// (2026-08-11 PICCADIL: held 7 in positions_db while the broker had 24).
	// Resolve the PARENT signal UUID once: child-order ids ("mkt-<uuid>", …)
	// are not UUIDs and must never reach positions.signal_id.
	if meta.Found {
		if canon := canonicalSignalUUID(meta); canon != "" {
			if canon != meta.SignalID {
				h.logger.Info("state-machine: child-order signal id normalized to parent UUID",
					zap.String("raw_signal_id", meta.SignalID), zap.String("signal_id", canon),
					zap.String("broker_order_id", ev.BrokerOrderID))
			}
			meta.SignalID = canon
		} else {
			h.logger.Warn("state-machine: Manthan order without a resolvable signal UUID — recording as USER_MANUAL origin",
				zap.String("raw_signal_id", meta.SignalID), zap.String("broker_order_id", ev.BrokerOrderID))
			meta.Found = false
		}
	}
	if meta.Found && meta.SignalID != "" {
		if existing, ferr := h.store.FindManthanLotBySignalID(ctx, meta.SignalID); ferr == nil &&
			existing != nil && existing.EntryBrokerOrderID != ev.BrokerOrderID {
			mergeEvent := &store.PositionEvent{
				EventType:      store.EventTypeTopupMerged,
				BrokerOrderID:  ev.BrokerOrderID,
				SignalID:       meta.SignalID,
				DeltaQty:       qty,
				FillPrice:      entryPrice,
				RawSourceEvent: ev.RawMessage,
				SourceEventID:  ev.EventID,
			}
			merged, mErr := h.store.MergeTopupWithEvent(ctx, existing.PositionID, qty, entryPrice, ev.BrokerOrderID, mergeEvent)
			if mErr != nil {
				return fmt.Errorf("BUY top-up merge: %w", mErr)
			}
			h.logger.Info("state-machine: BUY top-up merged into existing lot",
				zap.String("position_id", existing.PositionID.String()),
				zap.String("symbol", existing.Symbol),
				zap.String("signal_id", meta.SignalID),
				zap.Int("add_qty", qty),
				zap.Float64("fill_price", entryPrice),
				zap.String("topup_broker_order_id", ev.BrokerOrderID),
				zap.Bool("applied", merged))
			return nil
		}
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
		EventType:      eventType,
		BrokerOrderID:  ev.BrokerOrderID,
		SignalID:       pos.SignalID,
		DeltaQty:       qty,
		FillPrice:      entryPrice,
		RawSourceEvent: ev.RawMessage,
		SourceEventID:  ev.EventID,
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

	// Race-close: if trade-execution already knows about an SL row for this
	// entry (parent_order_id self-FK on manthan_orders), apply its trigger
	// price now. Covers the Kafka partition-ordering case where the SL
	// event arrived at positions svc BEFORE this BUY event and was silently
	// dropped by the state machine as "no ACTIVE parent position." Without
	// this pull, positions_db.current_sl stays NULL until a reconciler
	// backfills — verified missed on AADHARHFC + CUB on 2026-07-16.
	//
	// Only ENTRY rows carry an SL child, so guard on Origin==MANTHAN
	// (USER_MANUAL buys don't get a Manthan SL) and on meta.SLTriggerPrice
	// being non-zero (0 = LookupOrderMeta found no SL row yet).
	if pos.Origin == store.OriginManthan && meta.SLTriggerPrice > 0 && meta.SLBrokerOrderID != "" {
		slAudit := &store.PositionEvent{
			EventType:     store.EventTypeSLModified,
			BrokerOrderID: meta.SLBrokerOrderID,
			SignalID:      pos.SignalID, // parent BUY's signal (SL rows carry sl-<uuid> which is not a UUID)
			FillPrice:     meta.SLTriggerPrice,
			// SourceEventID mirrors handleSLTrackingEvent's format so a
			// later SL Kafka event is deduped rather than double-applied.
			SourceEventID:  "sl-pull-" + meta.SLBrokerOrderID,
			SourceTopic:    "buy-handler-sl-pull",
			RawSourceEvent: ev.RawMessage,
		}
		if err := h.store.UpdateCurrentSLWithEvent(ctx, pos.PositionID, meta.SLTriggerPrice, slAudit); err != nil {
			// Non-fatal — the position is already inserted correctly. Log
			// and let the SL Kafka event (or reconciler) retry.
			h.logger.Warn("state-machine: BUY-time SL pull failed — leaving to Kafka SL event or reconciler",
				zap.String("position_id", pos.PositionID.String()),
				zap.String("sl_broker_order_id", meta.SLBrokerOrderID),
				zap.Float64("sl_trigger_price", meta.SLTriggerPrice),
				zap.Error(err))
		} else {
			h.logger.Info("state-machine: BUY-time SL pull applied (race-close)",
				zap.String("position_id", pos.PositionID.String()),
				zap.String("symbol", pos.Symbol),
				zap.Float64("current_sl", meta.SLTriggerPrice),
				zap.String("sl_broker_order_id", meta.SLBrokerOrderID))
			pos.CurrentSL = meta.SLTriggerPrice
		}
	}

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

// correctPartialFill downsizes an already-inserted position to reflect the
// broker's actual final fill count. Called from handleBuyFill when a
// CANCELLED-with-fill event arrives for a broker_order_id that already has
// a position row.
//
// Why we need this: the broker's WSS occasionally emits a "FILLED EXECUTED"
// event with filled_qty=null a few ms before it re-syncs with the exchange
// and emits the correcting CANCELLED-with-partial-fill. Positions svc's
// initial FILLED path falls back to ev.Quantity (the order quantity) when
// filled_qty is 0 — and so opens the position with an inflated size. The
// authoritative post-facto CANCELLED event carries the real fill_qty; this
// method uses it to right-size both quantity and invested_amount.
//
// Verified on 2026-07-17: AADHARHFC opened with qty=38 from a premature
// WSS FILLED (fq=null, qty=38), corrected here by the follow-up CANCELLED
// (fq=4) that would otherwise have been dropped by the "position exists →
// skip" guard.
//
// Semantic guardrails:
//   - Never grows a position — the store method WHERE-clauses on
//     `quantity > $newQty` so an accidental widen is a no-op.
//   - No-op when position already matches (ev.FilledQty == pos.Quantity) —
//     the CANCELLED event is redundant with the FILLED that came first.
//   - Emits a PARTIAL_FILL_FIX audit row with the delta + reason so the
//     correction is traceable in position_events.
func (h *Handler) correctPartialFill(ctx context.Context, ev *consumer.OrderEvent) error {
	pos, err := h.store.FindPositionByEntryBrokerOrderID(ctx, ev.BrokerOrderID)
	if err != nil {
		return fmt.Errorf("correctPartialFill: find position: %w", err)
	}
	if pos == nil {
		// Race: PositionExistsForEntry said yes but the row has been exited
		// between then and now. Nothing to correct.
		return nil
	}
	if ev.FilledQty >= pos.Quantity {
		// Broker's CANCELLED-with-fill matches (or exceeds — shouldn't happen)
		// the position quantity. No correction needed.
		h.logger.Debug("state-machine: CANCELLED-with-fill matches position quantity — no correction",
			zap.String("position_id", pos.PositionID.String()),
			zap.Int("pos_qty", pos.Quantity),
			zap.Int("ev_filled_qty", ev.FilledQty))
		return nil
	}

	newQty := ev.FilledQty
	newInvested := pos.EntryPrice * float64(newQty)
	deltaShares := pos.Quantity - newQty

	audit := &store.PositionEvent{
		EventType:      store.EventTypePartialFillFix,
		BrokerOrderID:  ev.BrokerOrderID,
		SignalID:       pos.SignalID,
		DeltaQty:       -deltaShares, // negative = shares removed from position
		FillPrice:      pos.EntryPrice,
		SourceTopic:    "order.events",
		SourceEventID:  "pf-" + ev.EventID, // "pf-" prefix so a later replay of the same CANCELLED still dedupes distinctly from the original FILLED audit row
		RawSourceEvent: ev.RawMessage,
	}

	if err := h.store.CorrectEntryQuantityWithEvent(ctx, pos.PositionID, newQty, newInvested, audit); err != nil {
		return fmt.Errorf("correctPartialFill: store update: %w", err)
	}

	h.logger.Info("state-machine: partial-fill correction applied — position downsized to real broker fill",
		zap.String("position_id", pos.PositionID.String()),
		zap.String("symbol", pos.Symbol),
		zap.String("broker_order_id", ev.BrokerOrderID),
		zap.Int("old_qty", pos.Quantity),
		zap.Int("new_qty", newQty),
		zap.Float64("new_invested", newInvested))
	return nil
}

// ----------------------------------------------------------------------
// SL tracking — §7.5 (added 2026-07-16)
//
// SL orders don't move a position's status (that's what fills do). They
// mutate a single column, positions.current_sl, so mobile UI + portfolio
// summary can render "SL @ 1512.90" and later "SL ratcheted to 1520.30".
//
// The link between an SL order (broker_order_id = NYMZX0021B@7) and its
// parent position (whose entry_broker_order_id = NYMZX00219@7) lives in
// trade-execution's manthan_orders.parent_order_id. We resolve it via
// the existing LookupOrderMeta gRPC — one hop, cache-wrapped, no schema
// changes.
//
// Edge cases handled:
//   Meta.Found == false      → user-manual SL (not tracked in positions_db
//                              today; deferred to future scope) — no-op.
//   No matching ACTIVE lot   → drift or race (SL modify arrived after
//                              position exited) — warn + no-op; drift
//                              detector will surface if the situation
//                              persists.
//   TriggerPrice = 0 on non-cancel event → skip; the CANCELLED branch
//                              is the ONLY intentional "clear" path.
// ----------------------------------------------------------------------

func (h *Handler) handleSLTrackingEvent(ctx context.Context, ev *consumer.OrderEvent) error {
	// Look up the parent BUY via trade-execution's manthan_orders join.
	// meta.Found == true means we know this SL — it's a Manthan-placed SL
	// (or trade-execution knows about it via user-config-events sync).
	// meta.EntryBrokerOrderID is the parent BUY's broker_order_id, which
	// FindPositionByEntryBrokerOrderID uses to locate the position row.
	meta, err := h.lookup.LookupOrderMeta(ctx, ev.BrokerOrderID)
	if err != nil {
		return fmt.Errorf("SL LookupOrderMeta: %w", err)
	}
	if !meta.Found {
		h.logger.Debug("state-machine: SL event but not in manthan_orders (user-manual SL) — skipping",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("event_id", ev.EventID))
		return nil
	}
	if meta.EntryBrokerOrderID == "" {
		h.logger.Warn("state-machine: SL event missing EntryBrokerOrderID from meta — skipping",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("event_id", ev.EventID))
		return nil
	}

	pos, err := h.store.FindPositionByEntryBrokerOrderID(ctx, meta.EntryBrokerOrderID)
	if err != nil {
		return fmt.Errorf("SL find position: %w", err)
	}
	if pos == nil {
		// No ACTIVE lot for the parent BUY. Could be:
		//   (a) drift — parent BUY never got processed by positions svc
		//   (b) race  — position exited between fill and this event
		// Neither is a fatal error; drift detector (Chunk P.G) will
		// surface (a) if it persists. Log at Info to make debugging easy.
		h.logger.Info("state-machine: SL event but no ACTIVE parent position — skipping",
			zap.String("sl_broker_order_id", ev.BrokerOrderID),
			zap.String("entry_broker_order_id", meta.EntryBrokerOrderID),
			zap.String("event_id", ev.EventID))
		return nil
	}

	// Audit rows use the PARENT (entry) signal_id — position_events.signal_id
	// is UUID-typed. Trade-execution stores SL rows with signal_id prefixed
	// "sl-<uuid>" (per rules-engine's SL_MODIFY deterministic-id pattern) —
	// that string won't parse as UUID. EntrySignalID is the plain UUID of
	// the parent BUY signal, which IS the position's signal_id already
	// stored on positions.signal_id. Every event on a position references
	// the same signal_id → consistent audit trail.
	auditSignalID := meta.EntrySignalID
	if auditSignalID == "" {
		// Defensive: LookupOrderMeta falls back to SL's own signal_id when
		// EntrySignalID is empty (shouldn't happen for SL orders since
		// they always have a parent, but if it does, better to skip the
		// audit-row signal_id than write garbage).
		h.logger.Debug("state-machine: SL event missing EntrySignalID — audit row will have NULL signal_id",
			zap.String("broker_order_id", ev.BrokerOrderID))
	}

	// Route: CANCELLED (with no fill) → clear; else set/update to trigger_price.
	if ev.EventType == "CANCELLED" && ev.FilledQty == 0 {
		auditEvent := &store.PositionEvent{
			EventType:      store.EventTypeSLCancelled,
			BrokerOrderID:  ev.BrokerOrderID,
			SignalID:       auditSignalID,
			Reason:         "SL cancelled at broker (filled_qty=0)",
			RawSourceEvent: ev.RawMessage,
			SourceEventID:  ev.EventID,
		}
		if err := h.store.ClearCurrentSLWithEvent(ctx, pos.PositionID, auditEvent); err != nil {
			return fmt.Errorf("clear current_sl: %w", err)
		}
		h.logger.Info("state-machine: SL cancelled — cleared current_sl",
			zap.String("position_id", pos.PositionID.String()),
			zap.String("symbol", pos.Symbol),
			zap.String("sl_broker_order_id", ev.BrokerOrderID))
		return nil
	}

	// Set/update path — needs a real trigger_price.
	if ev.TriggerPrice <= 0 {
		h.logger.Debug("state-machine: SL event with no trigger_price — skipping",
			zap.String("broker_order_id", ev.BrokerOrderID),
			zap.String("event_type", ev.EventType))
		return nil
	}

	// Reason line shows the SL walk visible in position_events:
	//   "SL @ 1512.90" (first observation, prior was null)
	//   "SL 1512.90 → 1520.30" (ratchet up)
	reason := fmt.Sprintf("SL @ %.4f", ev.TriggerPrice)
	if pos.CurrentSL > 0 && pos.CurrentSL != ev.TriggerPrice {
		reason = fmt.Sprintf("SL %.4f → %.4f", pos.CurrentSL, ev.TriggerPrice)
	}
	auditEvent := &store.PositionEvent{
		EventType:      store.EventTypeSLModified,
		BrokerOrderID:  ev.BrokerOrderID,
		SignalID:       auditSignalID,
		FillPrice:      ev.TriggerPrice, // repurposed: SL trigger value (position_events has no dedicated column)
		Reason:         reason,
		RawSourceEvent: ev.RawMessage,
		SourceEventID:  ev.EventID,
	}
	if err := h.store.UpdateCurrentSLWithEvent(ctx, pos.PositionID, ev.TriggerPrice, auditEvent); err != nil {
		return fmt.Errorf("update current_sl: %w", err)
	}
	h.logger.Info("state-machine: SL trigger set",
		zap.String("position_id", pos.PositionID.String()),
		zap.String("symbol", pos.Symbol),
		zap.Float64("prev_sl", pos.CurrentSL),
		zap.Float64("new_sl", ev.TriggerPrice),
		zap.String("sl_broker_order_id", ev.BrokerOrderID))
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

	// Audit row's signal_id references the POSITION's signal_id (the parent
	// BUY's UUID), not the SL order's own signal_id (which trade-execution
	// stores prefixed "sl-<uuid>" and won't parse as the UUID-typed column).
	// See the note in handleSLTrackingEvent for the same fix on the modify
	// path (2026-07-16).
	auditEvent := &store.PositionEvent{
		EventType:        store.EventTypeSLFilled,
		BrokerOrderID:    ev.BrokerOrderID,
		SignalID:         meta.EntrySignalID,
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
//  1. USER_MANUAL lots first (entry_time ASC)
//  2. MANTHAN lots next        (entry_time ASC)
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
	//
	// Exchange filter matters: same ticker on NSE vs BSE are separate positions
	// (not fungible for delivery) — the FIFO query MUST scope to the exchange
	// this SELL was placed on, else an NSE SELL could match a BSE lot.
	// Fallback: if wire form was unrecognized, exchange=="" degrades to
	// no exchange filter (backward compat for older WSS-only callers).
	normSymbol, normExchange := normalizeSymbol(ev.Symbol)
	if normExchange == "" {
		normExchange = ev.Exchange
	}

	lots, err := h.store.FindActiveLotsFIFO(ctx, ev.UserID, normSymbol, normExchange)
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

// canonicalSignalUUID returns the PARENT signal's UUID for a Manthan order's
// meta. positions.signal_id is a UUID column, but trade-execution derives
// child order ids from the entry signal: "mkt-<uuid>" for the market
// fallback after a partial LIMIT fill, "sl-<uuid>" for stops,
// "protective-<uuid>-<date>" for replayed stops. Passing those raw broke the
// INSERT ("invalid input syntax for type uuid") and silently dropped the
// fill — 2026-08-11 S4450 CUB: the 91-share market leg never became a
// position, so the live-algo API showed 16 holdings while the broker held
// 17 (found 2026-08-19). Preference: meta.EntrySignalID when it is a UUID,
// else the first UUID embedded in meta.SignalID, else "" (caller logs).
func canonicalSignalUUID(meta tradeexec.OrderMeta) string {
	if u, err := uuid.Parse(strings.TrimSpace(meta.EntrySignalID)); err == nil {
		return u.String()
	}
	if m := uuidInSignalID.FindString(meta.SignalID); m != "" {
		if u, err := uuid.Parse(m); err == nil {
			return u.String()
		}
	}
	return ""
}

var uuidInSignalID = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
