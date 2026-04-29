package manthan

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// ProtectiveReplay implements the custom-GTC mechanism on top of Indira.
//
// Why this exists
// ───────────────
// Indira silently downgrades GTC SL orders to DAY validity, so every
// in-session SL gets cancelled by the broker at 15:30 EOD. Without this
// replayer, the next morning's pre-open is unprotected — a gap-down at 9:15
// liquidates the position before SafetyMonitor's 2-second poll can react.
//
// Three-phase design
// ──────────────────
//   Phase A — 15:35 IST (post-EOD):   submit AMO+SL for next session
//   Phase B — 09:14 IST (pre-open):   re-validate against fresh DPR
//   Phase C — 09:15:30 IST (post-open): reconcile, hot-place rejections
//
// Validation evidence: AMO+SL submission accepted + converted on Indira
// (2026-04-29 KINGFA / NATIONALUM live test). Sole rejection cause was DPR
// breach from a too-aggressive 5% trigger; this replayer biases triggers
// 50bps inside DPR (AMODprBuffer) and re-validates against fresh DPR before
// market open.
type ProtectiveReplay struct {
	broker   *BrokerAdapter
	repo     *Repository
	getAuth  func(userID string) *BrokerAuth
	logger   *zap.Logger

	// eventPub is optional; used to emit AMO_QUEUED / AMO_REJECTED audit
	// events to manthan.execution.events for observability.
	eventPub *ManthanEventPublisher

	// IST clock — every cron decision is anchored to Asia/Kolkata.
	ist *time.Location
}

func NewProtectiveReplay(
	broker *BrokerAdapter,
	repo *Repository,
	getAuth func(userID string) *BrokerAuth,
	logger *zap.Logger,
) *ProtectiveReplay {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil || loc == nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}
	return &ProtectiveReplay{
		broker:  broker,
		repo:    repo,
		getAuth: getAuth,
		logger:  logger,
		ist:     loc,
	}
}

func (p *ProtectiveReplay) SetEventPublisher(ep *ManthanEventPublisher) {
	p.eventPub = ep
}

// Start runs the three-phase cron loop until ctx is cancelled. Each phase
// has its own goroutine so a slow Phase A doesn't delay Phase B at 09:14.
//
// Skip behaviour:
//   - Saturday/Sunday: all phases skip (NSE closed; AMO window also closed)
//   - Phase A: skipped if no positions need protection
//   - Phase B/C: skipped if no AMO rows exist for tomorrow / today
//
// Startup recovery: if the service restarts past today's scheduled time for
// any phase, that phase runs once immediately so a deploy/crash mid-day
// doesn't drop a day's protection. All phases are idempotent
// (uniq_active_sl_per_day index + InsertAMOOrder ON CONFLICT DO NOTHING).
func (p *ProtectiveReplay) Start(ctx context.Context) {
	p.logger.Info("Protective replayer started",
		zap.String("phase_a", "15:35 IST EOD AMO submit"),
		zap.String("phase_b", "09:14 IST pre-open re-validate"),
		zap.String("phase_c", "09:15:30 IST post-open reconcile"))

	p.runStartupRecovery(ctx)

	go p.runPhase(ctx, "A", 15, 35, p.PhaseAEODSubmit)
	go p.runPhase(ctx, "B", 9, 14, p.PhaseBPreOpenRevalidate)
	go p.runPhase(ctx, "C", 9, 15, p.PhaseCPostOpenReconcile) // 9:15 + 30s sleep inside Phase C
}

// runStartupRecovery fires any phase whose scheduled-time window has already
// passed today. Idempotent thanks to manthan_orders unique index — re-running
// after a restart cannot place duplicate AMOs.
func (p *ProtectiveReplay) runStartupRecovery(ctx context.Context) {
	now := time.Now().In(p.ist)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return
	}
	minute := now.Hour()*60 + now.Minute()

	// Phase A window: 15:35–23:59 — broker has cancelled DAY-validity SLs at
	// 15:30 and the AMO queue is open until early morning.
	if minute >= 15*60+35 && minute < 24*60 {
		go p.recover(ctx, "A", p.PhaseAEODSubmit)
	}
	// Phase B window: 09:14–09:15:30. After 09:15:30 Phase C takes over.
	if minute == 9*60+14 || (minute == 9*60+15 && now.Second() < 30) {
		go p.recover(ctx, "B", p.PhaseBPreOpenRevalidate)
	}
	// Phase C window: 09:15:30–10:00. After 10:00 the SafetyMonitor's 2s loop
	// has already done what Phase C would do, so don't double-up.
	if (minute == 9*60+15 && now.Second() >= 30) || (minute > 9*60+15 && minute < 10*60) {
		go p.recover(ctx, "C", p.PhaseCPostOpenReconcile)
	}
}

func (p *ProtectiveReplay) recover(ctx context.Context, name string, fn func(context.Context) error) {
	p.logger.Info("Startup-recovery: running missed phase", zap.String("phase", name))
	runCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	if err := fn(runCtx); err != nil {
		p.logger.Error("Startup-recovery phase failed",
			zap.String("phase", name), zap.Error(err))
	}
}

// runPhase blocks until the next IST occurrence of (hour, minute) and
// invokes fn. Repeats daily. Skips Sat/Sun.
func (p *ProtectiveReplay) runPhase(ctx context.Context, name string, hour, minute int, fn func(context.Context) error) {
	for {
		next := p.nextRun(hour, minute)
		wait := time.Until(next)
		p.logger.Info("Protective replayer phase scheduled",
			zap.String("phase", name),
			zap.Time("next_run", next),
			zap.Duration("wait", wait))

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		runCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		err := fn(runCtx)
		cancel()
		if err != nil {
			p.logger.Error("Protective replayer phase failed",
				zap.String("phase", name), zap.Error(err))
		}
	}
}

func (p *ProtectiveReplay) nextRun(hour, minute int) time.Time {
	now := time.Now().In(p.ist)
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, p.ist)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	// Skip Sat (6) / Sun (0): NSE closed and AMO window also closed.
	for target.Weekday() == time.Saturday || target.Weekday() == time.Sunday {
		target = target.Add(24 * time.Hour)
	}
	return target
}

// ─────────────────────────── Phase A ───────────────────────────────────────

// PhaseAEODSubmit runs at 15:35 IST after broker has cancelled all DAY-validity
// SL orders at 15:30 close. For every active position with no AMO yet for
// tomorrow's trade_date, submit AMO+SL biased 50bps inside DPR.
//
// Idempotent: re-running this method on the same trade_date is safe due to
// the uniq_active_sl_per_day partial index from migration 011.
func (p *ProtectiveReplay) PhaseAEODSubmit(ctx context.Context) error {
	tradeDate := p.nextTradingDay(time.Now().In(p.ist))
	p.logger.Info("Phase A: EOD AMO submit starting", zap.Time("for_trade_date", tradeDate))

	positions, err := p.repo.ListPositionsNeedingProtection(ctx)
	if err != nil {
		return fmt.Errorf("list positions: %w", err)
	}
	if len(positions) == 0 {
		p.logger.Info("Phase A: no active positions — nothing to protect")
		return nil
	}

	queued, skipped, failed := 0, 0, 0
	for _, pos := range positions {
		auth := p.getAuth(pos.UserID)
		if auth == nil {
			p.logger.Warn("Phase A: skipping — no auth for user",
				zap.String("user", pos.UserID), zap.String("symbol", pos.Symbol))
			skipped++
			continue
		}

		// Prefer stored indira_symbol + exchange_token from the position row —
		// they're guaranteed accurate (we placed the entry order with them).
		// Fall back to ISIN→token Redis lookup only if missing. The fallback
		// path is mainly for legacy positions written before the row carried
		// these fields. Either way we still pull DPR + tick_size from the
		// market-data feed for accurate clamping.
		var info *SymbolInfo
		if pos.IndiraSymbol != "" && pos.ExchangeToken != "" {
			info = &SymbolInfo{
				Symbol:        pos.Symbol,
				IndiraSymbol:  pos.IndiraSymbol,
				ExchangeToken: pos.ExchangeToken,
				Exchange:      strDefault(pos.Exchange, "NSE"),
				TickSize:      0.05,
			}
			p.enrichDPR(ctx, info)
		} else {
			resolved, err := p.broker.ResolveSymbol(ctx, pos.Symbol, pos.ISIN)
			if err != nil {
				p.logger.Warn("Phase A: resolve symbol failed — skipping",
					zap.String("symbol", pos.Symbol), zap.Error(err))
				skipped++
				continue
			}
			info = resolved
		}

		// Use latest TSL trigger from prior SL row; else default to entry × 0.92
		// (8% loss cap — same default as live SL handler).
		trigger := pos.LatestTrigger
		limit := pos.LatestLimit
		if trigger <= 0 {
			ltp, ltpErr := p.broker.FetchLTP(ctx, info.ExchangeToken)
			if ltpErr != nil || ltp <= 0 {
				p.logger.Warn("Phase A: no prior trigger and LTP unavailable — skipping",
					zap.String("symbol", pos.Symbol))
				skipped++
				continue
			}
			trigger = ltp * 0.92
			limit = trigger - SLLimitGap(trigger, info.TickSize)
		}

		brokerOrderID, err := p.broker.PlaceAMOSLSell(ctx, *auth, info, pos.NetQty, trigger, limit)
		if err != nil {
			p.logger.Error("Phase A: AMO submission failed",
				zap.String("symbol", pos.Symbol), zap.Error(err))
			failed++
			continue
		}

		// Note: PlaceAMOSLSell may have biased trigger above DPR floor — the
		// row records the requested trigger so Phase B can re-validate
		// against fresh DPR using the same algorithmic intent.
		id, dup, err := p.repo.InsertAMOOrder(ctx, pos.EntryOrderID, pos, tradeDate, trigger, limit)
		if dup {
			// Crash-recovery: a previous run of Phase A already inserted this
			// row. Cancel the duplicate broker order to keep state clean.
			p.logger.Info("Phase A: duplicate insert — cancelling extra broker order",
				zap.String("symbol", pos.Symbol), zap.String("dup_broker_id", brokerOrderID))
			_ = p.broker.CancelOrder(ctx, *auth, info, brokerOrderID)
			skipped++
			continue
		}
		if err != nil {
			p.logger.Error("Phase A: DB insert failed — cancelling broker order",
				zap.String("symbol", pos.Symbol), zap.Error(err))
			_ = p.broker.CancelOrder(ctx, *auth, info, brokerOrderID)
			failed++
			continue
		}

		// Carry the AMO queue ID on the row — Phase B can cancel via this ID;
		// Phase C swaps it for the live conversion ID.
		if err := p.repo.UpdateOrderPlaced(ctx, id, brokerOrderID); err != nil {
			p.logger.Warn("Phase A: failed to record broker_order_id",
				zap.Int64("row_id", id), zap.Error(err))
		}
		_ = p.repo.InsertEvent(ctx, id, "AMO_SUBMITTED", "PENDING", string(StatusAMOPending),
			"", trigger, pos.NetQty, fmt.Sprintf("amo_id=%s trigger=%.2f limit=%.2f", brokerOrderID, trigger, limit))
		queued++
	}

	p.logger.Info("Phase A complete",
		zap.Int("positions_total", len(positions)),
		zap.Int("amo_queued", queued),
		zap.Int("skipped", skipped),
		zap.Int("failed", failed),
		zap.Time("trade_date", tradeDate))
	return nil
}

// ─────────────────────────── Phase B ───────────────────────────────────────

// PhaseBPreOpenRevalidate runs at 09:14 IST. NSE has just published today's
// per-stock DPR via the pre-open auction (~9:00–9:08 IST). Our external
// market-data feed picks it up; this phase re-fetches DPR for every pending
// AMO and cancels-and-re-places any whose trigger is now below the new DPR
// floor — preventing the rejection-at-conversion seen in the 2026-04-29
// KINGFA / NATIONALUM test.
func (p *ProtectiveReplay) PhaseBPreOpenRevalidate(ctx context.Context) error {
	today := time.Now().In(p.ist).Truncate(24 * time.Hour)
	p.logger.Info("Phase B: pre-open re-validate starting", zap.Time("trade_date", today))

	rows, err := p.repo.ListPendingAMOForDate(ctx, today)
	if err != nil {
		return fmt.Errorf("list pending AMO: %w", err)
	}
	if len(rows) == 0 {
		p.logger.Info("Phase B: no pending AMO — nothing to re-validate")
		return nil
	}

	repaired, ok, failed := 0, 0, 0
	for _, row := range rows {
		auth := p.getAuth(row.UserID)
		if auth == nil {
			p.logger.Warn("Phase B: no auth for user — skipping",
				zap.String("user", row.UserID), zap.String("symbol", row.Symbol))
			failed++
			continue
		}

		info, err := p.broker.ResolveSymbol(ctx, row.Symbol, "")
		if err != nil {
			p.logger.Warn("Phase B: resolve symbol failed",
				zap.String("symbol", row.Symbol), zap.Error(err))
			failed++
			continue
		}

		// Trigger still inside today's DPR_lower (with 50bps buffer)? Leave it.
		floor := info.DPRLower
		if info.DPRLower > 0 {
			floor = info.DPRLower * AMODprBuffer
		}
		if row.TriggerPrice >= floor {
			ok++
			continue
		}

		// Drift detected. Cancel the AMO at broker and re-place with a
		// trigger biased above today's fresh DPR floor.
		p.logger.Warn("Phase B: trigger drift detected — cancelling + re-placing",
			zap.String("symbol", row.Symbol),
			zap.Float64("orig_trigger", row.TriggerPrice),
			zap.Float64("new_dpr_lower", info.DPRLower),
			zap.Float64("safe_floor", floor))

		if row.BrokerOrderID != "" {
			if err := p.broker.CancelOrder(ctx, *auth, info, row.BrokerOrderID); err != nil {
				// Cancel may fail if already converted/rejected — proceed anyway,
				// Phase C will reconcile via the order book.
				p.logger.Warn("Phase B: cancel failed — proceeding with re-place",
					zap.String("broker_id", row.BrokerOrderID), zap.Error(err))
			}
		}

		// PlaceAMOSLSell internally re-applies the AMODprBuffer clamp.
		newBrokerID, err := p.broker.PlaceAMOSLSell(ctx, *auth, info, row.Qty, row.TriggerPrice, row.LimitPrice)
		if err != nil {
			p.logger.Error("Phase B: re-place failed — Phase C will hot-place fresh SL after open",
				zap.String("symbol", row.Symbol), zap.Error(err))
			_ = p.repo.MarkAMORejected(ctx, row.ID, "Phase B re-place failed: "+err.Error())
			failed++
			continue
		}

		// Re-read effective trigger that PlaceAMOSLSell ended up with after
		// its own clamp: re-resolve symbol to get fresh DPR-derived floor.
		newTrigger := row.TriggerPrice
		newLimit := row.LimitPrice
		if newTrigger < floor {
			newTrigger = floor
			newLimit = floor - SLLimitGap(floor, info.TickSize)
		}
		if err := p.repo.UpdateAMOTrigger(ctx, row.ID, newTrigger, newLimit, newBrokerID); err != nil {
			p.logger.Warn("Phase B: row update failed",
				zap.Int64("row_id", row.ID), zap.Error(err))
		}
		_ = p.repo.InsertEvent(ctx, row.ID, "AMO_REVALIDATED", string(StatusAMOPending),
			string(StatusAMOPending), "", newTrigger, row.Qty,
			fmt.Sprintf("re-clamped to %.2f (was %.2f) due to fresh DPR_lower=%.2f",
				newTrigger, row.TriggerPrice, info.DPRLower))
		repaired++
	}

	p.logger.Info("Phase B complete",
		zap.Int("rows", len(rows)),
		zap.Int("ok_inside_dpr", ok),
		zap.Int("repaired", repaired),
		zap.Int("failed", failed))
	return nil
}

// ─────────────────────────── Phase C ───────────────────────────────────────

// PhaseCPostOpenReconcile runs at 09:15:30 IST. By now AMO conversion has
// either:
//   (a) succeeded — broker order book contains a fresh live order ID with
//       status "Trigger Pending" or "Open"; the original AMO queue ID is gone.
//   (b) failed at exchange — the converted order shows status "Rejected",
//       typically with `Order entered has invalid data` (DPR breach).
//   (c) silently dropped (rare).
//
// For (a), promote the AMO row to a regular SL_SELL row with the new ID so
// SafetyMonitor takes over normally.
// For (b)/(c), hot-place a fresh SL using existing sl_handler primitives —
// or fall back to SL-M / MARKET if LTP already breached the trigger.
func (p *ProtectiveReplay) PhaseCPostOpenReconcile(ctx context.Context) error {
	// Wait the extra 30 seconds inside Phase C so cron schedule lands at 9:15:00
	// but reconciliation runs at 9:15:30 — gives the broker time to settle
	// AMO conversions and exchange rejections.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(30 * time.Second):
	}

	today := time.Now().In(p.ist).Truncate(24 * time.Hour)
	p.logger.Info("Phase C: post-open reconcile starting", zap.Time("trade_date", today))

	rows, err := p.repo.ListPendingAMOForDate(ctx, today)
	if err != nil {
		return fmt.Errorf("list pending AMO: %w", err)
	}
	if len(rows) == 0 {
		p.logger.Info("Phase C: no pending AMO — nothing to reconcile")
		return nil
	}

	// Group rows by user_id so we fetch each user's order book once.
	rowsByUser := map[string][]*AMOReplayRow{}
	for _, row := range rows {
		rowsByUser[row.UserID] = append(rowsByUser[row.UserID], row)
	}

	promoted, hotPlaced, escalated, failed := 0, 0, 0, 0
	for userID, userRows := range rowsByUser {
		auth := p.getAuth(userID)
		if auth == nil {
			p.logger.Warn("Phase C: no auth — skipping user", zap.String("user", userID))
			failed += len(userRows)
			continue
		}

		book, err := p.broker.GetOrderBook(ctx, *auth)
		if err != nil {
			p.logger.Error("Phase C: order-book fetch failed — skipping user",
				zap.String("user", userID), zap.Error(err))
			failed += len(userRows)
			continue
		}

		for _, row := range userRows {
			info, err := p.broker.ResolveSymbol(ctx, row.Symbol, "")
			if err != nil {
				p.logger.Warn("Phase C: resolve failed",
					zap.String("symbol", row.Symbol), zap.Error(err))
				failed++
				continue
			}

			// Find the converted live order: same symbol+qty+SELL+SL+today.
			converted := findConvertedSL(book, row, today)
			if converted != nil {
				st := strings.ToUpper(strings.TrimSpace(converted.Status))
				if isLiveSLStatus(st) {
					if err := p.repo.PromoteAMOToActiveSL(ctx, row.ID, converted.OrdId); err != nil {
						p.logger.Warn("Phase C: promote failed",
							zap.Int64("row_id", row.ID), zap.Error(err))
						failed++
						continue
					}
					_ = p.repo.InsertEvent(ctx, row.ID, "AMO_PROMOTED",
						string(StatusAMOPending), string(StatusSLPlaced),
						converted.Status, row.TriggerPrice, row.Qty,
						"AMO converted to live SL "+converted.OrdId)
					promoted++
					continue
				}
				if isRejectedSLStatus(st) {
					_ = p.repo.MarkAMORejected(ctx, row.ID,
						fmt.Sprintf("converted but rejected: %s (%s)", converted.Status, converted.RejReason))
					_ = p.repo.InsertEvent(ctx, row.ID, "AMO_REJECTED",
						string(StatusAMOPending), string(StatusAMORejected),
						converted.Status, row.TriggerPrice, row.Qty, converted.RejReason)
					if p.hotPlaceFreshSL(ctx, *auth, info, row) {
						hotPlaced++
					} else {
						escalated++
					}
					continue
				}
			}

			// No matching live order found — broker silently dropped the AMO.
			// Treat as rejected and hot-place fresh.
			p.logger.Warn("Phase C: AMO not found in order-book — hot-placing fresh SL",
				zap.String("symbol", row.Symbol), zap.String("amo_id", row.BrokerOrderID))
			_ = p.repo.MarkAMORejected(ctx, row.ID, "AMO not found in post-open order-book")
			if p.hotPlaceFreshSL(ctx, *auth, info, row) {
				hotPlaced++
			} else {
				escalated++
			}
		}
	}

	p.logger.Info("Phase C complete",
		zap.Int("rows", len(rows)),
		zap.Int("promoted_to_live_sl", promoted),
		zap.Int("hot_placed_fresh", hotPlaced),
		zap.Int("escalated_market_sell", escalated),
		zap.Int("failed", failed))
	return nil
}

// hotPlaceFreshSL handles the rejected-AMO recovery: place a regular
// (non-AMO) SL with currently-valid DPR. If LTP already < trigger,
// escalate to MARKET SELL (existing emergency path). Returns true if a
// fresh SL got placed; false if escalation to MARKET SELL fired.
func (p *ProtectiveReplay) hotPlaceFreshSL(ctx context.Context, auth BrokerAuth, info *SymbolInfo, row *AMOReplayRow) bool {
	ltp, ltpErr := p.broker.FetchLTP(ctx, info.ExchangeToken)
	if ltpErr == nil && ltp > 0 && ltp <= row.TriggerPrice {
		// Already below trigger — SL would never trigger at level above LTP.
		// Use SL-M for immediate exit at next tick (preserves SL_EXIT semantics).
		p.logger.Warn("Phase C: LTP below trigger — placing SL-M for immediate exit",
			zap.String("symbol", row.Symbol), zap.Float64("ltp", ltp), zap.Float64("trigger", row.TriggerPrice))
		_, err := p.broker.PlaceSLMSell(ctx, auth, info, row.Qty, row.TriggerPrice)
		if err != nil {
			// Last-resort MARKET SELL.
			p.logger.Error("Phase C: SL-M failed — MARKET SELL",
				zap.String("symbol", row.Symbol), zap.Error(err))
			_, _ = p.broker.PlaceMarketSell(ctx, auth, info, row.Qty)
			return false
		}
		return true
	}

	// Normal case: LTP above trigger, place SL-Limit. PlaceSLSell already
	// applies the same DPR clamp.
	_, err := p.broker.PlaceSLSell(ctx, auth, info, row.Qty, row.TriggerPrice, row.LimitPrice)
	if err != nil {
		p.logger.Error("Phase C: hot-place SL failed — MARKET SELL",
			zap.String("symbol", row.Symbol), zap.Error(err))
		_, _ = p.broker.PlaceMarketSell(ctx, auth, info, row.Qty)
		return false
	}
	return true
}

// ─────────────────────────── helpers ───────────────────────────────────────

// findConvertedSL scans an order-book response for the live order generated
// by AMO conversion of the given row. Match criteria: same symbol+side+qty,
// type=SL/Stop-loss, ordDate today, NOT amo (because conversion strips the
// AMO flag).
func findConvertedSL(book []indiraClient.OrderBook, row *AMOReplayRow, today time.Time) *indiraClient.OrderBook {
	dateStr := today.Format("2006-01-02")
	for i := range book {
		o := &book[i]
		if o.AMO {
			continue
		}
		if !strings.EqualFold(o.OrdAction, "SELL") {
			continue
		}
		ot := strings.ToUpper(o.OrdType)
		if !(strings.Contains(ot, "SL") || strings.Contains(ot, "STOP")) {
			continue
		}
		if o.Qty != row.Qty {
			continue
		}
		if !strings.EqualFold(o.Symbol.DispSym, row.Symbol) && !strings.Contains(o.Symbol.Symbol, "_"+row.Symbol+"_") {
			continue
		}
		// Must be a today-dated order.
		if !strings.HasPrefix(o.OrdDate, dateStr) {
			continue
		}
		return o
	}
	return nil
}

func isLiveSLStatus(st string) bool {
	return strings.Contains(st, "TRIGGER") || st == "OPEN" || st == "PENDING" ||
		st == "REQUESTED" || st == "MODIFIED"
}

func isRejectedSLStatus(st string) bool {
	switch st {
	case "REJECTED", "CANCELLED", "CANCELED", "EXPIRED":
		return true
	}
	return false
}

// strDefault returns def when s is empty.
func strDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// enrichDPR best-effort fills DPRLower/DPRUpper/TickSize from the external
// Redis market-data feed using the exchange_token. Tolerates failure — info
// stays with default tick + zero DPR if the feed isn't reachable, in which
// case roundAndClamp/PlaceAMOSLSell skip the clamp (broker still validates
// against the exchange band at conversion time).
func (p *ProtectiveReplay) enrichDPR(ctx context.Context, info *SymbolInfo) {
	if info == nil || info.ExchangeToken == "" || p.broker == nil || p.broker.extRedis == nil {
		return
	}
	raw, err := p.broker.extRedis.Get(ctx, "market:nse:"+info.ExchangeToken).Result()
	if err != nil {
		p.logger.Debug("enrichDPR: market data not in redis",
			zap.String("symbol", info.Symbol), zap.String("token", info.ExchangeToken))
		return
	}
	var mkt struct {
		TickSize float64 `json:"tick_size"`
		DPRLower float64 `json:"dpr_lower"`
		DPRUpper float64 `json:"dpr_upper"`
	}
	if err := json.Unmarshal([]byte(raw), &mkt); err != nil {
		return
	}
	if mkt.TickSize > 0 {
		info.TickSize = mkt.TickSize
	}
	info.DPRLower = mkt.DPRLower
	info.DPRUpper = mkt.DPRUpper
}

// nextTradingDay returns the next IST date that's not Sat/Sun. Holidays are
// not modelled — broker will reject any AMO submitted for a holiday and the
// row stays AMO_PENDING; Phase B/C the next actual trading day will reconcile.
func (p *ProtectiveReplay) nextTradingDay(now time.Time) time.Time {
	t := now.Add(24 * time.Hour)
	for t.Weekday() == time.Saturday || t.Weekday() == time.Sunday {
		t = t.Add(24 * time.Hour)
	}
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, p.ist)
}
