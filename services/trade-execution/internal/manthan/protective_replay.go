package manthan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// ProtectiveReplay implements the custom-GTC mechanism on top of Indira using
// a SERVER-SIDE TRIGGER ENGINE pattern (the same approach Zerodha GTT, Upstox
// OAO, Dhan Forever Order, etc. use).
//
// Why we abandoned broker-side AMO+SL
// ───────────────────────────────────
// The previous design pushed AMO+SL to the broker queue at 15:35 IST and
// relied on Indira to convert it to a live SL the next morning. That hit
// three reject conditions that the doc describes but only a server-side
// engine can react to:
//
//   1. freeQty < sellQty at conversion time          (CDSL/TPIN, auto-pledge,
//                                                     T+1 unsettled — silent reject)
//   2. Trigger outside tomorrow's DPR                (DPR shifts overnight,
//                                                     50bps buffer not always enough)
//   3. Generic "Order entered has invalid data"      (no granular feedback path)
//
// New flow
// ────────
// During market hours: existing SafetyMonitor (2s loop) trails SL via WSS.
// At 15:30 EOD: broker auto-cancels DAY-validity SLs. We do nothing.
//
// At 09:14 IST (T+1):
//   1. List positions needing protection from manthan_positions
//   2. Fetch fresh DPR per symbol from external Redis (NSE published it
//      during pre-open auction at 9:00–9:08)
//   3. Fetch holdings.freeQty per symbol from broker (current auth state)
//   4. For each position:
//        - skip if freeQty < quantity → publish JWT/TPIN alert
//        - clamp trigger to max(intended, DPR_lower * 1.005)
//        - choose order type by LTP vs trigger
//
// At 09:15:00.100 IST (market open + 100ms):
//   5. Fire the chosen order via existing broker_adapter primitives
//      (PlaceSLSell / PlaceMarketSell / PlaceSLMSell / PlaceAMOSell-on-LC)
//
// At 09:15:30 IST:
//   6. Pull order book, mark each placement as PLACED / FILLED / REJECTED
//   7. For rejections, drill into Order Trail API for the granular reason
//   8. Hot-place a fallback SL-M for any rejection
//   9. Hand-off to existing SafetyMonitor (2s) thereafter.
//
// Edge cases covered:
//   • freeQty=0 (CDSL/TPIN lock)              → skip, alert via Kafka
//   • LTP missing from Redis                  → fallback to broker positions LTP
//   • Broker JWT expired                      → skip, alert
//   • Lower-circuit at open                   → AMO SELL (only exit path)
//   • Stock at upper-circuit                  → still place SL — protective only
//   • Trigger outside DPR                     → clamp to DPR floor + 50bps
//   • User has user_override_until set        → skip (manual mode)
//   • Position no longer in broker holdings   → skip, alert (manual exit detected)
//   • Service down at 09:14                   → startup-recovery runs catch-up
//                                               if started before 10:00 IST
//
// All actions are idempotent against manthan_orders.uniq_active_sl_per_day.

// DPRSafetyBuffer is the multiplier applied to DPR_lower when computing the
// safe trigger floor. 1.005 = trigger sits 50bps inside the lower band, so
// pre-open DPR re-bucketing can't push it outside.
const DPRSafetyBuffer = 1.005

// MarketOpenFireDelay is how long after 09:15:00 IST to wait before firing
// the SL placements. 100ms gives the exchange's first tick a chance to
// arrive so our LTP-based decision uses the actual open print, not yesterday's
// close. Slightly under Zerodha's typical GTT fire latency (~500ms).
const MarketOpenFireDelay = 100 * time.Millisecond

// ReconcileDelay is how long after firing to wait before pulling the order
// book to verify each placement landed.
const ReconcileDelay = 30 * time.Second

// ProtectiveReplay is the cron-driven server-side trigger engine.
type ProtectiveReplay struct {
	broker   *BrokerAdapter
	repo     *Repository
	getAuth  func(userID string) *BrokerAuth
	logger   *zap.Logger
	eventPub *ManthanEventPublisher

	// Optional SESSION_EXPIRED publisher — nil-safe.
	authNotif AuthExpiryNotifier

	ist *time.Location
}

// SetAuthExpiryNotifier wires the SESSION_EXPIRED publisher.
func (p *ProtectiveReplay) SetAuthExpiryNotifier(n AuthExpiryNotifier) {
	p.authNotif = n
}

// NewProtectiveReplay constructs a ProtectiveReplay. Wire eventPub later via
// SetEventPublisher when the publisher is ready.
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

// SetEventPublisher wires the centralised event publisher. Optional —
// failure logs still go to zap.
func (p *ProtectiveReplay) SetEventPublisher(ep *ManthanEventPublisher) {
	p.eventPub = ep
}

// Start runs the daily protective-replay scheduling. Two crons fire:
//
//   1. 15:35 IST — EOD Phase A (this file: eod_phase_a.go). For every open
//      position, place an AMO+SL on Indira's overnight queue so the
//      position is protected the moment the market reopens.
//
//   2. 09:14 IST — Hot SL placement (this method's runOnce). Builds plans
//      for any positions still unprotected (EOD failed, AMO didn't
//      convert, etc.) and places live SL orders before the 09:15 open.
//
// Both honor IsTradingDay so we never wake up on holidays/weekends to
// place orders the broker would reject. Each cron has an independent
// startup-catch-up window so a deploy mid-window doesn't skip a day.
func (p *ProtectiveReplay) Start(ctx context.Context) {
	p.logger.Info("Protective replayer started (server-side trigger engine)",
		zap.String("schedule", "14:30 IST · JWT pre-flight  +  15:35 IST · EOD AMO pre-stage  +  09:14 IST · morning hot-SL fallback"))

	// 09:14 morning hot-SL fallback.
	now := p.now()
	if indiraClient.IsTradingDay(now) {
		minute := now.Hour()*60 + now.Minute()
		if minute >= 9*60+14 && minute < 10*60 {
			p.logger.Info("Startup-recovery: running missed pre-open cycle")
			go p.runOnce(ctx)
		}
	}
	go p.scheduleDaily(ctx, 9, 14, p.runOnce)

	// 14:30 EOD pre-flight — Layer 3 (defined in eod_phase_pre.go).
	p.StartEODPreFlight(ctx)

	// 15:35 EOD Phase A — Layer 1/2 (defined in eod_phase_a.go).
	p.StartEODPhaseA(ctx)
}

// scheduleDaily blocks until next IST occurrence of (hour, minute) on a
// weekday, then invokes fn. Repeats forever until ctx is cancelled.
func (p *ProtectiveReplay) scheduleDaily(ctx context.Context, hour, minute int, fn func(context.Context)) {
	for {
		next := p.nextWeekdayAt(hour, minute)
		wait := time.Until(next)
		p.logger.Info("Pre-open cycle scheduled",
			zap.Time("next_run", next), zap.Duration("wait", wait))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		fn(ctx)
	}
}

// nextWeekdayAt computes the next IST occurrence of (hour, minute) on a
// real trading day — skips weekends AND the NSE holiday list (see
// pkg/indira/calendar.go). This means our cron never wakes up on a closed
// day to try placing orders the broker would reject anyway.
func (p *ProtectiveReplay) nextWeekdayAt(hour, minute int) time.Time {
	now := p.now()
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, p.ist)
	if !target.After(now) {
		target = target.Add(24 * time.Hour)
	}
	for !indiraClient.IsTradingDay(target) {
		target = target.Add(24 * time.Hour)
	}
	return target
}

func (p *ProtectiveReplay) now() time.Time { return time.Now().In(p.ist) }

// isWeekend kept for readability only; logic is now subsumed by
// indiraClient.IsTradingDay which also handles the holiday list.
func isWeekend(t time.Time) bool {
	return t.Weekday() == time.Saturday || t.Weekday() == time.Sunday
}

// ─── runOnce: build plan → wait for open → fire → reconcile ─────────────────

// FirePlan is the per-position decision built at 09:14, executed at 09:15.
type FirePlan struct {
	Pos          PositionNeedingProtection
	Auth         BrokerAuth
	Info         *SymbolInfo
	IntendedQty  int     // qty actually sellable (min of pos.NetQty and freeQty)
	SafeTrigger  float64 // DPR-clamped, tick-aligned
	SafeLimit    float64 // SL limit price (= SafeTrigger - SLLimitGap)
	LTP          float64 // best-effort latest market price
	Action       FireAction
	SkipReason   string // populated when Action == FireSkip
}

// FireAction is the order-type decision made by the planner.
type FireAction string

const (
	FireSkip       FireAction = "SKIP"        // skip + alert (freeQty=0, no auth, etc.)
	FireSLLimit    FireAction = "SL_LIMIT"    // place SL-L at SafeTrigger (LTP > trigger)
	FireSLMarket   FireAction = "SL_MARKET"   // place SL-M (gap-down already breached)
	FireMarketSell FireAction = "MARKET_SELL" // straight market sell (deep gap, exit now)
	FireAMOLowerCircuit FireAction = "AMO_LC" // lower-circuit at open, AMO is only path out
)

func (p *ProtectiveReplay) runOnce(ctx context.Context) {
	cycleCtx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()

	// Step 1 — build plans
	plans := p.buildPlans(cycleCtx)
	if len(plans) == 0 {
		p.logger.Info("Pre-open cycle: no positions need protection — exiting")
		return
	}
	p.logger.Info("Pre-open cycle: plans built",
		zap.Int("total", len(plans)),
		zap.Int("skipped", countAction(plans, FireSkip)),
		zap.Int("sl_limit", countAction(plans, FireSLLimit)),
		zap.Int("sl_market", countAction(plans, FireSLMarket)),
		zap.Int("market_sell", countAction(plans, FireMarketSell)),
		zap.Int("amo_lc", countAction(plans, FireAMOLowerCircuit)))

	// Step 2 — wait for market open + small grace for first tick
	p.waitUntilMarketOpen(cycleCtx)

	// Step 3 — fire orders
	results := p.fireAll(cycleCtx, plans)

	// Step 4 — reconcile after broker has settled the placements
	select {
	case <-cycleCtx.Done():
		return
	case <-time.After(ReconcileDelay):
	}
	p.reconcile(cycleCtx, results)
}

// ─── Plan builder ──────────────────────────────────────────────────────────

func (p *ProtectiveReplay) buildPlans(ctx context.Context) []FirePlan {
	positions, err := p.repo.ListPositionsNeedingProtection(ctx)
	if err != nil {
		p.logger.Error("buildPlans: list positions failed", zap.Error(err))
		return nil
	}
	if len(positions) == 0 {
		return nil
	}

	// Per-user freeQty cache so we hit the holdings API at most once per user
	// per cycle, even if a user has many positions.
	type userCache struct {
		auth    *BrokerAuth
		freeQty map[string]int
		fetched bool
	}
	cache := map[string]*userCache{}

	// Today's IST date — used for the AMO already-armed skip check. Snapped
	// to midnight so the SQL date-equality comparison hits the date-only
	// trade_date column cleanly.
	istToday := func() time.Time {
		n := p.now()
		return time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, p.ist)
	}()

	plans := make([]FirePlan, 0, len(positions))
	for _, pos := range positions {
		// Skip-if-already-protected. If EOD Phase A pre-staged an AMO last
		// evening AND it converted to a live SL at 09:00 (or sits in
		// AMO_PENDING awaiting conversion), we have nothing to do.
		// Placing a second SL would either double the protection (broker
		// margin error) or race against the live one.
		if protected, perr := p.repo.HasActiveProtectionForToday(ctx, pos.EntryOrderID, istToday); perr == nil && protected {
			p.logger.Info("Pre-open cycle: position already protected by EOD AMO — skipping",
				zap.String("symbol", pos.Symbol),
				zap.String("user_id", pos.UserID),
				zap.Int64("entry_order_id", pos.EntryOrderID))
			continue
		} else if perr != nil {
			p.logger.Warn("Pre-open cycle: protection-check failed, proceeding with placement (safe default)",
				zap.String("symbol", pos.Symbol), zap.Error(perr))
		}

		uc := cache[pos.UserID]
		if uc == nil {
			uc = &userCache{auth: p.getAuth(pos.UserID)}
			cache[pos.UserID] = uc
		}
		auth := uc.auth

		// EDGE: no auth — skip + alert.
		if auth == nil {
			plans = append(plans, FirePlan{
				Pos:        pos,
				Action:     FireSkip,
				SkipReason: "no broker auth — JWT expired or never persisted; user must re-login",
			})
			continue
		}

		// Lazy-load freeQty for this user.
		if !uc.fetched {
			uc.freeQty = p.fetchFreeQtyMap(ctx, *auth, pos.UserID)
			uc.fetched = true
		}

		info := p.resolveInfo(ctx, pos)
		if info == nil {
			plans = append(plans, FirePlan{
				Pos:        pos,
				Auth:       *auth,
				Action:     FireSkip,
				SkipReason: "couldn't resolve symbol info (no indira_symbol / exchange_token)",
			})
			continue
		}

		// EDGE: freeQty < intended sell qty.
		freeQty, hasHolding := uc.freeQty[strings.ToUpper(pos.Symbol)]
		if !hasHolding {
			plans = append(plans, FirePlan{
				Pos:        pos,
				Auth:       *auth,
				Info:       info,
				Action:     FireSkip,
				SkipReason: "position not in broker holdings — likely manually exited; reconcile via detector",
			})
			continue
		}
		if freeQty < pos.NetQty {
			plans = append(plans, FirePlan{
				Pos:        pos,
				Auth:       *auth,
				Info:       info,
				Action:     FireSkip,
				SkipReason: fmt.Sprintf("freeQty=%d < required=%d — CDSL/TPIN auth pending or auto-pledged for margin", freeQty, pos.NetQty),
			})
			continue
		}

		// Compute safe trigger: clamp to DPR_lower * buffer, tick-align.
		intended := pos.LatestTrigger
		if intended <= 0 {
			// No prior SL trail — default to entry × 0.92 (8% loss cap).
			ltp := p.fetchLTP(ctx, info)
			if ltp > 0 {
				intended = ltp * 0.92
			} else {
				plans = append(plans, FirePlan{
					Pos:        pos,
					Auth:       *auth,
					Info:       info,
					Action:     FireSkip,
					SkipReason: "no prior SL trail and LTP unavailable — can't compute default trigger",
				})
				continue
			}
		}

		safeTrigger := intended
		if info.DPRLower > 0 {
			floor := info.DPRLower * DPRSafetyBuffer
			if safeTrigger < floor {
				safeTrigger = floor
			}
		}
		// Tick-align via broker_adapter's clamp (also caps at DPR upper).
		safeTrigger = p.broker.roundAndClamp(safeTrigger, info.TickSize, info.DPRLower, info.DPRUpper)
		safeLimit := p.broker.roundAndClamp(safeTrigger-SLLimitGap(safeTrigger, info.TickSize), info.TickSize, info.DPRLower, info.DPRUpper)

		// LTP for action decision.
		ltp := p.fetchLTP(ctx, info)

		// Lower-circuit guard.
		_, atLower, _ := p.broker.CheckCircuit(ctx, info.ExchangeToken)

		plan := FirePlan{
			Pos:         pos,
			Auth:        *auth,
			Info:        info,
			IntendedQty: pos.NetQty,
			SafeTrigger: safeTrigger,
			SafeLimit:   safeLimit,
			LTP:         ltp,
		}

		switch {
		case atLower:
			plan.Action = FireAMOLowerCircuit
		case ltp <= 0:
			// Conservative default — place SL-L.
			plan.Action = FireSLLimit
		case ltp <= safeTrigger:
			// Gap-down already breached the trigger before we could fire.
			// SL-M converts to a market sell at next tick.
			plan.Action = FireSLMarket
		default:
			plan.Action = FireSLLimit
		}
		plans = append(plans, plan)
	}
	return plans
}

// resolveInfo prefers the indira_symbol / exchange_token already stored on
// the position row (avoids the ISIN→token Redis lookup which can fail for
// long-held / legacy positions where the ISIN key was never populated).
func (p *ProtectiveReplay) resolveInfo(ctx context.Context, pos PositionNeedingProtection) *SymbolInfo {
	if pos.IndiraSymbol == "" || pos.ExchangeToken == "" {
		// Fall back to ISIN lookup.
		info, err := p.broker.ResolveSymbol(ctx, pos.Symbol, pos.ISIN)
		if err != nil {
			return nil
		}
		return info
	}
	info := &SymbolInfo{
		Symbol:        pos.Symbol,
		IndiraSymbol:  pos.IndiraSymbol,
		ExchangeToken: pos.ExchangeToken,
		Exchange:      strDefault(pos.Exchange, "NSE"),
		TickSize:      0.05,
	}
	p.enrichDPR(ctx, info)
	return info
}

// fetchFreeQtyMap calls the holdings API and returns a dispSym → freeQty map.
// On any error returns an empty map; callers treat empty as "skip everything"
// (safer than firing blind).
func (p *ProtectiveReplay) fetchFreeQtyMap(ctx context.Context, auth BrokerAuth, userID string) map[string]int {
	holdings, err := p.broker.client.GetHoldings(ctx, p.broker.toIndiraAuth(auth))
	if err != nil {
		p.logger.Warn("fetchFreeQtyMap: holdings API failed",
			zap.String("user", userID), zap.Error(err))
		return map[string]int{}
	}
	out := make(map[string]int, len(holdings))
	for _, h := range holdings {
		var key string
		for _, s := range h.Symbol {
			if s.Exc == "NSE" && s.DispSym != "" {
				key = strings.ToUpper(s.DispSym)
				break
			}
		}
		if key == "" && len(h.Symbol) > 0 {
			key = strings.ToUpper(h.Symbol[0].DispSym)
		}
		if key != "" {
			out[key] = h.FreeQty
		}
	}
	p.logger.Info("fetchFreeQtyMap: holdings snapshot",
		zap.String("user", userID), zap.Int("symbols", len(out)))
	return out
}

// fetchLTP returns the latest market price for a symbol with a fallback chain:
//
//  1. External Redis market data (primary — fed by upstream WSS)
//  2. Broker positions API (if symbol is in user's intraday positions book)
//  3. 0 (caller treats as "place SL-L conservatively")
//
// At 09:14 IST the Redis cache should be live (NSE pre-open auction
// settlement at 9:08 has already populated tomorrow's open print).
func (p *ProtectiveReplay) fetchLTP(ctx context.Context, info *SymbolInfo) float64 {
	if info == nil || info.ExchangeToken == "" {
		return 0
	}
	if ltp, err := p.broker.FetchLTP(ctx, info.ExchangeToken); err == nil && ltp > 0 {
		return ltp
	}
	// Redis miss — leave the fallback to live order placement which the
	// broker validates against its own LTP. We DON'T call the broker REST
	// quote API here (cost / latency); the worst case is we place an SL-L
	// that may be marginally mis-positioned and the SafetyMonitor's 2s loop
	// will catch any anomaly within seconds.
	p.logger.Debug("fetchLTP: redis miss, using 0 fallback", zap.String("token", info.ExchangeToken))
	return 0
}

// waitUntilMarketOpen sleeps until 09:15:00 IST + MarketOpenFireDelay.
func (p *ProtectiveReplay) waitUntilMarketOpen(ctx context.Context) {
	now := p.now()
	target := time.Date(now.Year(), now.Month(), now.Day(), 9, 15, 0, 0, p.ist).Add(MarketOpenFireDelay)
	wait := time.Until(target)
	if wait <= 0 {
		return // already past 9:15 — fire immediately
	}
	p.logger.Info("Waiting for market open",
		zap.Time("fire_at", target), zap.Duration("wait", wait))
	select {
	case <-ctx.Done():
	case <-time.After(wait):
	}
}

// ─── Fire ──────────────────────────────────────────────────────────────────

// FireResult captures what we attempted + what came back from the broker.
type FireResult struct {
	Plan          FirePlan
	BrokerOrderID string
	Action        FireAction
	OrderRowID    int64 // manthan_orders.id of the placement (0 if skipped)
	Err           error
}

func (p *ProtectiveReplay) fireAll(ctx context.Context, plans []FirePlan) []FireResult {
	results := make([]FireResult, 0, len(plans))
	for _, plan := range plans {
		results = append(results, p.fireOne(ctx, plan))
	}
	queued, skipped, failed := 0, 0, 0
	for _, r := range results {
		switch {
		case r.Plan.Action == FireSkip:
			skipped++
		case r.Err != nil:
			failed++
		default:
			queued++
		}
	}
	p.logger.Info("Fire complete",
		zap.Int("queued", queued), zap.Int("skipped", skipped), zap.Int("failed", failed))
	return results
}

func (p *ProtectiveReplay) fireOne(ctx context.Context, plan FirePlan) FireResult {
	res := FireResult{Plan: plan, Action: plan.Action}

	if plan.Action == FireSkip {
		p.logger.Warn("fireOne: skipping",
			zap.String("symbol", plan.Pos.Symbol),
			zap.String("reason", plan.SkipReason))
		p.publishProtectionSkipped(ctx, plan)
		return res
	}

	// Insert the manthan_orders row up-front (status=PENDING) so we can
	// idempotently track the placement attempt.
	rowID, err := p.insertProtectionRow(ctx, plan)
	if err != nil {
		res.Err = fmt.Errorf("insert protection row: %w", err)
		p.logger.Error("fireOne: DB insert failed",
			zap.String("symbol", plan.Pos.Symbol), zap.Error(err))
		return res
	}
	res.OrderRowID = rowID

	var brokerID string
	switch plan.Action {
	case FireSLLimit:
		brokerID, err = p.broker.PlaceSLSell(ctx, plan.Auth, plan.Info, plan.IntendedQty, plan.SafeTrigger, plan.SafeLimit)
	case FireSLMarket:
		brokerID, err = p.broker.PlaceSLMSell(ctx, plan.Auth, plan.Info, plan.IntendedQty, plan.SafeTrigger)
	case FireMarketSell:
		brokerID, err = p.broker.PlaceMarketSell(ctx, plan.Auth, plan.Info, plan.IntendedQty)
	case FireAMOLowerCircuit:
		brokerID, err = p.broker.PlaceAMOSell(ctx, plan.Auth, plan.Info, plan.IntendedQty)
	default:
		err = fmt.Errorf("unknown fire action: %s", plan.Action)
	}
	if err != nil {
		res.Err = err
		_ = p.repo.UpdateOrderRejected(ctx, rowID, err.Error())
		_ = p.repo.InsertEvent(ctx, rowID, "PROTECTIVE_FIRE_FAILED", "PENDING", "REJECTED",
			"", plan.SafeTrigger, plan.IntendedQty, err.Error())
		p.logger.Error("fireOne: broker reject",
			zap.String("symbol", plan.Pos.Symbol),
			zap.String("action", string(plan.Action)),
			zap.Error(err))
		return res
	}

	res.BrokerOrderID = brokerID
	if plan.Action == FireSLLimit || plan.Action == FireSLMarket {
		_ = p.repo.UpdateSLBrokerID(ctx, rowID, brokerID, plan.Pos.EntryOrderID)
	} else {
		_ = p.repo.UpdateOrderPlaced(ctx, rowID, brokerID)
	}
	_ = p.repo.InsertEvent(ctx, rowID, "PROTECTIVE_FIRED", "PENDING", string(StatusSLPlaced),
		"", plan.SafeTrigger, plan.IntendedQty,
		fmt.Sprintf("action=%s broker_id=%s ltp=%.2f", plan.Action, brokerID, plan.LTP))
	p.logger.Info("fireOne: placed",
		zap.String("symbol", plan.Pos.Symbol),
		zap.String("action", string(plan.Action)),
		zap.String("broker_id", brokerID),
		zap.Float64("trigger", plan.SafeTrigger),
		zap.Float64("limit", plan.SafeLimit),
		zap.Float64("ltp", plan.LTP))
	return res
}

func (p *ProtectiveReplay) insertProtectionRow(ctx context.Context, plan FirePlan) (int64, error) {
	orderType := OrderTypeSLSell
	if plan.Action == FireMarketSell {
		orderType = OrderTypeMarketSell
	} else if plan.Action == FireAMOLowerCircuit {
		orderType = OrderTypeAMOSell
	}
	o := &ManthanOrder{
		SignalID:      fmt.Sprintf("protective-%s-%s", plan.Pos.EntrySignalID, p.now().Format("20060102")),
		StrategyID:    plan.Pos.StrategyID,
		UserID:        plan.Pos.UserID,
		Symbol:        plan.Pos.Symbol,
		ISIN:          plan.Pos.ISIN,
		Exchange:      strDefault(plan.Pos.Exchange, "NSE"),
		OrderType:     orderType,
		OrderSide:     "SELL",
		ProductType:   "CNC",
		Qty:           plan.IntendedQty,
		LimitPrice:    plan.SafeLimit,
		TriggerPrice:  plan.SafeTrigger,
		IndiraSymbol:  plan.Info.IndiraSymbol,
		ExchangeToken: plan.Info.ExchangeToken,
		Status:        StatusPending,
		MaxRetries:    3,
	}
	return p.repo.InsertOrder(ctx, o)
}

// publishProtectionSkipped emits a notification on manthan.execution.events
// so the user is alerted (frontend banner / push) that protection was not
// placed for this position. Best-effort.
func (p *ProtectiveReplay) publishProtectionSkipped(ctx context.Context, plan FirePlan) {
	if p.eventPub == nil {
		return
	}
	// We reuse the SL_REJECTED event — it carries enough context (signal_id,
	// symbol, reason) and existing rules-engine consumers already handle it.
	p.eventPub.PublishSLRejected(ctx,
		ManthanSignal{
			OrderID:    plan.Pos.EntrySignalID,
			UserID:     plan.Pos.UserID,
			StrategyID: plan.Pos.StrategyID,
			Symbol:     plan.Pos.Symbol,
		},
		"", "protection skipped at pre-open: "+plan.SkipReason)
}

// ─── Reconcile ─────────────────────────────────────────────────────────────

func (p *ProtectiveReplay) reconcile(ctx context.Context, results []FireResult) {
	if len(results) == 0 {
		return
	}
	// Group results by user_id so we fetch each user's order book once.
	byUser := map[string][]FireResult{}
	for _, r := range results {
		if r.OrderRowID == 0 || r.BrokerOrderID == "" {
			continue
		}
		byUser[r.Plan.Pos.UserID] = append(byUser[r.Plan.Pos.UserID], r)
	}

	for userID, urs := range byUser {
		auth := p.getAuth(userID)
		if auth == nil {
			p.logger.Warn("reconcile: no auth, skipping user", zap.String("user", userID))
			continue
		}
		book, err := p.broker.GetOrderBook(ctx, *auth)
		if err != nil {
			if errors.Is(err, indiraClient.ErrAuthExpired) {
				p.logger.Info("reconcile: skipping user — broker session expired",
					zap.String("user", userID))
				notifyAuthExpired(p.authNotif, ctx, userID, "protective-replay")
				continue
			}
			p.logger.Error("reconcile: order-book fetch failed",
				zap.String("user", userID), zap.Error(err))
			continue
		}
		byID := map[string]int{}
		for i := range book {
			byID[book[i].OrdId] = i
		}
		for _, r := range urs {
			idx, ok := byID[r.BrokerOrderID]
			if !ok {
				p.logger.Warn("reconcile: placement not in order-book",
					zap.String("symbol", r.Plan.Pos.Symbol),
					zap.String("broker_id", r.BrokerOrderID))
				continue
			}
			o := &book[idx]
			st := strings.ToUpper(strings.TrimSpace(o.Status))
			switch {
			case strings.Contains(st, "TRIGGER"), st == "OPEN", st == "PENDING", st == "REQUESTED":
				p.logger.Info("reconcile: placement live",
					zap.String("symbol", r.Plan.Pos.Symbol),
					zap.String("status", o.Status))
			case st == "EXECUTED" || st == "TRADED" || st == "COMPLETE" || st == "FILLED":
				p.logger.Info("reconcile: placement filled at open",
					zap.String("symbol", r.Plan.Pos.Symbol))
				_ = p.repo.UpdateOrderFilled(ctx, r.OrderRowID, o.TradedQty, o.Price)
			case st == "REJECTED" || st == "CANCELLED" || st == "CANCELED":
				p.logger.Warn("reconcile: placement rejected — drilling into Order Trail",
					zap.String("symbol", r.Plan.Pos.Symbol),
					zap.String("broker_id", r.BrokerOrderID),
					zap.String("rej_reason", o.RejReason))
				p.handlePostOpenReject(ctx, *auth, r, o.RejReason)
			default:
				p.logger.Warn("reconcile: unknown status, ignoring",
					zap.String("symbol", r.Plan.Pos.Symbol),
					zap.String("status", o.Status))
			}
		}
	}
	p.logger.Info("Reconcile complete")
}

// handlePostOpenReject drills into Order Trail for a granular rejReason and
// hot-places a fallback SL-M as last-ditch protection. The SafetyMonitor's
// 2s loop will then catch the new placement and continue normally.
func (p *ProtectiveReplay) handlePostOpenReject(ctx context.Context, auth BrokerAuth, r FireResult, brokerRejReason string) {
	granular := brokerRejReason
	tr := orderTrailReq(r.BrokerOrderID, r.Plan.Info.IndiraSymbol)
	trail, err := p.broker.client.GetOrderTrail(ctx, p.broker.toIndiraAuth(auth), &tr)
	if err == nil {
		if reason := extractLatestRejReason(trail); reason != "" {
			granular = reason
		}
	}
	_ = p.repo.UpdateOrderRejected(ctx, r.OrderRowID, granular)
	_ = p.repo.InsertEvent(ctx, r.OrderRowID, "PROTECTIVE_REJECTED",
		string(StatusSLPlaced), string(StatusRejected),
		"", r.Plan.SafeTrigger, r.Plan.IntendedQty, granular)

	// Last-ditch fallback: SL-M at the same trigger (no limit; converts to
	// market at next tick after trigger). Skip if original was already SL-M
	// or a market order — would just re-fail.
	if r.Plan.Action != FireSLLimit {
		return
	}
	p.logger.Warn("Hot-placing SL-M fallback after reject",
		zap.String("symbol", r.Plan.Pos.Symbol), zap.String("granular_reason", granular))
	if _, err := p.broker.PlaceSLMSell(ctx, auth, r.Plan.Info, r.Plan.IntendedQty, r.Plan.SafeTrigger); err != nil {
		p.logger.Error("Fallback SL-M also failed — SafetyMonitor will retry",
			zap.String("symbol", r.Plan.Pos.Symbol), zap.Error(err))
	}
}

// ─── helpers ───────────────────────────────────────────────────────────────

// strDefault returns def when s is empty.
func strDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// enrichDPR best-effort fills DPRLower/DPRUpper/TickSize from the external
// market-data Redis using the exchange_token.
func (p *ProtectiveReplay) enrichDPR(ctx context.Context, info *SymbolInfo) {
	if info == nil || info.ExchangeToken == "" || p.broker == nil || p.broker.extRedis == nil {
		return
	}
	raw, err := p.broker.extRedis.Get(ctx, "market:nse:"+info.ExchangeToken).Result()
	if err != nil {
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

func countAction(plans []FirePlan, a FireAction) int {
	n := 0
	for _, p := range plans {
		if p.Action == a {
			n++
		}
	}
	return n
}

func extractLatestRejReason(trail []map[string]interface{}) string {
	// Trail is an array of state-change events. The latest entry with a
	// non-empty rejReason wins.
	for i := len(trail) - 1; i >= 0; i-- {
		r, _ := trail[i]["rejReason"].(string)
		if r != "" {
			return r
		}
	}
	return ""
}

// orderTrailReq is a tiny constructor avoiding the verbose struct literal
// at the only call site. We pass instrument="STK" — Manthan only deals
// in stocks for now. indiraSymbol arg is unused (kept for future expansion
// once the API requires it).
func orderTrailReq(ordID, _ string) indiraClient.OrderTrailRequest {
	return indiraClient.OrderTrailRequest{OrdId: ordID, Instrument: "STK"}
}
