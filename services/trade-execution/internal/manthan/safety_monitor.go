package manthan

// TODO(orderstatus): moves to orderstatus svc when extracted.
// See docs/orderstatus_service_design.md — this file's WSS-driven / poll-driven
// concern belongs to the status-observation layer, not the placement layer.

import (
	"context"
	"errors"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// SafetyMonitor runs every poll interval (15 s in production) checking ALL active Manthan positions.
//
// Checks:
//  1. Position has SL order on broker? If not → re-place immediately
//  2. LTP below SL trigger? Verify SL filled → if not → MARKET SELL
//  3. Lower circuit? → AMO SELL for next day
//  4. Auth still valid? Ping every 5 min
//
// This is the insurance policy — prevents unlimited downside even if
// WSS drops, SL order expires, or broker cancels our SL.
type SafetyMonitor struct {
	broker    *BrokerAdapter
	repo      *Repository
	slHandler *SLHandler
	logger    *zap.Logger

	pollInterval      time.Duration
	authCheckInterval time.Duration
	lastAuthCheck     time.Time

	// getActivePositions returns all active Manthan positions from DB.
	getActivePositions func(ctx context.Context) ([]*ManthanOrder, error)
	// getAuth returns auth for a user (from credentials cache).
	getAuth func(userID string) *BrokerAuth

	// eventPub publishes SL_FILLED, EXIT_FILLED to manthan.execution.events.
	// Optional — nil-safe; falls back to local DB events only.
	eventPub *ManthanEventPublisher

	// Optional SESSION_EXPIRED publisher — nil-safe.
	authNotif AuthExpiryNotifier
}

// SetEventPublisher wires the centralized publisher used to emit SL_FILLED
// and EXIT_FILLED on broker-confirmed exits detected by polling.
func (m *SafetyMonitor) SetEventPublisher(p *ManthanEventPublisher) {
	m.eventPub = p
}

// SetAuthExpiryNotifier wires the SESSION_EXPIRED publisher fired when a
// safety-monitor SL poll returns AU004.
func (m *SafetyMonitor) SetAuthExpiryNotifier(n AuthExpiryNotifier) {
	m.authNotif = n
}

type SafetyMonitorConfig struct {
	PollInterval      time.Duration // default 2s
	AuthCheckInterval time.Duration // default 5min
}

func NewSafetyMonitor(
	broker *BrokerAdapter,
	repo *Repository,
	slHandler *SLHandler,
	getActivePositions func(ctx context.Context) ([]*ManthanOrder, error),
	getAuth func(userID string) *BrokerAuth,
	logger *zap.Logger,
	cfg SafetyMonitorConfig,
) *SafetyMonitor {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.AuthCheckInterval <= 0 {
		cfg.AuthCheckInterval = 5 * time.Minute
	}
	return &SafetyMonitor{
		broker:             broker,
		repo:               repo,
		slHandler:          slHandler,
		logger:             logger,
		pollInterval:       cfg.PollInterval,
		authCheckInterval:  cfg.AuthCheckInterval,
		getActivePositions: getActivePositions,
		getAuth:            getAuth,
	}
}

// Start begins the safety monitor loop. Blocks until ctx cancelled.
func (m *SafetyMonitor) Start(ctx context.Context) {
	m.logger.Info("Safety monitor started", zap.Duration("interval", m.pollInterval))
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Safety monitor stopped")
			return
		case <-ticker.C:
			m.check(ctx)
		}
	}
}

// marketOpenForSL reports whether a LIVE stop-loss order can be placed/modified
// right now: an exchange trading day (IsTradingDay handles weekends AND holidays)
// AND within the regular session, 09:15–15:30 IST. Outside this window Indira
// rejects SL placement and DAY SLs are auto-cancelled at 15:30 — overnight
// protection is the EOD Phase A AMO cron's responsibility, so the safety
// monitor must not attempt anything.
func (m *SafetyMonitor) marketOpenForSL() bool {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	if loc == nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}
	now := time.Now().In(loc)
	if !indiraClient.IsTradingDay(now) {
		return false // Saturday / Sunday / exchange holiday
	}
	mod := now.Hour()*60 + now.Minute()
	const open = 9*60 + 15            // 09:15
	const close = 15*60 + 30          // 15:30 (broker auto-cancels DAY SLs at close)
	return mod >= open && mod < close // 15:30:xx excluded — the exchange sweeps DAY orders at 15:30:00
}

func (m *SafetyMonitor) check(ctx context.Context) {
	// Market-hours guard (2026-08-08 incident): stand down entirely when the
	// exchange is closed. Every action this monitor takes — placing a naked
	// position's SL, re-placing a vanished SL, MARKET-selling on a breach —
	// requires a LIVE session and an OPEN market; none can succeed after
	// 15:30, before 09:15, or on a weekend/holiday. With no guard the 15s
	// ticker hammered the broker with 7,464 place-order attempts across a
	// single Saturday, each returning AU004. Overnight/weekend protection is
	// the EOD Phase A AMO cron's job, not this monitor's.
	if !m.marketOpenForSL() {
		return
	}

	// FIX 3: enforce "every OPEN filled BUY position has exactly one active SL".
	// Runs first so a naked position (fill whose SL never landed) gets protected
	// within one 2s cycle — before we even look at existing SL orders.
	m.checkNakedPositions(ctx)

	// 2026-10-22: exits are LIMIT orders that can rest — confirm fills and
	// re-price every cycle so a stopped-out position actually leaves the book.
	m.checkPendingExits(ctx)

	// Get all active SL orders
	slOrders, err := m.repo.GetActiveSLOrders(ctx)
	if err != nil {
		m.logger.Error("Safety monitor: failed to get SL orders", zap.Error(err))
		return
	}

	if len(slOrders) == 0 {
		return
	}

	for _, sl := range slOrders {
		m.checkPosition(ctx, sl)
	}
}

// checkNakedPositions places the initial SL for every OPEN filled BUY position
// that currently has no active stop-loss (FIX 3). Idempotent: PlaceInitialSL
// reuses any prior terminal SL row (never a duplicate broker SL), and a
// position already protected won't appear in the query. Liquidation-hazard-safe
// — it only PLACES stops, never cancels or exits.
func (m *SafetyMonitor) checkNakedPositions(ctx context.Context) {
	naked, err := m.repo.ListNakedOpenBuyPositions(ctx)
	if err != nil {
		m.logger.Error("Safety monitor: naked-position query failed", zap.Error(err))
		return
	}
	for _, pos := range naked {
		// Defensive: the query returns the UNCOVERED share count (net − active
		// SL coverage). If it's ≤ 0 the position is already fully protected —
		// never place a stop for zero/negative qty (that would over-commit or
		// error at the broker). Belt-and-suspenders alongside the query's own
		// `n.net > covered` guard (2026-08-11 PICCADIL over-commit fix).
		if pos.NetQty <= 0 {
			continue
		}
		// Skip users whose JWT is known-dead this cycle (avoid AU004 spam); the
		// next cycle retries once creds refresh.
		if authGated(m.authNotif, pos.UserID) {
			continue
		}
		info, rerr := m.broker.ResolveSymbol(ctx, pos.Symbol, pos.ISIN)
		if rerr != nil || info == nil {
			if pos.ExchangeToken == "" {
				m.logger.Warn("Safety monitor: naked position — symbol resolve failed and no stored token, will retry next cycle",
					zap.String("symbol", pos.Symbol), zap.Error(rerr))
				continue
			}
			// No ISIN key (legacy row): fall back to the stored token.
			info = &SymbolInfo{
				Symbol: pos.Symbol, ISIN: pos.ISIN, IndiraSymbol: pos.IndiraSymbol,
				ExchangeToken: pos.ExchangeToken, Exchange: "NSE", TickSize: 0.05,
			}
			m.broker.EnrichMarketInfo(ctx, info)
		}
		// Baseline protection: 20% below the entry fill. If rules-engine has
		// trailed higher, its next SL_MODIFY ratchets up (the ratchet guard
		// prevents lowering), so this can never over-tighten a trailed stop.
		trigger := pos.EntryFillPrice * slFactor(m.repo.GetEntryStopLossPct(ctx, pos.EntrySignalID))
		limit := trigger - SLLimitGap(trigger, info.TickSize)

		signal := ManthanSignal{
			OrderID:     pos.EntrySignalID,
			UserID:      pos.UserID,
			StrategyID:  pos.StrategyID,
			Symbol:      pos.Symbol,
			ISIN:        pos.ISIN,
			TradingMode: "LIVE", // query already excludes PAPER-% broker orders
		}
		m.logger.Warn("Safety monitor: NAKED position detected — placing initial SL",
			zap.String("symbol", pos.Symbol),
			zap.Int("net_qty", pos.NetQty),
			zap.Float64("entry_fill", pos.EntryFillPrice),
			zap.Float64("trigger", trigger))
		if slErr := m.slHandler.PlaceInitialSL(ctx, pos.EntryOrderID, signal, info, pos.NetQty, trigger, limit); slErr != nil {
			m.logger.Error("Safety monitor: naked-position SL placement failed — will retry next cycle",
				zap.String("symbol", pos.Symbol), zap.Error(slErr))
		}
	}
}

// checkPosition runs the per-stop checks under the position's advisory lock
// so they serialise with the inbox exit path (which cancels the stop and
// sells) — the two must never act on the same quantity at once.
func (m *SafetyMonitor) checkPosition(ctx context.Context, sl *ManthanOrder) {
	if m.repo == nil {
		m.checkPositionLocked(ctx, sl)
		return
	}
	if err := m.repo.WithPositionLock(ctx, sl.StrategyID, sl.Symbol, func(ctx context.Context) error {
		m.checkPositionLocked(ctx, sl)
		return nil
	}); err != nil {
		m.logger.Warn("Safety monitor: position lock failed — skipping this cycle",
			zap.String("symbol", sl.Symbol), zap.Error(err))
	}
}

func (m *SafetyMonitor) checkPositionLocked(ctx context.Context, sl *ManthanOrder) {
	// Gate: a recent AU004 keeps this user "expired" for 5 min. Skip silently
	// — every other position for the same user will hit the same dead JWT,
	// so attempting them just spams the HTTP layer with identical AU004s.
	if authGated(m.authNotif, sl.UserID) {
		return
	}
	auth := m.getAuth(sl.UserID)
	if auth == nil {
		return // no auth available — skip this cycle
	}

	// An exit sell of ours already owns this position's quantity: the exit
	// path (checkPendingExits) confirms or chases it. Touching the stop or
	// selling again here would double-sell or be rejected "Free Qty=0".
	if open, oerr := m.repo.HasOpenExitSell(ctx, sl.StrategyID, sl.Symbol); oerr == nil && open {
		return
	}

	// CHECK 1: Does SL order still exist on broker?
	if sl.BrokerOrderID != "" && sl.Status == StatusSLPlaced {
		status, filledQty, avgPrice, err := m.broker.GetOrderStatus(ctx, *auth, sl.BrokerOrderID)
		if err != nil {
			// Broker session dead — surface a SESSION_EXPIRED notification so
			// the user sees the re-login flash. The dedup window in the
			// notifier collapses the per-position bursts (this monitor ticks
			// once per active position every 15s) into one event per ~5min.
			if errors.Is(err, indiraClient.ErrAuthExpired) {
				notifyAuthExpired(m.authNotif, ctx, sl.UserID, "safety-monitor")
				return
			}
			// The book was read fine and the stop is simply not in it (swept
			// GTC, cancelled long ago): that is a LOST stop, not "can't check"
			// (2026-10-22 review — such rows were never re-protected). Give the
			// book a minute after placement before concluding.
			placed := sl.CreatedAt
			if sl.PlacedAt != nil {
				placed = *sl.PlacedAt
			}
			if IsOrderNotInBook(err) && time.Since(placed) > exitLostAfter {
				m.logger.Warn("Safety monitor: stop not in the broker book — re-protecting",
					zap.String("symbol", sl.Symbol), zap.String("broker_order_id", sl.BrokerOrderID))
				_ = m.repo.InsertEvent(ctx, sl.ID, "SL_LOST", "SL_PLACED", "CANCELLED",
					"NOT_IN_BOOK", 0, 0, "stop absent from the broker book — re-protecting")
				m.handleLostStop(ctx, auth, sl, "NOT_IN_BOOK")
			}
			return
		}

		// SL was filled while we weren't looking (e.g., during restart)
		if isFilledStatus(status) {
			m.logger.Info("Safety monitor: SL filled (detected on check)",
				zap.String("symbol", sl.Symbol),
				zap.Int("filled_qty", filledQty),
				zap.Float64("avg_price", avgPrice))

			_ = m.repo.UpdateOrderFilled(ctx, sl.ID, filledQty, avgPrice)
			_ = m.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", "SL_PLACED", "SL_FILLED",
				status, avgPrice, filledQty, "detected by safety monitor")

			// Publish SL_FILLED so rules-engine FillConsumer can flip
			// manthan_positions to EXITED + insert a position event. Resolve
			// entry signal_id via parent_order_id (SL rows carry the entry FK).
			if m.eventPub != nil {
				if entrySignalID, err := m.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); err == nil && entrySignalID != "" {
					m.eventPub.PublishSLFilled(ctx, entrySignalID, sl.StrategyID, sl.UserID,
						sl.Symbol, sl.BrokerOrderID, avgPrice, int32(filledQty), 0)
				}
			}
			return
		}

		// SL was cancelled/rejected externally. REST order-book statuses are
		// mixed case ("Cancelled" / "Rejected") — normalise before comparing.
		if isCancelledBrokerStatus(strings.ToUpper(strings.TrimSpace(status))) {
			if filledQty > 0 {
				// Partly filled, then cancelled: the filled part is real money —
				// book it; the remainder is uncovered and the naked scan places a
				// fresh stop for exactly that quantity next cycle.
				if uerr := m.repo.UpdateOrderFilled(ctx, sl.ID, filledQty, avgPrice); uerr != nil {
					m.logger.Error("Safety monitor: partial stop fill could not be booked", zap.Int64("row", sl.ID), zap.Error(uerr))
					return
				}
				_ = m.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", "SL_PLACED", "SL_FILLED",
					status, avgPrice, filledQty, "stop partly filled then cancelled — partial booked; remainder re-protected by the naked scan")
				if m.eventPub != nil {
					if entrySignalID, e := m.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); e == nil && entrySignalID != "" {
						m.eventPub.PublishSLFilled(ctx, entrySignalID, sl.StrategyID, sl.UserID,
							sl.Symbol, sl.BrokerOrderID, avgPrice, int32(filledQty), 0)
					}
				}
				return
			}
			m.logger.Warn("Safety monitor: SL order missing at broker — re-protecting",
				zap.String("symbol", sl.Symbol),
				zap.String("broker_status", status))
			_ = m.repo.InsertEvent(ctx, sl.ID, "SL_LOST", "SL_PLACED", "CANCELLED",
				status, 0, 0, "SL cancelled/rejected externally — re-protecting")
			m.handleLostStop(ctx, auth, sl, status)
			return
		}
	}

	// CHECK 2: LTP below SL trigger? (backup for WSS lag). The price comes from
	// the LIVE token row (ISIN key); a stale row (dead token) never drives this.
	if sl.TriggerPrice > 0 && sl.Status == StatusSLPlaced {
		info := &SymbolInfo{
			Symbol:        sl.Symbol,
			ISIN:          sl.ISIN,
			IndiraSymbol:  sl.IndiraSymbol,
			ExchangeToken: sl.ExchangeToken,
			Exchange:      "NSE",
			TickSize:      0.05,
		}
		ltp, stale, _, lerr := m.broker.FreshLTP(ctx, info)
		if lerr != nil || stale || ltp <= 0 {
			return
		}
		if info.ExchangeToken != "" && info.ExchangeToken != sl.ExchangeToken {
			_, _ = m.repo.UpdateSymbolToken(ctx, sl.StrategyID, sl.Symbol, info.ExchangeToken, info.IndiraSymbol)
		}
		if ltp > sl.TriggerPrice {
			return
		}

		// SL should have triggered — check if it filled
		status, filledQty, avgPrice, _ := m.broker.GetOrderStatus(ctx, *auth, sl.BrokerOrderID)
		if isFilledStatus(status) {
			_ = m.repo.UpdateOrderFilled(ctx, sl.ID, filledQty, avgPrice)
			_ = m.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", "SL_PLACED", "SL_FILLED",
				status, avgPrice, filledQty, "LTP below trigger — confirmed filled by safety monitor")
			if m.eventPub != nil {
				if entrySignalID, err := m.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); err == nil && entrySignalID != "" {
					m.eventPub.PublishSLFilled(ctx, entrySignalID, sl.StrategyID, sl.UserID,
						sl.Symbol, sl.BrokerOrderID, avgPrice, int32(filledQty), 0)
				}
			}
			return
		}

		// SL-L triggered but NOT filled (limit leg blown through by a fast
		// down-move), or the feed shows the stop crossed and the resting
		// order never triggered. Escalation (2026-10-22 revision):
		//   1. Cancel the stuck SL-L (frees the quantity at the broker)
		//   2. Lower circuit → AMO LIMIT at the band floor (only exit path)
		//   3. Else a marketable LIMIT sell NOW (LTP − slippage, floored at
		//      the band). SL-M and Market orders are rejected for
		//      algo-tagged orders.
		// The sell is booked as a PLACED exit row; checkPendingExits
		// confirms the fill or re-prices it if it rests.
		m.logger.Warn("Safety monitor: LTP below SL but not filled — escalating exit",
			zap.String("symbol", sl.Symbol),
			zap.Float64("ltp", ltp),
			zap.Float64("trigger", sl.TriggerPrice))
		m.broker.EnrichMarketInfo(ctx, info)

		if cerr := m.broker.CancelOrder(ctx, *auth, info, sl.BrokerOrderID); cerr != nil && !IsOrderNotFoundError(cerr) {
			m.logger.Error("Safety monitor: stuck SL cancel failed — retrying next cycle",
				zap.String("symbol", sl.Symbol), zap.Error(cerr))
			return
		}
		// The cancel may have raced a fill at the exchange — re-check first.
		if st, fq, ap, serr := m.broker.GetOrderStatus(ctx, *auth, sl.BrokerOrderID); serr == nil && isFilledStatus(st) {
			_ = m.repo.UpdateOrderFilled(ctx, sl.ID, fq, ap)
			_ = m.repo.InsertEvent(ctx, sl.ID, "SL_FILLED", "SL_PLACED", "SL_FILLED",
				st, ap, fq, "filled during escalation cancel")
			if m.eventPub != nil {
				if entrySignalID, err := m.repo.GetEntrySignalIDByOrderID(ctx, sl.ID); err == nil && entrySignalID != "" {
					m.eventPub.PublishSLFilled(ctx, entrySignalID, sl.StrategyID, sl.UserID,
						sl.Symbol, sl.BrokerOrderID, ap, int32(fq), 0)
				}
			}
			return
		}
		if uerr := m.repo.UpdateOrderCancelled(ctx, sl.ID); uerr != nil {
			m.logger.Error("Safety monitor: could not mark the cancelled stop — not selling this cycle",
				zap.String("symbol", sl.Symbol), zap.Error(uerr))
			return
		}
		_ = m.repo.InsertEvent(ctx, sl.ID, "SL_ESCALATED", "SL_PLACED", "CANCELLED",
			"", ltp, sl.Qty, "stop crossed but unfilled — cancelled; exiting with a marketable LIMIT")

		_, atLower, _ := m.broker.CheckCircuit(ctx, info.ExchangeToken)
		if atLower {
			m.logger.Warn("Safety monitor: lower circuit — AMO LIMIT SELL at the band floor",
				zap.String("symbol", sl.Symbol))
			amoBrokerID, amoLimit, aerr := m.broker.PlaceAMOSellPriced(ctx, *auth, info, sl.Qty)
			if aerr != nil {
				m.logger.Error("Safety monitor: AMO exit FAILED — position has no stop; manual intervention required",
					zap.String("symbol", sl.Symbol), zap.Error(aerr))
				_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL_FAILED", "CANCELLED", "CANCELLED",
					"", ltp, sl.Qty, "lower circuit AMO exit failed: "+aerr.Error())
				notifyOps(m.authNotif, ctx, sl.UserID, sl.Symbol, "EXIT_FAILED", "stop crossed, AMO exit rejected: "+aerr.Error())
				return
			}
			m.recordExit(ctx, sl, OrderTypeAMOSell, info, sl.Qty, amoLimit, amoBrokerID, "lower circuit — AMO exit after stuck SL")
			return
		}

		mkBrokerID, limit, mkErr := m.broker.PlaceMarketableLimitSell(ctx, *auth, info, sl.Qty)
		if mkErr != nil {
			m.logger.Error("EMERGENCY LIMIT SELL FAILED — position has no stop; manual intervention required",
				zap.String("symbol", sl.Symbol),
				zap.Int("qty", sl.Qty), zap.Error(mkErr))
			_ = m.repo.InsertEvent(ctx, sl.ID, "EMERGENCY_SELL_FAILED", "CANCELLED", "CANCELLED",
				"", ltp, sl.Qty, "LIMIT exit failed: "+mkErr.Error())
			notifyOps(m.authNotif, ctx, sl.UserID, sl.Symbol, "EXIT_FAILED", "stop crossed, LIMIT exit rejected: "+mkErr.Error())
			return
		}
		m.recordExit(ctx, sl, OrderTypeMarketSell, info, sl.Qty, limit, mkBrokerID, "LTP below trigger, stop unfilled → marketable LIMIT exit")
	}
}
