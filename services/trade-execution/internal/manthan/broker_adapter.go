package manthan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// BrokerAdapter wraps pkg/indira with Manthan-specific logic:
// symbol resolution, tick rounding, DPR clamping, and typed order methods.
type BrokerAdapter struct {
	client   *indiraClient.Client
	extRedis *redis.Client // external Redis for LTP, tick size, DPR, ISIN→token
	logger   *zap.Logger
}

// BrokerRejection is the structured form of a broker/exchange rejection,
// returned by every BrokerAdapter order method so callers can decide retry
// vs terminal-fail based on Category without parsing free text.
//
// Built from indiraClient.ParseCodifiResponse over the response's infoID,
// infoMsg, RejReason fields. See pkg/indira/error_codes.go for the full
// pattern catalog (built from 2026-06-06 NSE Contingency Drill captures).
type BrokerRejection struct {
	indiraClient.ExchangeRejection
	Operation string // "place_limit_buy", "place_market_buy", "place_sl_sell", "place_amo_sl", "modify_sl"
	OrderID   string // broker_order_id if the rejection arrives post-acceptance (Order Book), else ""
}

// Error implements error so callers can do `errors.Is/As` and existing
// log.Error(err) sites keep working.
func (e *BrokerRejection) Error() string {
	if e.OrderID != "" {
		return fmt.Sprintf("%s rejected (%s): %s [order %s]",
			e.Operation, e.ExchangeRejection.String(), e.Raw, e.OrderID)
	}
	return fmt.Sprintf("%s rejected (%s): %s",
		e.Operation, e.ExchangeRejection.String(), e.Raw)
}

// IsRetryable reports whether the rejection category permits an automatic
// retry after recovering inputs (tick-round, DPR-clamp, etc.).
func (e *BrokerRejection) IsRetryable() bool { return e.Retryable }

// parseRejection builds a *BrokerRejection from a Codifi PlaceOrder response.
// Returns nil if the response indicates the order was NOT rejected.
//
// The op parameter identifies the call site so logs / audit can tell which
// adapter method produced the rejection. Common values:
//
//	"place_limit_buy", "place_market_buy", "place_sl_sell",
//	"place_amo_sl",   "modify_sl_trigger", "cancel_sl".
func parseRejection(op string, resp *indiraClient.PlaceOrderResponse) *BrokerRejection {
	if resp == nil {
		return nil
	}
	// Codifi's data.ordStatus + data.rejReason. Also accept the legacy
	// top-level Status="error" / Message variant a few endpoints use.
	rejected := resp.OrdStatus == "Rejected" || resp.Status == "Rejected" || resp.Status == "error"
	if !rejected {
		return nil
	}
	// We don't have the API-envelope infoID/infoMsg here (the indira client
	// already promoted those to BrokerBusinessError before returning).
	// Parse against rejReason / Message.
	parsed := indiraClient.ParseExchangeRejection(firstNonEmpty(resp.RejReason, resp.Message))
	return &BrokerRejection{
		ExchangeRejection: parsed,
		Operation:         op,
		OrderID:           firstNonEmpty(resp.OrdId, resp.OrderId),
	}
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

// AMODprBuffer is the safety multiplier applied to DPR_lower when we're
// preparing an AMO+SL trigger that has to survive overnight conversion.
//
// Why: today's DPR (cached at 16:00 IST submission) ≠ tomorrow's DPR (re-
// computed by NSE pre-open based on volatility bucket). Triggers placed at
// exactly today's DPR_lower may fall outside tomorrow's slightly-tighter band
// and get rejected at AMO conversion (08:50 IST), as observed in the
// 2026-04-29 KINGFA / NATIONALUM live test.
//
// 50bps inside DPR absorbs almost any realistic overnight band shift, costs
// at most an additional 0.5% drawdown if the SL fires, and keeps the
// algorithmic trigger when it's already comfortably inside DPR.
const AMODprBuffer = 1.005 // RETIRED 2026-10-22: the adapter no longer biases triggers (see PlaceAMOSLSell); kept for reference only

func NewBrokerAdapter(client *indiraClient.Client, extRedis *redis.Client, logger *zap.Logger) *BrokerAdapter {
	logger.Info("Manthan pricing knobs (effective values; env blocks are whitelists — forward them in ecosystem.config.js)",
		zap.Float64("MANTHAN_EXIT_LIMIT_SLIPPAGE_PCT", exitLimitSlippagePct),
		zap.Float64("MANTHAN_SL_LIMIT_GAP_PCT", slLimitGapPct),
		zap.Float64("MANTHAN_ENTRY_LIMIT_SLIPPAGE_PCT", entryLimitSlippagePct),
		zap.Duration("MANTHAN_FEED_STALE_AFTER", feedStaleAfter))
	return &BrokerAdapter{client: client, extRedis: extRedis, logger: logger}
}

// ResolveSymbol resolves a stock symbol to full Indira symbol format + tick/DPR.
func (b *BrokerAdapter) ResolveSymbol(ctx context.Context, symbol, isin string) (*SymbolInfo, error) {
	token, err := b.resolveToken(ctx, isin)
	if err != nil {
		return nil, fmt.Errorf("resolve token for %s: %w", symbol, err)
	}

	info := &SymbolInfo{
		Symbol:        symbol,
		ISIN:          isin,
		ExchangeToken: token,
		IndiraSymbol:  fmt.Sprintf("STK_%s_EQ_NSE_%s", symbol, token),
		Exchange:      "NSE",
		TickSize:      0.05,
	}

	b.EnrichMarketInfo(ctx, info)

	return info, nil
}

// FetchLTP gets the current last traded price from external Redis.
func (b *BrokerAdapter) FetchLTP(ctx context.Context, token string) (float64, error) {
	raw, err := b.extRedis.Get(ctx, "market:nse:"+token).Result()
	if err != nil {
		return 0, fmt.Errorf("ltp fetch failed for token %s: %w", token, err)
	}
	var mkt struct {
		LTP float64 `json:"ltp"`
	}
	if err := json.Unmarshal([]byte(raw), &mkt); err != nil {
		return 0, err
	}
	if mkt.LTP <= 0 {
		return 0, fmt.Errorf("ltp is 0 for token %s", token)
	}
	return mkt.LTP, nil
}

// PlaceLimitBuy places a LIMIT BUY order for DELIVERY.
func (b *BrokerAdapter) PlaceLimitBuy(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, price float64) (string, error) {
	roundedPrice := b.roundAndClamp(price, info.TickSize, info.DPRLower, info.DPRUpper)

	req := &indiraClient.PlaceOrderRequest{
		Symbol:       info.IndiraSymbol,
		ExcToken:     info.ExchangeToken,
		Exc:          "NSE",
		OrdAction:    "BUY",
		OrdValidity:  "DAY",
		OrdType:      "Limit",
		PrdType:      "DELIVERY",
		LimitPrice:   indiraClient.Price2DP(roundedPrice),
		TriggerPrice: 0, // explicit 0 for non-SL orders
		Qty:          qty,
		DisQty:       0,
		LotSize:      1,
		Instrument:   "STK",
		Amo:          false,
	}

	resp, err := b.client.PlaceOrder(ctx, b.toIndiraAuth(auth), req)
	if err != nil {
		return "", err
	}

	orderID := resp.OrderId
	if orderID == "" {
		orderID = resp.OrdId
	}
	if orderID == "" {
		return "", fmt.Errorf("broker returned empty order ID")
	}

	// Check immediate rejection from response (per API docs: data.ordStatus + data.rejReason).
	// Parsed into a structured BrokerRejection so callers can inspect Code/Tag/Category
	// (see pkg/indira/error_codes.go for the catalog).
	if rej := parseRejection("place_limit_buy", resp); rej != nil {
		return "", rej
	}

	b.logger.Info("LIMIT BUY placed",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("price", roundedPrice),
		zap.String("broker_order_id", orderID))

	return orderID, nil
}

// PlaceMarketBuy places a MARKET BUY order for DELIVERY — used as the
// guaranteed-fill fallback when LIMIT retries fail on an illiquid stock.
// Sizing caller's responsibility (allocator sets qty based on per_call).
//
// 2026-10-22: the broker rejects OrdType "Market" for SEBI algo-tagged orders
// ("Market order not allowed for Algo order"), so this is now a marketable
// LIMIT BUY: LTP × (1 + MANTHAN_ENTRY_LIMIT_SLIPPAGE_PCT, default 1%), capped
// at the upper band, priced from the LIVE Redis row (token re-resolved from
// the ISIN key when the carried row is missing/stale). Returns ErrUnpriced
// when no fresh price exists — nothing is sent blind. Name kept for the two
// call sites (LIMIT-retries-exhausted fallback, partial-fill top-up).
func (b *BrokerAdapter) PlaceMarketBuy(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int) (string, error) {
	ltp, stale, asOf, err := b.FreshLTP(ctx, info)
	if err != nil || stale || ltp <= 0 {
		return "", fmt.Errorf("%w: marketable BUY %s: no fresh LTP (stale=%v as_of=%s err=%v)",
			ErrUnpriced, info.Symbol, stale, asOf.Format(time.RFC3339), err)
	}
	b.EnrichMarketInfo(ctx, info)
	price := ltp * (1 + entryLimitSlippagePct/100)
	if info.DPRUpper > 0 && price > info.DPRUpper {
		price = info.DPRUpper
	}
	orderID, err := b.PlaceLimitBuy(ctx, auth, info, qty, price)
	if err != nil {
		return "", err
	}
	b.logger.Info("MARKET BUY requested → marketable LIMIT BUY placed (Market is rejected for algo orders)",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("ltp", ltp),
		zap.Float64("limit", price),
		zap.String("broker_order_id", orderID))
	return orderID, nil
}

// IsAuthError reports whether the error indicates the broker rejected the
// bearer token as invalid/expired. Used by the auto-refresh retry pattern:
// on such an error, callers should invalidate the credentials cache,
// re-fetch from DB, and retry the broker call once.
//
// Indira-specific signals:
//
//	HTTP 401 "Session expired"            — no Bearer / invalid shape
//	HTTP 200 "AU004 Session data not received" — JWT signature valid but
//	                                        the server-side trading session
//	                                        was invalidated (typically by a
//	                                        newer browser login)
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}
	// Indira overloads the AU004 code: "AU004 Pending Order Not Found" means
	// the TARGET ORDER is gone (modify/cancel of a dead order), not that the
	// session is bad. Classifying it as auth caused a refresh+retry loop and,
	// worse, escalation to emergency sell (2026-08-26 IOLCP incident: GTC SL
	// silently downgraded to DAY expired at close → modify hit "not found" →
	// position market-sold). Order-not-found is never an auth failure.
	if IsOrderNotFoundError(err) {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "AU004") ||
		strings.Contains(s, "Session expired") ||
		strings.Contains(s, "Session data not received") ||
		strings.Contains(s, "HTTP error 401") ||
		strings.Contains(s, "status code 401")
}

// ErrTriggerOutsideBand: a stop trigger below the day's band was refused by the
// adapter (the trigger is the exact strategy stop and is never moved).
var ErrTriggerOutsideBand = errors.New("stop trigger outside the band")

// ErrOrderNotInBook: GetOrderStatus fetched the order book successfully and the
// id was not in it. Typed so callers never match free text ("not found") that a
// transport or auth error might also contain.
var ErrOrderNotInBook = errors.New("order not in broker order book")

// IsOrderNotInBook reports whether err is (or wraps) ErrOrderNotInBook.
func IsOrderNotInBook(err error) bool { return errors.Is(err, ErrOrderNotInBook) }

// IsOrderNotFoundError reports whether the broker says the order we tried to
// modify/cancel no longer exists in its book ("AU004 Pending Order Not
// Found"). The position is unaffected — the correct reaction is to mark our
// row dead and place a FRESH order, never to touch the position itself.
func IsOrderNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Pending Order Not Found")
}

// SLLimitGap computes the gap between SL trigger and SL limit. Returns the
// wider of (tickSize × 5) and (trigger × 0.003 = 0.3%). The old hardcoded
// `tickSize × 5` was too tight on high-priced stocks — on APARINDS
// (tick ₹0.50) that's only ₹2.50 = 0.02%, which means any fast down-move
// (gap-down, circuit) blows past the limit without filling. The SL order
// sits triggered-but-unfilled, our safety monitor then cancels + emergency
// market-sells at whatever price (2026-04-22 APARINDS exit: -₹177 vs. target).
// 0.3% gives enough slack for a falling market to fill inside.
//
// 2026-10-22: 0.3% was NOT enough. The open gapped through trigger AND limit
// on several stops, the SL-L sat triggered-but-unfilled, and every fallback
// (SL-M / Market) was rejected by the broker for algo-tagged orders. The gap
// is now MANTHAN_SL_LIMIT_GAP_PCT (default 2%, see broker_exit.go) — the
// TRIGGER is untouched (exact strategy stop); only how far below it the
// limit leg may fill. The placement-time clamp floors it at the band.
func SLLimitGap(trigger, tickSize float64) float64 {
	pctGap := trigger * slLimitGapPct / 100
	tickGap := tickSize * 5
	if pctGap > tickGap {
		return pctGap
	}
	return tickGap
}

// PlaceSLMSell historically placed an SL-M (stop-loss market). The broker
// rejects SL-M for SEBI algo-tagged orders (first seen 2026-10-22, "Market
// order not allowed for Algo order"), so it now places an SL-LIMIT at the SAME
// trigger with the wide limit leg (SLLimitGap, default 2%, floored at the
// band by PlaceSLSell's clamp): fires at the trigger, fills anywhere down to
// the limit. GTC like before; same return shape so every caller is unchanged.
func (b *BrokerAdapter) PlaceSLMSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, triggerPrice float64) (string, error) {
	limit := triggerPrice - SLLimitGap(triggerPrice, info.TickSize)
	if info.DPRLower > 0 && limit < info.DPRLower {
		limit = info.DPRLower
	}
	orderID, roundedTrigger, roundedLimit, err := b.PlaceSLSell(ctx, auth, info, qty, triggerPrice, limit)
	if err != nil {
		return "", err
	}
	b.logger.Info("SL-M requested → SL-L placed (SL-M is rejected for algo orders)",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("trigger", roundedTrigger),
		zap.Float64("limit", roundedLimit),
		zap.String("broker_order_id", orderID))
	return orderID, nil
}

// PlaceSLSell places an SL-L (stop-loss limit) SELL order. Returns the broker
// order id plus the ACTUAL trigger/limit it sent to the exchange (post DPR +
// tick clamp) so the caller can record the broker-real value separately from
// the intended (un-clamped) stop. The caller passes the intended trigger; the
// DPR clamp lives here (placement-time only) — never store the clamped value as
// the trail baseline.
func (b *BrokerAdapter) PlaceSLSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, triggerPrice, limitPrice float64) (brokerOrderID string, brokerTrigger float64, brokerLimit float64, err error) {
	// HARD RULE: a trigger below today's band is refused, never clamped up to
	// the floor (callers defer via resolveBandFloor; this is the backstop).
	if info.DPRLower > 0 && triggerPrice < info.DPRLower {
		return "", 0, 0, fmt.Errorf("%w: stop trigger %.2f below dpr_lower %.2f for %s",
			ErrTriggerOutsideBand, triggerPrice, info.DPRLower, info.Symbol)
	}
	roundedTrigger := b.roundAndClamp(triggerPrice, info.TickSize, info.DPRLower, info.DPRUpper)
	roundedLimit := b.roundAndClamp(limitPrice, info.TickSize, info.DPRLower, info.DPRUpper)

	// GTC so the SL order persists across trading days for positional strategies.
	// Indira rejects SL with ordValidity=DAY at EOD (standard NSE behaviour);
	// GTC lives until the order triggers, fills, or is explicitly cancelled.
	// Verified 2026-04-21: ordType="SL" + ordValidity="GTC" accepted on NSE equity.
	req := &indiraClient.PlaceOrderRequest{
		Symbol:       info.IndiraSymbol,
		ExcToken:     info.ExchangeToken,
		Exc:          "NSE",
		OrdAction:    "SELL",
		OrdValidity:  "GTC",
		OrdType:      "SL",
		PrdType:      "DELIVERY",
		LimitPrice:   indiraClient.Price2DP(roundedLimit),
		TriggerPrice: indiraClient.Price2DP(roundedTrigger),
		Qty:          qty,
		DisQty:       0,
		LotSize:      1,
		Instrument:   "STK",
		Amo:          false,
	}

	resp, err := b.client.PlaceOrder(ctx, b.toIndiraAuth(auth), req)
	if err != nil {
		return "", 0, 0, err
	}

	orderID := resp.OrderId
	if orderID == "" {
		orderID = resp.OrdId
	}
	if orderID == "" {
		return "", 0, 0, fmt.Errorf("broker returned empty SL order ID")
	}

	if rej := parseRejection("place_sl_sell", resp); rej != nil {
		return "", 0, 0, rej
	}

	b.logger.Info("SL-L SELL placed",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("trigger", roundedTrigger),
		zap.Float64("limit", roundedLimit),
		zap.String("broker_order_id", orderID))

	return orderID, roundedTrigger, roundedLimit, nil
}

// ModifySLOrder modifies an existing SL order's trigger and limit (atomic).
// Fetches real tradedQty from orderbook (API requires it for validation).
// Returns the ACTUAL trigger/limit it sent to the exchange (post DPR + tick
// clamp) so the caller records the broker-real value alongside the intended.
func (b *BrokerAdapter) ModifySLOrder(ctx context.Context, auth BrokerAuth, info *SymbolInfo, brokerOrderID string, qty int, newTrigger, newLimit float64) (brokerTrigger float64, brokerLimit float64, err error) {
	roundedTrigger := b.roundAndClamp(newTrigger, info.TickSize, info.DPRLower, info.DPRUpper)
	roundedLimit := b.roundAndClamp(newLimit, info.TickSize, info.DPRLower, info.DPRUpper)

	// Fetch real tradedQty from broker (API requires it — must be >= actual traded)
	tradedQty := 0
	if b.client != nil {
		_, tq, _, gerr := b.GetOrderStatus(ctx, auth, brokerOrderID)
		if gerr == nil {
			tradedQty = tq
		}
	}

	// Preserve GTC on modify — original SL was placed as GTC (see PlaceSLSell).
	// Indira accepts modify of trigger/limit on a GTC SL without resubmission;
	// verified 2026-04-21. This is the TSL hot path.
	req := &indiraClient.ModifyOrderRequest{
		OrdId:         brokerOrderID,
		Symbol:        info.IndiraSymbol,
		OrdAction:     "SELL",
		OrdValidity:   "GTC",
		ExchangeToken: info.ExchangeToken,
		Exc:           "NSE",
		Qty:           qty,
		TradedQty:     tradedQty,
		LimitPrice:    roundedLimit,
		TriggerPrice:  roundedTrigger,
		OrdType:       "SL",
		PrdType:       "DELIVERY",
		Instrument:    "STK",
		LotSize:       1,
		DisQty:        0,
	}

	if merr := b.client.ModifyOrder(ctx, b.toIndiraAuth(auth), req); merr != nil {
		return 0, 0, merr
	}

	b.logger.Info("SL order modified",
		zap.String("symbol", info.Symbol),
		zap.String("broker_order_id", brokerOrderID),
		zap.Float64("new_trigger", roundedTrigger),
		zap.Float64("new_limit", roundedLimit),
		zap.Int("traded_qty", tradedQty))

	return roundedTrigger, roundedLimit, nil
}

// CancelOrder cancels an order by broker order ID.
func (b *BrokerAdapter) CancelOrder(ctx context.Context, auth BrokerAuth, info *SymbolInfo, brokerOrderID string) error {
	req := &indiraClient.CancelOrderRequest{
		Symbol: info.IndiraSymbol,
		Exc:    "NSE",
		OrdId:  brokerOrderID,
	}
	return b.client.CancelOrder(ctx, b.toIndiraAuth(auth), req)
}

// PlaceMarketSell is the "sell NOW" primitive behind every emergency and
// protective exit. Since 2026-10-22 it never sends OrdType=Market: the broker
// rejects Market (and SL-M) orders that carry the SEBI algoId ("Market order
// not allowed for Algo order"), which silently killed every fallback that
// morning. It places a marketable LIMIT instead — pricing rule in
// PlaceMarketableLimitSell (broker_exit.go). Signature kept so all call
// sites stay source-compatible; callers that want the limit use the
// underlying method directly.
func (b *BrokerAdapter) PlaceMarketSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int) (string, error) {
	orderID, _, err := b.PlaceMarketableLimitSell(ctx, auth, info, qty)
	return orderID, err
}

// PlaceAMOSLSell submits an After-Market Order with SL-Limit type for overnight
// protection. Used by the protective replayer's Phase A (16:35 IST EOD cron).
//
// Trigger handling:
//   - The caller passes the algorithmic trigger (e.g. TSL trigger at -8%).
//   - We clamp it to MAX(trigger, DPR_lower * AMODprBuffer) to survive
//     overnight DPR re-bucketing (see AMODprBuffer comment).
//   - Then standard roundAndClamp guarantees tick-aligned + within band.
//
// Returns the broker's AMO queue ID (different from the live order ID that
// gets generated at 08:50 IST conversion — Phase C swaps it).
func (b *BrokerAdapter) PlaceAMOSLSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, triggerPrice, limitPrice float64) (string, error) {
	// HARD RULE (2026-10-22 review): the adapter never MOVES a trigger. A stop
	// below the overnight safety floor is refused with a typed error — the EOD
	// planner's resolveBandFloor gate defers such stops before reaching here,
	// so this only catches a caller that bypassed the gate (or a sub-tick
	// rounding edge). The old AMODprBuffer bias silently lifted the trigger.
	if info.DPRLower > 0 && triggerPrice < info.DPRLower*DPRSafetyBuffer {
		return "", fmt.Errorf("%w: AMO stop trigger %.2f below the overnight band floor %.2f (dpr_lower %.2f × %.3f) for %s",
			ErrTriggerOutsideBand, triggerPrice, info.DPRLower*DPRSafetyBuffer, info.DPRLower, DPRSafetyBuffer, info.Symbol)
	}
	safeTrigger := triggerPrice
	safeLimit := limitPrice

	// Step 2: tick-round + clamp inside [DPR_lower, DPR_upper].
	roundedTrigger := b.roundAndClamp(safeTrigger, info.TickSize, info.DPRLower, info.DPRUpper)
	roundedLimit := b.roundAndClamp(safeLimit, info.TickSize, info.DPRLower, info.DPRUpper)

	req := &indiraClient.PlaceOrderRequest{
		Symbol:       info.IndiraSymbol,
		ExcToken:     info.ExchangeToken,
		Exc:          "NSE",
		OrdAction:    "SELL",
		OrdValidity:  "DAY", // Indira ignores GTC/GTD for AMO; DAY is the only honoured value
		OrdType:      "SL",
		PrdType:      "DELIVERY",
		LimitPrice:   indiraClient.Price2DP(roundedLimit),
		TriggerPrice: indiraClient.Price2DP(roundedTrigger),
		Qty:          qty,
		DisQty:       0,
		LotSize:      1,
		Instrument:   "STK",
		Amo:          true,
	}

	resp, err := b.client.PlaceOrder(ctx, b.toIndiraAuth(auth), req)
	if err != nil {
		return "", err
	}

	orderID := resp.OrderId
	if orderID == "" {
		orderID = resp.OrdId
	}
	if orderID == "" {
		return "", fmt.Errorf("broker returned empty AMO+SL order ID")
	}

	if rej := parseRejection("place_amo_sl_sell", resp); rej != nil {
		return "", rej
	}

	b.logger.Info("AMO+SL SELL queued for next session",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("trigger", roundedTrigger),
		zap.Float64("limit", roundedLimit),
		zap.String("broker_order_id", orderID))

	return orderID, nil
}

// PlaceAMOSell queues the lower-circuit exit for the next session. Market
// AMOs are rejected for algo-tagged orders (2026-10-22), so this is an AMO
// LIMIT at today's lower band — the lowest price the exchange accepts today,
// and tomorrow's band is centred on today's close so it stays inside it. If
// the stock opens lower still, the morning protective cycle / safety monitor
// re-prices the exit from the live LTP.
func (b *BrokerAdapter) PlaceAMOSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int) (string, error) {
	id, _, err := b.PlaceAMOSellPriced(ctx, auth, info, qty)
	return id, err
}

// PlaceAMOSellPriced is PlaceAMOSell returning the limit actually queued. The
// price comes from ExitLimitPrice — live token (ISIN key), stale rows ignored:
// at lower circuit LTP == lower band so the limit is the band floor; for an
// unfilled DAY exit re-queued at 16:35 it is LTP − slippage floored at the band.
func (b *BrokerAdapter) PlaceAMOSellPriced(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int) (string, float64, error) {
	price, ltp, src, err := b.ExitLimitPrice(ctx, auth, info)
	if err != nil {
		return "", 0, fmt.Errorf("amo exit for %s: %w", info.Symbol, err)
	}
	orderID, err := b.PlaceAMOLimitSell(ctx, auth, info, qty, price)
	if err != nil {
		return "", price, err
	}
	b.logger.Warn("AMO LIMIT SELL queued for the next session",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("ltp", ltp),
		zap.String("ltp_source", src),
		zap.Float64("limit", price),
		zap.String("broker_order_id", orderID))
	return orderID, price, nil
}

// GetOrderStatus fetches current order status from broker orderbook.
// Handles nested response format where orders are under data.orders
// and symbol is a complex object (not a string).
//
// avgPrice contract: when status is Executed/Traded AND tradedQty>0, fetch
// the qty-weighted average from the trade book (authoritative exchange fill
// price). The order book ONLY carries the LIMIT price (`price`), not the
// executed avg — it's literally absent from the response schema. Returning
// the limit price as "avg" is wrong because:
//   - Partial-fill orders may execute at multiple prices below the limit
//   - The downstream SL math uses avgPrice x slFactor(pct) to compute trigger; a 0 or
//     a wrong avg here cascades into bogus SLs and trail-modifications
//     (verified live 2026-05-12: 8 entries had entry_price=0 in
//     manthan_positions because we returned o.LimitPrice — a legacy field
//     that the Indira API never populates — yielding avg=0).
func (b *BrokerAdapter) GetOrderStatus(ctx context.Context, auth BrokerAuth, brokerOrderID string) (status string, filledQty int, avgPrice float64, err error) {
	// First try the standard client
	orders, clientErr := b.client.GetOrderBook(ctx, b.toIndiraAuth(auth))
	if clientErr == nil {
		for _, o := range orders {
			if o.OrdId == brokerOrderID {
				avgPx := o.Price // LIMIT price as fallback if trade book lookup fails
				if o.TradedQty > 0 && isFilledOrPartialBrokerStatus(o.Status) {
					if tradeAvg, terr := b.fetchAvgFillPrice(ctx, auth, brokerOrderID); terr == nil && tradeAvg > 0 {
						avgPx = tradeAvg
					} else if terr != nil {
						b.logger.Warn("trade book avg-price lookup failed — using limit price as fallback",
							zap.String("broker_order_id", brokerOrderID),
							zap.Float64("limit_price", o.Price),
							zap.Error(terr))
					}
				}
				return o.Status, o.TradedQty, avgPx, nil
			}
		}
		return "", 0, 0, fmt.Errorf("%w: order %s not found in orderbook", ErrOrderNotInBook, brokerOrderID)
	}

	// Auth failures are terminal for this poll — the raw fallback below issues
	// the SAME order-book request and would hit AU004 again, doubling the
	// log spam during a re-login window. Surface the typed error directly.
	if errors.Is(clientErr, indiraClient.ErrAuthExpired) {
		return "", 0, 0, clientErr
	}

	// Fallback: parse raw response manually (handles nested symbol object)
	b.logger.Debug("Standard orderbook parse failed, trying raw parse", zap.Error(clientErr))
	return b.getOrderStatusRaw(ctx, auth, brokerOrderID)
}

// TradeBookAvg returns the quantity-weighted average price of the trades the
// broker's trade book shows for one order; (0, nil) when it shows none yet.
func (b *BrokerAdapter) TradeBookAvg(ctx context.Context, auth BrokerAuth, brokerOrderID string) (float64, error) {
	return b.fetchAvgFillPrice(ctx, auth, brokerOrderID)
}

// GetOrderRejectReason returns the order book's rejReason for one order (the
// RMS "Limit exceeded Set= …" text, an exchange validation message, …).
// Empty with a nil error when the order is in the book without a reason;
// ErrOrderNotInBook when the book was read and the order is absent.
func (b *BrokerAdapter) GetOrderRejectReason(ctx context.Context, auth BrokerAuth, brokerOrderID string) (string, error) {
	orders, err := b.client.GetOrderBook(ctx, b.toIndiraAuth(auth))
	if err != nil {
		return "", err
	}
	for _, o := range orders {
		if o.OrdId == brokerOrderID {
			return strings.TrimSpace(o.RejReason), nil
		}
	}
	return "", fmt.Errorf("%w: order %s not found in orderbook", ErrOrderNotInBook, brokerOrderID)
}

// getOrderStatusRaw does a raw HTTP call and parses the orderbook JSON manually.
func (b *BrokerAdapter) getOrderStatusRaw(ctx context.Context, auth BrokerAuth, brokerOrderID string) (string, int, float64, error) {
	resp, err := b.client.GetOrderBookRaw(ctx, b.toIndiraAuth(auth))
	if err != nil {
		return "", 0, 0, err
	}

	// resp.Data is already the "data" field from the response.
	// It contains: {"orders": [...]} (not wrapped in another "data" layer)
	var raw struct {
		Orders []struct {
			OrdId     string  `json:"ordId"`
			Status    string  `json:"status"`
			TradedQty int     `json:"tradedQty"`
			Price     float64 `json:"price"`
		} `json:"orders"`
	}
	if err := json.Unmarshal(resp, &raw); err != nil {
		return "", 0, 0, fmt.Errorf("raw orderbook parse failed: %w", err)
	}

	for _, o := range raw.Orders {
		if o.OrdId == brokerOrderID {
			avgPx := o.Price
			if o.TradedQty > 0 && isFilledOrPartialBrokerStatus(o.Status) {
				if tradeAvg, terr := b.fetchAvgFillPrice(ctx, auth, brokerOrderID); terr == nil && tradeAvg > 0 {
					avgPx = tradeAvg
				}
			}
			return o.Status, o.TradedQty, avgPx, nil
		}
	}
	return "", 0, 0, fmt.Errorf("%w: order %s not found in raw orderbook (%d orders)", ErrOrderNotInBook, brokerOrderID, len(raw.Orders))
}

// fetchAvgFillPrice returns the quantity-weighted average traded price for a
// single broker order, derived from the trade book (authoritative source).
//
// Indira's GET /trade-book may return either one row per fill leg or one row
// aggregated per order; both cases collapse cleanly: sum(qty*price) / sum(qty).
// Returns (0, nil) only if the trade book legitimately has no rows yet — the
// caller should fall back to the limit price in that case rather than
// blocking the entry pipeline.
func (b *BrokerAdapter) fetchAvgFillPrice(ctx context.Context, auth BrokerAuth, brokerOrderID string) (float64, error) {
	trades, err := b.client.GetTradeBook(ctx, b.toIndiraAuth(auth), brokerOrderID)
	if err != nil {
		return 0, fmt.Errorf("trade book fetch: %w", err)
	}
	var totalQty int
	var totalNotional float64
	for _, t := range trades {
		if t.OrdId != brokerOrderID || t.TradedQty <= 0 || t.TradedPrice <= 0 {
			continue
		}
		totalQty += t.TradedQty
		totalNotional += float64(t.TradedQty) * t.TradedPrice
	}
	if totalQty == 0 {
		return 0, nil
	}
	return totalNotional / float64(totalQty), nil
}

// isFilledOrPartialBrokerStatus matches the broker-side status strings that
// imply at least one executed share. Kept local to broker_adapter so we don't
// drag the WSS helper's casing assumptions into REST-orderbook parsing.
func isFilledOrPartialBrokerStatus(s string) bool {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "EXECUTED", "TRADED", "COMPLETE", "FILLED",
		"PARTIALLY TRADED", "PARTIALLY EXECUTED":
		return true
	}
	return false
}

// GetAvailableMargin returns the user's free margin at the broker. Used by
// the pre-check pipeline to reject LIVE entries that would be rejected by the
// broker anyway with "Margin Limit exceeded" (saves a broker round-trip +
// prevents polluting the order book with margin-rejected orders).
func (b *BrokerAdapter) GetAvailableMargin(ctx context.Context, auth BrokerAuth) (float64, error) {
	fl, err := b.client.GetFundLimit(ctx, b.toIndiraAuth(auth))
	if err != nil {
		return 0, err
	}
	return fl.AvailableMargin, nil
}

// GetOrderBook returns the full orderbook for reconciliation.
func (b *BrokerAdapter) GetOrderBook(ctx context.Context, auth BrokerAuth) ([]indiraClient.OrderBook, error) {
	return b.client.GetOrderBook(ctx, b.toIndiraAuth(auth))
}

// CheckCircuit returns true if stock is at upper or lower circuit.
func (b *BrokerAdapter) CheckCircuit(ctx context.Context, token string) (atUpper, atLower bool, err error) {
	raw, err := b.extRedis.Get(ctx, "market:nse:"+token).Result()
	if err != nil {
		return false, false, err
	}
	snap, err := parseMarketSnap([]byte(raw))
	if err != nil {
		return false, false, err
	}
	// A row that has not ticked today cannot say anything about today's
	// circuit state (2026-10-22: BODALCHEM's dead row would have reported
	// "no circuit" at a price the stock left weeks earlier).
	if feedStale(snap.AsOf, time.Now()) {
		return false, false, fmt.Errorf("circuit eval: feed row for token %s is stale (as of %s)", token, snap.AsOf.Format(time.RFC3339))
	}
	return evalCircuit(snap.LTP, snap.DPRUpper, snap.DPRLower)
}

// evalCircuit decides band state from a market snapshot. Pure — unit-tested.
// Zero-guards matter: a feed row with a missing dpr_upper (0) and any LTP>0
// previously reported a SPURIOUS upper circuit (LTP >= 0 is always true),
// which under the UPPER_CIRCUIT retry class would hold an entry all session
// on garbage data. Missing LTP (0) likewise must never report lower circuit.
func evalCircuit(ltp, dprUpper, dprLower float64) (atUpper, atLower bool, err error) {
	if ltp <= 0 {
		return false, false, fmt.Errorf("circuit eval: no usable LTP in feed (ltp=%v)", ltp)
	}
	atUpper = dprUpper > 0 && ltp >= dprUpper
	atLower = dprLower > 0 && ltp <= dprLower
	return atUpper, atLower, nil
}

// --- Helpers ---

func (b *BrokerAdapter) resolveToken(ctx context.Context, isin string) (string, error) {
	raw, err := b.extRedis.Get(ctx, "isin:"+isin).Result()
	if err != nil {
		return "", fmt.Errorf("isin lookup failed: %w", err)
	}
	var data struct {
		NSECode string `json:"nsecode"`
	}
	if err := json.Unmarshal([]byte(raw), &data); err != nil || data.NSECode == "" {
		return "", fmt.Errorf("invalid isin data for %s", isin)
	}
	return data.NSECode, nil
}

func (b *BrokerAdapter) roundAndClamp(price, tickSize, dprLower, dprUpper float64) float64 {
	if tickSize <= 0 {
		tickSize = 0.05
	}
	rounded := roundToTick(price, tickSize)
	if dprLower > 0 && rounded < dprLower {
		rounded = dprLower
	}
	if dprUpper > 0 && rounded > dprUpper {
		rounded = dprUpper
	}
	return rounded
}

func roundToTick(price, tickSize float64) float64 {
	if tickSize <= 0 {
		return price
	}
	paise := int64(math.Round(price * 100))
	tickPaise := int64(math.Round(tickSize * 100))
	if tickPaise <= 0 {
		return price
	}
	rounded := ((paise + tickPaise/2) / tickPaise) * tickPaise
	return float64(rounded) / 100.0
}

func (b *BrokerAdapter) toIndiraAuth(auth BrokerAuth) *indiraClient.AuthContext {
	return &indiraClient.AuthContext{
		UserId:      auth.UserID,
		BearerToken: auth.BearerToken,
		AppId:       auth.AppID,
		Source:      auth.Source,
	}
}

// modifyLimitBuyOrder modifies an existing LIMIT BUY order's price.
func (b *BrokerAdapter) modifyLimitBuyOrder(ctx context.Context, auth BrokerAuth, req *modifyBuyRequest) error {
	modReq := &indiraClient.ModifyOrderRequest{
		OrdId:         req.ordId,
		Symbol:        req.symbol,
		OrdAction:     "BUY",
		OrdValidity:   "DAY",
		ExchangeToken: req.exchangeToken,
		Exc:           "NSE",
		Qty:           req.qty,
		TradedQty:     0,
		LimitPrice:    req.limitPrice,
		TriggerPrice:  0,
		OrdType:       "Limit",
		PrdType:       "DELIVERY",
		Instrument:    "STK",
		LotSize:       1,
		DisQty:        0,
	}
	return b.client.ModifyOrder(ctx, b.toIndiraAuth(auth), modReq)
}

// GetEffectiveHoldings returns a per-symbol map of effective DELIVERY holdings
// as the broker sees them: CNC position netQty + holdings qty (T+1 settled
// positions migrate from position-book to holdings, so we sum both).
//
// IMPORTANT: We filter position-book to delivery rows only. Intraday (MIS)
// rows are excluded — they belong to the user's separate intraday book and
// must not affect our CNC position accounting.
//
// Concrete example: user holds 10 CNC RELIANCE (ours) and short-sells 5 MIS
// RELIANCE intraday via mobile. Broker returns 2 rows — CNC: +10, MIS: -5.
// Naive sum gives +5 → detector wrongly publishes MANUAL_PARTIAL_EXIT.
// PrdType filter prevents this — we only count the CNC +10. At 3:30 PM the
// broker auto-squares the MIS short anyway; our position is untouched.
//
// Used by ExternalActivityDetector to compare what the broker says we own
// (delivery) vs what our manthan_orders table believes we hold. Mismatches
// indicate manual user interference (sold via mobile app, cancelled SL, etc.).
//
// Returns map keyed by display symbol (e.g. "KINGFA"), uppercased + trimmed
// to match the convention in manthan_orders.symbol.
func (b *BrokerAdapter) GetEffectiveHoldings(ctx context.Context, auth BrokerAuth) (map[string]int, error) {
	out := make(map[string]int)

	// 1) Position-book — today's un-settled delivery + intraday rows.
	positions, err := b.client.GetPositions(ctx, b.toIndiraAuth(auth))
	if err != nil {
		// If positions fetch fails we still try holdings — better partial truth
		// than no truth. Log but continue.
		b.logger.Warn("GetEffectiveHoldings: positions fetch failed",
			zap.String("user", auth.UserID), zap.Error(err))
	} else {
		for _, p := range positions {
			// Skip intraday (MIS) rows — they're a separate book from our CNC
			// positions. See function-level docstring for the short-sell case.
			if !isDeliveryProductType(p.PrdType) {
				continue
			}
			// Skip equity-derivative rows defensively — Manthan only trades stocks.
			if p.Symbol.Instrument != "" &&
				strings.ToUpper(p.Symbol.Instrument) != "STK" &&
				strings.ToUpper(p.Symbol.Instrument) != "EQ" {
				continue
			}
			sym := strings.ToUpper(strings.TrimSpace(p.Symbol.DispSym))
			if sym == "" {
				continue
			}
			if p.NetQty != 0 {
				out[sym] += p.NetQty
			}
		}
	}

	// 2) Holdings — settled CNC positions from previous trading day(s).
	holdings, err := b.client.GetHoldings(ctx, b.toIndiraAuth(auth))
	if err != nil {
		b.logger.Warn("GetEffectiveHoldings: holdings fetch failed",
			zap.String("user", auth.UserID), zap.Error(err))
	} else {
		for _, h := range holdings {
			// h.Symbol is now an array of HoldingSymbol entries (Indira returns
			// one row per exchange listing — typically NSE + BSE). Use the NSE
			// dispSym ("KINGFA") as the canonical key.
			sym := ""
			for _, s := range h.Symbol {
				if s.Exc == "NSE" && s.DispSym != "" {
					sym = s.DispSym
					break
				}
			}
			if sym == "" && len(h.Symbol) > 0 {
				sym = h.Symbol[0].DispSym
			}
			if sym == "" {
				continue
			}
			if h.Qty > 0 {
				out[sym] += h.Qty
			}
		}
	}

	return out, nil
}

// isDeliveryProductType reports whether a broker prdType string represents a
// delivery (CNC) position. Intraday (MIS / INTRADAY) rows are excluded so a
// user's intraday short-sell on a stock we hold long doesn't get counted
// against our delivery quantity.
//
// Indira returns prdType in a few flavours across endpoints — accept all the
// common spellings to avoid silent miscounting if the broker tweaks the
// string.
func isDeliveryProductType(prdType string) bool {
	switch strings.ToUpper(strings.TrimSpace(prdType)) {
	case "DELIVERY", "CNC", "DEL", "":
		// Empty prdType: defensive default — Indira sometimes omits the field
		// on holdings-derived rows. Treat as delivery (the holdings API only
		// ever returns settled delivery anyway).
		return true
	}
	return false
}

// extractDisplaySymbol pulls the readable symbol out of Indira's full
// "STK_KINGFA_EQ_NSE_18944" form. For inputs that already look like a plain
// symbol it returns the input unchanged.
func extractDisplaySymbol(full string) string {
	full = strings.ToUpper(strings.TrimSpace(full))
	if !strings.Contains(full, "_") {
		return full
	}
	parts := strings.Split(full, "_")
	// Standard Indira format: STK_<SYMBOL>_<SERIES>_<EXCHANGE>_<TOKEN>
	if len(parts) >= 2 {
		return parts[1]
	}
	return full
}

// RetryWithBackoff retries a function with exponential backoff.
func RetryWithBackoff(ctx context.Context, maxRetries int, fn func() error) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := fn(); err != nil {
			lastErr = err
			wait := time.Duration(1<<uint(i)) * time.Second
			if wait > 10*time.Second {
				wait = 10 * time.Second
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		return nil
	}
	return lastErr
}
