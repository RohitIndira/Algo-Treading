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
const AMODprBuffer = 1.005

func NewBrokerAdapter(client *indiraClient.Client, extRedis *redis.Client, logger *zap.Logger) *BrokerAdapter {
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
		ExchangeToken: token,
		IndiraSymbol:  fmt.Sprintf("STK_%s_EQ_NSE_%s", symbol, token),
		Exchange:      "NSE",
		TickSize:      0.05,
	}

	raw, err := b.extRedis.Get(ctx, "market:nse:"+token).Result()
	if err == nil {
		var mkt struct {
			TickSize  float64 `json:"tick_size"`
			DPRLower  float64 `json:"dpr_lower"`
			DPRUpper  float64 `json:"dpr_upper"`
			FreezeQty int     `json:"freeze_qty"`
		}
		if json.Unmarshal([]byte(raw), &mkt) == nil {
			if mkt.TickSize > 0 {
				info.TickSize = mkt.TickSize
			}
			info.DPRLower = mkt.DPRLower
			info.DPRUpper = mkt.DPRUpper
			info.FreezeQty = mkt.FreezeQty
		}
	}

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

	// Check immediate rejection from response (per API docs: data.ordStatus + data.rejReason)
	if resp.OrdStatus == "Rejected" || resp.Status == "Rejected" {
		reason := resp.RejReason
		if reason == "" {
			reason = resp.Message
		}
		return "", fmt.Errorf("broker rejected: %s", reason)
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
func (b *BrokerAdapter) PlaceMarketBuy(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int) (string, error) {
	req := &indiraClient.PlaceOrderRequest{
		Symbol:      info.IndiraSymbol,
		ExcToken:    info.ExchangeToken,
		Exc:         "NSE",
		OrdAction:   "BUY",
		OrdValidity: "DAY",
		OrdType:     "Market",
		PrdType:     "DELIVERY",
		Qty:         qty,
		DisQty:      0,
		LotSize:     1,
		Instrument:  "STK",
		Amo:         false,
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
		return "", fmt.Errorf("broker returned empty market-buy order ID")
	}

	if resp.OrdStatus == "Rejected" || resp.Status == "Rejected" {
		reason := resp.RejReason
		if reason == "" {
			reason = resp.Message
		}
		return "", fmt.Errorf("market buy rejected: %s", reason)
	}

	b.logger.Info("MARKET BUY placed (LIMIT fallback)",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.String("broker_order_id", orderID))

	return orderID, nil
}

// IsAuthError reports whether the error indicates the broker rejected the
// bearer token as invalid/expired. Used by the auto-refresh retry pattern:
// on such an error, callers should invalidate the credentials cache,
// re-fetch from DB, and retry the broker call once.
//
// Indira-specific signals:
//   HTTP 401 "Session expired"            — no Bearer / invalid shape
//   HTTP 200 "AU004 Session data not received" — JWT signature valid but
//                                           the server-side trading session
//                                           was invalidated (typically by a
//                                           newer browser login)
func IsAuthError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "AU004") ||
		strings.Contains(s, "Session expired") ||
		strings.Contains(s, "Session data not received") ||
		strings.Contains(s, "HTTP error 401") ||
		strings.Contains(s, "status code 401")
}

// SLLimitGap computes the gap between SL trigger and SL limit. Returns the
// wider of (tickSize × 5) and (trigger × 0.003 = 0.3%). The old hardcoded
// `tickSize × 5` was too tight on high-priced stocks — on APARINDS
// (tick ₹0.50) that's only ₹2.50 = 0.02%, which means any fast down-move
// (gap-down, circuit) blows past the limit without filling. The SL order
// sits triggered-but-unfilled, our safety monitor then cancels + emergency
// market-sells at whatever price (2026-04-22 APARINDS exit: -₹177 vs. target).
// 0.3% gives enough slack for a falling market to fill inside.
func SLLimitGap(trigger, tickSize float64) float64 {
	pctGap := trigger * 0.003 // 0.3%
	tickGap := tickSize * 5
	if pctGap > tickGap {
		return pctGap
	}
	return tickGap
}

// PlaceSLMSell places an SL-M (stop-loss market) SELL — no limit price, fills
// at whatever market price is when trigger hits. Used as a fallback when SL-L
// is stuck (trigger hit but limit price couldn't be matched). Guarantees exit
// at cost of slippage; safer than holding an unprotected position.
func (b *BrokerAdapter) PlaceSLMSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, triggerPrice float64) (string, error) {
	roundedTrigger := b.roundAndClamp(triggerPrice, info.TickSize, info.DPRLower, info.DPRUpper)

	req := &indiraClient.PlaceOrderRequest{
		Symbol:       info.IndiraSymbol,
		ExcToken:     info.ExchangeToken,
		Exc:          "NSE",
		OrdAction:    "SELL",
		OrdValidity:  "GTC",
		OrdType:      "SL-M",
		PrdType:      "DELIVERY",
		TriggerPrice: indiraClient.Price2DP(roundedTrigger),
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
		return "", fmt.Errorf("broker returned empty SL-M order ID")
	}

	if resp.OrdStatus == "Rejected" || resp.Status == "Rejected" {
		reason := resp.RejReason
		if reason == "" {
			reason = resp.Message
		}
		return "", fmt.Errorf("SL-M order rejected: %s", reason)
	}

	b.logger.Info("SL-M SELL placed (fallback)",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("trigger", roundedTrigger),
		zap.String("broker_order_id", orderID))

	return orderID, nil
}

// PlaceSLSell places an SL-L (stop-loss limit) SELL order.
func (b *BrokerAdapter) PlaceSLSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, triggerPrice, limitPrice float64) (string, error) {
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
		return "", err
	}

	orderID := resp.OrderId
	if orderID == "" {
		orderID = resp.OrdId
	}
	if orderID == "" {
		return "", fmt.Errorf("broker returned empty SL order ID")
	}

	if resp.OrdStatus == "Rejected" || resp.Status == "Rejected" {
		reason := resp.RejReason
		if reason == "" {
			reason = resp.Message
		}
		return "", fmt.Errorf("SL order rejected: %s", reason)
	}

	b.logger.Info("SL-L SELL placed",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("trigger", roundedTrigger),
		zap.Float64("limit", roundedLimit),
		zap.String("broker_order_id", orderID))

	return orderID, nil
}

// ModifySLOrder modifies an existing SL order's trigger and limit prices (atomic).
// Fetches real tradedQty from orderbook (API requires it for validation).
func (b *BrokerAdapter) ModifySLOrder(ctx context.Context, auth BrokerAuth, info *SymbolInfo, brokerOrderID string, qty int, newTrigger, newLimit float64) error {
	roundedTrigger := b.roundAndClamp(newTrigger, info.TickSize, info.DPRLower, info.DPRUpper)
	roundedLimit := b.roundAndClamp(newLimit, info.TickSize, info.DPRLower, info.DPRUpper)

	// Fetch real tradedQty from broker (API requires it — must be >= actual traded)
	tradedQty := 0
	if b.client != nil {
		_, tq, _, err := b.GetOrderStatus(ctx, auth, brokerOrderID)
		if err == nil {
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

	if err := b.client.ModifyOrder(ctx, b.toIndiraAuth(auth), req); err != nil {
		return err
	}

	b.logger.Info("SL order modified",
		zap.String("symbol", info.Symbol),
		zap.String("broker_order_id", brokerOrderID),
		zap.Float64("new_trigger", roundedTrigger),
		zap.Float64("new_limit", roundedLimit),
		zap.Int("traded_qty", tradedQty))

	return nil
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

// PlaceMarketSell places an emergency MARKET SELL (safety net).
func (b *BrokerAdapter) PlaceMarketSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int) (string, error) {
	req := &indiraClient.PlaceOrderRequest{
		Symbol:     info.IndiraSymbol,
		ExcToken:   info.ExchangeToken,
		Exc:        "NSE",
		OrdAction:  "SELL",
		OrdValidity: "DAY",
		OrdType:    "Market",
		PrdType:    "DELIVERY",
		Qty:        qty,
		DisQty:     0,
		LotSize:    1,
		Instrument: "STK",
		Amo:        false,
	}

	resp, err := b.client.PlaceOrder(ctx, b.toIndiraAuth(auth), req)
	if err != nil {
		return "", err
	}

	orderID := resp.OrderId
	if orderID == "" {
		orderID = resp.OrdId
	}

	b.logger.Warn("EMERGENCY MARKET SELL placed",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.String("broker_order_id", orderID))

	return orderID, nil
}

// PlaceAMOSLSell submits an After-Market Order with SL-Limit type for overnight
// protection. Used by the protective replayer's Phase A (15:35 IST EOD cron).
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
	// Step 1: bias trigger 50bps inside DPR_lower to survive overnight band shift.
	safeTrigger := triggerPrice
	safeLimit := limitPrice
	if info.DPRLower > 0 {
		floor := info.DPRLower * AMODprBuffer
		if safeTrigger < floor {
			b.logger.Info("AMO SL trigger biased above DPR floor for overnight safety",
				zap.String("symbol", info.Symbol),
				zap.Float64("requested_trigger", triggerPrice),
				zap.Float64("dpr_lower", info.DPRLower),
				zap.Float64("safe_floor", floor))
			safeTrigger = floor
			safeLimit = floor - SLLimitGap(floor, info.TickSize)
			if safeLimit < info.DPRLower {
				safeLimit = info.DPRLower
			}
		}
	}

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

	if resp.OrdStatus == "Rejected" || resp.Status == "Rejected" {
		reason := resp.RejReason
		if reason == "" {
			reason = resp.Message
		}
		return "", fmt.Errorf("AMO+SL submission rejected: %s", reason)
	}

	b.logger.Info("AMO+SL SELL queued for next session",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("trigger", roundedTrigger),
		zap.Float64("limit", roundedLimit),
		zap.String("broker_order_id", orderID))

	return orderID, nil
}

// PlaceAMOSell places an After Market Order SELL (for lower circuit exit).
func (b *BrokerAdapter) PlaceAMOSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int) (string, error) {
	req := &indiraClient.PlaceOrderRequest{
		Symbol:     info.IndiraSymbol,
		ExcToken:   info.ExchangeToken,
		Exc:        "NSE",
		OrdAction:  "SELL",
		OrdValidity: "DAY",
		OrdType:    "Market",
		PrdType:    "DELIVERY",
		Qty:        qty,
		DisQty:     0,
		LotSize:    1,
		Instrument: "STK",
		Amo:        true,
	}

	resp, err := b.client.PlaceOrder(ctx, b.toIndiraAuth(auth), req)
	if err != nil {
		return "", err
	}

	orderID := resp.OrderId
	if orderID == "" {
		orderID = resp.OrdId
	}

	b.logger.Warn("AMO SELL placed (lower circuit exit)",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.String("broker_order_id", orderID))

	return orderID, nil
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
//   - The downstream SL math uses avgPrice * 0.80 to compute trigger; a 0 or
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
		return "", 0, 0, fmt.Errorf("order %s not found in orderbook", brokerOrderID)
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
	return "", 0, 0, fmt.Errorf("order %s not found in raw orderbook (%d orders)", brokerOrderID, len(raw.Orders))
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
	var mkt struct {
		LTP      float64 `json:"ltp"`
		DPRUpper float64 `json:"dpr_upper"`
		DPRLower float64 `json:"dpr_lower"`
	}
	if err := json.Unmarshal([]byte(raw), &mkt); err != nil {
		return false, false, err
	}
	return mkt.LTP >= mkt.DPRUpper, mkt.LTP <= mkt.DPRLower, nil
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
