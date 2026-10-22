package manthan

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// ─── Exit pricing policy (2026-10-22) ───────────────────────────────────────
//
// On 2026-10-22 twelve positions crossed their stops at the open and none of
// the software exits reached the exchange:
//   - the exit payload carried no ISIN → token lookup "redis: nil" (fixed in
//     rules-engine, plus the entry-row fallback in SLHandler.resolveExitSymbol);
//   - every fallback was a Market or SL-M order, and the broker rejects both
//     for SEBI algo-tagged orders ("Market order not allowed for Algo order").
//
// Exits are therefore LIMIT orders priced to fill immediately:
//
//	exit limit = max(DPR lower band, LTP × (1 − MANTHAN_EXIT_LIMIT_SLIPPAGE_PCT/100))
//
// 2% below the last trade is far inside normal depth for Manthan's liquidity
// filter yet bounds the worst fill — a plain Market order had no bound at all.
// When the LTP feed is missing, the band floor is the most aggressive sell
// price the exchange accepts today, so that becomes the limit.
//
// The stop-loss LIMIT leg uses the same idea: the TRIGGER stays the exact
// strategy stop (20% trail / 10% LARGE — never moved, HARD RULE), while the
// limit sits MANTHAN_SL_LIMIT_GAP_PCT (default 2%) below it, floored at the
// band at placement time. The old 0.3% leg was blown through on the
// 2026-10-22 gap-down and the SL-L sat triggered-but-unfilled.

const (
	defaultExitLimitSlippagePct = 2.0
	defaultSLLimitGapPct        = 2.0
)

var (
	// exitLimitSlippagePct — percent below LTP for a marketable exit LIMIT.
	exitLimitSlippagePct = envPct("MANTHAN_EXIT_LIMIT_SLIPPAGE_PCT", defaultExitLimitSlippagePct, 0.1, 10)
	// slLimitGapPct — percent below the stop trigger for the SL-L limit leg.
	slLimitGapPct = envPct("MANTHAN_SL_LIMIT_GAP_PCT", defaultSLLimitGapPct, 0.3, 10)
)

// envPct reads a percentage from the environment with a default and a sane
// range; anything unparsable or out of range falls back to the default so a
// typo in .env can never yield a 0% (unfillable) or 90% (give-away) exit.
func envPct(name string, def, lo, hi float64) float64 {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v < lo || v > hi {
		return def
	}
	return v
}

// exitLimitFor prices a sell that must fill now. Pure — unit-tested.
//   - ltp > 0  → ltp × (1 − slip%), never below the lower band, tick-rounded,
//     clamped into [dprLower, dprUpper].
//   - ltp == 0 → the lower band itself (deepest price the exchange takes today).
//   - neither  → error: there is nothing to price against.
func exitLimitFor(ltp, tickSize, dprLower, dprUpper, slipPct float64) (float64, error) {
	if tickSize <= 0 {
		tickSize = 0.05
	}
	var price float64
	switch {
	case ltp > 0:
		price = ltp * (1 - slipPct/100)
		if dprLower > 0 && price < dprLower {
			price = dprLower
		}
	case dprLower > 0:
		price = dprLower
	default:
		return 0, fmt.Errorf("exit limit: no LTP and no lower band — cannot price the sell")
	}
	rounded := roundToTick(price, tickSize)
	if dprLower > 0 && rounded < dprLower {
		rounded = dprLower
	}
	if dprUpper > 0 && rounded > dprUpper {
		rounded = dprUpper
	}
	if rounded <= 0 {
		return 0, fmt.Errorf("exit limit: computed %.2f from ltp=%.2f band=[%.2f,%.2f]", rounded, ltp, dprLower, dprUpper)
	}
	return rounded, nil
}

// EnrichMarketInfo fills tick size / DPR band / freeze qty from the live
// market feed (market:nse:<token>) for a SymbolInfo built from a DB row.
// No-op without a token or when the feed has no row; never overwrites a
// known value with zero.
func (b *BrokerAdapter) EnrichMarketInfo(ctx context.Context, info *SymbolInfo) {
	if info == nil || info.ExchangeToken == "" || b.extRedis == nil {
		return
	}
	raw, err := b.extRedis.Get(ctx, "market:nse:"+info.ExchangeToken).Result()
	if err != nil {
		return
	}
	var mkt struct {
		TickSize  float64 `json:"tick_size"`
		DPRLower  float64 `json:"dpr_lower"`
		DPRUpper  float64 `json:"dpr_upper"`
		FreezeQty int     `json:"freeze_qty"`
	}
	if json.Unmarshal([]byte(raw), &mkt) != nil {
		return
	}
	if mkt.TickSize > 0 {
		info.TickSize = mkt.TickSize
	}
	if mkt.DPRLower > 0 {
		info.DPRLower = mkt.DPRLower
	}
	if mkt.DPRUpper > 0 {
		info.DPRUpper = mkt.DPRUpper
	}
	if mkt.FreezeQty > 0 {
		info.FreezeQty = mkt.FreezeQty
	}
}

// ExitLimitPrice returns the marketable exit limit for info plus the LTP it
// was derived from (0 when the feed had none).
func (b *BrokerAdapter) ExitLimitPrice(ctx context.Context, info *SymbolInfo) (limit, ltp float64, err error) {
	if info.DPRLower <= 0 || info.TickSize <= 0 {
		b.EnrichMarketInfo(ctx, info)
	}
	if info.ExchangeToken != "" {
		ltp, _ = b.FetchLTP(ctx, info.ExchangeToken)
	}
	limit, err = exitLimitFor(ltp, info.TickSize, info.DPRLower, info.DPRUpper, exitLimitSlippagePct)
	return limit, ltp, err
}

// PlaceLimitSell places a plain DAY LIMIT SELL (DELIVERY) — the only order
// shape the broker accepts for an immediate algo exit.
func (b *BrokerAdapter) PlaceLimitSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, price float64) (string, error) {
	return b.placeLimitSell(ctx, auth, info, qty, price, false)
}

// PlaceAMOLimitSell queues a LIMIT SELL on the after-market queue (used when
// the stock is at lower circuit: nothing trades until the next session).
func (b *BrokerAdapter) PlaceAMOLimitSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, price float64) (string, error) {
	return b.placeLimitSell(ctx, auth, info, qty, price, true)
}

func (b *BrokerAdapter) placeLimitSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int, price float64, amo bool) (string, error) {
	if qty <= 0 {
		return "", fmt.Errorf("limit sell %s: qty must be > 0 (got %d)", info.Symbol, qty)
	}
	rounded := b.roundAndClamp(price, info.TickSize, info.DPRLower, info.DPRUpper)
	if rounded <= 0 {
		return "", fmt.Errorf("limit sell %s: invalid price %.2f", info.Symbol, price)
	}
	op := "place_limit_sell"
	if amo {
		op = "place_amo_limit_sell"
	}

	req := &indiraClient.PlaceOrderRequest{
		Symbol:       info.IndiraSymbol,
		ExcToken:     info.ExchangeToken,
		Exc:          "NSE",
		OrdAction:    "SELL",
		OrdValidity:  "DAY",
		OrdType:      "Limit",
		PrdType:      "DELIVERY",
		LimitPrice:   indiraClient.Price2DP(rounded),
		TriggerPrice: 0, // explicit 0 for non-SL orders
		Qty:          qty,
		DisQty:       0,
		LotSize:      1,
		Instrument:   "STK",
		Amo:          amo,
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
		return "", fmt.Errorf("broker returned empty order ID (%s)", op)
	}
	if rej := parseRejection(op, resp); rej != nil {
		return "", rej
	}

	b.logger.Warn("EXIT LIMIT SELL placed",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("limit", rounded),
		zap.Bool("amo", amo),
		zap.String("broker_order_id", orderID))
	return orderID, nil
}

// PlaceMarketableLimitSell is the "sell now" primitive: prices the LIMIT from
// the live LTP (or the band floor) and places it. Returns the broker id and
// the limit actually sent so callers can persist it on the exit row.
func (b *BrokerAdapter) PlaceMarketableLimitSell(ctx context.Context, auth BrokerAuth, info *SymbolInfo, qty int) (string, float64, error) {
	limit, ltp, err := b.ExitLimitPrice(ctx, info)
	if err != nil {
		return "", 0, fmt.Errorf("%s: %w", info.Symbol, err)
	}
	b.logger.Info("Marketable LIMIT exit priced",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("ltp", ltp),
		zap.Float64("limit", limit),
		zap.Float64("slippage_pct", exitLimitSlippagePct),
		zap.Float64("dpr_lower", info.DPRLower))
	orderID, err := b.PlaceLimitSell(ctx, auth, info, qty, limit)
	if err != nil {
		return "", limit, err
	}
	return orderID, limit, nil
}
