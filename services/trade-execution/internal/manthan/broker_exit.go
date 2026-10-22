package manthan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

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
	// entryLimitSlippagePct — percent ABOVE LTP for a marketable entry LIMIT
	// (the former Market BUY fallback / top-up; Market is rejected for algo
	// orders).
	entryLimitSlippagePct = envPct("MANTHAN_ENTRY_LIMIT_SLIPPAGE_PCT", 1.0, 0.1, 10)
)

// ErrUnpriced marks an order that could not be priced at all (no fresh LTP
// from Redis under any token, no band, no broker-side price). Nothing was sent
// to the broker, so callers may release loop guards and retry.
var ErrUnpriced = errors.New("order could not be priced")

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
		FreezeQty int     `json:"freeze_qty"`
	}
	if json.Unmarshal([]byte(raw), &mkt) != nil {
		return
	}
	if mkt.TickSize > 0 {
		info.TickSize = mkt.TickSize
	}
	if mkt.FreezeQty > 0 {
		info.FreezeQty = mkt.FreezeQty
	}
	// The band comes only from a row updated today: a stale row's band would
	// make PlaceInitialSL defer a placeable stop or send one the exchange
	// rejects (2026-10-22 review; consistent with enrichDPR).
	snap, err := parseMarketSnap([]byte(raw))
	if err != nil || feedStale(snap.AsOf, time.Now()) {
		return
	}
	if snap.DPRLower > 0 {
		info.DPRLower = snap.DPRLower
	}
	if snap.DPRUpper > 0 {
		info.DPRUpper = snap.DPRUpper
	}
}

// ─── Feed freshness (2026-10-22, BODALCHEM) ─────────────────────────────────
//
// The market feed row for a token can go stale for weeks (BODALCHEM's row
// last ticked 2026-09-29; the exit priced 2% under a price the stock had
// left 23 days earlier and rested above the market all morning, and the
// chase never fired because the "LTP" still looked higher than our limit).
// An exit must never be priced from a stale row: when the row was not
// updated during today's session (or is older than MANTHAN_FEED_STALE_AFTER
// while the market is open) its LTP AND band are discarded and the price
// comes from the broker's own books instead (holdings, then position book).

// feedStaleAfter — a row older than this while the market is open is stale
// even if it was touched earlier today (mid-session feed death).
var feedStaleAfter = envDuration("MANTHAN_FEED_STALE_AFTER", 6*time.Hour, time.Minute, 24*time.Hour)

func envDuration(name string, def, lo, hi time.Duration) time.Duration {
	raw := os.Getenv(name)
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < lo || d > hi {
		return def
	}
	return d
}

// marketSnap is one market:nse:<token> row with its freshness stamp.
type marketSnap struct {
	LTP, DPRLower, DPRUpper, TickSize float64
	AsOf                              time.Time // zero when the producer stamped nothing
}

func parseMarketSnap(raw []byte) (marketSnap, error) {
	var mkt struct {
		LTP         float64 `json:"ltp"`
		DPRLower    float64 `json:"dpr_lower"`
		DPRUpper    float64 `json:"dpr_upper"`
		TickSize    float64 `json:"tick_size"`
		TimestampMs int64   `json:"timestamp"`
		LastUpdated string  `json:"last_updated"`
	}
	if err := json.Unmarshal(raw, &mkt); err != nil {
		return marketSnap{}, err
	}
	snap := marketSnap{LTP: mkt.LTP, DPRLower: mkt.DPRLower, DPRUpper: mkt.DPRUpper, TickSize: mkt.TickSize}
	switch {
	case mkt.TimestampMs > 1e12: // epoch milliseconds
		snap.AsOf = time.UnixMilli(mkt.TimestampMs)
	case mkt.TimestampMs > 1e9: // epoch seconds
		snap.AsOf = time.Unix(mkt.TimestampMs, 0)
	case mkt.LastUpdated != "":
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999Z07:00", "2006-01-02 15:04:05"} {
			if t, err := time.Parse(layout, strings.TrimSpace(mkt.LastUpdated)); err == nil {
				snap.AsOf = t
				break
			}
		}
	}
	return snap, nil
}

// feedStale decides whether a row stamped asOf can price an exit at `now`.
// Judged only on a trading day between 09:20 and 15:30 IST (before the open
// the last tick is legitimately yesterday's). Unstamped rows are trusted.
func feedStale(asOf, now time.Time) bool {
	if asOf.IsZero() {
		return false
	}
	ist := istLocation()
	n := now.In(ist)
	if !indiraClient.IsTradingDay(n) {
		return false
	}
	mod := n.Hour()*60 + n.Minute()
	// Judged from the open: the pre-open auction print (~09:08) is already
	// after today's 08:00 cut, so a row with no update today is stale from
	// 09:15:00 — the old 09:20 grace let a weeks-old row drive the first five
	// minutes of the session (2026-10-22 review).
	if mod < 9*60+15 || mod > 15*60+30 {
		return false
	}
	sessionStart := time.Date(n.Year(), n.Month(), n.Day(), 8, 0, 0, 0, ist)
	if asOf.Before(sessionStart) {
		return true
	}
	return n.Sub(asOf) > feedStaleAfter
}

func (b *BrokerAdapter) fetchMarketSnap(ctx context.Context, token string) (marketSnap, error) {
	if token == "" || b.extRedis == nil {
		return marketSnap{}, fmt.Errorf("no token / feed")
	}
	raw, err := b.extRedis.Get(ctx, "market:nse:"+token).Result()
	if err != nil {
		return marketSnap{}, err
	}
	return parseMarketSnap([]byte(raw))
}

// refreshToken re-resolves the exchange token from the ISIN key — the LIVE
// mapping — and, when it differs from the token the caller carried, swaps
// info over to it. A token stored on a DB row can be weeks stale: BODALCHEM
// moved from 25017 to 25021 and every reader (exit pricing, the 09:14
// planner, the safety monitor) kept staring at the dead row under 25017 —
// the stop looked "below the band" for weeks while the stock was 25% lower.
// Returns true when the token changed.
func (b *BrokerAdapter) refreshToken(ctx context.Context, info *SymbolInfo) bool {
	if info == nil || info.ISIN == "" {
		return false
	}
	tok, err := b.resolveToken(ctx, info.ISIN)
	if err != nil || tok == "" || tok == info.ExchangeToken {
		return false
	}
	b.logger.Warn("exchange token changed — switching to the live mapping from the ISIN key",
		zap.String("symbol", info.Symbol), zap.String("isin", info.ISIN),
		zap.String("stored_token", info.ExchangeToken), zap.String("live_token", tok))
	info.ExchangeToken = tok
	info.IndiraSymbol = fmt.Sprintf("STK_%s_EQ_NSE_%s", info.Symbol, tok)
	info.DPRLower, info.DPRUpper = 0, 0
	b.EnrichMarketInfo(ctx, info)
	return true
}

// FreshLTP is the Redis LTP for info. When the row under the carried token is
// missing or stale it re-resolves the token from the ISIN key and reads the
// live row. stale=true means no live row exists under any token.
func (b *BrokerAdapter) FreshLTP(ctx context.Context, info *SymbolInfo) (ltp float64, stale bool, asOf time.Time, err error) {
	// ISIN key FIRST, every time (one Redis GET): the carried token is only a
	// hint. Reading the stored-token row first would trust a drifted token
	// whenever its dead row still looks "fresh" by timestamp (2026-10-22 review).
	b.refreshToken(ctx, info)
	snap, err := b.fetchMarketSnap(ctx, info.ExchangeToken)
	if err != nil || feedStale(snap.AsOf, time.Now()) {
		if b.refreshToken(ctx, info) {
			snap, err = b.fetchMarketSnap(ctx, info.ExchangeToken)
		}
	}
	if err != nil {
		return 0, false, time.Time{}, err
	}
	if feedStale(snap.AsOf, time.Now()) {
		return 0, true, snap.AsOf, nil
	}
	return snap.LTP, false, snap.AsOf, nil
}

// brokerLTP asks the broker's own books for the last traded price of a
// holding — fresh by construction. Holdings first (DELIVERY lots), then the
// position book (today's trades). Note: a holding whose quantity is locked by
// a resting SELL is hidden from v2/holdings, so callers cancel first.
func (b *BrokerAdapter) brokerLTP(ctx context.Context, auth BrokerAuth, symbol string) (float64, string, error) {
	want := strings.ToUpper(strings.TrimSpace(symbol))
	if holdings, err := b.client.GetHoldings(ctx, b.toIndiraAuth(auth)); err == nil {
		for _, h := range holdings {
			for _, s := range h.Symbol {
				if strings.EqualFold(s.DispSym, want) || strings.EqualFold(s.BaseSym, want) {
					if h.LTP > 0 {
						return h.LTP, "broker-holdings", nil
					}
				}
			}
		}
	} else if errors.Is(err, indiraClient.ErrAuthExpired) || IsAuthError(err) {
		return 0, "", err
	}
	if positions, err := b.client.GetPositions(ctx, b.toIndiraAuth(auth)); err == nil {
		for _, p := range positions {
			if strings.EqualFold(p.Symbol.DispSym, want) || strings.EqualFold(p.Symbol.BaseSym, want) {
				if p.LTP > 0 {
					return p.LTP, "broker-positions", nil
				}
			}
		}
	}
	return 0, "", fmt.Errorf("broker books have no price for %s", symbol)
}

// ExitLimitPrice returns the marketable exit limit for info plus the LTP it
// was derived from and where that LTP came from ("feed", "broker-holdings",
// "broker-positions", "band-floor"). A stale feed row contributes nothing —
// neither its LTP nor its band.
func (b *BrokerAdapter) ExitLimitPrice(ctx context.Context, auth BrokerAuth, info *SymbolInfo) (limit, ltp float64, src string, err error) {
	// ISIN key first, every time — the carried token is only a hint.
	if b.refreshToken(ctx, info) {
		info.DPRLower, info.DPRUpper = 0, 0
		b.EnrichMarketInfo(ctx, info)
	}
	dprLo, dprHi := info.DPRLower, info.DPRUpper
	snap, serr := b.fetchMarketSnap(ctx, info.ExchangeToken)
	if serr != nil || feedStale(snap.AsOf, time.Now()) {
		// Missing or stale row under the carried token: the ISIN key is the
		// live mapping — re-resolve and read the live row (Redis stays the
		// price source; the broker's books are only the last resort).
		if b.refreshToken(ctx, info) {
			snap, serr = b.fetchMarketSnap(ctx, info.ExchangeToken)
			dprLo, dprHi = info.DPRLower, info.DPRUpper
		}
	}
	switch {
	case serr != nil:
		// no row — keep whatever band the caller already had
	case feedStale(snap.AsOf, time.Now()):
		b.logger.Warn("exit pricing: market feed row is STALE — ignoring its LTP and band, asking the broker for a price",
			zap.String("symbol", info.Symbol), zap.String("token", info.ExchangeToken),
			zap.Time("feed_as_of", snap.AsOf), zap.Float64("stale_ltp", snap.LTP))
		dprLo, dprHi = 0, 0
		// The caller's info may carry the same stale band (EnrichMarketInfo on
		// the dead row); placeLimitSell re-clamps with info.DPR*, so clear it
		// here too or the broker-priced limit is dragged back into a band the
		// stock has left (2026-10-22 review).
		info.DPRLower, info.DPRUpper = 0, 0
		if snap.TickSize > 0 {
			info.TickSize = snap.TickSize
		}
	default:
		ltp, src = snap.LTP, "feed"
		if snap.TickSize > 0 {
			info.TickSize = snap.TickSize
		}
		if snap.DPRLower > 0 {
			dprLo, info.DPRLower = snap.DPRLower, snap.DPRLower
		}
		if snap.DPRUpper > 0 {
			dprHi, info.DPRUpper = snap.DPRUpper, snap.DPRUpper
		}
	}
	if ltp <= 0 {
		if bl, bsrc, berr := b.brokerLTP(ctx, auth, info.Symbol); berr == nil && bl > 0 {
			ltp, src = bl, bsrc
		} else if berr != nil {
			b.logger.Warn("exit pricing: broker price lookup failed", zap.String("symbol", info.Symbol), zap.Error(berr))
		}
	}
	if ltp <= 0 && dprLo > 0 {
		src = "band-floor"
	}
	limit, err = exitLimitFor(ltp, info.TickSize, dprLo, dprHi, exitLimitSlippagePct)
	if err != nil {
		err = fmt.Errorf("%w: %v", ErrUnpriced, err)
	}
	return limit, ltp, src, err
}

// marketOpenNowIST reports whether NSE is in its regular session right now
// (trading day, 09:15–15:30 IST). Shared gate for every path that places or
// cancels a non-AMO order.
func marketOpenNowIST() bool {
	n := time.Now().In(istLocation())
	if !indiraClient.IsTradingDay(n) {
		return false
	}
	mod := n.Hour()*60 + n.Minute()
	return mod >= 9*60+15 && mod <= 15*60+30
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
	limit, ltp, src, err := b.ExitLimitPrice(ctx, auth, info)
	if err != nil {
		return "", 0, fmt.Errorf("%s: %w", info.Symbol, err)
	}
	b.logger.Info("Marketable LIMIT exit priced",
		zap.String("symbol", info.Symbol),
		zap.Int("qty", qty),
		zap.Float64("ltp", ltp),
		zap.String("ltp_source", src),
		zap.Float64("limit", limit),
		zap.Float64("slippage_pct", exitLimitSlippagePct),
		zap.Float64("dpr_lower", info.DPRLower))
	orderID, err := b.PlaceLimitSell(ctx, auth, info, qty, limit)
	if err != nil {
		return "", limit, err
	}
	return orderID, limit, nil
}
