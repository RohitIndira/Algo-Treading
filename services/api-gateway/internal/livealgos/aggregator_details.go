package livealgos

import (
	"database/sql"
	"math"
	"sort"
	"time"
)

// This file holds the "pure" aggregation logic for the Details page
// endpoints: (main details / holdings / trades / stock P&L). Every
// function is deterministic — same inputs, same output — no IO. This
// keeps the handlers thin and makes it possible to unit-test the math
// without spinning up a DB or Redis.
//
// Convention: rupee amounts are int64 (rounded to whole rupees before
// the frontend sees them). Percentages are float64. Live LTP figures
// arrive as float64 (from Redis JSON) and stay floats through the
// per-share math to avoid rounding drift, only being rounded to int64
// when they enter the response struct.

// ─── Main details endpoint ───────────────────────────────────────────

// BuildDetails assembles the DetailsResponse for one strategy.
//
//	meta         — strategy header + capital
//	positions    — every position (ACTIVE + EXITED) for the strategy
//	ltps         — live LTPs keyed by exchange_token; missing tokens are fine
//	algoID/name  — from the static Catalog, resolved by StrategyType
//
// The response includes chart data as a placeholder (empty) — historical
// NAV via daily_ohlcv is a follow-up; the endpoint returns everything
// else real, and the chart gets filled in a subsequent PR.
func BuildDetails(
	meta *StrategyMetaRow,
	positions []PositionRow,
	ltps map[string]LTPQuote,
	algoID, algoName string,
) DetailsResponse {
	deployed := int64(meta.TotalCapital)
	status := statusFromMeta(meta)

	// Split by status once.
	var active, exited []PositionRow
	for _, p := range positions {
		if p.Status == "ACTIVE" {
			active = append(active, p)
		} else {
			exited = append(exited, p)
		}
	}

	// Realised P&L across all exited positions (only counts positions
	// with an actual exit_price; ORPHAN_CLEANUP rows carry NULL).
	var realised int64
	for _, p := range exited {
		if p.RealizedPnL.Valid {
			realised += int64(p.RealizedPnL.Float64)
		}
	}

	// Unrealised + today's P&L across active positions.
	unrealised, todayAmt := unrealisedAndToday(active, ltps)
	netAmount := realised + unrealised

	// Percentages against the deployed capital (the frontend footer
	// already shows "based on capital of X").
	var netPct, todayPct float64
	if deployed > 0 {
		netPct = (float64(netAmount) / float64(deployed)) * 100
		todayPct = (float64(todayAmt) / float64(deployed)) * 100
	}

	// Latest data point — most recent entry_time OR exit_time. Used
	// to render the "As of" note on the chart.
	latest := latestActivityDate(positions)

	// Top holdings: 5 largest by invested_amt, computed on ACTIVE only.
	topHoldings := buildHoldingsList(active, ltps)
	if len(topHoldings) > 5 {
		topHoldings = topHoldings[:5]
	}

	// Top past trades: 5 most-recently-exited, EXITED status with an
	// exit_price (skip ORPHAN_CLEANUP).
	trades := buildTradesList(exited)
	sort.SliceStable(trades, func(i, j int) bool {
		// Reverse chronological — most-recent first.
		return trades[i].ExitDate > trades[j].ExitDate
	})
	if len(trades) > 5 {
		trades = trades[:5]
	}

	return DetailsResponse{
		StrategyID:      meta.StrategyID,
		AlgoID:          algoID,
		Name:            algoName,
		Status:          status,
		DeployedCapital: deployed,
		AsOf:            latest,
		NetPnL:          PnL{Amount: netAmount, Percent: round2(netPct)},
		TodayPnL:        PnL{Amount: todayAmt, Percent: round2(todayPct)},
		Chart:           DetailsChart{Points: []DetailsChartPoint{}}, // filled by nav.go later
		Metrics:         computeMetrics(exited, deployed),
		TopHoldings:     topHoldings,
		AllocationSector:  allocationBy(active, func(p PositionRow) string { return nullStr(p.Industry) }),
		AllocationMCap:    allocationBy(active, func(p PositionRow) string { return nullStr(p.MCapBucket) }),
		TopPastTrades:     trades,
	}
}

// ─── Holdings list endpoint ──────────────────────────────────────────

// BuildHoldings returns EVERY active position, no truncation.
func BuildHoldings(active []PositionRow, ltps map[string]LTPQuote) HoldingsResponse {
	return HoldingsResponse{Holdings: buildHoldingsList(active, ltps)}
}

// ─── Trades list endpoint ────────────────────────────────────────────

// BuildTrades returns EVERY exited position, most-recent first.
func BuildTrades(exited []PositionRow) TradesResponse {
	list := buildTradesList(exited)
	sort.SliceStable(list, func(i, j int) bool {
		return list[i].ExitDate > list[j].ExitDate
	})
	return TradesResponse{Trades: list}
}

// ─── Stock P&L drilldown endpoint ────────────────────────────────────

// BuildStockPnL assembles Screen 7 for one (strategy, symbol) pair.
// positions may include multiple entries (e.g. algo averaged into the
// same symbol twice); we aggregate across them.
func BuildStockPnL(symbol string, positions []PositionRow, orders []OrderRow) StockPnLResponse {
	var (
		industry             string
		totalBuyQty          int
		totalSellQty         int
		totalBuyValue        float64
		totalSellValue       float64
		totalRealised        int64
	)
	for _, p := range positions {
		if industry == "" && p.Industry.Valid {
			industry = p.Industry.String
		}
		// Every ACTIVE and EXITED position started with a BUY at
		// entry_price for `quantity` shares.
		totalBuyQty += p.Quantity
		totalBuyValue += p.EntryPrice * float64(p.Quantity)

		if p.ExitPrice.Valid && p.Status == "EXITED" {
			totalSellQty += p.Quantity
			totalSellValue += p.ExitPrice.Float64 * float64(p.Quantity)
		}
		if p.RealizedPnL.Valid {
			totalRealised += int64(p.RealizedPnL.Float64)
		}
	}

	avgBuy := safeDiv(totalBuyValue, float64(totalBuyQty))
	avgSell := safeDiv(totalSellValue, float64(totalSellQty))

	// Trades table: BUY + SELL fills from manthan_orders. Most recent first.
	// Track which order sides already have a real filled row so we don't
	// double-count when we synthesize SELL rows from position exits below.
	trades := make([]StockPnLTrade, 0, len(orders))
	var sellFilledQty int
	for _, o := range orders {
		if !o.FilledAt.Valid || !o.AvgFillPrice.Valid {
			continue
		}
		qty := o.Qty
		if o.FilledQty.Valid && o.FilledQty.Int32 > 0 {
			qty = int(o.FilledQty.Int32)
		}
		if o.OrderSide == "SELL" {
			sellFilledQty += qty
		}
		trades = append(trades, StockPnLTrade{
			Date:     o.FilledAt.Time.Format("2006-01-02"),
			Type:     o.OrderSide,
			Quantity: qty,
			Price:    round2(o.AvgFillPrice.Float64),
		})
	}

	// Synthesize SELL rows for EXITED positions whose exit didn't land in
	// manthan_orders as FILLED — e.g. safety-monitor market-sell path
	// closes the position but doesn't write back a matching filled order.
	// Skip cases already covered by real SELL fills to avoid double-count.
	for _, p := range positions {
		if p.Status != "EXITED" || !p.ExitPrice.Valid || !p.ExitTime.Valid {
			continue
		}
		if sellFilledQty >= p.Quantity {
			sellFilledQty -= p.Quantity
			continue
		}
		trades = append(trades, StockPnLTrade{
			Date:     p.ExitTime.Time.Format("2006-01-02"),
			Type:     "SELL",
			Quantity: p.Quantity - sellFilledQty,
			Price:    round2(p.ExitPrice.Float64),
		})
		sellFilledQty = 0
	}
	sort.SliceStable(trades, func(i, j int) bool {
		return trades[i].Date > trades[j].Date
	})

	return StockPnLResponse{
		Symbol:      symbol,
		Industry:    industry,
		ProductType: "DELIVERY", // Manthan is a positional/CNC algo
		RealisedPnL: totalRealised,
		Overview: StockPnLOverview{
			Quantity:       totalBuyQty,
			AvgBuyPrice:    round2(avgBuy),
			TotalBuyValue:  int64(totalBuyValue),
			AvgSellPrice:   round2(avgSell),
			TotalSellValue: int64(totalSellValue),
		},
		Trades: trades,
	}
}

// ─── Shared helpers ──────────────────────────────────────────────────

// statusFromMeta derives Status from strategies.stopped_at + active +
// trading_mode. Same semantics as aggregator.go's statusFrom (see there
// for the terminal-STOP rationale).
//
//	stopped_at != nil                  → STOPPED  (terminal; row survives
//	                                              purely for history)
//	active=true,  trading_mode=LIVE    → LIVE
//	active=true,  trading_mode=PAPER   → PAUSED
//	active=false                        → PAUSED  (Pause modal)
func statusFromMeta(m *StrategyMetaRow) string {
	if m.StoppedAt != nil {
		return StatusStopped
	}
	if !m.Active {
		return StatusPaused
	}
	if m.TradingMode == "PAPER" || m.TradingMode == "TRADING_MODE_PAPER" {
		return StatusPaused
	}
	return StatusLive
}

// unrealisedAndToday returns (totalUnrealisedRs, totalTodayMtmRs) across
// active positions. Uses LTP from the map; positions with no LTP mapping
// contribute 0 to both.
func unrealisedAndToday(active []PositionRow, ltps map[string]LTPQuote) (int64, int64) {
	var unreal, today float64
	for _, p := range active {
		q, ok := ltpForPosition(p, ltps)
		if !ok {
			continue
		}
		unreal += (q.LTP - p.EntryPrice) * float64(p.Quantity)
		if q.PrevClose > 0 {
			today += (q.LTP - q.PrevClose) * float64(p.Quantity)
		}
	}
	return int64(unreal), int64(today)
}

// ltpForPosition looks up an LTPQuote for a position via the position's
// LEFT-JOINed exchange_token. Returns (zero, false) when the position
// wasn't joined to a token OR the token has no Redis entry.
func ltpForPosition(p PositionRow, ltps map[string]LTPQuote) (LTPQuote, bool) {
	return LTPForPosition(p, ltps)
}

// LTPForPosition is the exported form of ltpForPosition — the handler
// (in package handlers) needs to run the same lookup when computing
// per-strategy metrics for the LIST endpoint.
func LTPForPosition(p PositionRow, ltps map[string]LTPQuote) (LTPQuote, bool) {
	if !p.ExchangeToken.Valid || p.ExchangeToken.String == "" {
		return LTPQuote{}, false
	}
	q, ok := ltps[p.ExchangeToken.String]
	return q, ok
}

// latestActivityDate returns the most-recent entry_time OR exit_time
// across positions, ISO-formatted. Used for the "As of" note.
func latestActivityDate(positions []PositionRow) string {
	var latest time.Time
	for _, p := range positions {
		if p.EntryTime.After(latest) {
			latest = p.EntryTime
		}
		if p.ExitTime.Valid && p.ExitTime.Time.After(latest) {
			latest = p.ExitTime.Time
		}
	}
	if latest.IsZero() {
		return ""
	}
	return latest.Format("2006-01-02")
}

// buildHoldingsList materialises the Holding structs for a slice of
// ACTIVE positions, sorted by invested_amt DESC.
func buildHoldingsList(active []PositionRow, ltps map[string]LTPQuote) []Holding {
	list := make([]Holding, 0, len(active))
	for _, p := range active {
		q, ok := ltpForPosition(p, ltps)
		var curValue, netPnL float64
		var netPct, ltpVal, ltpChange float64
		if ok {
			ltpVal = q.LTP
			ltpChange = q.PercentChange
			curValue = q.LTP * float64(p.Quantity)
			netPnL = curValue - p.InvestedAmt
			if p.InvestedAmt > 0 {
				netPct = (netPnL / p.InvestedAmt) * 100
			}
		}
		list = append(list, Holding{
			Symbol:       p.Symbol,
			Industry:     nullStr(p.Industry),
			MCapBucket:   nullStr(p.MCapBucket),
			Quantity:     p.Quantity,
			EntryPrice:   p.EntryPrice,
			Invested:     int64(p.InvestedAmt),
			CurrentValue: int64(curValue),
			NetPnL:       int64(netPnL),
			NetPnLPct:    round2(netPct),
			LTP:          round2(ltpVal),
			LTPChangePct: round2(ltpChange),
		})
	}
	sort.SliceStable(list, func(i, j int) bool {
		return list[i].Invested > list[j].Invested
	})
	return list
}

// buildTradesList materialises TradeRow structs for a slice of EXITED
// positions. Skips positions with no exit_price (e.g. ORPHAN_CLEANUP)
// since they're not user-facing "trades".
func buildTradesList(exited []PositionRow) []TradeRow {
	out := make([]TradeRow, 0, len(exited))
	for _, p := range exited {
		if !p.ExitPrice.Valid {
			continue
		}
		var realisedAmt int64
		if p.RealizedPnL.Valid {
			realisedAmt = int64(p.RealizedPnL.Float64)
		} else {
			realisedAmt = int64((p.ExitPrice.Float64 - p.EntryPrice) * float64(p.Quantity))
		}
		var pct float64
		if p.InvestedAmt > 0 {
			pct = (float64(realisedAmt) / p.InvestedAmt) * 100
		}
		var days int
		if p.ExitTime.Valid {
			days = int(p.ExitTime.Time.Sub(p.EntryTime).Hours() / 24)
		}
		exitDate := ""
		if p.ExitTime.Valid {
			exitDate = p.ExitTime.Time.Format("2006-01-02")
		}
		out = append(out, TradeRow{
			Symbol:      p.Symbol,
			Industry:    nullStr(p.Industry),
			ProductType: "DELIVERY",
			EntryPrice:  p.EntryPrice,
			ExitPrice:   p.ExitPrice.Float64,
			Quantity:    p.Quantity,
			RealisedPnL: realisedAmt,
			RealisedPct: round2(pct),
			ExitReason:  nullStr(p.ExitReason),
			HoldingDays: days,
			ExitDate:    exitDate,
		})
	}
	return out
}

// allocationBy groups active positions by a string key (extractor
// closure — usually industry or mcap_bucket), sums invested_amt per
// bucket, and returns the top-N buckets by %.
func allocationBy(active []PositionRow, extract func(PositionRow) string) []AllocationEntry {
	type bucket struct {
		Label    string
		Invested float64
	}
	byLabel := make(map[string]*bucket)
	var total float64
	for _, p := range active {
		lbl := extract(p)
		if lbl == "" {
			lbl = "Uncategorised"
		}
		b, ok := byLabel[lbl]
		if !ok {
			b = &bucket{Label: lbl}
			byLabel[lbl] = b
		}
		b.Invested += p.InvestedAmt
		total += p.InvestedAmt
	}
	out := make([]AllocationEntry, 0, len(byLabel))
	for _, b := range byLabel {
		var pct float64
		if total > 0 {
			pct = (b.Invested / total) * 100
		}
		out = append(out, AllocationEntry{Label: b.Label, Pct: round2(pct)})
	}
	// Largest bucket first
	sort.SliceStable(out, func(i, j int) bool { return out[i].Pct > out[j].Pct })
	return out
}

// computeMetrics fills the 6-tile grid. CAGR / Max Drawdown / Sharpe
// need a NAV series (Phase 2) — Phase 1 returns 0 for those and
// populates the ones we can compute today from closed-position data.
func computeMetrics(exited []PositionRow, deployed int64) Metrics {
	var wins, closed int
	var totalDays float64
	var realisedSum int64

	for _, p := range exited {
		if !p.ExitPrice.Valid {
			continue // ORPHAN_CLEANUP etc — not a real trade
		}
		closed++
		if p.RealizedPnL.Valid && p.RealizedPnL.Float64 > 0 {
			wins++
		}
		if p.ExitTime.Valid {
			totalDays += p.ExitTime.Time.Sub(p.EntryTime).Hours() / 24
		}
		if p.RealizedPnL.Valid {
			realisedSum += int64(p.RealizedPnL.Float64)
		}
	}

	var winRate, avgHolding, totalReturnPct float64
	if closed > 0 {
		winRate = float64(wins) / float64(closed) * 100
		avgHolding = totalDays / float64(closed)
	}
	if deployed > 0 {
		totalReturnPct = float64(realisedSum) / float64(deployed) * 100
	}

	return Metrics{
		TotalReturnPct: round2(totalReturnPct),
		CAGRPct:        0, // Phase 2 — NAV history required
		WinRatePct:     round2(winRate),
		MaxDrawdownPct: 0, // Phase 2
		AvgHoldingDays: round1(avgHolding),
		SharpeRatio:    0, // Phase 2
	}
}

// nullStr flattens sql.NullString → string ("" when NULL). Note that
// sql.NullString's `String` is a struct field (not a method), so we
// return that field, not call it. When Valid is false the field is "".
func nullStr(n sql.NullString) string {
	return n.String
}

// round2 rounds to 2 decimal places.
func round2(f float64) float64 {
	return math.Round(f*100) / 100
}

// round1 rounds to 1 decimal place.
func round1(f float64) float64 {
	return math.Round(f*10) / 10
}

// safeDiv returns a/b, or 0 if b is 0. Avoids NaN in the response.
func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}
