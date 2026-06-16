package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

// ManthanHandler serves aggregated data for the Manthan frontend page.
//
// Data lives across TWO Postgres databases:
//   - market_data.manthan_signals     — today's eligible universe (written by
//                                        data-ingestion/manthan-live)
//   - trading_db.manthan_positions    — open positions with trailing-SL trail
//                                        (written by rules-engine)
//   - trading_db.manthan_cooldown     — symbols locked after recent exits
//
// Plus Redis:
//   - Local Redis  manthan:ema:allocations — daily EMA-indexed allocation weights
//   - External Redis (optional) for live LTP:
//       isin:{ISIN}         → {"nsecode":"13337", ...}   (ISIN → NSE token)
//       market:nse:{token}  → {"ltp":916.15, ...}         (current LTP)
type ManthanHandler struct {
	signalsDB   *sql.DB       // market_data DB       — manthan_signals
	positionsDB *sql.DB       // trading_db DB        — manthan_positions, manthan_cooldown
	ordersDB    *sql.DB       // trading_execution DB — manthan_orders, manthan_order_events
	redisClient *redis.Client // local Redis (EMA weights pub/sub)
	extRedis    *redis.Client // external Redis (live LTP feed); may be nil

	// ISIN → NSE token cache. Mapping doesn't change during market hours, so
	// caching avoids an extra Redis round-trip per signal on every request.
	tokenMu    sync.RWMutex
	tokenCache map[string]string
}

// NewManthanHandler constructs a handler. Any dependency may be nil; the handler
// degrades gracefully (empty sections / no LTP enrichment) rather than 500-ing.
func NewManthanHandler(signalsDB, positionsDB, ordersDB *sql.DB, redisClient, extRedis *redis.Client) *ManthanHandler {
	return &ManthanHandler{
		signalsDB:   signalsDB,
		positionsDB: positionsDB,
		ordersDB:    ordersDB,
		redisClient: redisClient,
		extRedis:    extRedis,
		tokenCache:  make(map[string]string),
	}
}

// ── Response DTOs ────────────────────────────────────────────────────────────

type manthanOverviewResponse struct {
	Success     bool                   `json:"success"`
	UserID      string                 `json:"user_id"`
	StrategyID  string                 `json:"strategy_id,omitempty"`
	Positions   []manthanPosition      `json:"positions"`
	SectorAlloc []allocationBucket     `json:"sector_allocation"`
	MCapAlloc   []allocationBucket     `json:"mcap_allocation"`
	IndexAlloc  []allocationBucket     `json:"index_allocation"`
	EMAWeights  map[string]float64     `json:"ema_weights"`
	TodaySignals []todaySignal         `json:"today_signals"`
	Cooldowns   []cooldownEntry        `json:"cooldowns"`
	Orders      []manthanOrderRow      `json:"orders"`
	Totals      manthanTotals          `json:"totals"`
	GeneratedAt string                 `json:"generated_at"`
}

type manthanOrderRow struct {
	ID            int64   `json:"id"`
	SignalID      string  `json:"signal_id,omitempty"`
	StrategyID    string  `json:"strategy_id"`
	Symbol        string  `json:"symbol"`
	Exchange      string  `json:"exchange"`
	OrderType     string  `json:"order_type"`     // MARKET / LIMIT / SL
	OrderSide     string  `json:"order_side"`     // BUY / SELL
	Qty           int     `json:"qty"`
	FilledQty     int     `json:"filled_qty"`
	LimitPrice    float64 `json:"limit_price"`
	TriggerPrice  float64 `json:"trigger_price,omitempty"`
	AvgFillPrice  float64 `json:"avg_fill_price,omitempty"`
	BrokerOrderID string  `json:"broker_order_id,omitempty"`
	BrokerStatus  string  `json:"broker_status,omitempty"`
	Status        string  `json:"status"`                 // our internal status: PENDING, PLACED, FILLED, REJECTED, CANCELLED
	LastError     string  `json:"last_error,omitempty"`   // populated on rejection
	RetryCount    int     `json:"retry_count"`
	LatestEvent   string  `json:"latest_event,omitempty"` // detail from most recent manthan_order_events row
	CreatedAt     string  `json:"created_at"`
	UpdatedAt     string  `json:"updated_at"`
}

type manthanPosition struct {
	Symbol         string  `json:"symbol"`
	ISIN           string  `json:"isin"`
	Industry       string  `json:"industry"`
	MCapBucket     string  `json:"mcap_bucket"`
	IndexName      string  `json:"index_name"`
	EntryPrice     float64 `json:"entry_price"`
	Quantity       int     `json:"quantity"`
	InvestedAmt    float64 `json:"invested_amt"`
	EMAAllocPct    float64 `json:"ema_alloc_pct"`
	HighSinceEntry float64 `json:"high_since_entry"`
	CurrentSL      float64 `json:"current_sl"`
	LastTrailLevel float64 `json:"last_trail_level"`
	Status         string  `json:"status"`
	EntryTime      string  `json:"entry_time"`
	DaysHeld       int     `json:"days_held"`
	StrategyID     string  `json:"strategy_id"`
	LiveLTP        float64 `json:"live_ltp,omitempty"` // from external Redis, 0 when unavailable
}

type allocationBucket struct {
	Label      string  `json:"label"`
	Invested   float64 `json:"invested"`
	Percent    float64 `json:"percent"`
	CapPercent float64 `json:"cap_percent"`  // strategy-defined cap (e.g. 25 for sector)
	OverCap    bool    `json:"over_cap"`
}

type todaySignal struct {
	Symbol      string  `json:"symbol"`
	ISIN        string  `json:"isin"`
	Industry    string  `json:"industry"`
	MCapBucket  string  `json:"mcap_bucket"`
	IndexName   string  `json:"index_name"`
	LatestPrice float64 `json:"latest_price"` // sheet snapshot (stale as soon as the sheet ran)
	ATHClose    float64 `json:"ath_close"`
	MarketCap   float64 `json:"market_cap"`
	PE          float64 `json:"pe"`
	FScore      float64 `json:"fscore"`
	TakenState  string  `json:"taken_state"`        // "taken" | "cooldown" | "eligible"
	LiveLTP     float64 `json:"live_ltp,omitempty"` // from external Redis, 0 when unavailable
}

type cooldownEntry struct {
	Symbol       string `json:"symbol"`
	EligibleFrom string `json:"eligible_from"`
	Reason       string `json:"reason"`
}

type manthanTotals struct {
	Invested       float64 `json:"invested"`
	OpenPositions  int     `json:"open_positions"`
	EligibleToday  int     `json:"eligible_today"`
	InCooldown     int     `json:"in_cooldown"`
}

// ── Route handler ────────────────────────────────────────────────────────────

// GetOverview handles GET /api/v1/manthan/overview?user_id=X&strategy_id=Y
// strategy_id is optional; when absent, positions across all user's Manthan
// strategies are aggregated.
func (h *ManthanHandler) GetOverview(w http.ResponseWriter, r *http.Request) {
	userID := strings.TrimSpace(r.URL.Query().Get("user_id"))
	strategyID := strings.TrimSpace(r.URL.Query().Get("strategy_id"))

	if userID == "" {
		respondWithError(w, http.StatusBadRequest, "user_id query param is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	resp := manthanOverviewResponse{
		Success:     true,
		UserID:      userID,
		StrategyID:  strategyID,
		Positions:   []manthanPosition{},
		SectorAlloc: []allocationBucket{},
		MCapAlloc:   []allocationBucket{},
		IndexAlloc:  []allocationBucket{},
		EMAWeights:  map[string]float64{},
		TodaySignals: []todaySignal{},
		Cooldowns:   []cooldownEntry{},
		Orders:      []manthanOrderRow{},
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// 1. Active positions + cooldowns (trading_db)
	if h.positionsDB != nil {
		positions, err := h.loadActivePositions(ctx, userID, strategyID)
		if err == nil {
			resp.Positions = positions
			resp.Totals.OpenPositions = len(positions)
			for _, p := range positions {
				resp.Totals.Invested += p.InvestedAmt
			}
			resp.SectorAlloc = bucketize(positions, func(p manthanPosition) string { return firstNonEmpty(p.Industry, "Unknown") }, resp.Totals.Invested, 25.0)
			resp.MCapAlloc = bucketize(positions, func(p manthanPosition) string { return firstNonEmpty(p.MCapBucket, "UNKNOWN") }, resp.Totals.Invested, 50.0)
			resp.IndexAlloc = bucketize(positions, func(p manthanPosition) string { return firstNonEmpty(p.IndexName, "UNKNOWN") }, resp.Totals.Invested, 0)
		}
		cooldowns, _ := h.loadCooldowns(ctx, strategyID)
		resp.Cooldowns = cooldowns
		resp.Totals.InCooldown = len(cooldowns)
	}

	// 2. Today's eligible signals (market_data) + taken/cooldown state join
	if h.signalsDB != nil {
		signals, _ := h.loadTodaySignals(ctx)
		cooldownSet := make(map[string]struct{}, len(resp.Cooldowns))
		for _, c := range resp.Cooldowns {
			cooldownSet[c.Symbol] = struct{}{}
		}
		heldSet := make(map[string]struct{}, len(resp.Positions))
		for _, p := range resp.Positions {
			heldSet[p.Symbol] = struct{}{}
		}
		for i := range signals {
			if _, held := heldSet[signals[i].Symbol]; held {
				signals[i].TakenState = "taken"
			} else if _, cd := cooldownSet[signals[i].Symbol]; cd {
				signals[i].TakenState = "cooldown"
			} else {
				signals[i].TakenState = "eligible"
			}
		}
		resp.TodaySignals = signals
		resp.Totals.EligibleToday = len(signals)
	}

	// 3. EMA allocations from Redis (daily cache set by manthan-live)
	if h.redisClient != nil {
		raw, err := h.redisClient.Get(ctx, "manthan:ema:allocations").Result()
		if err == nil && raw != "" {
			weights := map[string]float64{}
			if err := json.Unmarshal([]byte(raw), &weights); err == nil {
				resp.EMAWeights = weights
			}
		}
	}

	// 4. Live LTP enrichment from external Redis (Indira's market data feed).
	// Mutates in place — positions/signals already in resp keep their LTP.
	resp.Positions, resp.TodaySignals = h.enrichLiveLTP(ctx, resp.Positions, resp.TodaySignals)

	// 5. Broker-side Manthan orders (trading_execution DB) — so the frontend
	// can show placed / filled / rejected orders in the Order Activity panel.
	if h.ordersDB != nil {
		if orders, err := h.loadManthanOrders(ctx, userID, strategyID); err == nil {
			resp.Orders = orders
		}
	}

	respondWithJSON(w, http.StatusOK, resp)
}

// loadManthanOrders reads broker-side Manthan orders for this user (optionally
// scoped to a strategy) and joins in the latest `manthan_order_events` detail
// so the frontend can show retry / rejection context inline.
func (h *ManthanHandler) loadManthanOrders(ctx context.Context, userID, strategyID string) ([]manthanOrderRow, error) {
	// NOTE: strategy_id here is the UUID from rules-engine; manthan_orders stores
	// it as a string. Filter is optional so a handler with no strategy scope
	// returns everything for the user.
	// NOTE: manthan_orders.strategy_id is a UUID column in Postgres and
	// database/sql passes strings as TEXT, which Postgres won't implicitly
	// cast on equality. So we cast the column to text and compare as strings.
	query := `
		SELECT mo.id,
		       COALESCE(mo.signal_id::text,''),
		       mo.strategy_id::text,
		       mo.symbol,
		       COALESCE(mo.exchange,''),
		       COALESCE(mo.order_type::text,''),
		       COALESCE(mo.order_side,''),
		       COALESCE(mo.qty, 0),
		       COALESCE(mo.filled_qty, 0),
		       COALESCE(mo.limit_price, 0),
		       COALESCE(mo.trigger_price, 0),
		       COALESCE(mo.avg_fill_price, 0),
		       COALESCE(mo.broker_order_id,''),
		       COALESCE(mo.broker_status,''),
		       mo.status,
		       COALESCE(mo.last_error,''),
		       COALESCE(mo.retry_count, 0),
		       COALESCE((
		           SELECT CONCAT_WS(' · ', NULLIF(e.event_type,''), NULLIF(e.detail,''))
		           FROM manthan_order_events e
		           WHERE e.order_id = mo.id
		           ORDER BY e.created_at DESC
		           LIMIT 1
		       ),'') AS latest_event,
		       mo.created_at,
		       mo.updated_at
		FROM manthan_orders mo
		WHERE mo.user_id = $1`
	args := []any{userID}
	if strategyID != "" {
		query += " AND mo.strategy_id::text = $2"
		args = append(args, strategyID)
	}
	query += " ORDER BY mo.created_at DESC LIMIT 100"

	rows, err := h.ordersDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []manthanOrderRow
	for rows.Next() {
		var r manthanOrderRow
		var createdAt, updatedAt time.Time
		if err := rows.Scan(
			&r.ID, &r.SignalID, &r.StrategyID, &r.Symbol, &r.Exchange,
			&r.OrderType, &r.OrderSide, &r.Qty, &r.FilledQty,
			&r.LimitPrice, &r.TriggerPrice, &r.AvgFillPrice,
			&r.BrokerOrderID, &r.BrokerStatus, &r.Status, &r.LastError,
			&r.RetryCount, &r.LatestEvent,
			&createdAt, &updatedAt,
		); err != nil {
			continue
		}
		r.CreatedAt = createdAt.Format(time.RFC3339)
		r.UpdatedAt = updatedAt.Format(time.RFC3339)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ── Internal loaders ─────────────────────────────────────────────────────────

func (h *ManthanHandler) loadActivePositions(ctx context.Context, userID, strategyID string) ([]manthanPosition, error) {
	query := `
		SELECT strategy_id, symbol, COALESCE(isin,''), COALESCE(industry,''),
		       COALESCE(mcap_bucket,''), COALESCE(index_name,''),
		       entry_price, quantity, invested_amt,
		       COALESCE(ema_alloc_pct, 0),
		       COALESCE(high_since_entry, entry_price),
		       COALESCE(current_sl, 0),
		       COALESCE(last_trail_level, entry_price),
		       status, entry_time
		FROM manthan_positions
		WHERE user_id = $1 AND status = 'ACTIVE'`
	args := []any{userID}
	if strategyID != "" {
		query += " AND strategy_id = $2"
		args = append(args, strategyID)
	}
	query += " ORDER BY entry_time DESC"

	rows, err := h.positionsDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	now := time.Now().UTC()
	var out []manthanPosition
	for rows.Next() {
		var p manthanPosition
		var entryTime time.Time
		if err := rows.Scan(
			&p.StrategyID, &p.Symbol, &p.ISIN, &p.Industry,
			&p.MCapBucket, &p.IndexName,
			&p.EntryPrice, &p.Quantity, &p.InvestedAmt,
			&p.EMAAllocPct, &p.HighSinceEntry, &p.CurrentSL, &p.LastTrailLevel,
			&p.Status, &entryTime,
		); err != nil {
			continue
		}
		p.EntryTime = entryTime.Format(time.RFC3339)
		p.DaysHeld = int(now.Sub(entryTime).Hours() / 24)
		if p.DaysHeld < 0 {
			p.DaysHeld = 0
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (h *ManthanHandler) loadTodaySignals(ctx context.Context) ([]todaySignal, error) {
	rows, err := h.signalsDB.QueryContext(ctx, `
		SELECT symbol, COALESCE(isin,''),
		       COALESCE(industry,''), COALESCE(mcap_bucket,''), COALESCE(index_name,''),
		       COALESCE(latest_price,0), COALESCE(ath_close,0), COALESCE(market_cap,0),
		       COALESCE(pe,0), COALESCE(fscore,0)
		FROM manthan_signals
		WHERE run_date = CURRENT_DATE
		ORDER BY symbol`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []todaySignal
	for rows.Next() {
		var s todaySignal
		if err := rows.Scan(&s.Symbol, &s.ISIN, &s.Industry, &s.MCapBucket, &s.IndexName,
			&s.LatestPrice, &s.ATHClose, &s.MarketCap, &s.PE, &s.FScore); err != nil {
			continue
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// enrichLiveLTP looks up current LTP for every position + signal from the
// external Redis feed and mutates the slices in place. Two-stage lookup:
//
//  1. ISIN → NSE token via  isin:{ISIN}   (cached after first hit — mapping is
//     static during market hours).
//  2. Token → LTP        via  market:nse:{token}
//
// No-op when extRedis is nil. Silent on individual lookup failures so a single
// missing symbol doesn't blank every tile.
func (h *ManthanHandler) enrichLiveLTP(ctx context.Context, positions []manthanPosition, signals []todaySignal) ([]manthanPosition, []todaySignal) {
	if h.extRedis == nil {
		return positions, signals
	}

	// Collect the unique ISINs we need to resolve
	isinSet := make(map[string]struct{}, len(positions)+len(signals))
	for _, p := range positions {
		if p.ISIN != "" {
			isinSet[p.ISIN] = struct{}{}
		}
	}
	for _, s := range signals {
		if s.ISIN != "" {
			isinSet[s.ISIN] = struct{}{}
		}
	}
	if len(isinSet) == 0 {
		return positions, signals
	}

	// Stage 1: ISIN → token (using cache first, fetching misses in one MGET)
	h.tokenMu.RLock()
	missing := make([]string, 0, len(isinSet))
	for isin := range isinSet {
		if _, ok := h.tokenCache[isin]; !ok {
			missing = append(missing, isin)
		}
	}
	h.tokenMu.RUnlock()

	if len(missing) > 0 {
		keys := make([]string, len(missing))
		for i, isin := range missing {
			keys[i] = "isin:" + isin
		}
		vals, err := h.extRedis.MGet(ctx, keys...).Result()
		if err == nil {
			h.tokenMu.Lock()
			for i, v := range vals {
				raw, ok := v.(string)
				if !ok || raw == "" {
					continue
				}
				var parsed struct {
					NSECode string `json:"nsecode"`
				}
				if json.Unmarshal([]byte(raw), &parsed) != nil || parsed.NSECode == "" {
					continue
				}
				h.tokenCache[missing[i]] = parsed.NSECode
			}
			h.tokenMu.Unlock()
		}
	}

	// Build ISIN → token map for this request
	h.tokenMu.RLock()
	tokens := make(map[string]string, len(isinSet))
	tokenKeys := make([]string, 0, len(isinSet))
	tokenToIsin := make(map[string]string, len(isinSet))
	for isin := range isinSet {
		if tok, ok := h.tokenCache[isin]; ok {
			tokens[isin] = tok
			tokenKeys = append(tokenKeys, "market:nse:"+tok)
			tokenToIsin[tok] = isin
		}
	}
	h.tokenMu.RUnlock()

	if len(tokenKeys) == 0 {
		return positions, signals
	}

	// Stage 2: token → LTP (one MGET for everyone)
	ltpByIsin := make(map[string]float64, len(tokenKeys))
	vals, err := h.extRedis.MGet(ctx, tokenKeys...).Result()
	if err == nil {
		for i, v := range vals {
			raw, ok := v.(string)
			if !ok || raw == "" {
				continue
			}
			var parsed struct {
				LTP float64 `json:"ltp"`
			}
			if json.Unmarshal([]byte(raw), &parsed) != nil || parsed.LTP <= 0 {
				continue
			}
			// Recover the ISIN from the token we just queried
			tok := strings.TrimPrefix(tokenKeys[i], "market:nse:")
			if isin, ok := tokenToIsin[tok]; ok {
				ltpByIsin[isin] = parsed.LTP
			}
		}
	}

	// Apply LTPs
	for i := range positions {
		if ltp, ok := ltpByIsin[positions[i].ISIN]; ok {
			positions[i].LiveLTP = ltp
		}
	}
	for i := range signals {
		if ltp, ok := ltpByIsin[signals[i].ISIN]; ok {
			signals[i].LiveLTP = ltp
		}
	}
	return positions, signals
}

func (h *ManthanHandler) loadCooldowns(ctx context.Context, strategyID string) ([]cooldownEntry, error) {
	// Cooldown schema varies; query defensively against common column names.
	// If table doesn't exist / schema different, return empty silently.
	query := `
		SELECT symbol,
		       COALESCE(eligible_from::text, ''),
		       COALESCE(reason, '20%_correction_required')
		FROM manthan_cooldown
		WHERE ($1 = '' OR strategy_id::text = $1)
		ORDER BY eligible_from DESC NULLS LAST`
	rows, err := h.positionsDB.QueryContext(ctx, query, strategyID)
	if err != nil {
		return []cooldownEntry{}, nil // schema mismatch → degrade gracefully
	}
	defer rows.Close()

	var out []cooldownEntry
	for rows.Next() {
		var c cooldownEntry
		if err := rows.Scan(&c.Symbol, &c.EligibleFrom, &c.Reason); err != nil {
			continue
		}
		out = append(out, c)
	}
	return out, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func bucketize(positions []manthanPosition, key func(manthanPosition) string, total float64, capPct float64) []allocationBucket {
	if total <= 0 || len(positions) == 0 {
		return []allocationBucket{}
	}
	agg := map[string]float64{}
	for _, p := range positions {
		agg[key(p)] += p.InvestedAmt
	}
	out := make([]allocationBucket, 0, len(agg))
	for label, invested := range agg {
		pct := (invested / total) * 100
		out = append(out, allocationBucket{
			Label:      label,
			Invested:   round2(invested),
			Percent:    round2(pct),
			CapPercent: capPct,
			OverCap:    capPct > 0 && pct > capPct,
		})
	}
	return out
}

func firstNonEmpty(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

// Note: respondWithJSON and respondWithError used by GetOverview are defined
// in the shared errors.go in this package. Route registration happens in
// router.NewRouter via dependency injection.
