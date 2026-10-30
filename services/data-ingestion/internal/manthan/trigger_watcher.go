package manthan

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// ─── Mechanical Buy trigger (operator decision 2026-10-30) ───────────────────
//
// The sheet's hand-typed ATH_Entry="Buy" column is replaced by a rule:
//
//	a stock enters the day's Buy list the first time its LIVE price (external
//	price Redis, the same feed every downstream step uses) trades ABOVE its
//	stored close-based all-time high (ref_ath_close), while it passes the
//	fundamentals filters on the latest reference data.
//
// The watcher runs inside manthan-live in db mode. Every refresh it rebuilds
// the watchlist from the DB reader (same data, same thresholds as the
// pipeline), every tick it reads the LTPs for that list in one MGET, and
// writes ref_buy_triggers for the crossers. The pipeline then treats those
// rows exactly as it treated sheet rows marked Buy.
//
// Guards:
//   - trading window only (default 09:15:30–15:15 IST: trade-execution
//     refuses entries after 15:20 — SGFIN's six dead-lettered entries on
//     2026-10-29 were generated at 15:24)
//   - a price row older than priceMaxAge is ignored (dead token, not a tick)
//   - strict `ltp > ath_close`; equality is not a breakout
//   - PRIMARY KEY (symbol, trade_date) makes a second cross the same day a no-op

// TriggerCandidate is one watched stock.
type TriggerCandidate struct {
	Symbol   string
	ISIN     string
	ATHClose float64
}

// TriggerWatcher polls the price feed for ATH crossings.
type TriggerWatcher struct {
	db     *sql.DB
	reader *DBReader
	get    func(ctx context.Context, key string) (string, error)
	mget   func(ctx context.Context, keys []string) ([]any, error)
	now    func() time.Time
	ist    *time.Location
	logger *zap.Logger

	// OnTrigger, when set, runs after new rows were written (main wires the
	// pipeline run here). Called synchronously from the tick.
	OnTrigger func(symbols []string)

	Tick         time.Duration     // price poll interval (default 15s)
	Refresh      time.Duration     // watchlist rebuild interval (default 10m)
	WindowOpen   int               // seconds after midnight IST (default 09:15:30)
	WindowClose  int               // (default 15:15:00)
	PriceMaxAge  time.Duration     // default 5m
	tokenCache   map[string]string // ISIN → exchange token
	tokenCacheMu sync.Mutex
}

// NewTriggerWatcher connects to the external price Redis.
func NewTriggerWatcher(db *sql.DB, reader *DBReader, redisAddr, redisPassword string, logger *zap.Logger) *TriggerWatcher {
	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr, Password: redisPassword, DB: 0,
		DialTimeout: 3 * time.Second, ReadTimeout: 3 * time.Second})
	w := newTriggerWatcherWithGetters(db, reader,
		func(ctx context.Context, key string) (string, error) { return rdb.Get(ctx, key).Result() },
		func(ctx context.Context, keys []string) ([]any, error) { return rdb.MGet(ctx, keys...).Result() },
		time.Now, logger)
	return w
}

// newTriggerWatcherWithGetters is the test seam.
func newTriggerWatcherWithGetters(db *sql.DB, reader *DBReader,
	get func(ctx context.Context, key string) (string, error),
	mget func(ctx context.Context, keys []string) ([]any, error),
	now func() time.Time, logger *zap.Logger) *TriggerWatcher {
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		ist = time.FixedZone("IST", 5*3600+1800)
	}
	return &TriggerWatcher{
		db: db, reader: reader, get: get, mget: mget, now: now, ist: ist, logger: logger,
		Tick: 15 * time.Second, Refresh: 10 * time.Minute,
		WindowOpen: 9*3600 + 15*60 + 30, WindowClose: 15*3600 + 15*60,
		PriceMaxAge: 5 * time.Minute,
		tokenCache:  map[string]string{},
	}
}

// Watchlist derives today's candidates from the DB reader's view: every
// stock with an ATH and fundamentals that pass the same filters the
// pipeline applies (market-cap band, PE, PAT, F-score). Stocks already
// triggered today are excluded.
func (w *TriggerWatcher) Watchlist(ctx context.Context) ([]TriggerCandidate, error) {
	raw, err := w.reader.ReadAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("trigger watchlist: %w", err)
	}
	fs := map[string]float64{}
	for _, f := range raw.FScores {
		fs[f.ISIN] = f.Score
	}
	ath := map[string]float64{}
	for _, l := range raw.LifeTimeHighs {
		ath[l.ScripName] = l.AllTimeHigh
	}
	already := map[string]bool{}
	for _, b := range raw.BuySignals {
		already[b.ScripName] = true
	}
	var out []TriggerCandidate
	for _, pe := range raw.PEAndNetProfit {
		sym := pe.NSESymbol
		if sym == "" || pe.ISIN == "" || already[sym] {
			continue
		}
		a, ok := ath[sym]
		if !ok || a <= 0 {
			continue
		}
		if pe.LatestMarketCap < MCapMinCr || pe.LatestMarketCap > MCapMaxCr {
			continue
		}
		if pe.TTMPE <= 0 || pe.TTMPE > PEMax || pe.PAT <= NetProfitMin || fs[pe.ISIN] < FScoreMin {
			continue
		}
		out = append(out, TriggerCandidate{Symbol: sym, ISIN: pe.ISIN, ATHClose: a})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out, nil
}

// inWindow reports whether now (IST) is inside the trigger window on a weekday.
func (w *TriggerWatcher) inWindow(now time.Time) bool {
	n := now.In(w.ist)
	if n.Weekday() == time.Saturday || n.Weekday() == time.Sunday {
		return false
	}
	s := n.Hour()*3600 + n.Minute()*60 + n.Second()
	return s >= w.WindowOpen && s <= w.WindowClose
}

// token resolves ISIN → exchange token via the feed's isin: key, cached.
func (w *TriggerWatcher) token(ctx context.Context, isin string) (string, error) {
	w.tokenCacheMu.Lock()
	t, ok := w.tokenCache[isin]
	w.tokenCacheMu.Unlock()
	if ok {
		return t, nil
	}
	raw, err := w.get(ctx, "isin:"+isin)
	if err != nil {
		return "", err
	}
	var m struct {
		NSECode string `json:"nsecode"`
	}
	if json.Unmarshal([]byte(raw), &m) != nil || strings.TrimSpace(m.NSECode) == "" {
		return "", fmt.Errorf("isin %s: no nsecode in feed", isin)
	}
	t = strings.TrimSpace(m.NSECode)
	w.tokenCacheMu.Lock()
	w.tokenCache[isin] = t
	w.tokenCacheMu.Unlock()
	return t, nil
}

type priceRow struct {
	LTP         float64 `json:"ltp"`
	TimestampMs int64   `json:"timestamp"`
	LastUpdated string  `json:"last_updated"`
}

func (p priceRow) asOf() time.Time {
	switch {
	case p.TimestampMs > 1e12:
		return time.UnixMilli(p.TimestampMs)
	case p.TimestampMs > 1e9:
		return time.Unix(p.TimestampMs, 0)
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, p.LastUpdated); err == nil {
			return t
		}
	}
	return time.Time{}
}

// Crossing is one detected breakout.
type Crossing struct {
	Symbol   string
	LTP      float64
	ATHClose float64
}

// Scan reads LTPs for the candidates and returns those trading above their
// ATH close with a fresh price. Unresolvable tokens are skipped and counted.
func (w *TriggerWatcher) Scan(ctx context.Context, cands []TriggerCandidate) ([]Crossing, error) {
	if len(cands) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(cands))
	idx := make([]int, 0, len(cands))
	unresolved := 0
	for i, c := range cands {
		t, err := w.token(ctx, c.ISIN)
		if err != nil {
			if !errors.Is(err, goredis.Nil) && !strings.Contains(err.Error(), "no nsecode") {
				return nil, fmt.Errorf("trigger scan: feed unreachable: %w", err)
			}
			unresolved++
			continue
		}
		keys = append(keys, "market:nse:"+t)
		idx = append(idx, i)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	vals, err := w.mget(ctx, keys)
	if err != nil {
		return nil, fmt.Errorf("trigger scan: mget: %w", err)
	}
	now := w.now()
	var out []Crossing
	stale, noRow := 0, 0
	for k, v := range vals {
		if k >= len(idx) {
			break
		}
		c := cands[idx[k]]
		s, ok := v.(string)
		if !ok || s == "" {
			noRow++
			continue
		}
		var p priceRow
		if json.Unmarshal([]byte(s), &p) != nil || p.LTP <= 0 {
			noRow++
			continue
		}
		if as := p.asOf(); as.IsZero() || now.Sub(as) > w.PriceMaxAge {
			stale++
			continue
		}
		if p.LTP > c.ATHClose {
			out = append(out, Crossing{Symbol: c.Symbol, LTP: p.LTP, ATHClose: c.ATHClose})
		}
	}
	if unresolved+stale+noRow > 0 {
		w.logger.Debug("trigger scan: skipped", zap.Int("unresolved_isin", unresolved), zap.Int("stale_price", stale), zap.Int("no_price_row", noRow))
	}
	return out, nil
}

// Record writes crossings to ref_buy_triggers; returns the symbols that were
// NEW today (already-present rows are left untouched).
func (w *TriggerWatcher) Record(ctx context.Context, xs []Crossing) ([]string, error) {
	if len(xs) == 0 {
		return nil, nil
	}
	today := w.now().In(w.ist).Format("2006-01-02")
	var fresh []string
	for _, x := range xs {
		res, err := w.db.ExecContext(ctx, `
			INSERT INTO ref_buy_triggers (symbol, trade_date, triggered_at, ltp, ath_close, source)
			VALUES ($1, $2, now(), $3, $4, 'live:ext-redis')
			ON CONFLICT (symbol, trade_date) DO NOTHING`, x.Symbol, today, x.LTP, x.ATHClose)
		if err != nil {
			return fresh, fmt.Errorf("trigger record %s: %w", x.Symbol, err)
		}
		if n, _ := res.RowsAffected(); n == 1 {
			fresh = append(fresh, x.Symbol)
			w.logger.Info("BUY TRIGGER — live price crossed close-based ATH",
				zap.String("symbol", x.Symbol), zap.Float64("ltp", x.LTP), zap.Float64("ath_close", x.ATHClose),
				zap.String("trade_date", today))
		}
	}
	return fresh, nil
}

// Run loops until ctx is done: rebuild the watchlist every Refresh, scan
// every Tick inside the window. Errors are logged, never fatal.
func (w *TriggerWatcher) Run(ctx context.Context) {
	w.logger.Info("trigger watcher started", zap.Duration("tick", w.Tick), zap.Duration("refresh", w.Refresh),
		zap.String("window_ist", fmt.Sprintf("%02d:%02d:%02d–%02d:%02d:%02d",
			w.WindowOpen/3600, w.WindowOpen%3600/60, w.WindowOpen%60, w.WindowClose/3600, w.WindowClose%3600/60, w.WindowClose%60)))
	var cands []TriggerCandidate
	var lastRefresh time.Time
	ticker := time.NewTicker(w.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		now := w.now()
		if !w.inWindow(now) {
			continue
		}
		if cands == nil || now.Sub(lastRefresh) >= w.Refresh {
			c, err := w.Watchlist(ctx)
			if err != nil {
				w.logger.Warn("trigger watcher: watchlist rebuild failed — keeping previous", zap.Error(err))
			} else {
				cands, lastRefresh = c, now
				w.logger.Info("trigger watcher: watchlist rebuilt", zap.Int("candidates", len(cands)))
			}
		}
		xs, err := w.Scan(ctx, cands)
		if err != nil {
			w.logger.Warn("trigger watcher: scan failed", zap.Error(err))
			continue
		}
		fresh, err := w.Record(ctx, xs)
		if err != nil {
			w.logger.Error("trigger watcher: record failed", zap.Error(err))
			continue
		}
		if len(fresh) > 0 {
			// Drop triggered names from the in-memory list until the next refresh.
			set := map[string]bool{}
			for _, s := range fresh {
				set[s] = true
			}
			kept := cands[:0]
			for _, c := range cands {
				if !set[c.Symbol] {
					kept = append(kept, c)
				}
			}
			cands = kept
			if w.OnTrigger != nil {
				w.OnTrigger(fresh)
			}
		}
	}
}

// InWindow is the exported form of the trading-window check (probe tool).
func (w *TriggerWatcher) InWindow(now time.Time) bool { return w.inWindow(now) }

// ProbeResult explains one candidate's state on the feed right now.
type ProbeResult struct {
	Token        string
	LTP          float64
	Age          time.Duration
	WouldTrigger bool
	Note         string
}

// Probe reads one candidate through the same token/price path as Scan and
// says why it would or would not trigger. Read-only.
func (w *TriggerWatcher) Probe(ctx context.Context, c TriggerCandidate) ProbeResult {
	var r ProbeResult
	t, err := w.token(ctx, c.ISIN)
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			r.Note = "ISIN not in the price feed (isin: key missing)"
		} else {
			r.Note = "token lookup failed: " + err.Error()
		}
		return r
	}
	r.Token = t
	vals, err := w.mget(ctx, []string{"market:nse:" + t})
	if err != nil || len(vals) == 0 {
		r.Note = fmt.Sprintf("price read failed: %v", err)
		return r
	}
	s, ok := vals[0].(string)
	if !ok || s == "" {
		r.Note = "no price row for token"
		return r
	}
	var p priceRow
	if json.Unmarshal([]byte(s), &p) != nil || p.LTP <= 0 {
		r.Note = "price row has no LTP"
		return r
	}
	r.LTP = p.LTP
	as := p.asOf()
	if as.IsZero() {
		r.Note = "price row has no timestamp"
		return r
	}
	r.Age = w.now().Sub(as).Round(time.Second)
	switch {
	case r.Age > w.PriceMaxAge:
		r.Note = fmt.Sprintf("price stale (> %s) — would be ignored", w.PriceMaxAge)
	case p.LTP > c.ATHClose:
		r.WouldTrigger = true
		r.Note = "above ATH close"
	default:
		r.Note = fmt.Sprintf("%.2f%% below ATH close", (c.ATHClose-p.LTP)/c.ATHClose*100)
	}
	return r
}
