package backfill

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/holiday"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// Price-change buckets returned by classifyPctChange. The comparison is on the
// SIGNED current-day change %, so only upward moves place/monitor; a stock that's
// up beyond Max% is excluded, and anything below Min% (including stocks that are
// down) becomes a monitor that buys if it rises to +Min%.
const (
	bucketPlace   = "place"   // %change within [min,max] (or no filter) → order fires now
	bucketMonitor = "monitor" // %change below min → price-watch at the Min% target
	bucketExclude = "exclude" // %change at/above max → skipped
)

// classifyPctChange mirrors matcher.evaluatePctChange's three-way logic against
// the live current-day price change. The comparison is on the SIGNED change %
// (positive-only): up within [min,max] → place now; up beyond max → exclude;
// below min (including stocks that are down) → monitor. Returns the bucket and,
// for "monitor", the target trigger price = referencePrice*(1+min/100)
// (referencePrice = prevClose, or ltp when prevClose is unavailable — same
// fallback as the live handler).
func classifyPctChange(min, max, pctChange, prevClose, ltp float64) (bucket string, target float64) {
	// No filter → everything places now.
	if min == 0 && max == 0 {
		return bucketPlace, 0
	}
	effectiveMax := max
	if effectiveMax == 0 {
		effectiveMax = math.MaxFloat64 // max unset → only a lower bound
	}
	if pctChange >= effectiveMax {
		return bucketExclude, 0
	}
	if pctChange >= min {
		return bucketPlace, 0
	}
	// Below min → monitor candidate; compute the Min% target level.
	ref := prevClose
	if ref <= 0 {
		ref = ltp
	}
	return bucketMonitor, ref * (1 + min/100)
}

// PreviewConditions mirrors the strategy conditions sent from the frontend.
type PreviewConditions struct {
	MatchAllNews   bool     `json:"match_all_news"`
	ImpactScoreMin int32    `json:"impact_score_min"`
	ImpactScoreMax int32    `json:"impact_score_max"`
	Sentiments     []string `json:"sentiments"`
	Categories     []string `json:"categories"`
	MarketCapTypes []string `json:"market_cap_types"`
	Exchanges      []string `json:"exchanges"`
	// MinPriceChangePct / MaxPriceChangePct filter on the live (current-day) price
	// change %. In [min,max] → place now; below min → price-monitoring candidate;
	// above max (when max>0) → excluded. Both 0 → no filter (everything places now).
	MinPriceChangePct float64 `json:"min_price_change_pct"`
	MaxPriceChangePct float64 `json:"max_price_change_pct"`
}

type previewRequest struct {
	Conditions PreviewConditions `json:"conditions"`
	// MaxAmountPerStock is the per-stock budget. When > 0, stocks whose single
	// share costs more than this are excluded (cannot afford 1 share → no order),
	// and quantity is derived from this budget exactly as the real backfill does.
	MaxAmountPerStock float64 `json:"max_amount_per_stock"`
	// MaxTrades is informational here — the UI uses it to cap selection. The
	// preview returns all affordable matches so the user can choose among them.
	MaxTrades int32 `json:"max_trades"`
}

// PreviewItem is one matched, affordable, deduped stock priced at live LTP.
type PreviewItem struct {
	CompanyName    string  `json:"company_name"`
	Symbol         string  `json:"symbol,omitempty"`
	NSECode        int64   `json:"nse_code,omitempty"`
	ISIN           string  `json:"isin"`
	Sentiment      string  `json:"sentiment"`
	NewsCategory   string  `json:"category"` // news category (e.g. "Order Wins")
	ImpactScore    float64 `json:"impact_score"`
	Timestamp      string  `json:"timestamp"`
	ShortSummary   string  `json:"short_summary,omitempty"`
	LTP            float64 `json:"ltp"`
	PctChange      float64 `json:"pct_change"` // live current-day price change %
	// Bucket is "place" (order fires now at live LTP) or "monitor" (below the Min%
	// range — a price watch that triggers at TargetPrice when the move reaches Min%).
	Bucket         string  `json:"bucket"`
	TargetPrice    float64 `json:"target_price,omitempty"` // monitor trigger level
	EntryPrice     float64 `json:"entry_price"`            // expected fill price, tick-rounded
	Quantity       int32   `json:"quantity"`               // derived from MaxAmountPerStock (0 if no budget)
	InvestedamtAmt float64 `json:"invested_amount"`        // EntryPrice * Quantity
}

// PreviewResponse is the JSON body returned by POST /internal/amn-preview.
type PreviewResponse struct {
	Items          []PreviewItem `json:"items"`
	TotalCount     int           `json:"total_count"`
	TotalInvested  float64       `json:"total_invested"` // sum of InvestedAmount across all rows
	From           string        `json:"from"`
	To             string        `json:"to"`
	SnapshotAgeSec float64       `json:"snapshot_age_sec"` // how stale the live prices are
}

// snapCandidate is one NSE-resolved news item in the current window.
type snapCandidate struct {
	doc RawNewsDoc
	co  CompanyInfo
}

// amnSnapshot is the shared, periodically-refreshed view of the AMN window. All
// the expensive I/O (Mongo news + company reads, Redis MGET) is done ONCE per
// refresh to build it; every preview request then filters it purely in memory,
// so the hot path makes zero DB/Redis calls and scales to many concurrent users.
type amnSnapshot struct {
	builtAt time.Time
	fromStr string
	toStr   string
	cands   []snapCandidate            // NSE-resolved news in dt_tm order
	md      map[int64]*consumer.MarketDataResult // live market data keyed by NSE token
}

// PreviewRunner is a read-only dry-run of the AMN backfill. It reuses NewsReader,
// CompanyLookup, the SAME matcher.Evaluator, and the SAME pricing/sizing helpers
// that the real backfill uses, so a preview row is exactly what AMNRunner would
// place. A background goroutine refreshes a shared in-memory snapshot every
// refreshInterval; requests filter that snapshot without touching Mongo/Redis.
type PreviewRunner struct {
	newsReader      *NewsReader
	companyLookup   *CompanyLookup
	evaluator       *matcher.Evaluator
	redisCache      *cache.RedisCache
	holidayChecker  *holiday.Checker // may be nil → weekend-only logic
	logger          *zap.Logger
	refreshInterval time.Duration

	snap    atomic.Pointer[amnSnapshot]
	buildMu sync.Mutex // serializes snapshot builds
}

// NewPreviewRunner creates a PreviewRunner.
func NewPreviewRunner(
	newsReader *NewsReader,
	companyLookup *CompanyLookup,
	evaluator *matcher.Evaluator,
	redisCache *cache.RedisCache,
	holidayChecker *holiday.Checker,
	refreshInterval time.Duration,
	logger *zap.Logger,
) *PreviewRunner {
	if refreshInterval <= 0 {
		refreshInterval = 30 * time.Second
	}
	return &PreviewRunner{
		newsReader:      newsReader,
		companyLookup:   companyLookup,
		evaluator:       evaluator,
		redisCache:      redisCache,
		holidayChecker:  holidayChecker,
		refreshInterval: refreshInterval,
		logger:          logger,
	}
}

// Start builds the first snapshot, then refreshes it on refreshInterval until ctx
// is cancelled. On refresh failure the previous snapshot is kept (stale-while-
// revalidate), so requests are never blocked on Mongo/Redis.
func (pr *PreviewRunner) Start(ctx context.Context) {
	go func() {
		// Initial build off the startup path so the service doesn't block on it.
		pr.safeRebuild(ctx, "initial snapshot build")

		t := time.NewTicker(pr.refreshInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pr.safeRebuild(ctx, "snapshot refresh")
			}
		}
	}()
}

// safeRebuild runs one bounded rebuild and recovers from any panic so a single
// bad refresh can never kill the refresh loop (it would otherwise freeze the
// snapshot forever). On failure the previous snapshot is retained.
func (pr *PreviewRunner) safeRebuild(ctx context.Context, what string) {
	defer func() {
		if rec := recover(); rec != nil {
			pr.logger.Error("amn-preview: panic during "+what+" (keeping previous snapshot)",
				zap.Any("panic", rec))
		}
	}()
	rctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	if _, err := pr.rebuild(rctx); err != nil {
		pr.logger.Warn("amn-preview: "+what+" failed (keeping previous snapshot)", zap.Error(err))
	}
}

// rebuild fetches a fresh snapshot and atomically swaps it in. Only stores on
// success so a failed refresh never clears a good snapshot.
func (pr *PreviewRunner) rebuild(ctx context.Context) (*amnSnapshot, error) {
	pr.buildMu.Lock()
	defer pr.buildMu.Unlock()
	s, err := pr.build(ctx)
	if err != nil {
		return nil, err
	}
	pr.snap.Store(s)
	return s, nil
}

// ensureSnapshot returns the current snapshot, building one synchronously on the
// first request if the background builder hasn't produced one yet.
func (pr *PreviewRunner) ensureSnapshot(ctx context.Context) (*amnSnapshot, error) {
	if s := pr.snap.Load(); s != nil {
		return s, nil
	}
	pr.buildMu.Lock()
	defer pr.buildMu.Unlock()
	if s := pr.snap.Load(); s != nil { // double-check after acquiring the lock
		return s, nil
	}
	s, err := pr.build(ctx)
	if err != nil {
		return nil, err
	}
	pr.snap.Store(s)
	return s, nil
}

// build does all the expensive I/O once: news read, company resolve, and a single
// batched Redis MGET for live market data across all NSE candidates.
func (pr *PreviewRunner) build(ctx context.Context) (*amnSnapshot, error) {
	holidays := map[string]struct{}{}
	if pr.holidayChecker != nil {
		holidays = pr.holidayChecker.NSEHolidays()
	}
	prevDay := PreviousTradingDay(time.Now(), holidays)
	from, to := AMNTimeRange(time.Now(), prevDay)

	docs, err := pr.newsReader.ReadSince(ctx, from, to)
	if err != nil {
		return nil, err
	}
	companies, err := pr.companyLookup.BatchLookup(ctx, UniqueISINs(docs))
	if err != nil {
		pr.logger.Warn("amn-preview: company lookup partial failure", zap.Error(err))
	}

	cands := make([]snapCandidate, 0, len(docs))
	tokens := make([]int64, 0, len(docs))
	tokenSeen := make(map[int64]struct{}, len(docs))
	for _, doc := range docs {
		co, ok := companies[doc.ISIN]
		if !ok || co.Exchange != "NSE" || co.NSECode <= 0 {
			continue
		}
		cands = append(cands, snapCandidate{doc: doc, co: co})
		if _, ok := tokenSeen[co.NSECode]; !ok {
			tokenSeen[co.NSECode] = struct{}{}
			tokens = append(tokens, co.NSECode)
		}
	}

	md := consumer.GetMarketDataBatchNSE(ctx, pr.redisCache, tokens, pr.logger)

	pr.logger.Info("amn-preview: snapshot built",
		zap.Int("news_docs", len(docs)),
		zap.Int("nse_candidates", len(cands)),
		zap.Int("priced_tokens", len(md)))

	return &amnSnapshot{
		builtAt: time.Now(),
		fromStr: from.UTC().Format("02 Jan 2006 15:04") + " IST",
		toStr:   to.UTC().Format("02 Jan 2006 15:04") + " IST",
		cands:   cands,
		md:      md,
	}, nil
}

// ServeHTTP implements http.Handler for POST /internal/amn-preview.
func (pr *PreviewRunner) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req previewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Read the shared snapshot (build on first request if not ready). All the
	// heavy I/O happened at snapshot-build time; the filtering below is pure CPU.
	snap, err := pr.ensureSnapshot(ctx)
	if err != nil {
		pr.logger.Error("amn-preview: snapshot unavailable", zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "preview data unavailable"})
		return
	}

	// Synthetic strategy for evaluation; pct_change/price zeroed exactly like backfill.
	evalStrategy := pr.buildEvalStrategy(req.Conditions)

	items := make([]PreviewItem, 0, 64)
	seen := make(map[string]struct{}, len(snap.cands)) // dedupe per stock (first match wins)
	var totalInvested float64
	matched, noPrice, aboveMax, unaffordable := 0, 0, 0, 0

	for _, c := range snap.cands {
		if _, dup := seen[c.doc.ISIN]; dup {
			continue
		}
		// Non-pct conditions via the matcher (pct zeroed → auto-passed).
		event := previewEvent(c.doc, c.co)
		if !pr.evaluator.Evaluate(event, evalStrategy).IsFullMatch() {
			continue
		}
		seen[c.doc.ISIN] = struct{}{}
		matched++

		md := snap.md[c.co.NSECode]
		if md == nil {
			noPrice++
			continue
		}

		bucket, target := classifyPctChange(req.Conditions.MinPriceChangePct, req.Conditions.MaxPriceChangePct,
			md.PercentChange, md.PrevClose, md.LTP)
		if bucket == bucketExclude {
			aboveMax++
			continue
		}

		var fill float64
		if bucket == bucketMonitor {
			fill = consumer.RoundToTickSize(target*1.005, md.TickSize)
		} else {
			fill = consumer.RoundToTickSize(md.LTP*1.005, md.TickSize)
		}

		var qty int32
		if req.MaxAmountPerStock > 0 {
			qty = int32(math.Floor(req.MaxAmountPerStock * 0.98 / fill))
			if qty < 1 {
				unaffordable++
				continue
			}
		}
		invested := fill * float64(qty)

		item := PreviewItem{
			CompanyName:    c.co.CompanyName,
			Symbol:         c.co.Symbol,
			NSECode:        c.co.NSECode,
			ISIN:           c.doc.ISIN,
			Sentiment:      c.doc.Sentiment,
			NewsCategory:   c.doc.Category,
			ImpactScore:    float64(c.doc.ImpactScoreInt32()),
			Timestamp:      c.doc.DtTm.UTC().Format("02 Jan 15:04"),
			ShortSummary:   c.doc.ShortSummary,
			LTP:            md.LTP,
			PctChange:      md.PercentChange,
			Bucket:         bucket,
			EntryPrice:     fill,
			Quantity:       qty,
			InvestedamtAmt: invested,
		}
		if bucket == bucketMonitor {
			item.TargetPrice = consumer.RoundToTickSize(target, md.TickSize)
		}

		items = append(items, item)
		totalInvested += invested
	}

	pr.logger.Debug("amn-preview: filtered snapshot",
		zap.Int("candidates", len(snap.cands)),
		zap.Int("matched", matched),
		zap.Int("no_price", noPrice),
		zap.Int("above_max", aboveMax),
		zap.Int("unaffordable", unaffordable),
		zap.Int("returned", len(items)),
		zap.Float64("snapshot_age_sec", time.Since(snap.builtAt).Seconds()))

	writeJSON(w, http.StatusOK, PreviewResponse{
		Items:          items,
		TotalCount:     len(items),
		TotalInvested:  totalInvested,
		From:           snap.fromStr,
		To:             snap.toStr,
		SnapshotAgeSec: time.Since(snap.builtAt).Seconds(),
	})
}

// buildEvalStrategy maps the preview payload onto a models.Strategy whose
// conditions match what the user configured. pct_change and price range are left
// at zero so the matcher auto-passes them — identical to AMNRunner.run.
func (pr *PreviewRunner) buildEvalStrategy(c PreviewConditions) *models.Strategy {
	return &models.Strategy{
		Conditions: models.Conditions{
			MatchAllNews:   c.MatchAllNews,
			ImpactScoreMin: c.ImpactScoreMin,
			ImpactScoreMax: c.ImpactScoreMax,
			Sentiments:     c.Sentiments,
			Categories:     c.Categories,
			MarketCapTypes: c.MarketCapTypes,
			Exchanges:      c.Exchanges,
		},
	}
}

// previewEvent builds the minimal MarketEvent the evaluator reads.
func previewEvent(doc RawNewsDoc, co CompanyInfo) *models.MarketEvent {
	return &models.MarketEvent{
		EventType: "news",
		Timestamp: doc.DtTm,
		StockData: models.StockData{
			StockCode:   co.NSECode,
			NSECode:     co.NSECode,
			BSECode:     co.BSECode,
			Exchange:    co.Exchange,
			Symbol:      co.Symbol,
			ISIN:        doc.ISIN,
			CompanyName: co.CompanyName,
			MCap:        co.MCap,
			MCapType:    co.MCapType,
		},
		NewsData: models.NewsData{
			Category:     doc.Category,
			ShortSummary: doc.ShortSummary,
		},
		Analysis: models.Analysis{
			Sentiment:   doc.Sentiment,
			ImpactScore: doc.ImpactScoreInt32(),
		},
	}
}

// LimitConcurrency bounds the number of in-flight preview requests. Beyond the
// cap it sheds load with 503 + Retry-After instead of piling up goroutines and
// exhausting the DB/Redis pools — keeping tail latency bounded under a spike.
func LimitConcurrency(h http.Handler, max int) http.Handler {
	if max <= 0 {
		return h
	}
	sem := make(chan struct{}, max)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			h.ServeHTTP(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			http.Error(w, `{"error":"preview busy, retry shortly"}`, http.StatusServiceUnavailable)
		}
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
