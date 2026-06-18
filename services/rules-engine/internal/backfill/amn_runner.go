// Package backfill implements the After-Market News (AMN) backfill feature.
//
// When a strategy is created with process_after_market_news=true the runner:
//  1. Finds the previous NSE_EQ trading day.
//  2. Reads CAG_CHATBOT.NewsImpactDashboard for news in [prevDay 15:31 IST, now).
//  3. Joins OdinMasterData.CompanyMaster on ISIN to get stock codes.
//  4. Evaluates each news item against the strategy's conditions
//     (impact_score, sentiment, category, exchange, market_cap — pct_change is
//     skipped because historical pct_change is not available).
//  5. Places orders at the current live LTP, tagged signal_source="BACKFILL_AMN".
//  6. Rate-limits to 10 orders/sec per user (shared across all strategies).
package backfill

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/holiday"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

const (
	// ordersPerSecondPerUser is the maximum order publication rate per user,
	// enforced across all of that user's concurrent backfill goroutines.
	ordersPerSecondPerUser = 10

	// backfillTimeout caps how long a single backfill run may take.
	backfillTimeout = 10 * time.Minute

	// signalSource is the tag written to every order originating from AMN backfill.
	signalSource = "BACKFILL_AMN"
)

// AMNRunner orchestrates after-market news backfill for a single strategy.
// Safe for concurrent use across multiple users and strategies.
type AMNRunner struct {
	newsReader    *NewsReader
	companyLookup *CompanyLookup
	evaluator     *matcher.Evaluator
	holidayChecker *holiday.Checker
	redisCache    *cache.RedisCache
	kafkaPub      *publisher.KafkaPublisher
	signalRepo    *repository.TradeSignalRepository // may be nil
	logger        *zap.Logger

	// running tracks in-flight backfills keyed by strategy_id.
	// Prevents duplicate goroutines for the same strategy.
	running sync.Map // map[strategyID string] → struct{}

	// limiters holds one *rate.Limiter per user (10 tokens/sec, burst 10).
	// All strategies belonging to the same user share one limiter so the
	// aggregate rate across concurrent backfills stays within the 10 OPS cap.
	limiters sync.Map // map[userID string] → *rate.Limiter
}

// Config bundles the dependencies needed to build an AMNRunner.
type Config struct {
	MongoClient    *mongo.Client
	Evaluator      *matcher.Evaluator
	HolidayChecker *holiday.Checker // may be nil — holidays then not enforced
	RedisCache     *cache.RedisCache
	KafkaPub       *publisher.KafkaPublisher
	SignalRepo     *repository.TradeSignalRepository // may be nil
	Logger         *zap.Logger
}

// NewAMNRunner constructs an AMNRunner from the provided config.
func NewAMNRunner(cfg Config) *AMNRunner {
	return &AMNRunner{
		newsReader:     NewNewsReader(cfg.MongoClient, cfg.Logger),
		companyLookup:  NewCompanyLookup(cfg.MongoClient, cfg.Logger),
		evaluator:      cfg.Evaluator,
		holidayChecker: cfg.HolidayChecker,
		redisCache:     cfg.RedisCache,
		kafkaPub:       cfg.KafkaPub,
		signalRepo:     cfg.SignalRepo,
		logger:         cfg.Logger,
	}
}

// TriggerBackfill starts a detached goroutine for the given strategy if one is not
// already running. svcCtx should be the service's root context so the backfill
// respects graceful shutdown; it is NOT the HTTP request context.
//
// Returns immediately (non-blocking). The API can respond in <500ms.
func (r *AMNRunner) TriggerBackfill(svcCtx context.Context, strategy *models.Strategy) {
	if _, loaded := r.running.LoadOrStore(strategy.StrategyID, struct{}{}); loaded {
		r.logger.Info("AMN backfill already running for strategy — skipping duplicate trigger",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("user_id", strategy.UserID))
		return
	}

	go func() {
		defer r.running.Delete(strategy.StrategyID)

		ctx, cancel := context.WithTimeout(svcCtx, backfillTimeout)
		defer cancel()

		r.logger.Info("AMN backfill started",
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("user_id", strategy.UserID),
			zap.String("strategy_name", strategy.StrategyName))

		if err := r.run(ctx, strategy); err != nil {
			r.logger.Error("AMN backfill failed",
				zap.String("strategy_id", strategy.StrategyID),
				zap.String("user_id", strategy.UserID),
				zap.Error(err))
		}
	}()
}

// run is the synchronous backfill logic executed inside the detached goroutine.
func (r *AMNRunner) run(ctx context.Context, strategy *models.Strategy) error {
	// ── 1. Resolve NSE_EQ holiday set ───────────────────────────────────────
	var nseHolidays map[string]struct{}
	if r.holidayChecker != nil {
		nseHolidays = r.holidayChecker.NSEHolidays()
	} else {
		nseHolidays = map[string]struct{}{}
	}

	// ── 2. Compute time window ────────────────────────────────────────────────
	now := time.Now()
	prevDay := PreviousTradingDay(now, nseHolidays)
	from, to := AMNTimeRange(now, prevDay)

	r.logger.Info("AMN backfill time range",
		zap.String("strategy_id", strategy.StrategyID),
		zap.String("prev_trading_day", prevDay.In(ist).Format("2006-01-02")),
		zap.Time("query_from", from),
		zap.Time("query_to", to))

	// ── 3. Fetch news from MongoDB ────────────────────────────────────────────
	docs, err := r.newsReader.ReadSince(ctx, from, to)
	if err != nil {
		return fmt.Errorf("news fetch: %w", err)
	}
	if len(docs) == 0 {
		r.logger.Info("AMN backfill: no news found in window",
			zap.String("strategy_id", strategy.StrategyID))
		return nil
	}

	// ── 4. Batch-resolve ISIN → CompanyInfo ──────────────────────────────────
	isins := UniqueISINs(docs)
	companyMap, err := r.companyLookup.BatchLookup(ctx, isins)
	if err != nil {
		return fmt.Errorf("company lookup: %w", err)
	}

	// ── 5. Evaluate + publish ─────────────────────────────────────────────────
	limiter := r.getLimiter(strategy.UserID)

	// When the user picked specific stocks in the AMN preview, restrict placement
	// to exactly those ISINs. Empty → place all affordable matches (fallback).
	var selected map[string]struct{}
	if len(strategy.AMNSelectedStocks) > 0 {
		selected = make(map[string]struct{}, len(strategy.AMNSelectedStocks))
		for _, isin := range strategy.AMNSelectedStocks {
			selected[isin] = struct{}{}
		}
	}
	// Dedupe placement to one order per stock (matches the preview's per-stock view).
	placedISIN := make(map[string]struct{})

	var (
		scanned  int
		matched  int
		placed   int
		skipped  int
		maxTrades = strategy.RiskLimits.MaxTradesPerStrategy
	)

	for _, doc := range docs {
		if ctx.Err() != nil {
			break
		}
		scanned++

		// Restrict to user-selected stocks when a selection was provided.
		if selected != nil {
			if _, ok := selected[doc.ISIN]; !ok {
				skipped++
				continue
			}
		}

		// One order per stock: skip if we already placed for this ISIN.
		if _, done := placedISIN[doc.ISIN]; done {
			skipped++
			continue
		}

		company, ok := companyMap[doc.ISIN]
		if !ok {
			skipped++
			continue
		}

		// Only process NSE-listed stocks for AMN (NSE_EQ backfill).
		if company.Exchange != "NSE" {
			skipped++
			continue
		}
		if company.NSECode <= 0 {
			skipped++
			continue
		}

		event := r.buildMarketEvent(doc, company)

		// Evaluate all NON-pct conditions first (pct zeroed on a copy); the live
		// price-change classification is done separately below from Redis data.
		evalStrategy := *strategy
		evalStrategy.Conditions.MinPctChange = 0
		evalStrategy.Conditions.MaxPctChange = 0

		res := r.evaluator.Evaluate(event, &evalStrategy)
		if !res.IsFullMatch() {
			skipped++
			continue
		}
		matched++

		// Per-strategy daily trade cap (local counter; no DB lookup for backfill).
		// Counts both immediate placements and price-monitor watches (shared cap).
		if maxTrades > 0 && int32(placed) >= maxTrades {
			r.logger.Info("AMN backfill: reached MaxTradesPerStrategy cap",
				zap.String("strategy_id", strategy.StrategyID),
				zap.Int32("cap", maxTrades))
			break
		}

		// ── Rate limit: 10 OPS per user across all concurrent backfills ──────
		if err := limiter.Wait(ctx); err != nil {
			break // ctx cancelled
		}

		// ── Fetch live LTP + prev close + tick size from Redis ────────────────
		md, err := consumer.GetMarketDataFromRedis(ctx, r.redisCache, event.StockData, r.logger)
		if err != nil {
			r.logger.Warn("AMN backfill: Redis market data unavailable, skipping stock",
				zap.String("isin", doc.ISIN),
				zap.String("symbol", company.Symbol),
				zap.Error(err))
			skipped++
			continue
		}

		// ── Classify by live current-day price change % ───────────────────────
		bucket, target := classifyPctChange(
			strategy.Conditions.MinPctChange, strategy.Conditions.MaxPctChange,
			md.PercentChange, md.PrevClose, md.LTP)
		if bucket == bucketExclude {
			skipped++ // moved beyond Max% → skip
			continue
		}
		// Fallback (no explicit selection): place in-range matches only; below-min
		// stocks become price-monitor watches only when the user opted them in.
		if bucket == bucketMonitor && selected == nil {
			skipped++
			continue
		}

		// ── Build and publish order (immediate place, or price-monitor watch) ──
		orderReq, err := r.buildOrderRequest(event, strategy, md, bucket, target)
		if err != nil {
			r.logger.Warn("AMN backfill: failed to build order request",
				zap.String("symbol", company.Symbol),
				zap.Error(err))
			skipped++
			continue
		}

		r.publishOrder(ctx, orderReq)
		placed++
		placedISIN[doc.ISIN] = struct{}{}

		r.logger.Info("AMN backfill: order published",
			zap.String("order_id", orderReq.OrderID),
			zap.String("symbol", orderReq.Symbol),
			zap.String("bucket", bucket),
			zap.Int64("stock_code", orderReq.StockCode),
			zap.Float64("price", orderReq.Price),
			zap.Float64("stop_loss", orderReq.StopLoss),
			zap.Float64("take_profit", orderReq.TakeProfit),
			zap.String("signal_source", orderReq.SignalSource))
	}

	r.logger.Info("AMN backfill complete",
		zap.String("strategy_id", strategy.StrategyID),
		zap.String("user_id", strategy.UserID),
		zap.Int("scanned", scanned),
		zap.Int("matched", matched),
		zap.Int("placed", placed),
		zap.Int("skipped", skipped))

	return nil
}

// buildMarketEvent constructs a MarketEvent from a news document and its company
// metadata. PctChange is left at 0 because historical pct_change is unavailable;
// the caller zeroes the strategy's pct_change conditions before evaluating.
func (r *AMNRunner) buildMarketEvent(doc RawNewsDoc, company CompanyInfo) *models.MarketEvent {
	eventID := fmt.Sprintf("%v", doc.ID)
	if eventID == "" || eventID == "<nil>" {
		eventID = uuid.New().String()
	}

	return &models.MarketEvent{
		EventID:   eventID,
		EventType: "news",
		Timestamp: doc.DtTm,
		StockData: models.StockData{
			StockCode:   company.NSECode,
			NSECode:     company.NSECode,
			BSECode:     company.BSECode,
			Exchange:    company.Exchange,
			Symbol:      company.Symbol,
			ISIN:        doc.ISIN,
			CompanyName: company.CompanyName,
			MCap:        company.MCap,
			MCapType:    company.MCapType,
		},
		NewsData: models.NewsData{
			NewsID:       fmt.Sprintf("%v", doc.NewsID),
			Category:     doc.Category,
			ShortSummary: doc.ShortSummary,
			DocumentDate: doc.DtTm,
		},
		Analysis: models.Analysis{
			Sentiment:   doc.Sentiment,
			ImpactScore: doc.ImpactScoreInt32(),
		},
		// MarketData is intentionally empty here; LTP is fetched from Redis
		// separately and used only at order-build time.
	}
}

// buildOrderRequest constructs a trade signal for a backfill match.
//
//   - bucket == "place"   → immediate LIMIT order at live LTP + 0.5% (within range).
//   - bucket == "monitor" → STOP_LOSS + BRACKET order at the Min% target price; the
//     trade-execution PriceMonitor watches it and fires when the move reaches Min%.
//     (target is the trigger level computed by classifyPctChange.)
func (r *AMNRunner) buildOrderRequest(
	event *models.MarketEvent,
	strategy *models.Strategy,
	md *consumer.MarketDataResult,
	bucket string,
	target float64,
) (*models.OrderRequest, error) {
	ltp := md.LTP
	tickSize := md.TickSize
	if ltp <= 0 {
		return nil, fmt.Errorf("invalid LTP %.4f for %s", ltp, event.StockData.Symbol)
	}
	if tickSize <= 0 {
		return nil, fmt.Errorf("invalid tick_size %.4f for %s", tickSize, event.StockData.Symbol)
	}

	slPct := strategy.TradeConfig.StopLossPct
	tpPct := strategy.TradeConfig.TakeProfitPct
	isMultiLevelSL := strategy.TradeConfig.StopLossType == "MULTI_LEVEL"
	isMultiLevelTP := strategy.TradeConfig.TakeProfitType == "MULTI_LEVEL"
	isTrailingSL := strategy.TradeConfig.StopLossType == "TRAILING"

	// basePrice = the price from which SL/TP % and the entry fill are derived, and
	// sizingPrice = the price the order will actually fill at (used for budget qty).
	var orderType, productType string
	var orderPrice, basePrice, sizingPrice float64

	if bucket == bucketMonitor {
		// ── Case: price-monitor watch at the Min% target ──
		orderType = "STOP_LOSS"
		productType = "BRACKET" // PriceMonitor detects STOP_LOSS+BRACKET → watch queue
		orderPrice = consumer.RoundToTickSize(target, tickSize)
		basePrice = target
		sizingPrice = consumer.RoundToTickSize(target*1.005, tickSize)
	} else {
		// ── Case: immediate execution at live LTP ──
		orderType = "LIMIT"
		if isTrailingSL || isMultiLevelSL || isMultiLevelTP {
			productType = "INTRADAY"
		} else {
			productType = "BRACKET"
		}
		orderPrice = consumer.RoundToTickSize(ltp*1.005, tickSize)
		basePrice = ltp
		sizingPrice = orderPrice
	}
	if sizingPrice <= 0 {
		return nil, fmt.Errorf("invalid sizing price for %s", event.StockData.Symbol)
	}

	// Construct base order via the shared helper so all fields are wired correctly.
	// We build a synthetic RuleMatch since the evaluator isn't wired into this path.
	syntheticMatch := &models.RuleMatch{
		UserID:       strategy.UserID,
		StrategyID:   strategy.StrategyID,
		StrategyName: strategy.StrategyName,
		Strategy:     strategy,
		MatchScore:   100.0,
		Timestamp:    time.Now(),
		EventID:      event.EventID,
	}

	orderReq := models.NewOrderRequest(syntheticMatch, event, strategy)

	// Override fields that the handler normally sets after NewOrderRequest.
	orderReq.OrderType = orderType
	orderReq.ProductType = productType
	orderReq.Price = orderPrice
	orderReq.CurrentPctChange = md.PercentChange

	if bucket == bucketMonitor {
		orderReq.PctChangeStatus = "below_min"
		// Upper monitor bound, mirroring the live handler (Case 2).
		if maxPct := strategy.Conditions.MaxPctChange; maxPct > 0 {
			ref := md.PrevClose
			if ref <= 0 {
				ref = ltp
			}
			orderReq.MaxMonitorPrice = ref * (1 + maxPct/100)
		}
	} else if strategy.Conditions.MinPctChange > 0 || strategy.Conditions.MaxPctChange > 0 {
		orderReq.PctChangeStatus = "within_range"
	}

	// SL/TP derived from basePrice (LTP for place, target for monitor).
	if !isMultiLevelSL && slPct > 0 {
		orderReq.StopLoss = consumer.RoundToTickSize(basePrice*(1-slPct/100), tickSize)
	}
	if !isMultiLevelTP && tpPct > 0 {
		orderReq.TakeProfit = consumer.RoundToTickSize(basePrice*(1+tpPct/100), tickSize)
	}

	// Amount-based sizing: derive quantity from budget at the fill (sizing) price.
	if maxAmt := strategy.RiskLimits.MaxAmountPerStock; maxAmt > 0 {
		computedQty := int32(math.Floor(maxAmt*0.98 / sizingPrice))
		if computedQty < 1 {
			return nil, fmt.Errorf("cannot afford 1 share of %s within max_amount_per_stock=%.2f at price=%.2f",
				event.StockData.Symbol, maxAmt, sizingPrice)
		}
		orderReq.Quantity = computedQty
	}

	// Validate final quantity.
	if orderReq.Quantity <= 0 {
		return nil, fmt.Errorf("invalid quantity %d for %s", orderReq.Quantity, event.StockData.Symbol)
	}

	// Tag as backfill signal.
	orderReq.SignalSource = signalSource
	orderReq.RiskApproved = true
	orderReq.RiskScore = 0.0

	return orderReq, nil
}

// publishOrder writes the order to Kafka and (if available) PostgreSQL.
// Both writes are fire-and-forget within the backfill goroutine's context.
func (r *AMNRunner) publishOrder(ctx context.Context, orderReq *models.OrderRequest) {
	var wg sync.WaitGroup

	if r.signalRepo != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.signalRepo.SaveTradeSignal(ctx, orderReq); err != nil {
				r.logger.Error("AMN backfill: failed to save signal to DB",
					zap.String("order_id", orderReq.OrderID),
					zap.Error(err))
			}
		}()
	}

	if r.kafkaPub != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.kafkaPub.PublishTradeSignal(ctx, orderReq); err != nil {
				r.logger.Error("AMN backfill: failed to publish to Kafka",
					zap.String("order_id", orderReq.OrderID),
					zap.Error(err))
			}
		}()
	}

	wg.Wait()
}

// getLimiter returns the per-user rate limiter (10 tokens/sec, burst 10).
// Creates one on first call for a given user ID.
func (r *AMNRunner) getLimiter(userID string) *rate.Limiter {
	v, _ := r.limiters.LoadOrStore(userID, rate.NewLimiter(rate.Limit(ordersPerSecondPerUser), ordersPerSecondPerUser))
	return v.(*rate.Limiter)
}
