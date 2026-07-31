package manthan

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// Consumer reads eligible stock signals from the manthan.signals Kafka topic.
// For each batch of signals, it runs the allocator against every active MANTHAN
// strategy and publishes entry orders.
//
// Source-of-truth hierarchy for signals:
//   - Postgres `manthan_signals` (signals_db DB) — durable, authoritative.
//   - Kafka `manthan.signals`                     — transport/notification.
//   - Redis `manthan:signals:{date}`              — hot cache (rebuildable from DB).
type Consumer struct {
	reader *kafka.Reader
	rdb    *redis.Client

	// ltpFeed is wired AFTER NewConsumer via SetLTPFeed because of a
	// construction-order circular dependency (LTPFeed needs TickHandler,
	// TickHandler is built after Consumer, Consumer wants LTPFeed). Stored
	// in an atomic.Pointer so the post-construction Set is safely visible
	// to the consumer goroutine without relying on the happens-before edge
	// from the `go` statement that launches Start — without atomic, a
	// future caller flipping the feed mid-flight (hot-reconfig, dynamic
	// swap) would race with getLiveLTP reading c.ltpFeed.
	ltpFeed atomic.Pointer[LTPFeed]

	signalsDB    *sql.DB
	allocator    *Allocator
	orderGen     *OrderGenerator
	portfolioMgr *PortfolioManager
	slMgr        *TrailingSLManager
	publisher    OrderPublisher
	strategyFn   func() []types.UserStrategy
	emaFn        func() map[string]float64
	logger       *zap.Logger
}

// OrderPublisher is the interface for publishing trade signals + persisting the
// in-memory position/trail to manthan_positions for restart recovery (FIX F).
type OrderPublisher interface {
	PublishEntryOrder(ctx context.Context, order ManthanOrder) error
	PublishSLModify(ctx context.Context, order SLModifyOrder) error
	PublishSLExit(ctx context.Context, order SLExitOrder) error

	// FIX F — persist trail state so a restart resumes it (see positions_persist.go).
	PersistPositionOpen(ctx context.Context, order ManthanOrder) error
	PersistTrail(ctx context.Context, strategyID string, pos types.Position) error
	PersistExit(ctx context.Context, strategyID, symbol string, exitPrice, pnl float64) error
}

// ConsumerConfig configures the manthan signal consumer.
type ConsumerConfig struct {
	KafkaBrokers []string
	Topic        string // default: manthan.signals
	GroupID      string // default: rule-engine-manthan
}

// NewConsumer creates a new manthan signal consumer.
//
// signalsDB points at the Postgres `manthan_signals` table (signals_db DB) —
// used for startup warm-up and as a fallback for catch-up when Redis is cold.
// May be nil: consumer still works, but warm-up/fallback are no-ops.
func NewConsumer(
	cfg ConsumerConfig,
	rdb *redis.Client,
	signalsDB *sql.DB,
	ltpFeed *LTPFeed,
	allocator *Allocator,
	orderGen *OrderGenerator,
	portfolioMgr *PortfolioManager,
	slMgr *TrailingSLManager,
	publisher OrderPublisher,
	strategyFn func() []types.UserStrategy,
	emaFn func() map[string]float64,
	logger *zap.Logger,
) *Consumer {
	if cfg.Topic == "" {
		cfg.Topic = "manthan.signals"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "rule-engine-manthan"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.KafkaBrokers,
		Topic:    cfg.Topic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})

	c := &Consumer{
		reader:       reader,
		rdb:          rdb,
		signalsDB:    signalsDB,
		allocator:    allocator,
		orderGen:     orderGen,
		portfolioMgr: portfolioMgr,
		slMgr:        slMgr,
		publisher:    publisher,
		strategyFn:   strategyFn,
		emaFn:        emaFn,
		logger:       logger,
	}
	if ltpFeed != nil {
		c.ltpFeed.Store(ltpFeed)
	}
	return c
}

// Start begins consuming manthan signals. Blocks until ctx is cancelled.
//
// Signal storage strategy:
//   - Redis: manthan:signals:{date} → SET of signal JSONs (per-day bucket)
//   - Redis: manthan:signals:dates  → SET of all dates that have signals
//   - DB:    manthan_signals table  → permanent record (already written by data-ingestion)
//
// Each strategy only receives signals from its creation date onwards:
//   - New signal arrives → stored in Redis by date → processed for all existing strategies
//   - New strategy created → CatchUpNewStrategy replays today's signals from Redis
//   - Next day → new signals allocated to all strategies (including yesterday's)
func (c *Consumer) Start(ctx context.Context) {
	c.logger.Info("Manthan signal consumer started (manual-commit mode)",
		zap.String("topic", c.reader.Config().Topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				c.logger.Info("Manthan consumer shutting down")
				return
			}
			c.logger.Error("Failed to read manthan signal", zap.Error(err))
			continue
		}

		var signal types.ManthanSignal
		if err := json.Unmarshal(msg.Value, &signal); err != nil {
			// Poison message — can never parse. Commit and skip so it doesn't
			// block the partition.
			c.logger.Warn("Failed to unmarshal manthan signal — dropping",
				zap.Error(err), zap.String("raw", string(msg.Value)))
			if cmErr := c.reader.CommitMessages(ctx, msg); cmErr != nil {
				c.logger.Warn("Commit (poison drop) failed", zap.Error(cmErr))
			}
			continue
		}

		// storeSignal writes the Redis cache + manthan_signals (already
		// written by data-ingestion is the authoritative source); processSignal
		// runs the allocator and publishes entry orders. Either step failing
		// would normally leak the message via ReadMessage's auto-commit.
		// With FetchMessage we hold the offset until both succeed (or we
		// decide the failure is non-retryable).
		c.storeSignal(ctx, signal)
		c.processSignal(ctx, signal)

		// Commit AFTER processing. `processSignal` already logs internally
		// for every skip/error path; if the allocator/publisher hits a
		// transient Kafka issue, processSignal swallows it (best-effort
		// publish). The DB-authoritative `manthan_signals` table means a
		// restart-time WarmUpCache will re-process today's signals anyway,
		// so we always commit here — the partition shouldn't get stuck on
		// a single signal that can't be allocated.
		if cmErr := c.reader.CommitMessages(ctx, msg); cmErr != nil {
			c.logger.Warn("Manthan signal commit failed (redelivery on restart is safe)",
				zap.String("symbol", signal.Symbol), zap.Error(cmErr))
		}
	}
}

// CatchUpNewStrategy replays today's signals for a newly created strategy.
// Called when CONFIG_CREATED arrives for a MANTHAN strategy.
// Strategy only gets signals from TODAY (its creation date) — not historical.
//
// Reads Redis first (fast path); if cold, falls back to the authoritative
// Postgres `manthan_signals` table and repopulates Redis for future lookups.
func (c *Consumer) CatchUpNewStrategy(ctx context.Context, strategy types.UserStrategy) {
	if c.rdb == nil {
		return
	}

	today := todayIST()
	signals := c.loadSignalsForDate(ctx, today)
	if len(signals) == 0 {
		dbSignals, err := c.loadSignalsFromDB(ctx, today)
		if err != nil {
			c.logger.Warn("Catch-up: DB fallback query failed",
				zap.String("user", strategy.UserID),
				zap.String("date", today), zap.Error(err))
		}
		if len(dbSignals) == 0 {
			c.logger.Info("No signals today for catch-up",
				zap.String("user", strategy.UserID),
				zap.String("date", today))
			return
		}
		c.logger.Info("Catch-up: Redis cold, rehydrated from DB",
			zap.String("user", strategy.UserID),
			zap.String("date", today),
			zap.Int("signals", len(dbSignals)))
		for _, sig := range dbSignals {
			c.storeSignal(ctx, sig)
		}
		signals = dbSignals
	}

	c.logger.Info("Catching up new MANTHAN strategy with today's signals",
		zap.String("user", strategy.UserID),
		zap.String("strategy", strategy.StrategyID),
		zap.Int("signals", len(signals)))

	// Fetch live LTP for each from external Redis (production feed)
	for i := range signals {
		if ltp, ok := c.getLiveLTP(ctx, signals[i].ISIN, signals[i].Symbol); ok {
			c.logger.Info("Catch-up using live LTP",
				zap.String("symbol", signals[i].Symbol),
				zap.Float64("sheet", signals[i].LatestPrice),
				zap.Float64("live", ltp))
			signals[i].LatestPrice = ltp
		}
	}

	portfolio := c.portfolioMgr.GetOrCreate(strategy)
	emaByIndex := c.emaFn()

	result := c.allocator.Allocate(signals, portfolio, emaByIndex)

	// Always log skip reasons so we never silently drop a signal on catch-up.
	// This is the single biggest source of "why didn't it buy?" debugging time.
	for _, skip := range result.Skipped {
		c.logger.Info("Catch-up skipped signal",
			zap.String("user", strategy.UserID),
			zap.String("strategy", strategy.StrategyID),
			zap.String("symbol", skip.Symbol),
			zap.String("reason", skip.Reason))
	}

	if len(result.Allocations) == 0 {
		c.logger.Info("No allocations from catch-up",
			zap.String("user", strategy.UserID),
			zap.Int("signals_considered", len(signals)),
			zap.Int("skipped", len(result.Skipped)))
		return
	}

	orders := c.orderGen.GenerateEntryOrders(strategy, result.Allocations)
	for i, order := range orders {
		if err := c.publisher.PublishEntryOrder(ctx, order); err != nil {
			c.logger.Error("Catch-up order publish failed",
				zap.String("symbol", order.Symbol), zap.Error(err))
			continue
		}
		c.portfolioMgr.AddPosition(strategy.StrategyID, result.Allocations[i])
		// FIX F: persist for restart-recovery of the trail (best-effort, logged inside).
		_ = c.publisher.PersistPositionOpen(ctx, order)
		c.logger.Info("Catch-up entry order published",
			zap.String("user", strategy.UserID),
			zap.String("symbol", order.Symbol),
			zap.Int32("qty", order.Quantity))
	}
}

// storeSignal saves a signal to Redis keyed by date.
// Deduplicates by symbol within each date bucket.
func (c *Consumer) storeSignal(ctx context.Context, sig types.ManthanSignal) {
	if c.rdb == nil {
		return
	}

	date := sig.RunDate
	if date == "" {
		date = todayIST()
	}
	dateKey := "manthan:signals:" + date

	// Remove old entry for same symbol (update with fresh data).
	existing, err := c.rdb.SMembers(ctx, dateKey).Result()
	if err != nil {
		c.logger.Warn("storeSignal: Redis SMembers failed",
			zap.String("key", dateKey), zap.Error(err))
	}
	for _, raw := range existing {
		var old types.ManthanSignal
		if json.Unmarshal([]byte(raw), &old) == nil && old.Symbol == sig.Symbol {
			if err := c.rdb.SRem(ctx, dateKey, raw).Err(); err != nil {
				c.logger.Warn("storeSignal: Redis SRem failed",
					zap.String("key", dateKey), zap.String("symbol", sig.Symbol), zap.Error(err))
			}
			break
		}
	}

	body, _ := json.Marshal(sig)
	if err := c.rdb.SAdd(ctx, dateKey, string(body)).Err(); err != nil {
		c.logger.Warn("storeSignal: Redis SAdd (date bucket) failed",
			zap.String("key", dateKey), zap.String("symbol", sig.Symbol), zap.Error(err))
	}
	if err := c.rdb.SAdd(ctx, "manthan:signals:dates", date).Err(); err != nil {
		c.logger.Warn("storeSignal: Redis SAdd (dates index) failed",
			zap.String("date", date), zap.Error(err))
	}
}

// loadSignalsForDate reads all signals cached for a specific date.
func (c *Consumer) loadSignalsForDate(ctx context.Context, date string) []types.ManthanSignal {
	if c.rdb == nil {
		return nil
	}
	members, err := c.rdb.SMembers(ctx, "manthan:signals:"+date).Result()
	if err != nil {
		return nil
	}
	var signals []types.ManthanSignal
	for _, raw := range members {
		var sig types.ManthanSignal
		if json.Unmarshal([]byte(raw), &sig) == nil {
			signals = append(signals, sig)
		}
	}
	return signals
}

// loadSignalsFromDB reads signals for a given date from the authoritative
// Postgres `manthan_signals` table (lives in signals_db DB).
// Used for startup warm-up and as a fallback when Redis cache is cold.
func (c *Consumer) loadSignalsFromDB(ctx context.Context, date string) ([]types.ManthanSignal, error) {
	if c.signalsDB == nil {
		return nil, nil
	}
	rows, err := c.signalsDB.QueryContext(ctx, `
		SELECT run_date, symbol, COALESCE(isin,''), COALESCE(industry,''),
		       COALESCE(mcap_bucket,''), COALESCE(index_name,''),
		       COALESCE(market_cap,0), COALESCE(pe,0), COALESCE(fscore,0),
		       COALESCE(pat,0), COALESCE(latest_price,0),
		       COALESCE(ath_close,0), COALESCE(week52_high,0), created_at
		FROM manthan_signals
		WHERE run_date = $1
		ORDER BY symbol
	`, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var signals []types.ManthanSignal
	for rows.Next() {
		var sig types.ManthanSignal
		var runDate, createdAt time.Time
		if err := rows.Scan(
			&runDate, &sig.Symbol, &sig.ISIN, &sig.Industry,
			&sig.MCapBucket, &sig.IndexName,
			&sig.MarketCap, &sig.PE, &sig.FScore,
			&sig.PAT, &sig.LatestPrice,
			&sig.ATHClose, &sig.Week52High, &createdAt,
		); err != nil {
			c.logger.Warn("Failed to scan manthan_signals row", zap.Error(err))
			continue
		}
		sig.RunDate = runDate.Format("2006-01-02")
		sig.EmittedAt = createdAt.UTC().Format(time.RFC3339)
		signals = append(signals, sig)
	}
	return signals, rows.Err()
}

// WarmUpCache loads today's signals from the authoritative DB and populates
// the Redis cache. Called once at startup so catch-up always finds a warm cache
// when the DB has data — survives Redis flushes, service restarts, or missed
// Kafka messages (offsets committed by a prior crashed run).
func (c *Consumer) WarmUpCache(ctx context.Context) {
	if c.rdb == nil || c.signalsDB == nil {
		return
	}
	today := todayIST()
	signals, err := c.loadSignalsFromDB(ctx, today)
	if err != nil {
		c.logger.Warn("Warm-up: DB query failed",
			zap.String("date", today), zap.Error(err))
		return
	}
	if len(signals) == 0 {
		c.logger.Info("Warm-up: no signals in DB for today",
			zap.String("date", today))
		return
	}
	for _, sig := range signals {
		c.storeSignal(ctx, sig)
	}
	c.logger.Info("Warm-up: Redis cache populated from DB",
		zap.String("date", today),
		zap.Int("signals", len(signals)))
}

func todayIST() string {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	if loc == nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}
	return time.Now().In(loc).Format("2006-01-02")
}

// SetLTPFeed sets the LTP feed after construction (needed because of init order).
func (c *Consumer) SetLTPFeed(feed *LTPFeed) {
	// Atomic Store: safe to call after Start. The next getLiveLTP from the
	// consumer goroutine picks up the new feed without a torn-read.
	c.ltpFeed.Store(feed)
}

// Stop closes the Kafka reader.
func (c *Consumer) Stop() error {
	return c.reader.Close()
}

// fetchLiveLTP reads LTP from Redis. The WebSocket monitor stores it at
// market:nse:{token} as JSON with an "ltp" field. We also try the symbol-based
// key format used by some feeds.
func (c *Consumer) fetchLiveLTP(ctx context.Context, symbol string) (float64, bool) {
	if c.rdb == nil {
		return 0, false
	}

	// Try symbol-based key first (populated by some feeds)
	for _, pattern := range []string{
		"market:nse:" + symbol,
		"ltp:NSE:" + symbol,
	} {
		val, err := c.rdb.Get(ctx, pattern).Result()
		if err != nil {
			continue
		}
		// Try direct float parse
		if f, err := parseJSONLTP(val); err == nil && f > 0 {
			return f, true
		}
	}
	return 0, false
}

// getLiveLTP tries external Redis (production feed) first, falls back to local Redis.
func (c *Consumer) getLiveLTP(ctx context.Context, isin, symbol string) (float64, bool) {
	if feed := c.ltpFeed.Load(); feed != nil && isin != "" {
		if ltp, ok := feed.FetchLiveLTP(ctx, isin); ok {
			return ltp, true
		}
	}
	if ltp, ok := c.fetchLiveLTP(ctx, symbol); ok {
		return ltp, true
	}
	return 0, false
}

func parseJSONLTP(raw string) (float64, error) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &data); err != nil {
		return 0, err
	}
	if ltp, ok := data["ltp"]; ok {
		switch v := ltp.(type) {
		case float64:
			return v, nil
		case string:
			var f float64
			err := json.Unmarshal([]byte(v), &f)
			return f, err
		}
	}
	return 0, nil
}

func (c *Consumer) processSignal(ctx context.Context, signal types.ManthanSignal) {
	strategies := c.strategyFn()
	if len(strategies) == 0 {
		return
	}

	// Override sheet price with live LTP (external Redis → local Redis → sheet fallback)
	if ltp, ok := c.getLiveLTP(ctx, signal.ISIN, signal.Symbol); ok {
		c.logger.Info("Using live LTP for allocation",
			zap.String("symbol", signal.Symbol),
			zap.Float64("sheet_price", signal.LatestPrice),
			zap.Float64("live_ltp", ltp))
		signal.LatestPrice = ltp
	} else {
		c.logger.Warn("No live LTP available — using sheet price",
			zap.String("symbol", signal.Symbol),
			zap.Float64("sheet_price", signal.LatestPrice))
	}

	emaByIndex := c.emaFn()

	for _, strategy := range strategies {
		portfolio := c.portfolioMgr.GetOrCreate(strategy)

		result := c.allocator.Allocate(
			[]types.ManthanSignal{signal},
			portfolio,
			emaByIndex,
		)

		for _, skip := range result.Skipped {
			c.logger.Info("Signal skipped",
				zap.String("user", strategy.UserID),
				zap.String("symbol", skip.Symbol),
				zap.String("reason", skip.Reason),
			)
		}

		if len(result.Allocations) == 0 {
			continue
		}

		orders := c.orderGen.GenerateEntryOrders(strategy, result.Allocations)
		published := 0
		for i, order := range orders {
			if err := c.publisher.PublishEntryOrder(ctx, order); err != nil {
				c.logger.Error("Failed to publish entry order",
					zap.String("symbol", order.Symbol),
					zap.String("user", order.UserID),
					zap.Error(err),
				)
				continue
			}

			c.portfolioMgr.AddPosition(
				strategy.StrategyID,
				result.Allocations[i],
			)
			// FIX F: mirror the in-memory add to manthan_positions so a restart
			// resumes this position's trail. Best-effort (logged inside).
			_ = c.publisher.PersistPositionOpen(ctx, order)
			published++

			c.logger.Info("Manthan entry order published",
				zap.String("user", strategy.UserID),
				zap.String("symbol", order.Symbol),
				zap.Int32("qty", order.Quantity),
				zap.Float64("price", order.EntryPrice),
				zap.Float64("sl", order.StopLoss),
				zap.String("mode", strategy.TradingMode),
			)
		}

		// Update portfolio state in DB + Redis after all entries
		if published > 0 {
			if mp, ok := c.publisher.(*ManthanPublisher); ok {
				mp.UpdatePortfolioState(ctx, portfolio)
			}
		}
	}
}
