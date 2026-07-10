package manthan

// Wire — single entry point that constructs the entire Manthan stack from
// the dependencies main.go already has on hand. Returns a *Manthan whose
// Start launches the background goroutines and Close cleans them up.
//
// Before this file (2026-06-25), main.go held ~290 LOC of Manthan
// construction code inline. That made testing impossible and made the
// boot order hard to read. Wire collects every construction call, the
// closures over store/redis the consumer needs, and the start-up
// side-effects (WarmUpCache, RehydrateActivePositions, the
// SetOnManthanCreated callback wiring on the strategy-events consumer).

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// Deps is everything Wire needs that lives outside the manthan package.
// Anything Manthan-internal is constructed inside Wire.
type Deps struct {
	Logger *zap.Logger

	// Manthan portfolio DB (trading_db today, stockk_trading after Phase 3).
	ManthanDB *sql.DB

	// Manthan signals source — a separate PostgreSQL connection because
	// manthan_signals lives in market_data (today) / stockk_market (Phase 3).
	// Wire opens this connection itself; pass the postgres credentials.
	PostgresHost     string
	PostgresPort     string
	PostgresUser     string
	PostgresPassword string
	PostgresSSLMode  string
	SignalsDBName    string // env-driven, default "market_data"

	// Caches.
	Redis *cache.RedisCache

	// Kafka brokers — used by every Manthan publisher + consumer.
	KafkaBrokers []string

	// Strategy snapshot — read by the consumer to fan signals out across users.
	Store *configstore.ConfigStore

	// User-config gRPC client. Wire calls IsStrategyAlive on it during
	// orphan scanning + rehydration; only that one method is required so
	// we accept the narrow interface.
	AliveChecker StrategyAliveChecker

	// Strategy-events consumer wires its OnManthanCreated callback to our
	// CatchUpNewStrategy so a freshly-created strategy gets back-filled
	// with today's eligible signals immediately. Narrow interface keeps
	// this file independent of internal/kafka.
	ConfigConsumer ConfigConsumerCallbackSetter

	// Feature gates.
	DecisionLogEnabled   bool
	NotificationsEnabled bool

	// External Redis (Indira live LTP feed). Empty Addr disables the feed.
	ExtRedisAddr     string
	ExtRedisPassword string
}

// ConfigConsumerCallbackSetter is the slice of internal/kafka.ConfigConsumer
// we touch. Defined here so the manthan package doesn't import internal/kafka.
type ConfigConsumerCallbackSetter interface {
	SetOnManthanCreated(func(ctx context.Context, strategy *models.Strategy))
}

// Manthan is the wired stack. Start launches its goroutines, Close stops
// publishers + closes the per-Manthan signals DB connection Wire opened.
type Manthan struct {
	logger *zap.Logger

	// Components held for shutdown / debugging.
	publisher     *ManthanPublisher
	portfolioMgr  *PortfolioManager
	consumer      *Consumer
	ltpFeed       *LTPFeed // may be nil — external Redis was not configured
	signalsDB     *sql.DB  // opened by Wire; closed by Close
	manthanDB     *sql.DB  // borrowed — main.go owns the lifecycle
	aliveChecker  StrategyAliveChecker
	redis         *cache.RedisCache
	strategyByID  func(strategyID string) *types.UserStrategy
	getStrategies func() []types.UserStrategy

	decisionLogEnabled bool
}

// Wire builds the entire Manthan stack from Deps. Sync side-effects
// (WarmUpCache, RehydrateActivePositions, SetOnManthanCreated) all run
// before Wire returns so the system is ready to take signals as soon as
// Start is called.
//
// Wire does NOT start any goroutines — call Manthan.Start(ctx) for that.
// This separation lets callers do additional wiring between construction
// and "go live" (e.g. waiting for the strategy-events consumer to fully
// initialise before allowing catch-up callbacks to fire).
func Wire(ctx context.Context, deps Deps) (*Manthan, error) {
	if deps.Logger == nil {
		return nil, fmt.Errorf("Wire: Logger is required")
	}
	if deps.Redis == nil {
		return nil, fmt.Errorf("Wire: Redis is required")
	}
	if deps.Store == nil {
		return nil, fmt.Errorf("Wire: Store is required")
	}
	if deps.AliveChecker == nil {
		return nil, fmt.Errorf("Wire: AliveChecker is required")
	}
	if deps.ConfigConsumer == nil {
		return nil, fmt.Errorf("Wire: ConfigConsumer is required")
	}

	logger := deps.Logger
	m := &Manthan{
		logger:             logger,
		manthanDB:          deps.ManthanDB,
		redis:              deps.Redis,
		aliveChecker:       deps.AliveChecker,
		decisionLogEnabled: deps.DecisionLogEnabled,
	}

	// Publisher — trade-signals + portfolio.allocations + Postgres writes
	// for the CQRS decisions table when the flag is on.
	m.publisher = NewManthanPublisher(ManthanPublisherConfig{
		DB:                 deps.ManthanDB,
		Redis:              deps.Redis.Client(),
		KafkaBrokers:       deps.KafkaBrokers,
		DecisionLogEnabled: deps.DecisionLogEnabled,
	}, logger)

	// Signals DB — separate Postgres connection because manthan_signals
	// lives in a different DB from manthan_positions.
	signalsDBName := deps.SignalsDBName
	if signalsDBName == "" {
		signalsDBName = "market_data"
	}
	if deps.PostgresHost != "" {
		dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			deps.PostgresHost, deps.PostgresPort, deps.PostgresUser,
			deps.PostgresPassword, signalsDBName, deps.PostgresSSLMode)
		sdb, err := sql.Open("postgres", dsn)
		if err != nil {
			logger.Warn("Manthan signals DB open failed — warm-up/fallback disabled",
				zap.String("db", signalsDBName), zap.Error(err))
		} else {
			sdb.SetMaxOpenConns(5)
			sdb.SetMaxIdleConns(2)
			if err := sdb.Ping(); err != nil {
				logger.Warn("Manthan signals DB ping failed — warm-up/fallback disabled",
					zap.String("db", signalsDBName), zap.Error(err))
				_ = sdb.Close()
			} else {
				m.signalsDB = sdb
				logger.Info("Manthan signals DB connected", zap.String("db", signalsDBName))
			}
		}
	}

	// Stateless helpers.
	m.portfolioMgr = NewPortfolioManager(logger)
	allocator := NewAllocator(logger)
	allocator.SetDB(deps.ManthanDB)
	orderGen := NewOrderGenerator(logger)
	slMgr := NewTrailingSLManager(logger)

	// Closures over Store / Redis that the consumer + tick handler need.
	m.getStrategies = func() []types.UserStrategy {
		snap := deps.Store.GetSnapshot()
		var out []types.UserStrategy
		for _, uv := range snap.ByUser {
			for _, st := range uv.Active {
				if st.StrategyType != "MANTHAN" {
					continue
				}
				out = append(out, types.UserStrategy{
					StrategyID:    st.StrategyID,
					UserID:        st.UserID,
					StrategyName:  st.StrategyName,
					TradingMode:   st.TradingMode,
					TotalCapital:  st.TradeConfig.TotalCapital,
					MaxPositions:  st.TradeConfig.MaxPositions,
					PerStockBase:  st.TradeConfig.PerStockAmount,
					StopLossPct:   st.TradeConfig.StopLossPct,
					TrailingSLPct: st.TradeConfig.TrailingSLPct,
				})
			}
		}
		return out
	}
	m.strategyByID = func(sid string) *types.UserStrategy {
		for _, s := range m.getStrategies() {
			if s.StrategyID == sid {
				s := s
				return &s
			}
		}
		return nil
	}
	getEMAAllocations := func() map[string]float64 {
		fallback := map[string]float64{
			"NIFTY50":    0.30,
			"NFTYMCP150": 0.30,
			"NTYSLCP250": 0.30,
		}
		raw, err := deps.Redis.Client().Get(context.Background(), "manthan:ema:allocations").Result()
		if err != nil {
			logger.Warn("EMA allocations not in Redis — using fallback")
			return fallback
		}
		var ema map[string]float64
		if err := json.Unmarshal([]byte(raw), &ema); err != nil {
			logger.Warn("Failed to parse EMA allocations from Redis", zap.Error(err))
			return fallback
		}
		logger.Debug("EMA allocations loaded from Redis", zap.Any("ema", ema))
		return ema
	}

	// Consumer (manthan.signals topic). LTPFeed is set later — we have a
	// circular need (LTPFeed needs the tick handler, tick handler is built
	// after consumer, consumer wants LTPFeed). Same trick main.go used.
	m.consumer = NewConsumer(
		ConsumerConfig{
			KafkaBrokers: deps.KafkaBrokers,
			Topic:        "manthan.signals",
			GroupID:      "rule-engine-manthan",
		},
		deps.Redis.Client(),
		m.signalsDB,
		nil,
		allocator,
		orderGen,
		m.portfolioMgr,
		slMgr,
		m.publisher,
		m.getStrategies,
		getEMAAllocations,
		logger,
	)

	tickHandler := NewTickHandler(
		slMgr,
		m.portfolioMgr,
		orderGen,
		m.publisher,
		m.strategyByID,
		logger,
	)

	// Optional LTP feed.
	if deps.ExtRedisAddr != "" {
		feed, ltpErr := NewLTPFeed(LTPFeedConfig{
			Addr:         deps.ExtRedisAddr,
			Password:     deps.ExtRedisPassword,
			PollInterval: 1 * time.Second,
		}, tickHandler, m.portfolioMgr, logger)
		if ltpErr != nil {
			logger.Warn("External Redis LTP feed not available", zap.Error(ltpErr))
		} else {
			m.ltpFeed = feed
			m.consumer.SetLTPFeed(feed)
		}
	} else {
		logger.Warn("EXT_REDIS_ADDR not set — live LTP feed disabled, using sheet prices")
	}

	// Projector + fill_consumer + notifier removed 2026-07-10 as part of the
	// signal-engine-only refactor. Rules-engine no longer writes to
	// manthan_positions / manthan_position_events. Positions svc (to be built)
	// will own those writes; until then, positions/PnL/frontend state degrades.
	// See docs/rules_engine_refactor.md §4.5 for the ratified end state.

	// Sync warm-up — fill Redis cache from Postgres so newly-bootstrapped
	// strategies see today's signals without waiting for the next sheet
	// refresh.
	m.consumer.WarmUpCache(ctx)

	// Rehydrate ACTIVE positions from Postgres (source of truth) into the
	// in-memory portfolio + Redis. Without this, a restart loses every
	// live position and the LTP feed has nothing to trail.
	if restored, orphans, stale, err := m.portfolioMgr.RehydrateActivePositions(
		ctx, deps.ManthanDB, deps.Redis.Client(), deps.AliveChecker, m.strategyByID,
	); err != nil {
		// Error-level (not Warn): a failed rehydrate means live broker
		// positions exist with no in-memory TSL state. The trail handler
		// will never tick them and the orphan scanner won't see them,
		// leaving them effectively unmanaged until the next clean boot.
		// We continue boot rather than fail-fast because the positions
		// at the broker remain regardless — but operators MUST see this.
		logger.Error("Manthan rehydration failed — live positions are unmanaged until next clean boot",
			zap.Error(err))
	} else {
		logger.Info("Manthan positions rehydrated from DB",
			zap.Int("restored", restored),
			zap.Int("orphans_cleaned", orphans),
			zap.Int("redis_stale_evicted", stale))
	}

	// Wire the strategy-events catch-up callback. A freshly created
	// MANTHAN strategy is back-filled with today's eligible signals
	// immediately via the consumer.
	deps.ConfigConsumer.SetOnManthanCreated(func(ctx context.Context, strategy *models.Strategy) {
		us := types.UserStrategy{
			StrategyID:    strategy.StrategyID,
			UserID:        strategy.UserID,
			StrategyName:  strategy.StrategyName,
			TradingMode:   strategy.TradingMode,
			TotalCapital:  strategy.TradeConfig.TotalCapital,
			MaxPositions:  strategy.TradeConfig.MaxPositions,
			PerStockBase:  strategy.TradeConfig.PerStockAmount,
			StopLossPct:   strategy.TradeConfig.StopLossPct,
			TrailingSLPct: strategy.TradeConfig.TrailingSLPct,
		}
		m.consumer.CatchUpNewStrategy(ctx, us)
	})

	return m, nil
}

// Start launches every Manthan background goroutine bound to ctx. Returns
// immediately — the goroutines exit when ctx is cancelled.
func (m *Manthan) Start(ctx context.Context) {
	// Periodic orphan scanner — flips ACTIVE positions to EXITED when the
	// owning strategy is soft-deleted while live. Data-integrity only;
	// never sells.
	go m.portfolioMgr.StartOrphanScanner(ctx, m.manthanDB, m.redis.Client(),
		m.aliveChecker, m.strategyByID, 60*time.Second)

	// Signal consumer.
	go func() {
		m.consumer.Start(ctx)
	}()
	m.logger.Info("Manthan signal consumer started")

	// Fill consumer + stale-decision reaper removed 2026-07-10. Both depended
	// on the projector, which is gone. Signal decisions stay at PROPOSED /
	// DISPATCHED forever until positions svc takes over the outcome updates.
	// See docs/rules_engine_refactor.md §4.5.

	// LTP feed poller — drives trailing SL.
	if m.ltpFeed != nil {
		go func() {
			m.ltpFeed.Start(ctx)
		}()
		m.logger.Info("Manthan LTP feed started — trailing SL active")
	}
}

// Close stops publishers and frees the signals DB connection Wire opened.
// manthanDB is owned by the caller and NOT closed here.
func (m *Manthan) Close() {
	if m.ltpFeed != nil {
		m.ltpFeed.Close()
	}
	if m.publisher != nil {
		m.publisher.Close()
	}
	if m.signalsDB != nil {
		_ = m.signalsDB.Close()
	}
}
