package backfill

import (
	"context"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/holiday"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/repository"
	"go.uber.org/zap"
)

// SignalSourceBackfillAMN is stamped on every OrderRequest produced by the
// after-market-news backfill. trade-execution and the UI key off this exact
// value to distinguish backfilled orders from live-news orders.
const SignalSourceBackfillAMN = "BACKFILL_AMN"

// MatchDispatcher sends one matched historical news event through the existing
// order-build / risk-check / publish pipeline. Implemented by consumer.Handler
// (DispatchBackfillMatch). It resolves live market data, builds the order,
// stamps OrderRequest.SignalSource = signalSource, and publishes it.
//
// A nil error with no order placed is allowed (e.g. thinly-traded stock with
// no live LTP) — the dispatcher logs and skips such cases internally.
type MatchDispatcher interface {
	DispatchBackfillMatch(
		ctx context.Context,
		event *models.MarketEvent,
		strategy *models.Strategy,
		result *matcher.EvaluationResult,
		signalSource string,
	) error
}

// StrategyLookup resolves a strategy by identity. Satisfied by
// configstore.ConfigStore. Used by startup recovery to rebuild the strategy
// for a PENDING job (the job row stores only identity + window).
type StrategyLookup interface {
	GetStrategy(userID, strategyID string) (*models.Strategy, bool)
}

// Config bundles the BackfillService dependencies.
type Config struct {
	NewsRepo   *repository.MongoNewsRepository
	JobStore   *JobStore
	Evaluator  *matcher.Evaluator
	Dispatcher MatchDispatcher
	Strategies StrategyLookup
	Holidays   *holiday.Checker
	Timezone   *time.Location
	Logger     *zap.Logger
}

// Service orchestrates the after-market-news backfill.
//
// Concurrency model:
//   - Run is non-blocking — each CONFIG_CREATED is handled on its own
//     goroutine so the single-threaded config consumer never stalls and
//     multiple users' strategies backfill in parallel.
//   - JobStore.Claim (INSERT ON CONFLICT) is the cross-process / cross-replay
//     mutex: exactly one worker ever owns a strategy's backfill.
//   - inflight is a cheap in-process guard so a duplicate CONFIG_CREATED
//     doesn't even reach the DB twice.
//   - Per-strategy, matches dispatch sequentially in news-time order so the
//     order pipeline's per-stock daily lock and per-strategy trade cap behave
//     exactly as they do for live news.
type Service struct {
	cfg Config

	mu       sync.Mutex
	inflight map[string]struct{}
}

// New builds a Service. Timezone defaults to IST when nil; Holidays may be nil
// (weekend-only trading-day detection).
func New(cfg Config) *Service {
	if cfg.Timezone == nil {
		cfg.Timezone = time.FixedZone("IST", 5*60*60+30*60)
	}
	return &Service{cfg: cfg, inflight: make(map[string]struct{})}
}

// Run launches the backfill for a freshly created strategy. Non-blocking:
// returns immediately, work proceeds on a background goroutine.
//
// `ctx` should be a long-lived context (not a per-message one) — a deferred
// backfill may need to wait until the next 09:15 IST. If the process dies
// before then the job stays PENDING and RecoverPending re-schedules it on the
// next boot, so no orders are lost.
func (s *Service) Run(ctx context.Context, strategy *models.Strategy) {
	if s == nil || strategy == nil || !strategy.ProcessAfterMarketNews {
		return
	}
	go s.runForNewStrategy(ctx, strategy)
}

func (s *Service) runForNewStrategy(ctx context.Context, strategy *models.Strategy) {
	log := s.logger(strategy.UserID, strategy.StrategyID, strategy.StrategyName)

	if !s.acquire(strategy.StrategyID) {
		log.Info("backfill: already in progress in this process; skipping duplicate")
		return
	}
	defer s.release(strategy.StrategyID)

	now := time.Now().In(s.cfg.Timezone)
	w := ComputeWindow(now, s.cfg.Timezone, s.cfg.Holidays)

	job := Job{
		StrategyID:    strategy.StrategyID,
		UserID:        strategy.UserID,
		WindowStart:   w.Start,
		WindowEnd:     w.End,
		DispatchAfter: w.DispatchAfter,
	}
	claimed, err := s.cfg.JobStore.Claim(ctx, job)
	if err != nil {
		log.Error("backfill: claim failed; skipping", zap.Error(err))
		return
	}
	if !claimed {
		// Another worker, an earlier run, or startup recovery owns it.
		log.Info("backfill: not claimed (already owned elsewhere); skipping")
		return
	}

	log.Info("backfill: claimed",
		zap.Time("window_start", w.Start),
		zap.Time("window_end", w.End),
		zap.Time("dispatch_after", w.DispatchAfter),
		zap.Bool("immediate", w.Immediate(now)))

	s.schedule(ctx, strategy, w)
}

// RecoverPending re-drives every PENDING backfill_jobs row. Call once on
// startup, after the config store has been bootstrapped (so strategies can be
// resolved). It recovers:
//   - backfills whose dispatch was deferred to a future 09:15 IST, and
//   - backfills interrupted mid-run by a crash/restart.
func (s *Service) RecoverPending(ctx context.Context) {
	if s == nil {
		return
	}
	jobs, err := s.cfg.JobStore.ListPending(ctx)
	if err != nil {
		s.cfg.Logger.Error("backfill recovery: list pending failed", zap.Error(err))
		return
	}
	if len(jobs) == 0 {
		s.cfg.Logger.Info("backfill recovery: no pending jobs")
		return
	}
	s.cfg.Logger.Info("backfill recovery: rescheduling pending jobs", zap.Int("count", len(jobs)))

	for _, job := range jobs {
		log := s.logger(job.UserID, job.StrategyID, "")

		strategy, ok := s.cfg.Strategies.GetStrategy(job.UserID, job.StrategyID)
		if !ok || strategy == nil {
			// Strategy gone (deleted) — nothing to dispatch against.
			log.Warn("backfill recovery: strategy not found; marking job FAILED")
			if err := s.cfg.JobStore.MarkFailed(ctx, job.StrategyID, "strategy not found at recovery"); err != nil {
				log.Error("backfill recovery: mark failed errored", zap.Error(err))
			}
			continue
		}
		if !s.acquire(job.StrategyID) {
			continue // already running in this process
		}
		w := Window{Start: job.WindowStart, End: job.WindowEnd, DispatchAfter: job.DispatchAfter}
		st := strategy
		go func(strat *models.Strategy, win Window, sid string) {
			defer s.release(sid)
			s.schedule(ctx, strat, win)
		}(st, w, job.StrategyID)
	}
}

// schedule dispatches the backfill immediately or, when the window defers it,
// waits until DispatchAfter (respecting ctx cancellation) and dispatches then.
func (s *Service) schedule(ctx context.Context, strategy *models.Strategy, w Window) {
	log := s.logger(strategy.UserID, strategy.StrategyID, strategy.StrategyName)
	now := time.Now().In(s.cfg.Timezone)

	if w.Immediate(now) {
		s.scanAndDispatch(ctx, strategy, w)
		return
	}

	wait := w.DispatchAfter.Sub(now)
	log.Info("backfill: dispatch deferred", zap.Duration("wait", wait), zap.Time("dispatch_after", w.DispatchAfter))

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		// Shutdown before the dispatch window. Job stays PENDING and is
		// recovered by RecoverPending on the next boot — nothing is lost.
		log.Warn("backfill: shutdown before deferred dispatch; job left PENDING for recovery")
		return
	case <-timer.C:
		log.Info("backfill: deferred dispatch window reached")
		s.scanAndDispatch(ctx, strategy, w)
	}
}

// scanAndDispatch streams the window's news from MongoDB, evaluates each event
// against the strategy, dispatches matches, and finalizes the job row.
//
// Re-scanning at dispatch time (rather than caching matches in memory at claim
// time) is deliberate: it keeps zero in-memory state across the possibly
// multi-hour defer, and it naturally picks up any news that landed late with
// a back-dated dt_tm inside the window.
func (s *Service) scanAndDispatch(ctx context.Context, strategy *models.Strategy, w Window) {
	log := s.logger(strategy.UserID, strategy.StrategyID, strategy.StrategyName)

	if !strategy.Active {
		log.Info("backfill: strategy inactive at dispatch time; completing with no orders")
		_ = s.cfg.JobStore.MarkCompleted(ctx, strategy.StrategyID, 0, 0)
		return
	}

	start := time.Now()

	// Load CompanyMaster once per scan. Raw NewsImpactDashboard docs carry only
	// an ISIN — codes / exchange / market-cap come from this join, exactly as
	// data-ingestion does for live news.
	companies, err := s.cfg.NewsRepo.LoadCompanyMaster(ctx)
	if err != nil {
		log.Error("backfill: failed to load company master; marking job FAILED", zap.Error(err))
		if mErr := s.cfg.JobStore.MarkFailed(ctx, strategy.StrategyID, "company master load: "+err.Error()); mErr != nil {
			log.Error("backfill: mark failed errored", zap.Error(mErr))
		}
		return
	}
	log.Info("backfill: company master loaded", zap.Int("companies", len(companies)))

	var matches, dispatched, skippedNoCompany, skippedInvalid int

	scanned, err := s.cfg.NewsRepo.FindInRange(ctx, w.Start, w.End, func(doc *models.MongoDBEvent) error {
		// Enrich the raw doc with stock identity (exchange / codes / mcap).
		if !repository.EnrichWithCompany(doc, companies) {
			skippedNoCompany++
			return nil // ISIN unknown or company inactive — not tradable
		}
		event, convErr := doc.ToMarketEvent()
		if convErr != nil {
			skippedInvalid++
			return nil // skip un-convertible document
		}
		if event.Validate() != nil {
			skippedInvalid++
			return nil // skip invalid event (e.g. no stock code)
		}
		result := s.cfg.Evaluator.Evaluate(event, strategy)
		if result == nil || !result.IsFullMatch() {
			return nil
		}
		matches++
		if dErr := s.cfg.Dispatcher.DispatchBackfillMatch(
			ctx, event, strategy, result, SignalSourceBackfillAMN,
		); dErr != nil {
			// One bad dispatch must not abort the rest of the backfill.
			log.Error("backfill: dispatch failed for match",
				zap.String("event_id", event.EventID),
				zap.Int64("stock_code", event.StockData.StockCode),
				zap.Error(dErr))
			return nil
		}
		dispatched++
		return nil
	})

	dur := time.Since(start)
	if err != nil {
		log.Error("backfill: news scan failed; marking job FAILED",
			zap.Error(err), zap.Duration("duration", dur))
		if mErr := s.cfg.JobStore.MarkFailed(ctx, strategy.StrategyID, err.Error()); mErr != nil {
			log.Error("backfill: mark failed errored", zap.Error(mErr))
		}
		return
	}

	log.Info("backfill: completed",
		zap.Int("documents_scanned", scanned),
		zap.Int("skipped_no_company", skippedNoCompany),
		zap.Int("skipped_invalid", skippedInvalid),
		zap.Int("matches", matches),
		zap.Int("orders_dispatched", dispatched),
		zap.Duration("duration", dur))

	if mErr := s.cfg.JobStore.MarkCompleted(ctx, strategy.StrategyID, matches, dispatched); mErr != nil {
		log.Error("backfill: mark completed errored", zap.Error(mErr))
	}
}

func (s *Service) acquire(strategyID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inflight[strategyID]; ok {
		return false
	}
	s.inflight[strategyID] = struct{}{}
	return true
}

func (s *Service) release(strategyID string) {
	s.mu.Lock()
	delete(s.inflight, strategyID)
	s.mu.Unlock()
}

func (s *Service) logger(userID, strategyID, strategyName string) *zap.Logger {
	fields := []zap.Field{
		zap.String("user_id", userID),
		zap.String("strategy_id", strategyID),
	}
	if strategyName != "" {
		fields = append(fields, zap.String("strategy_name", strategyName))
	}
	return s.cfg.Logger.With(fields...)
}

// assert Service satisfies the trigger contract used by the config consumer.
var _ interface {
	Run(context.Context, *models.Strategy)
} = (*Service)(nil)
