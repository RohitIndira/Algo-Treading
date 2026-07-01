package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// Engine is the main hot-path evaluator.
// It provides:
//   - News dedup
//   - Worker pool fan-out
//   - Strict deterministic strategy evaluation order per event
//   - Fail-fast ordering semantics within an event (stop early on fatal errors)
type Engine struct {
	store      *configstore.ConfigStore
	evaluator  *matcher.Evaluator
	scorer     *matcher.Scorer
	workers    int
	logger     *zap.Logger
	dedup      *Deduper
	pool       *WorkerPool
	closeOnce  sync.Once
	closedChan chan struct{}
}

type Config struct {
	Workers          int
	DedupWindow      time.Duration
	DedupMaxEntries  int
	ProcessingTimout time.Duration
}

func New(store *configstore.ConfigStore, cfg Config, logger *zap.Logger) *Engine {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.DedupWindow <= 0 {
		cfg.DedupWindow = 2 * time.Minute
	}
	if cfg.DedupMaxEntries <= 0 {
		cfg.DedupMaxEntries = 100_000
	}

	e := &Engine{
		store:      store,
		evaluator:  matcher.NewEvaluator(logger),
		scorer:     matcher.NewScorer(),
		workers:    cfg.Workers,
		logger:     logger,
		dedup:      NewDeduper(cfg.DedupWindow, cfg.DedupMaxEntries),
		pool:       NewWorkerPool(cfg.Workers, cfg.Workers*16, logger),
		closedChan: make(chan struct{}),
	}
	return e
}

// Start starts the worker pool.
func (e *Engine) Start(ctx context.Context) {
	_ = ctx
}

func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		close(e.closedChan)
	})
}

// Shutdown drains pending jobs and waits for in-flight evaluations to finish.
// Call this AFTER stopping the news consumer (so no new jobs are submitted).
func (e *Engine) Shutdown() {
	// stop accepting work
	e.Close()
	// drain pool
	e.pool.Shutdown()
}

// EvaluateEvent evaluates one event against the current snapshot.
// It returns matches sorted in the same strict order as snapshot.AllActive.
func (e *Engine) EvaluateEvent(ctx context.Context, event *models.MarketEvent) ([]*models.RuleMatch, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}

	// News dedup (drop duplicates early, before any work)
	if !e.dedup.Allow(event.EventID) {
		return []*models.RuleMatch{}, nil
	}

	snap := e.store.GetSnapshot()
	strategies := snap.AllActive
	if len(strategies) == 0 {
		return []*models.RuleMatch{}, nil
	}

	matches := make([]*models.RuleMatch, len(strategies))

	var firstErr atomic.Value
	var wg sync.WaitGroup
	wg.Add(len(strategies))

	for i, s := range strategies {
		idx := i
		strat := s
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-e.closedChan:
			return nil, fmt.Errorf("engine closed")
		default:
		}
		err := e.pool.Submit(func() {
			defer wg.Done()
			m, err := e.evaluateOne(ctx, event, strat)
			if err != nil {
				// keep first error
				if firstErr.Load() == nil {
					firstErr.Store(err)
				}
				return
			}
			matches[idx] = m
		})
		if err != nil {
			wg.Done()
			return nil, err
		}
	}

	wg.Wait()

	out := make([]*models.RuleMatch, 0, len(strategies))
	for _, m := range matches {
		if m != nil {
			out = append(out, m)
		}
	}

	if v := firstErr.Load(); v != nil {
		return out, v.(error)
	}
	return out, nil
}

func (e *Engine) evaluateOne(ctx context.Context, event *models.MarketEvent, strategy *models.Strategy) (*models.RuleMatch, error) {
	if strategy == nil {
		return nil, nil
	}
	if !strategy.Active {
		return nil, nil
	}
	if err := strategy.Validate(); err != nil {
		// treat invalid strategy as non-fatal; skip.
		e.logger.Warn("Skipping invalid strategy", zap.String("strategy_id", strategy.StrategyID), zap.Error(err))
		return nil, nil
	}

	res := e.evaluator.Evaluate(event, strategy)
	// Exact match semantics: all evaluated conditions must pass.
	// We still compute score for observability, but it is NOT used to gate matches.
	if !res.IsFullMatch() {
		e.logger.Info("Strategy did not fully match event",
			zap.String("event_id", event.EventID),
			zap.String("user_id", strategy.UserID),
			zap.String("strategy_id", strategy.StrategyID),
			zap.String("strategy_name", strategy.StrategyName),
			zap.Strings("matched_conditions", res.MatchedConditions),
			zap.Strings("failed_conditions", res.FailedConditions),
		)
		return nil, nil
	}

	score := e.scorer.CalculateScore(res)

	match := &models.RuleMatch{
		UserID:            strategy.UserID,
		StrategyID:        strategy.StrategyID,
		StrategyName:      strategy.StrategyName,
		Strategy:          strategy,
		MatchScore:        score,
		MatchedConditions: res.MatchedConditions,
		FailedConditions:  res.FailedConditions,
		ApprovedByRisk:    false,
		Timestamp:         time.Now(),
		EventID:           event.EventID,
		PctChangeStatus:   string(res.PctChangeStatus),
	}
	_ = ctx
	return match, nil
}
