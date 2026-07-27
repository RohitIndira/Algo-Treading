// Package decisions persists a durable audit trail of every strategy-evaluation
// decision — both the signals that were published and, more importantly, the
// ones that were rejected and why.
//
// Before this package the only trace of a rejection was a log line, which made
// "why didn't my strategy fire on that news?" answerable only by grepping pm2
// logs. Those logs are also masked in some deployments (STRATEGY_NAME_LOG_OVERRIDE
// strips strategy_id), so the question was often unanswerable at all.
//
// # Hot-path safety
//
// News evaluation is latency-sensitive (e2e_ms is tracked per signal and runs in
// the single-digit milliseconds). Recording must therefore never block, never
// add a database round-trip to the caller, and never propagate an error upward.
// Record() is a non-blocking send onto a buffered channel; when that buffer is
// full the record is dropped and a counter incremented, in preference to slowing
// down order placement. A background goroutine batches the buffer into multi-row
// INSERTs. Every method is nil-safe so callers can hold a nil *Recorder when the
// feature is disabled and skip the nil checks entirely.
package decisions

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

// Outcome is the verdict for one (event, strategy) pair.
type Outcome string

const (
	// OutcomeMatched — the strategy matched and an order was published.
	OutcomeMatched Outcome = "MATCHED"
	// OutcomeRejected — the event was dropped for this strategy. ReasonCode says why.
	OutcomeRejected Outcome = "REJECTED"
)

// Stage identifies which part of the pipeline made the decision. Kept coarse so
// the admin UI can group by it, with ReasonCode carrying the detail.
type Stage string

const (
	// StageMatch — the condition evaluator (impact, sentiment, category, …).
	StageMatch Stage = "MATCH"
	// StageCompliance — post-match guards: banned token, qty/value/exposure caps,
	// DPR bands, price velocity.
	StageCompliance Stage = "COMPLIANCE"
	// StageSizing — amount-based position sizing could not produce a valid quantity.
	StageSizing Stage = "SIZING"
	// StageRisk — the risk-management service verdict.
	StageRisk Stage = "RISK"
	// StageCap — per-strategy daily trade cap or duplicate-stock guard.
	StageCap Stage = "CAP"
	// StageWindow — market hours / trade window / pct-change band.
	StageWindow Stage = "WINDOW"
)

// Reason codes. These are the fixed vocabulary the admin UI filters on, so they
// must stay stable — add new codes rather than renaming existing ones.
const (
	ReasonConditionsNotMet   = "CONDITIONS_NOT_MET"
	ReasonBannedToken        = "BANNED_TOKEN"
	ReasonQtyLimit           = "QTY_LIMIT_EXCEEDED"
	ReasonOrderValueLimit    = "ORDER_VALUE_LIMIT_EXCEEDED"
	ReasonExposureLimit      = "EXPOSURE_LIMIT_EXCEEDED"
	ReasonDPRLower           = "DPR_LOWER_BREACH"
	ReasonDPRUpper           = "DPR_UPPER_BREACH"
	ReasonVelocity           = "VELOCITY_BREACH"
	ReasonPctChangeAboveMax  = "PCT_CHANGE_ABOVE_MAX"
	ReasonBudgetTooSmall     = "BUDGET_TOO_SMALL"
	ReasonInvalidSizingPrice = "INVALID_SIZING_PRICE"
	ReasonDuplicateStock     = "DUPLICATE_STOCK_TODAY"
	ReasonStrategyCapReached = "STRATEGY_CAP_REACHED"
	ReasonRiskRejected       = "RISK_REJECTED"
	// ReasonRiskBypassed is recorded on every order while the risk check is
	// short-circuited in code. Surfacing it in the admin UI is deliberate: a
	// bypass that is visible on every row is far harder to forget than a TODO.
	ReasonRiskBypassed  = "RISK_BYPASSED"
	ReasonMarketClosed  = "MARKET_CLOSED"
	ReasonOutsideWindow = "OUTSIDE_TRADE_WINDOW"
	// ReasonStrategyInvalid — the strategy failed validation and was skipped
	// before any condition was evaluated (e.g. quantity unset). Recorded because
	// otherwise such a strategy produces no row at all, which in the admin UI is
	// indistinguishable from "no news arrived" — the exact blind spot this audit
	// trail exists to remove.
	ReasonStrategyInvalid = "STRATEGY_INVALID"
)

// Record is one decision row. Pointer fields are optional and stored as NULL
// when unset, so the UI can distinguish "zero" from "not applicable".
type Record struct {
	EventID       string
	UserID        string
	StrategyID    string
	StrategyName  string
	Symbol        string
	StockCode     int64
	Exchange      string
	Outcome       Outcome
	Stage         Stage
	ReasonCode    string
	ReasonDetail  string
	LimitValue    *float64
	ActualValue   *float64
	MatchedCond   []string
	FailedCond    []string
	CondScores    map[string]float64
	MatchScore    *float64
	ImpactScore   *int
	Sentiment     string
	NewsCategory  string
	PctChange     *float64
	LTP           *float64
	OrderID       string
	CorrelationID string
	CreatedAt     time.Time
}

// F returns a pointer to v. Convenience for populating Record's optional fields
// inline at call sites.
func F(v float64) *float64 { return &v }

// I returns a pointer to v.
func I(v int) *int { return &v }

// valid reports whether the record can be inserted. strategy_id is a NOT NULL
// UUID column, so a row without one would fail the whole batch — checking here
// keeps a single malformed record from discarding the 200 good ones beside it.
func (r *Record) valid() bool {
	return r != nil && r.StrategyID != "" && r.EventID != "" && r.UserID != ""
}

const (
	defaultBufferSize = 10000
	defaultBatchSize  = 200
	defaultFlushEvery = time.Second
	insertTimeout     = 5 * time.Second
	numCols           = 25
)

// Recorder buffers decision records and writes them in batches. The zero value
// is not usable; construct with New. A nil *Recorder is valid and silently does
// nothing, which is how the feature flag is implemented at every call site.
type Recorder struct {
	db     *sql.DB
	logger *zap.Logger

	ch chan *Record

	dropped  atomic.Int64
	written  atomic.Int64
	failed   atomic.Int64
	invalid  atomic.Int64
	stopOnce sync.Once
	done     chan struct{}
	wg       sync.WaitGroup
}

// New starts a Recorder writing to db. Returns nil when db is nil so a caller
// that could not reach Postgres degrades to a no-op rather than failing to boot.
func New(db *sql.DB, lgr *zap.Logger) *Recorder {
	if db == nil {
		return nil
	}
	r := &Recorder{
		db:     db,
		logger: lgr,
		ch:     make(chan *Record, defaultBufferSize),
		done:   make(chan struct{}),
	}
	r.wg.Add(1)
	go r.run()
	return r
}

// Record queues rec for persistence. It never blocks and never returns an error:
// on a full buffer the record is dropped and counted. Safe on a nil Recorder.
//
// CreatedAt is stamped here when unset so the row carries the moment the
// decision was made, not the moment the batch happened to flush.
func (r *Recorder) Record(rec *Record) {
	if r == nil || rec == nil {
		return
	}
	if !rec.valid() {
		r.invalid.Add(1)
		return
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	select {
	case r.ch <- rec:
	default:
		r.dropped.Add(1)
	}
}

// Stats returns cumulative counters for /metrics and the admin health view.
// A rising dropped count means the writer cannot keep up with evaluation.
func (r *Recorder) Stats() (written, dropped, failed, invalid int64) {
	if r == nil {
		return 0, 0, 0, 0
	}
	return r.written.Load(), r.dropped.Load(), r.failed.Load(), r.invalid.Load()
}

// Close stops the writer and flushes whatever is still buffered. Idempotent and
// nil-safe. It waits for the final flush so a graceful shutdown does not lose
// the last batch.
func (r *Recorder) Close() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		close(r.done)
		r.wg.Wait()
	})
}

// run batches buffered records into multi-row INSERTs. It flushes when the batch
// reaches defaultBatchSize or defaultFlushEvery elapses, whichever comes first,
// so a slow news day still gets its rows written promptly.
func (r *Recorder) run() {
	defer r.wg.Done()
	defer logger.RecoverGoroutine(r.logger, "decisions-recorder")

	ticker := time.NewTicker(defaultFlushEvery)
	defer ticker.Stop()

	batch := make([]*Record, 0, defaultBatchSize)

	for {
		select {
		case <-r.done:
			// Drain whatever is still queued, then write the remainder.
			for {
				select {
				case rec := <-r.ch:
					batch = append(batch, rec)
					if len(batch) >= defaultBatchSize {
						r.flush(batch)
						batch = batch[:0]
					}
					continue
				default:
				}
				break
			}
			r.flush(batch)
			return

		case rec := <-r.ch:
			batch = append(batch, rec)
			if len(batch) >= defaultBatchSize {
				r.flush(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				r.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

// flush writes one batch. Errors are logged and counted, never returned: losing
// audit rows is strictly preferable to disturbing the trading path.
func (r *Recorder) flush(batch []*Record) {
	if len(batch) == 0 {
		return
	}

	query, args := buildInsert(batch)

	ctx, cancel := context.WithTimeout(context.Background(), insertTimeout)
	defer cancel()

	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		r.failed.Add(int64(len(batch)))
		if r.logger != nil {
			r.logger.Error("failed to persist signal decisions",
				zap.Int("batch_size", len(batch)),
				zap.Error(err))
		}
		return
	}
	r.written.Add(int64(len(batch)))
}

// buildInsert renders a multi-row INSERT and its argument list. Split out from
// flush so it can be unit-tested without a database.
func buildInsert(batch []*Record) (string, []interface{}) {
	var sb strings.Builder
	sb.WriteString(`INSERT INTO signal_decisions (
		event_id, user_id, strategy_id, strategy_name,
		symbol, stock_code, exchange,
		outcome, stage, reason_code, reason_detail,
		limit_value, actual_value,
		matched_conditions, failed_conditions, condition_scores, match_score,
		impact_score, sentiment, news_category, pct_change, ltp,
		order_id, correlation_id, created_at
	) VALUES `)

	args := make([]interface{}, 0, len(batch)*numCols)

	for i, rec := range batch {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(")
		for c := 0; c < numCols; c++ {
			if c > 0 {
				sb.WriteString(",")
			}
			fmt.Fprintf(&sb, "$%d", i*numCols+c+1)
		}
		sb.WriteString(")")

		var scores interface{}
		if len(rec.CondScores) > 0 {
			if b, err := json.Marshal(rec.CondScores); err == nil {
				scores = string(b)
			}
		}

		args = append(args,
			rec.EventID,
			rec.UserID,
			rec.StrategyID,
			rec.StrategyName,
			nullIfEmpty(rec.Symbol),
			nullIfZero(rec.StockCode),
			nullIfEmpty(rec.Exchange),
			string(rec.Outcome),
			string(rec.Stage),
			rec.ReasonCode,
			rec.ReasonDetail,
			rec.LimitValue,
			rec.ActualValue,
			pq.Array(rec.MatchedCond),
			pq.Array(rec.FailedCond),
			scores,
			rec.MatchScore,
			rec.ImpactScore,
			nullIfEmpty(rec.Sentiment),
			nullIfEmpty(rec.NewsCategory),
			rec.PctChange,
			rec.LTP,
			nullIfEmpty(rec.OrderID),
			nullIfEmpty(rec.CorrelationID),
			rec.CreatedAt,
		)
	}

	return sb.String(), args
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nullIfZero(v int64) interface{} {
	if v == 0 {
		return nil
	}
	return v
}
