package manthan

// Flexi caps — I/O half (design brief 2026-09-30 §2.4/§2.5). The pure plan
// lives in types/flexi.go; this file owns everything that touches a DB, a
// clock or a logger:
//
//   - fetchFlexiUniverse  ONE signals_db snapshot per Kafka message (one
//                         REPEATABLE READ tx, three statements, each under
//                         MANTHAN_FLEXI_DB_TIMEOUT_MS; the tx as a whole under
//                         flexiUniverseStatements+1 × that — see
//                         flexiTxBudget). Any error ⇒ Err set ⇒ every
//                         strategy's plan fails closed to base caps.
//   - loadShadowBook      dry-run only: today's WOULD_ALLOCATE FLEXI_EVAL rows
//                         for the strategy (trading_db) — what on-mode would
//                         already hold. Load failure ⇒ no flexi for that
//                         strategy on this message (fail closed).
//   - flexiInputFor       per-strategy FlexiInput (allowlist, CreatedAt, IST
//                         clock, shadow) or nil.
//   - publishFlexiResult  persists FLEXI_EVALs, counts not-applied reasons,
//                         rate-limits the Warns.
//   - probeFlexiSchema    boot check that migration 014 is applied before a
//                         non-off mode is honoured (never a silent zero-row
//                         dry-run).
//
// Nothing here runs when MANTHAN_FLEXI_CAPS_MODE is unset/off.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

var (
	errFlexiNilSignalsDB = errors.New("flexi: signals_db handle is nil")
	errFlexiEmptyRunDate = errors.New("flexi: signal has empty run_date")
)

// flexiWarnEvery is the per-reason Warn rate limit.
const flexiWarnEvery = 5 * time.Minute

// flexiUniverseStatements is the number of statements fetchFlexiUniverse
// runs inside its transaction (stocks-by-status, signals rows, infra buckets).
const flexiUniverseStatements = 3

// flexiTxBudget is the whole-transaction deadline for fetchFlexiUniverse.
// database/sql binds a transaction to the context given to BeginTx and rolls
// it back when that context expires, so the tx context must outlive every
// statement: BEGIN + N statements + COMMIT, each allowed up to perStatement.
func flexiTxBudget(perStatement time.Duration) time.Duration {
	return perStatement * time.Duration(flexiUniverseStatements+1)
}

// flexiStats — in-process counters (rules-engine has no Prometheus
// registry today; these back the rate-limited logs and are exposed via
// Snapshot for a future /metrics or admin probe).
type flexiStats struct {
	mu       sync.Mutex
	counters map[string]int64
	lastWarn map[string]time.Time
	// universe query latency
	universeCount int64
	universeMsSum int64
	universeMsMax int64
}

func newFlexiStats() *flexiStats {
	return &flexiStats{counters: map[string]int64{}, lastWarn: map[string]time.Time{}}
}

// All methods are nil-receiver safe: a Consumer that never had SetFlexi
// called (mode off, or a test-built Consumer) must never panic here.
func (s *flexiStats) inc(key string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.counters[key]++
	s.mu.Unlock()
}

// allowWarn reports whether a Warn for key may be logged now (≤ 1 per
// flexiWarnEvery per key).
func (s *flexiStats) allowWarn(key string) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if last, ok := s.lastWarn[key]; ok && now.Sub(last) < flexiWarnEvery {
		return false
	}
	s.lastWarn[key] = now
	return true
}

func (s *flexiStats) observeUniverse(ms int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.universeCount++
	s.universeMsSum += ms
	if ms > s.universeMsMax {
		s.universeMsMax = ms
	}
	s.mu.Unlock()
}

// Snapshot returns a copy of the counters plus universe latency aggregates.
func (s *flexiStats) Snapshot() map[string]int64 {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.counters)+3)
	for k, v := range s.counters {
		out[k] = v
	}
	out["universe_query_count"] = s.universeCount
	out["universe_query_ms_max"] = s.universeMsMax
	if s.universeCount > 0 {
		out["universe_query_ms_avg"] = s.universeMsSum / s.universeCount
	}
	return out
}

// SetFlexi attaches the flexi configuration and the decisions DB
// (trading_db — for the dry-run shadow book). Called by Wire after the
// schema probe. With Mode off nothing else in this file ever runs.
func (c *Consumer) SetFlexi(cfg types.FlexiConfig, decisionsDB *sql.DB) {
	c.flexiCfg = cfg
	c.flexiDB = decisionsDB
	if c.flexiStats == nil {
		c.flexiStats = newFlexiStats()
	}
}

// FlexiStats returns the in-process flexi counters (nil when never enabled).
func (c *Consumer) FlexiStats() map[string]int64 {
	if c.flexiStats == nil {
		return nil
	}
	return c.flexiStats.Snapshot()
}

// nowIST is the IST wall clock (falls back to a fixed +05:30 zone).
func nowIST() time.Time {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	if loc == nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}
	return time.Now().In(loc)
}

// fetchFlexiUniverse reads the per-run_date evidence from signals_db in ONE
// REPEATABLE READ read-only transaction so stocks_rows, eligible rows and
// infra-unknown buckets describe the same pipeline run.
//
// Timeouts: each statement runs under its own cfg.DBTimeout (MANTHAN_FLEXI_
// DB_TIMEOUT_MS, default 300 ms); the transaction context handed to BeginTx
// — which database/sql uses to tear the tx down — gets flexiTxBudget
// (4 × per-statement) so the whole BEGIN + 3 statements + COMMIT cannot be
// cut short by the first statement's budget. Per-statement contexts derive
// from the tx context, so no statement can outlive the transaction.
// Any deadline ⇒ u.Err ⇒ universe unknown ⇒ base caps (fail closed).
// Never returns nil.
func (c *Consumer) fetchFlexiUniverse(ctx context.Context, runDate string) *types.FlexiUniverse {
	u := &types.FlexiUniverse{
		RunDate:        runDate,
		StocksByStatus: map[string]int{},
		InfraUnknown:   map[string]bool{},
		QueriedAt:      time.Now().UTC(),
	}
	start := time.Now()
	defer func() {
		u.QueryMs = time.Since(start).Milliseconds()
		c.flexiStats.observeUniverse(u.QueryMs) // nil-receiver safe
	}()
	if c.signalsDB == nil {
		u.Err = errFlexiNilSignalsDB
		return u
	}
	if strings.TrimSpace(runDate) == "" {
		u.Err = errFlexiEmptyRunDate
		return u
	}
	timeout := c.flexiCfg.DBTimeout
	if timeout <= 0 {
		timeout = 300 * time.Millisecond
	}

	txCtx, txCancel := context.WithTimeout(ctx, flexiTxBudget(timeout))
	defer txCancel()
	tx, err := c.signalsDB.BeginTx(txCtx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		u.Err = fmt.Errorf("begin tx: %w", err)
		return u
	}
	defer func() { _ = tx.Rollback() }()

	// 1. manthan_stocks rows for D, by status (UNIVERSE_PROVEN needs the total).
	if err := func() error {
		qctx, cancel := context.WithTimeout(txCtx, timeout)
		defer cancel()
		rows, err := tx.QueryContext(qctx, `
			SELECT status, count(*) FROM manthan_stocks
			WHERE run_date = $1::date GROUP BY status`, runDate)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var status string
			var n int
			if err := rows.Scan(&status, &n); err != nil {
				return err
			}
			u.StocksByStatus[status] = n
			u.StocksRows += n
		}
		return rows.Err()
	}(); err != nil {
		u.Err = fmt.Errorf("manthan_stocks count: %w", err)
		return u
	}

	// 2. Every manthan_signals row for D (no published_to_kafka_at filter).
	if err := func() error {
		qctx, cancel := context.WithTimeout(txCtx, timeout)
		defer cancel()
		rows, err := tx.QueryContext(qctx, `
			SELECT UPPER(TRIM(COALESCE(mcap_bucket,''))), symbol, COALESCE(industry,''),
			       first_seen_at, created_at
			FROM manthan_signals WHERE run_date = $1::date`, runDate)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var r types.FlexiUniverseRow
			var firstSeen sql.NullTime
			var createdAt time.Time
			if err := rows.Scan(&r.Bucket, &r.Symbol, &r.Industry, &firstSeen, &createdAt); err != nil {
				return err
			}
			if firstSeen.Valid {
				r.FirstSeenAt = firstSeen.Time
			}
			if createdAt.After(u.SnapshotMaxCreatedAt) {
				u.SnapshotMaxCreatedAt = createdAt
			}
			u.Eligible = append(u.Eligible, r)
		}
		return rows.Err()
	}(); err != nil {
		u.Err = fmt.Errorf("manthan_signals rows: %w", err)
		return u
	}

	// 3. INFRA_UNKNOWN buckets: FILTER_REJECTED for an infra reason (the
	//    data-ingestion pre-flight downgrade strings), not a structural one.
	if err := func() error {
		qctx, cancel := context.WithTimeout(txCtx, timeout)
		defer cancel()
		rows, err := tx.QueryContext(qctx, `
			SELECT UPPER(TRIM(COALESCE(mcap_bucket,''))) FROM manthan_stocks
			WHERE run_date = $1::date AND status = 'FILTER_REJECTED'
			  AND (reason LIKE 'company-master resolve failed%' OR reason LIKE 'ISIN %')
			GROUP BY 1`, runDate)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var b string
			if err := rows.Scan(&b); err != nil {
				return err
			}
			if b != "" {
				u.InfraUnknown[b] = true
			}
		}
		return rows.Err()
	}(); err != nil {
		u.Err = fmt.Errorf("infra-unknown buckets: %w", err)
		return u
	}

	if err := tx.Commit(); err != nil { // read-only commit; surfaces a broken tx
		u.Err = fmt.Errorf("commit: %w", err)
	}
	return u
}

// loadShadowBook returns the strategy's dry-run shadow book for run_date:
// symbols an earlier dry-run evaluation today marked WOULD_ALLOCATE, minus
// the symbols in exclude (this message's own signals — a redelivery must not
// count itself). Symbols now in Positions are dropped later by OverlayShadow.
func (c *Consumer) loadShadowBook(ctx context.Context, strategyID, runDate string, exclude map[string]bool) ([]types.ShadowEntry, error) {
	if c.flexiDB == nil {
		return nil, errors.New("flexi: decisions DB handle is nil")
	}
	timeout := c.flexiCfg.DBTimeout
	if timeout <= 0 {
		timeout = 300 * time.Millisecond
	}
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	rows, err := c.flexiDB.QueryContext(qctx, `
		SELECT symbol, COALESCE(industry,''), COALESCE(mcap_bucket,'')
		FROM manthan_signal_decisions
		WHERE strategy_id = $1
		  AND signal_type = 'FLEXI_EVAL' AND status = 'EVALUATED'
		  AND payload->>'run_date' = $2
		  AND payload->>'outcome' = $3`, strategyID, runDate, types.FlexiWouldAllocate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []types.ShadowEntry
	for rows.Next() {
		var e types.ShadowEntry
		if err := rows.Scan(&e.Symbol, &e.Industry, &e.Bucket); err != nil {
			return nil, err
		}
		if exclude[e.Symbol] {
			continue
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// flexiInputFor builds the per-strategy FlexiInput, or nil when flexi does
// not apply to this strategy on this message (off, not allowlisted, or a
// dry-run shadow load failed — fail closed).
func (c *Consumer) flexiInputFor(ctx context.Context, strategy types.UserStrategy, universe *types.FlexiUniverse, sigs []types.ManthanSignal) *FlexiInput {
	if !c.flexiCfg.Enabled() || len(sigs) == 0 {
		return nil
	}
	if !c.flexiCfg.AppliesTo(strategy.StrategyID) {
		c.flexiStats.inc("skipped_not_allowlisted")
		return nil
	}
	now := nowIST
	if c.flexiNow != nil {
		now = c.flexiNow
	}
	fx := &FlexiInput{
		Cfg:               c.flexiCfg,
		Universe:          universe,
		StrategyCreatedAt: strategy.CreatedAt,
		NowIST:            now(),
	}
	if c.flexiCfg.Mode == types.FlexiDryRun {
		exclude := make(map[string]bool, len(sigs))
		for _, s := range sigs {
			exclude[s.Symbol] = true
		}
		shadow, err := c.loadShadowBook(ctx, strategy.StrategyID, sigs[0].RunDate, exclude)
		if err != nil {
			c.flexiStats.inc("plan_not_applied.shadow_load_failed")
			if c.flexiStats.allowWarn("shadow_load_failed") {
				c.logger.Warn("Flexi dry-run: shadow book load failed — no flexi evaluation for this strategy on this signal (fail closed)",
					zap.String("strategy", strategy.StrategyID), zap.Error(err))
			}
			return nil
		}
		fx.Shadow = shadow
	}
	return fx
}

// flexiWanted reports whether a universe fetch is worth doing for this
// message: mode dry_run/on AND at least one loaded strategy is allowlisted.
func (c *Consumer) flexiWanted(strategies []types.UserStrategy) bool {
	if !c.flexiCfg.Enabled() {
		return false
	}
	for _, s := range strategies {
		if c.flexiCfg.AppliesTo(s.StrategyID) {
			return true
		}
	}
	return false
}

// publishFlexiResult persists every FLEXI_EVAL from an allocation and
// accounts for a plan that was not applied. Fail-closed reasons get a
// rate-limited Warn; structural reasons ("nothing to lend") get a
// rate-limited Info per (strategy, reason) — so a dry-run day that produces
// zero FLEXI_EVAL rows still leaves a positive trace of WHY (a misconfigured
// cutoff or an unexpectedly non-void donor is otherwise indistinguishable
// from a broken deploy, since main.go logs at Info and nothing reads the
// in-process counters). All flexiStats methods are nil-receiver safe, so a
// Consumer that never had SetFlexi called is fine here.
func (c *Consumer) publishFlexiResult(ctx context.Context, strategy types.UserStrategy, universe *types.FlexiUniverse, result *AllocateResult) {
	if result == nil {
		return
	}
	for _, ev := range result.FlexiEvals {
		c.flexiStats.inc("eval." + ev.Outcome)
		c.logger.Info("Flexi evaluation",
			zap.String("user", strategy.UserID),
			zap.String("strategy", strategy.StrategyID),
			zap.String("symbol", ev.Symbol),
			zap.String("mode", string(ev.Mode)),
			zap.String("outcome", ev.Outcome),
			zap.String("bucket", ev.Bucket),
			zap.Int("held", ev.EvalHeld),
			zap.Int("ceiling", ev.EvalLimit),
			zap.String("base_outcome", ev.BaseOutcome))
		c.publisher.PublishFlexiEval(ctx, strategy.UserID, strategy.StrategyID, ev)
	}
	if why := result.FlexiNotApplied; why != "" {
		c.flexiStats.inc("plan_not_applied." + why)
		fields := []zap.Field{
			zap.String("user", strategy.UserID),
			zap.String("strategy", strategy.StrategyID),
			zap.String("reason", why),
		}
		if universe != nil {
			fields = append(fields,
				zap.String("run_date", universe.RunDate),
				zap.Int("stocks_rows", universe.StocksRows),
				zap.Int("eligible_rows", len(universe.Eligible)),
				zap.Any("eligible_by_bucket", flexiEligibleByBucket(universe)),
				zap.Int64("universe_query_ms", universe.QueryMs))
			if universe.Err != nil {
				fields = append(fields, zap.NamedError("universe_err", universe.Err))
			}
		}
		if types.FlexiGuardIsFailClosed(why) {
			if c.flexiStats.allowWarn("plan_not_applied." + why) {
				c.logger.Warn("Flexi plan not applied — falling back to base caps (fail closed)", fields...)
			}
		} else if c.flexiStats.allowWarn("plan_not_applied." + why + "." + strategy.StrategyID) {
			// Structural: the normal "nothing to lend" case. One Info per
			// (strategy, reason) per flexiWarnEvery keeps the 09:00 batch from
			// flooding while guaranteeing the operator can see the dry-run IS
			// evaluating; the rest of the window logs at Debug below.
			c.logger.Info("Flexi plan not applied (nothing to lend) — base caps apply", fields...)
		} else {
			c.logger.Debug("Flexi plan not applied (nothing to lend)", fields...)
		}
	}
}

// flexiEligibleByBucket is the raw per-bucket count of manthan_signals rows
// in the snapshot (NOT the strategy-specific Opp — held/creation-gated names
// are still counted). Logged with a not-applied plan so a "no_void_donor"
// line shows at a glance which bucket had eligible names.
func flexiEligibleByBucket(u *types.FlexiUniverse) map[string]int {
	out := map[string]int{}
	if u == nil {
		return out
	}
	for _, r := range u.Eligible {
		out[r.Bucket]++
	}
	return out
}

// observeFlexiUniverse logs the fetch outcome: Debug normally, a
// rate-limited Warn on error or slow (> 200 ms) queries.
func (c *Consumer) observeFlexiUniverse(u *types.FlexiUniverse) {
	if u == nil {
		return
	}
	switch {
	case u.Err != nil:
		c.flexiStats.inc("universe_fetch_error")
		if c.flexiStats.allowWarn("universe_fetch_error") {
			c.logger.Warn("Flexi universe fetch failed — every plan on this signal fails closed to base caps",
				zap.String("run_date", u.RunDate), zap.Int64("ms", u.QueryMs), zap.Error(u.Err))
		}
	case u.QueryMs > 200:
		c.flexiStats.inc("universe_fetch_slow")
		if c.flexiStats.allowWarn("universe_fetch_slow") {
			c.logger.Warn("Flexi universe fetch slow", zap.String("run_date", u.RunDate), zap.Int64("ms", u.QueryMs))
		}
	default:
		c.flexiStats.inc("universe_fetch_ok")
		c.logger.Debug("Flexi universe fetched",
			zap.String("run_date", u.RunDate), zap.Int("stocks_rows", u.StocksRows),
			zap.Int("eligible_rows", len(u.Eligible)), zap.Int64("ms", u.QueryMs))
	}
}

// ErrFlexiSchemaNotMigrated marks a DEFINITIVE probe answer: the DB replied
// and migration 014 is not (fully) applied. Any other probe error is a DB /
// connectivity problem (timeout, auth, unreachable) and may be transient —
// Wire retries those before forcing the mode off, and words the Error log
// differently so the operator does not chase the migration for a blip.
var ErrFlexiSchemaNotMigrated = errors.New("flexi: audit schema not migrated (run migrations/014_flexi_audit.sql)")

// probeFlexiSchema verifies migration 014 is applied on the decisions DB:
// chk_msd_signal_type admits FLEXI_EVAL, chk_msd_status admits EVALUATED
// and flexi_grant exists. A definitive "not migrated" answer wraps
// ErrFlexiSchemaNotMigrated (errors.Is); a nil handle counts as definitive
// too (there is nowhere to write FLEXI_EVAL rows). Wire forces the mode off
// on any error so a dry-run can never silently write zero rows.
func probeFlexiSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("%w: trading_db handle is nil — FLEXI_EVAL rows cannot be written", ErrFlexiSchemaNotMigrated)
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	def := func(conname string) (string, error) {
		var d string
		err := db.QueryRowContext(pctx, `
			SELECT pg_get_constraintdef(oid) FROM pg_constraint
			WHERE conrelid = 'manthan_signal_decisions'::regclass AND conname = $1`, conname).Scan(&d)
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: constraint %s not found", ErrFlexiSchemaNotMigrated, conname)
		}
		if err != nil {
			return "", fmt.Errorf("probe %s: %w", conname, err)
		}
		return d, nil
	}
	sigType, err := def("chk_msd_signal_type")
	if err != nil {
		return err
	}
	if !strings.Contains(sigType, "'FLEXI_EVAL'") {
		return fmt.Errorf("%w: chk_msd_signal_type does not admit FLEXI_EVAL: %s", ErrFlexiSchemaNotMigrated, sigType)
	}
	status, err := def("chk_msd_status")
	if err != nil {
		return err
	}
	if !strings.Contains(status, "'EVALUATED'") {
		return fmt.Errorf("%w: chk_msd_status does not admit EVALUATED: %s", ErrFlexiSchemaNotMigrated, status)
	}
	var hasCol bool
	if err := db.QueryRowContext(pctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		               WHERE table_name = 'manthan_signal_decisions' AND column_name = 'flexi_grant')`).Scan(&hasCol); err != nil {
		return fmt.Errorf("flexi_grant column probe: %w", err)
	}
	if !hasCol {
		return fmt.Errorf("%w: manthan_signal_decisions.flexi_grant missing", ErrFlexiSchemaNotMigrated)
	}
	return nil
}

// flexiProbeAttempts / flexiProbeBackoff bound the boot-time retry of a
// probe that failed for a NON-definitive reason (DB unreachable at the 03:30
// PM2 restart, timeout). A definitive "not migrated" answer is never retried.
const (
	flexiProbeAttempts = 3
	flexiProbeBackoff  = 2 * time.Second
)

// probeFlexiSchemaWithRetry runs probeFlexiSchema up to flexiProbeAttempts
// times, sleeping flexiProbeBackoff between attempts, but only while the
// error is a DB/connectivity one. Returns the last error.
func probeFlexiSchemaWithRetry(ctx context.Context, db *sql.DB, logger *zap.Logger) error {
	var err error
	for attempt := 1; attempt <= flexiProbeAttempts; attempt++ {
		err = probeFlexiSchema(ctx, db)
		if err == nil || errors.Is(err, ErrFlexiSchemaNotMigrated) {
			return err
		}
		if attempt == flexiProbeAttempts {
			break
		}
		logger.Warn("Manthan flexi schema probe failed (DB error) — retrying",
			zap.Int("attempt", attempt), zap.Int("max_attempts", flexiProbeAttempts),
			zap.Duration("backoff", flexiProbeBackoff), zap.Error(err))
		select {
		case <-ctx.Done():
			return fmt.Errorf("probe aborted: %w", ctx.Err())
		case <-time.After(flexiProbeBackoff):
		}
	}
	return err
}
