package manthan

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// CompanyResolver lazily resolves an ISIN to the company-master record that
// trade-execution will need at order-placement time (NSE token lookup). The
// implementation is expected to be backed by Redis with MongoDB as the
// authoritative fallback — a Redis miss should trigger a Mongo fetch and
// cache-write so subsequent lookups are fast.
//
// Returning (nil, nil) means "ISIN not in master data" — caller treats this
// as un-tradeable and downgrades the signal.
type CompanyResolver interface {
	// ResolveCompany returns NSE/BSE codes + active exchange for an ISIN.
	// On Redis miss, implementations should populate the cache so the same
	// ISIN published to Kafka in this run is guaranteed resolvable when
	// trade-execution looks it up.
	ResolveCompany(ctx context.Context, isin string) (nseCode, bseCode, exchange string, err error)
}

// Publisher writes pipeline output to PostgreSQL and Kafka.
//
// Storage split:
//   - Postgres `manthan_stocks`   — EVERY candidate we processed (ELIGIBLE,
//                                   FILTER_REJECTED, DATA_DROPPED). Full audit.
//   - Postgres `manthan_signals`  — ONLY eligible stocks (downstream-ready).
//   - Kafka topic (default `manthan.signals`) — ONLY eligible stocks, one
//                                   message per symbol. Consumed by allocators.
//
// Pre-flight gate (when Resolver is set): each ELIGIBLE stock is checked
// against the company-master via Resolver.ResolveCompany BEFORE Kafka
// publish. ISINs without a resolvable NSE token get DOWNGRADED to
// FILTER_REJECTED with reason "ISIN unresolvable in CompanyMaster" — they
// stay in the audit (`manthan_stocks`) but are NOT sent to allocators or
// trade-execution. This prevents ghost-position bugs (e.g. NATIONALUM /
// LLOYDSME 2026-04-24) where an eligible signal hit trade-execution but
// the ISIN→token mapping was missing from Redis.
type Publisher struct {
	db           *sql.DB
	kafkaWriter  *kafka.Writer
	kafkaEnabled bool
	resolver     CompanyResolver // optional — when nil, pre-flight gate is skipped
	logger       *zap.Logger
}

// PublisherConfig configures the publisher.
type PublisherConfig struct {
	DB           *sql.DB
	KafkaBrokers []string // empty = kafka disabled
	KafkaTopic   string   // default "manthan.signals"
	// Resolver enables the ISIN pre-flight gate. Recommended: pass the
	// data-ingestion RedisManager (which falls back to MongoDB on miss
	// and populates Redis as a side effect).
	Resolver CompanyResolver
}

// NewPublisher creates a new publisher. DB is mandatory; Kafka is optional.
func NewPublisher(cfg PublisherConfig, logger *zap.Logger) *Publisher {
	p := &Publisher{db: cfg.DB, logger: logger, resolver: cfg.Resolver}
	if cfg.Resolver != nil {
		logger.Info("Manthan publisher pre-flight gate ENABLED — ELIGIBLE signals will be filtered against CompanyMaster before Kafka publish")
	}
	if len(cfg.KafkaBrokers) > 0 && cfg.KafkaBrokers[0] != "" {
		topic := cfg.KafkaTopic
		if topic == "" {
			topic = "manthan.signals"
		}
		p.kafkaWriter = &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 100 * time.Millisecond,
			RequiredAcks: kafka.RequireAll, // durability > latency for signals
		}
		p.kafkaEnabled = true
		logger.Info("Manthan Kafka producer initialized",
			zap.Strings("brokers", cfg.KafkaBrokers), zap.String("topic", topic))
	}
	return p
}

// Close releases the Kafka writer (DB is owned by caller).
func (p *Publisher) Close() error {
	if p.kafkaWriter != nil {
		return p.kafkaWriter.Close()
	}
	return nil
}

// Publish writes the pipeline result to DB and Kafka.
func (p *Publisher) Publish(ctx context.Context, result *PipelineResult) (*PublishStats, error) {
	stats := &PublishStats{}
	runDate := time.Now()

	// Pre-flight: ensure every ELIGIBLE stock has a resolvable ISIN→NSE-token
	// mapping in Redis (lazy-loaded from MongoDB CompanyMaster on miss). Any
	// stock that can't be resolved gets MOVED from result.Eligible to
	// result.FilteredOut so it's recorded as FILTER_REJECTED in the audit
	// table but NOT sent to allocators / trade-execution.
	//
	// The gate runs BEFORE the DB transaction so that:
	//   - manthan_signals (downstream truth) only ever contains tradeable rows
	//   - manthan_stocks (audit) reflects every candidate's true outcome
	//   - downgrade reasons carry forward into the FILTER_REJECTED audit row
	if p.resolver != nil && len(result.Eligible) > 0 {
		stillEligible := result.Eligible[:0]
		for _, s := range result.Eligible {
			if strings.TrimSpace(s.ISIN) == "" {
				p.logger.Warn("Pre-flight: ELIGIBLE stock has empty ISIN — downgrading",
					zap.String("symbol", s.Symbol))
				s.FilterReason = "ISIN missing on eligible stock"
				result.FilteredOut = append(result.FilteredOut, s)
				stats.PreflightDropped++
				continue
			}
			nseCode, _, _, rErr := p.resolver.ResolveCompany(ctx, s.ISIN)
			if rErr != nil {
				p.logger.Warn("Pre-flight: company-master resolve failed — downgrading to be safe",
					zap.String("symbol", s.Symbol),
					zap.String("isin", s.ISIN),
					zap.Error(rErr))
				s.FilterReason = "company-master resolve failed: " + rErr.Error()
				result.FilteredOut = append(result.FilteredOut, s)
				stats.PreflightDropped++
				continue
			}
			if strings.TrimSpace(nseCode) == "" {
				p.logger.Warn("Pre-flight: ISIN unresolvable in CompanyMaster — downgrading",
					zap.String("symbol", s.Symbol),
					zap.String("isin", s.ISIN))
				s.FilterReason = "ISIN unresolvable in CompanyMaster (no nse_code)"
				result.FilteredOut = append(result.FilteredOut, s)
				stats.PreflightDropped++
				continue
			}
			// Resolved — Redis key now exists (the resolver writes on miss).
			// trade-execution's broker_adapter.resolveToken will succeed.
			stillEligible = append(stillEligible, s)
		}
		result.Eligible = stillEligible
		if stats.PreflightDropped > 0 {
			p.logger.Info("Pre-flight gate: dropped unresolvable signals",
				zap.Int("dropped", stats.PreflightDropped),
				zap.Int("remaining_eligible", len(result.Eligible)))
		}
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return stats, fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// 1. Upsert ALL stocks (eligible, filter-rejected, data-dropped) → manthan_stocks
	for _, s := range result.Eligible {
		if err := upsertStock(ctx, tx, runDate, s, "ELIGIBLE", ""); err != nil {
			return stats, fmt.Errorf("upsert eligible %s: %w", s.Symbol, err)
		}
		stats.EligibleWritten++
	}
	for _, s := range result.FilteredOut {
		if err := upsertStock(ctx, tx, runDate, s, "FILTER_REJECTED", s.FilterReason); err != nil {
			return stats, fmt.Errorf("upsert filtered %s: %w", s.Symbol, err)
		}
		stats.FilterRejectedWritten++
	}
	for _, d := range result.Drops {
		if err := upsertDropped(ctx, tx, runDate, d); err != nil {
			return stats, fmt.Errorf("upsert dropped %s: %w", d.Symbol, err)
		}
		stats.DroppedWritten++
	}

	// 2. Insert ELIGIBLE stocks into manthan_signals (clear today's first to avoid dupes)
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM manthan_signals WHERE run_date = CURRENT_DATE`); err != nil {
		return stats, fmt.Errorf("clear signals: %w", err)
	}
	for _, s := range result.Eligible {
		if err := insertSignal(ctx, tx, runDate, s); err != nil {
			return stats, fmt.Errorf("insert signal %s: %w", s.Symbol, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return stats, fmt.Errorf("commit: %w", err)
	}
	committed = true

	// 3. Publish ELIGIBLE stocks to Kafka (post-commit, so consumers only see
	// durable messages). Failures here are logged — DB is source of truth.
	if p.kafkaEnabled && len(result.Eligible) > 0 {
		msgs := make([]kafka.Message, 0, len(result.Eligible))
		for _, s := range result.Eligible {
			payload := map[string]any{
				"run_date":     runDate.Format("2006-01-02"),
				"symbol":       s.Symbol,
				"isin":         s.ISIN,
				"industry":     s.Industry,
				"mcap_bucket":  s.MCapBucket,
				"index_name":   s.IndexName,
				"market_cap":   s.MarketCap,
				"pe":           s.PE,
				"fscore":       s.FScore,
				"pat":          s.PAT,
				"latest_price": s.LatestPrice,
				"ath_close":    s.ATHClose,
				"week52_high":  s.Week52High,
				"emitted_at":   time.Now().UTC().Format(time.RFC3339),
			}
			body, err := json.Marshal(payload)
			if err != nil {
				p.logger.Warn("Marshal failed", zap.String("symbol", s.Symbol), zap.Error(err))
				continue
			}
			msgs = append(msgs, kafka.Message{
				Key:   []byte(s.Symbol),
				Value: body,
			})
		}
		if err := p.kafkaWriter.WriteMessages(ctx, msgs...); err != nil {
			p.logger.Error("Kafka publish failed (DB already committed)",
				zap.Int("messages", len(msgs)), zap.Error(err))
			stats.KafkaError = err
		} else {
			stats.KafkaPublished = len(msgs)
			// Mark rows as published
			_, _ = p.db.ExecContext(ctx,
				`UPDATE manthan_signals SET published_to_kafka_at = NOW() WHERE run_date = CURRENT_DATE`)
		}
	}

	p.logger.Info("Manthan publish complete",
		zap.Int("eligible", stats.EligibleWritten),
		zap.Int("filter_rejected", stats.FilterRejectedWritten),
		zap.Int("dropped", stats.DroppedWritten),
		zap.Int("kafka_published", stats.KafkaPublished),
	)
	return stats, nil
}

// PublishStats reports what was written.
type PublishStats struct {
	EligibleWritten       int
	FilterRejectedWritten int
	DroppedWritten        int
	KafkaPublished        int
	KafkaError            error
	// PreflightDropped counts ELIGIBLE stocks that were downgraded to
	// FILTER_REJECTED at the pre-flight gate because their ISIN couldn't be
	// resolved against MongoDB CompanyMaster (and therefore wouldn't be
	// resolvable in Redis at trade-execution time). Always 0 when the
	// publisher was constructed without a Resolver.
	PreflightDropped int
}

func upsertStock(ctx context.Context, tx *sql.Tx, runDate time.Time, s *ManthanStock, status, reason string) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO manthan_stocks (
    run_date, symbol, isin, company_name, industry, bse_code, nse_symbol,
    market_cap, pe, pat, fscore, eps,
    latest_price, ath_close, week52_high, week52_low,
    mcap_bucket, index_name, allocation,
    status, reason, ath_entry
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12,
    $13, $14, $15, $16,
    $17, $18, $19,
    $20, $21, $22
)
ON CONFLICT (run_date, symbol) DO UPDATE SET
    isin         = EXCLUDED.isin,
    industry     = EXCLUDED.industry,
    market_cap   = EXCLUDED.market_cap,
    pe           = EXCLUDED.pe,
    pat          = EXCLUDED.pat,
    fscore       = EXCLUDED.fscore,
    eps          = EXCLUDED.eps,
    latest_price = EXCLUDED.latest_price,
    ath_close    = EXCLUDED.ath_close,
    week52_high  = EXCLUDED.week52_high,
    week52_low   = EXCLUDED.week52_low,
    mcap_bucket  = EXCLUDED.mcap_bucket,
    index_name   = EXCLUDED.index_name,
    allocation   = EXCLUDED.allocation,
    status       = EXCLUDED.status,
    reason       = EXCLUDED.reason,
    ath_entry    = EXCLUDED.ath_entry`,
		runDate, s.Symbol, nullIfEmpty(s.ISIN), s.CompanyName, s.Industry,
		nullIfEmpty(s.BSECode), nullIfEmpty(s.NSESymbol),
		s.MarketCap, s.PE, s.PAT, s.FScore, s.EPS,
		s.LatestPrice, s.ATHClose, s.Week52High, s.Week52Low,
		s.MCapBucket, s.IndexName, s.Allocation,
		status, nullIfEmpty(reason), nullIfEmpty(s.ATHEntry),
	)
	return err
}

// upsertDropped writes missing-data drops with minimal info.
func upsertDropped(ctx context.Context, tx *sql.Tx, runDate time.Time, d DropReason) error {
	sym := strings.TrimSpace(d.Symbol)
	if sym == "" || sym == "(blank)" {
		return nil
	}
	_, err := tx.ExecContext(ctx, `
INSERT INTO manthan_stocks (run_date, symbol, status, reason)
VALUES ($1, $2, 'DATA_DROPPED', $3)
ON CONFLICT (run_date, symbol) DO UPDATE SET
    status = EXCLUDED.status,
    reason = EXCLUDED.reason`,
		runDate, sym, d.Reason)
	return err
}

func insertSignal(ctx context.Context, tx *sql.Tx, runDate time.Time, s *ManthanStock) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO manthan_signals (
    run_date, symbol, isin, industry, mcap_bucket, index_name,
    market_cap, pe, fscore, pat,
    latest_price, ath_close, week52_high
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (run_date, symbol) DO UPDATE SET
    industry    = EXCLUDED.industry,
    market_cap  = EXCLUDED.market_cap,
    pe          = EXCLUDED.pe,
    fscore      = EXCLUDED.fscore,
    pat         = EXCLUDED.pat,
    latest_price= EXCLUDED.latest_price,
    ath_close   = EXCLUDED.ath_close,
    week52_high = EXCLUDED.week52_high`,
		runDate, s.Symbol, nullIfEmpty(s.ISIN), s.Industry, s.MCapBucket, s.IndexName,
		s.MarketCap, s.PE, s.FScore, s.PAT,
		s.LatestPrice, s.ATHClose, s.Week52High,
	)
	return err
}

func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}
