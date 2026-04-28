package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// manthanEntryMessage is the JSON shape that trade-execution's signal
// consumer expects on the `trade-signals` topic. Field names + tags MUST
// match rules-engine's manthan.ManthanOrder so trade-execution's existing
// parser handles our messages identically to the live-allocator path.
//
// We intentionally don't import that struct from rules-engine to keep
// services decoupled — duplicating ~25 fields is cheaper than the cross-
// service dependency.
type manthanEntryMessage struct {
	OrderID       string    `json:"order_id"`
	UserID        string    `json:"user_id"`
	StrategyID    string    `json:"strategy_id"`
	Symbol        string    `json:"symbol"`
	ISIN          string    `json:"isin"`
	Exchange      string    `json:"exchange"`
	OrderType     string    `json:"order_type"`
	OrderSide     string    `json:"order_side"`
	ProductType   string    `json:"product_type"`
	Quantity      int32     `json:"quantity"`
	EntryPrice    float64   `json:"entry_price"`
	StopLoss      float64   `json:"stop_loss"`
	StopLossType  string    `json:"stop_loss_type"`
	StopLossPct   float64   `json:"stop_loss_pct"`
	TrailingSLPct float64   `json:"trailing_sl_pct"`
	InvestedAmt   float64   `json:"invested_amt"`
	TxnCostPct    float64   `json:"txn_cost_pct"`
	Industry      string    `json:"industry"`
	MCapBucket    string    `json:"mcap_bucket"`
	IndexName     string    `json:"index_name"`
	EMAAllocPct   float64   `json:"ema_alloc_pct"`
	BearerToken   string    `json:"bearer_token,omitempty"`
	AppId         string    `json:"app_id,omitempty"`
	Source        string    `json:"source,omitempty"`
	TradingMode   string    `json:"trading_mode"`
	Timestamp     time.Time `json:"timestamp"`

	// TopUpForSignalID — non-empty when this entry is a top-up. Trade-
	// execution's entry_handler skips its "already holding" duplicate
	// check; rules-engine's projector uses this to ADD to the parent's
	// manthan_positions row instead of creating a new row.
	TopUpForSignalID string `json:"top_up_for_signal_id,omitempty"`
}

// EnsureDecisionRow writes a manthan_signal_decisions row for one planned
// entry in status='PROPOSED'. Idempotent via INSERT ... ON CONFLICT DO
// NOTHING (signal_id is the PK).
//
// CRITICAL: this MUST be called before the Kafka publish. The rules-engine
// projector's ENTRY_FILLED handler does INSERT manthan_positions FROM
// manthan_signal_decisions WHERE signal_id=$X — if the decision row doesn't
// exist by then, the projection silently inserts zero rows and we never
// build a manthan_positions row.
//
// For top-ups, the decision still gets written so the lifecycle audit
// (PROPOSED → DISPATCHED → CONFIRMED) is complete; the parent-position
// linkage lives on the eventual ENTRY_FILLED event's ParentSignalID, not on
// this row.
func EnsureDecisionRow(ctx context.Context, db *sql.DB, cfg StrategyConfig, e PlannedEntry) error {
	if db == nil {
		return fmt.Errorf("ensure decision: db is nil")
	}
	// Annotate top-ups in rejection_reason so audit queries can find them
	// without a schema migration. (Future: add a parent_signal_id column.)
	note := ""
	if e.TopUpForSignalID != "" {
		note = "topup_for=" + e.TopUpForSignalID
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO manthan_signal_decisions (
			signal_id, user_id, strategy_id, symbol, isin,
			ltp_at_decision, ema_alloc_pct, intended_qty, intended_invested,
			initial_sl_target, industry, mcap_bucket, index_name,
			status, rejection_reason
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, $11, $12, $13,
			'PROPOSED', NULLIF($14, '')
		)
		ON CONFLICT (signal_id) DO NOTHING`,
		e.SignalID, cfg.UserID, cfg.StrategyID, e.Symbol, e.ISIN,
		e.EntryPrice, e.EMAFraction, e.Quantity, e.InvestedAmt,
		e.StopLoss, e.Industry, e.MCapBucket, e.IndexName,
		note,
	)
	return err
}

// MarkDecisionDispatched flips PROPOSED → DISPATCHED after a successful
// Kafka publish. Guarded UPDATE so a late call after the FillConsumer has
// already set CONFIRMED won't regress the status.
func MarkDecisionDispatched(ctx context.Context, db *sql.DB, signalID string) error {
	if db == nil || signalID == "" {
		return nil
	}
	_, err := db.ExecContext(ctx, `
		UPDATE manthan_signal_decisions
		SET status = 'DISPATCHED', dispatched_at = NOW()
		WHERE signal_id = $1 AND status = 'PROPOSED'`,
		signalID)
	return err
}

// PublishOrders writes one MANTHAN_ENTRY message per planned entry to the
// shared `trade-signals` Kafka topic.
//
// dryRun=true: prints what would be sent, makes no network calls. This is
// the default safety mode for the CLI — a fresh dev should be able to run
// `go run ./services/rebalancer/cmd/ --dry-run` and see exactly what would
// happen without touching the broker.
//
// On Kafka failure the function returns the error AFTER attempting all
// remaining publishes — partial success is acceptable since each order has
// its own signal_id and downstream is idempotent.
func PublishOrders(
	ctx context.Context,
	writer *kafka.Writer,
	tradingDB *sql.DB,
	results []AllocResult,
	dryRun bool,
	logger *zap.Logger,
) (publishedCount int, firstErr error) {
	for _, res := range results {
		// Top-ups go FIRST so they reserve broker margin before fresh entries.
		// Both flow through the same code path — the only difference is the
		// `order_type` Kafka header and the JSON `top_up_for_signal_id` field.
		for _, entry := range res.TopUps {
			if err := publishOne(ctx, writer, tradingDB, res.Cfg, entry, "MANTHAN_TOPUP", dryRun, logger); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if !dryRun {
				publishedCount++
			}
		}
		for _, entry := range res.Planned {
			if err := publishOne(ctx, writer, tradingDB, res.Cfg, entry, "MANTHAN_ENTRY", dryRun, logger); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if !dryRun {
				publishedCount++
			}
		}
	}
	return publishedCount, firstErr
}

// publishOne handles a single entry/top-up — same JSON body, different Kafka
// `order_type` header. Trade-execution's signal_consumer routes by the header.
//
// Phase A: write manthan_signal_decisions row (PROPOSED). This is REQUIRED —
//          the rules-engine projector reads this row when projecting
//          ENTRY_FILLED into manthan_positions.
// Phase B: publish to trade-signals Kafka topic.
// Phase C: flip decision to DISPATCHED on successful publish. If publish
//          fails, the row stays PROPOSED — the 60s reaper will mark it
//          TIMED_OUT after 2min so it doesn't linger.
func publishOne(
	ctx context.Context,
	writer *kafka.Writer,
	tradingDB *sql.DB,
	cfg StrategyConfig,
	entry PlannedEntry,
	orderType string,
	dryRun bool,
	logger *zap.Logger,
) error {
	msg := buildEntryMessage(cfg, entry)
	if dryRun {
		logger.Info("rebalancer.PublishOrders: DRY RUN — would publish",
			zap.String("order_type", orderType),
			zap.String("user", cfg.UserID),
			zap.String("symbol", entry.Symbol),
			zap.String("signal_id", entry.SignalID),
			zap.String("top_up_for", entry.TopUpForSignalID),
			zap.Int32("qty", entry.Quantity),
			zap.Float64("invested", entry.InvestedAmt),
			zap.String("index", entry.IndexName),
			zap.Float64("ema", entry.EMAFraction))
		return nil
	}
	if writer == nil {
		return fmt.Errorf("kafka writer not configured (dry-run not set?)")
	}

	// Phase A — write the manthan_signal_decisions row BEFORE the Kafka
	// publish. Required for the rules-engine projector to project the
	// eventual ENTRY_FILLED into manthan_positions.
	if err := EnsureDecisionRow(ctx, tradingDB, cfg, entry); err != nil {
		logger.Error("rebalancer.PublishOrders: decision-row write failed — aborting publish",
			zap.String("signal_id", entry.SignalID),
			zap.String("symbol", entry.Symbol),
			zap.Error(err))
		return fmt.Errorf("ensure decision: %w", err)
	}

	body, err := json.Marshal(msg)
	if err != nil {
		logger.Warn("rebalancer.PublishOrders: marshal failed",
			zap.String("symbol", entry.Symbol), zap.Error(err))
		return err
	}
	headers := []kafka.Header{
		{Key: "order_type", Value: []byte(orderType)},
		{Key: "user_id", Value: []byte(cfg.UserID)},
		{Key: "strategy_id", Value: []byte(cfg.StrategyID)},
		{Key: "symbol", Value: []byte(entry.Symbol)},
		{Key: "signal_id", Value: []byte(entry.SignalID)},
		{Key: "produced_by", Value: []byte("rebalancer")},
	}
	if entry.TopUpForSignalID != "" {
		headers = append(headers, kafka.Header{
			Key: "top_up_for_signal_id", Value: []byte(entry.TopUpForSignalID),
		})
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := writer.WriteMessages(writeCtx, kafka.Message{
		Key:     []byte(entry.SignalID),
		Value:   body,
		Headers: headers,
	}); err != nil {
		logger.Error("rebalancer.PublishOrders: kafka write failed",
			zap.String("order_type", orderType),
			zap.String("user", cfg.UserID),
			zap.String("symbol", entry.Symbol),
			zap.String("signal_id", entry.SignalID),
			zap.Error(err))
		// Decision row stays PROPOSED — the 60s reaper will TIMED_OUT it
		// after the 2min window, so no manual cleanup is needed.
		return err
	}

	// Phase C — flip the decision to DISPATCHED. Best-effort; if this
	// fails the FillConsumer/projector will still flip directly to
	// CONFIRMED on broker fill (status check is "NOT IN ('CONFIRMED','CLOSED')").
	if err := MarkDecisionDispatched(ctx, tradingDB, entry.SignalID); err != nil {
		logger.Warn("rebalancer.PublishOrders: mark DISPATCHED failed (non-fatal)",
			zap.String("signal_id", entry.SignalID), zap.Error(err))
	}

	logger.Info("rebalancer.PublishOrders: published",
		zap.String("order_type", orderType),
		zap.String("user", cfg.UserID),
		zap.String("symbol", entry.Symbol),
		zap.String("signal_id", entry.SignalID),
		zap.String("top_up_for", entry.TopUpForSignalID),
		zap.Int32("qty", entry.Quantity),
		zap.Float64("invested", entry.InvestedAmt))
	return nil
}

func buildEntryMessage(cfg StrategyConfig, e PlannedEntry) manthanEntryMessage {
	return manthanEntryMessage{
		OrderID:       e.SignalID, // OrderID == SignalID; trade-execution echoes it back on every event for idempotency
		UserID:        cfg.UserID,
		StrategyID:    cfg.StrategyID,
		Symbol:        e.Symbol,
		ISIN:          e.ISIN,
		Exchange:      "NSE",
		OrderType:     "MARKET",
		OrderSide:     "BUY",
		ProductType:   "DELIVERY",
		Quantity:      e.Quantity,
		EntryPrice:    e.EntryPrice,
		StopLoss:      e.StopLoss,
		StopLossType:  "TRAILING",
		StopLossPct:   cfg.StopLossPct,
		TrailingSLPct: cfg.TrailingSLPct,
		InvestedAmt:   e.InvestedAmt,
		TxnCostPct:    txnCostFraction * 100, // 0.33 -> serialized as 0.33 percent
		Industry:      e.Industry,
		MCapBucket:    e.MCapBucket,
		IndexName:     e.IndexName,
		EMAAllocPct:   e.EMAFraction * 100,
		BearerToken:      cfg.BearerToken,
		AppId:            cfg.AppID,
		Source:           cfg.Source,
		TradingMode:      cfg.TradingMode,
		Timestamp:        time.Now(),
		TopUpForSignalID: e.TopUpForSignalID,
	}
}
