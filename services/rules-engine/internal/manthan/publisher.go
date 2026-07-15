package manthan

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// ErrDuplicateDecision is returned by dbInsertEntryDecision /
// dbInsertSLModifyDecision / dbInsertExitDecision when the row for this
// signal_id already exists (ON CONFLICT DO NOTHING short-circuited the
// insert). The publish functions catch it and SKIP the Kafka publish +
// mark-dispatched steps — the previous invocation already did all that,
// and re-publishing would produce a duplicate downstream order.
//
// This is the ONLY signal in rules-engine that says "this signal has
// already been handled end-to-end; do nothing." Every other error
// classifies as failure and gets retried.
var ErrDuplicateDecision = errors.New("manthan: signal_id already dispatched (duplicate decision)")

// ManthanPublisher publishes rules-engine's signals to the world.
//
// Kafka topics (both stay in rules-engine, per docs/rules_engine_refactor.md §4.5):
//   - trade-signals          → entry/SL-modify/exit orders (consumed by trade-execution)
//   - portfolio.allocations  → portfolio state changes (consumed by frontend/monitoring)
//
// Postgres writes (audit / outbox only):
//   - manthan_signal_decisions — INSERT one row per signal fired, then
//     UPDATE dispatched_at after Kafka ACK (transactional outbox pattern).
//     Row content differs by signal_type:
//       ENTRY_BUY   — allocator entry decision (existing columns populated)
//       SL_MODIFY   — trailing SL ratchet (payload JSONB)
//       EXIT_TSL    — TSL crossed → exit (payload JSONB)
//   - manthan_portfolio_state  — LEGACY UpdatePortfolioState still called from
//     consumer.go on batch end. Belongs to portfolio svc long-term; stays here
//     as a stub until that service exists.
//
// What this file NO LONGER does (deleted 2026-07-10 with the projector):
//   - Write manthan_positions (positions svc will own; nobody writes it today)
//   - Write manthan_position_events (positions svc)
//   - Update Redis :position: keys (positions svc will own)
//   - Notify users (moves to notification svc)
type ManthanPublisher struct {
	db              *sql.DB
	rdb             *redis.Client
	tradeWriter     *kafka.Writer // trade-signals topic
	portfolioWriter *kafka.Writer // portfolio.allocations topic
	logger          *zap.Logger
}

type ManthanPublisherConfig struct {
	DB           *sql.DB
	Redis        *redis.Client
	KafkaBrokers []string
}

func NewManthanPublisher(cfg ManthanPublisherConfig, logger *zap.Logger) *ManthanPublisher {
	p := &ManthanPublisher{
		db:     cfg.DB,
		rdb:    cfg.Redis,
		logger: logger,
	}

	if len(cfg.KafkaBrokers) > 0 && cfg.KafkaBrokers[0] != "" {
		p.tradeWriter = &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Topic:        "trade-signals",
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 1 * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
		}
		p.portfolioWriter = &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Topic:        "portfolio.allocations",
			Balancer:     &kafka.LeastBytes{},
			BatchTimeout: 1 * time.Millisecond,
			RequiredAcks: kafka.RequireAll,
		}
		logger.Info("Manthan Kafka publishers initialized",
			zap.String("trade_topic", "trade-signals"),
			zap.String("portfolio_topic", "portfolio.allocations"))
	}

	return p
}

func (p *ManthanPublisher) Close() {
	if p.tradeWriter != nil {
		_ = p.tradeWriter.Close()
	}
	if p.portfolioWriter != nil {
		_ = p.portfolioWriter.Close()
	}
}

// ────────────────────────────────────────────────────────────────────
// Entry — signal_type='ENTRY_BUY'
// ────────────────────────────────────────────────────────────────────

// PublishEntryOrder fires a MANTHAN entry signal:
//  1. INSERT signal_decisions row with status='PROPOSED' (outbox)
//  2. Publish to Kafka trade-signals
//  3. On Kafka ACK: UPDATE status='DISPATCHED', dispatched_at=NOW()
//  4. Publish to Kafka portfolio.allocations (best-effort)
//
// Startup recovery: rows stuck at PROPOSED with no dispatched_at get
// republished by the recovery worker (to be added; deferred until stale
// rows become a real problem).
func (p *ManthanPublisher) PublishEntryOrder(ctx context.Context, order ManthanOrder) error {
	// 1. Audit INSERT (status='PROPOSED') — outbox row.
	//    On ErrDuplicateDecision, this signal was already dispatched on a
	//    prior call (Kafka replay, rules-engine restart, manthan-live re-fire).
	//    Skip Kafka publish + portfolio broadcast — the previous invocation
	//    already did those. Returning nil here tells the consumer to commit
	//    the Kafka offset and move on: the work is done, redundantly.
	if p.db != nil {
		if err := p.dbInsertEntryDecision(ctx, order); err != nil {
			if errors.Is(err, ErrDuplicateDecision) {
				p.logger.Info("PublishEntryOrder: signal already dispatched — skipping Kafka publish (idempotent replay)",
					zap.String("symbol", order.Symbol),
					zap.String("signal_id", order.OrderID))
				return nil
			}
			p.logger.Error("PublishEntryOrder: signal_decisions INSERT failed",
				zap.String("symbol", order.Symbol),
				zap.String("signal_id", order.OrderID),
				zap.Error(err))
		}
	}

	// 2. Kafka — trade-signals
	kafkaOK := false
	if p.tradeWriter != nil {
		body, _ := json.Marshal(order)
		if err := p.tradeWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(order.OrderID),
			Value: body,
			Headers: []kafka.Header{
				{Key: "order_type", Value: []byte("MANTHAN_ENTRY")},
				{Key: "user_id", Value: []byte(order.UserID)},
				{Key: "strategy_id", Value: []byte(order.StrategyID)},
				{Key: "symbol", Value: []byte(order.Symbol)},
				{Key: "signal_id", Value: []byte(order.OrderID)},
				{Key: "signal_type", Value: []byte("ENTRY_BUY")},
			},
		}); err != nil {
			p.logger.Error("Kafka trade-signals publish failed", zap.Error(err))
		} else {
			kafkaOK = true
		}
	}

	// 3. Mark DISPATCHED after Kafka ACK. If Kafka failed, stays PROPOSED for
	//    retry by a future recovery worker.
	if p.db != nil && kafkaOK {
		if err := p.markDecisionDispatched(ctx, order.OrderID); err != nil {
			p.logger.Warn("PublishEntryOrder: mark DISPATCHED failed",
				zap.String("signal_id", order.OrderID), zap.Error(err))
		}
	}

	// 4. Kafka — portfolio.allocations (best-effort, feeds frontend)
	if p.portfolioWriter != nil {
		event := map[string]any{
			"type":        "POSITION_OPENED",
			"user_id":     order.UserID,
			"strategy_id": order.StrategyID,
			"symbol":      order.Symbol,
			"quantity":    order.Quantity,
			"entry_price": order.EntryPrice,
			"stop_loss":   order.StopLoss,
			"invested":    order.InvestedAmt,
			"ema_pct":     order.EMAAllocPct,
			"industry":    order.Industry,
			"mcap_bucket": order.MCapBucket,
			"mode":        order.TradingMode,
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
		}
		body, _ := json.Marshal(event)
		if err := p.portfolioWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("%s:%s", order.StrategyID, order.Symbol)),
			Value: body,
		}); err != nil {
			p.logger.Warn("portfolio.allocations publish failed (POSITION_OPENED)",
				zap.String("symbol", order.Symbol), zap.Error(err))
		}
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────
// SL modify — signal_type='SL_MODIFY'
// ────────────────────────────────────────────────────────────────────

// PublishSLModify fires a trailing SL ratchet signal:
//  1. INSERT audit row (signal_type='SL_MODIFY', parent = the entry signal)
//  2. Publish Kafka trade-signals so trade-execution modifies the broker SL
//  3. On Kafka ACK: UPDATE dispatched_at
//  4. Publish portfolio.allocations (best-effort notification)
//
// No more manthan_positions UPDATE (was here — moves to positions svc).
func (p *ManthanPublisher) PublishSLModify(ctx context.Context, order SLModifyOrder) error {
	// 1. Audit
	//    ErrDuplicateDecision short-circuit — see PublishEntryOrder doc.
	//    Same-level SL modifications from tick oscillation dedup here.
	if p.db != nil {
		if err := p.dbInsertSLModifyDecision(ctx, order); err != nil {
			if errors.Is(err, ErrDuplicateDecision) {
				p.logger.Info("PublishSLModify: signal already dispatched — skipping (same SL level already modified)",
					zap.String("symbol", order.Symbol),
					zap.String("signal_id", order.OrderID),
					zap.Float64("new_sl", order.NewSL))
				return nil
			}
			p.logger.Warn("PublishSLModify: signal_decisions INSERT failed",
				zap.String("symbol", order.Symbol),
				zap.String("signal_id", order.OrderID),
				zap.Error(err))
		}
	}

	// 2. Kafka — trade-signals (critical: without this, broker SL never updates)
	kafkaOK := false
	if p.tradeWriter != nil {
		body, _ := json.Marshal(order)
		if err := p.tradeWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(order.OrderID),
			Value: body,
			Headers: []kafka.Header{
				{Key: "order_type", Value: []byte("MANTHAN_SL_MODIFY")},
				{Key: "user_id", Value: []byte(order.UserID)},
				{Key: "symbol", Value: []byte(order.Symbol)},
				{Key: "signal_id", Value: []byte(order.OrderID)},
				{Key: "signal_type", Value: []byte("SL_MODIFY")},
			},
		}); err != nil {
			p.logger.Error("PublishSLModify: Kafka publish failed (broker SL NOT updated)",
				zap.String("symbol", order.Symbol),
				zap.String("order_id", order.OrderID),
				zap.Float64("new_sl", order.NewSL), zap.Error(err))
		} else {
			kafkaOK = true
		}
	}

	// 3. Mark DISPATCHED
	if p.db != nil && kafkaOK {
		if err := p.markDecisionDispatched(ctx, order.OrderID); err != nil {
			p.logger.Warn("PublishSLModify: mark DISPATCHED failed",
				zap.String("signal_id", order.OrderID), zap.Error(err))
		}
	}

	// 4. Kafka — portfolio.allocations
	if p.portfolioWriter != nil {
		event := map[string]any{
			"type":        "SL_MODIFIED",
			"user_id":     order.UserID,
			"strategy_id": order.StrategyID,
			"symbol":      order.Symbol,
			"old_sl":      order.OldSL,
			"new_sl":      order.NewSL,
			"new_high":    order.NewHigh,
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
		}
		body, _ := json.Marshal(event)
		if err := p.portfolioWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("%s:%s", order.StrategyID, order.Symbol)),
			Value: body,
		}); err != nil {
			p.logger.Warn("portfolio.allocations publish failed (SL_MODIFIED)",
				zap.String("symbol", order.Symbol), zap.Error(err))
		}
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────
// SL exit (TSL crossed) — signal_type='EXIT_TSL'
// ────────────────────────────────────────────────────────────────────

// PublishSLExit fires a TSL-triggered exit signal:
//  1. INSERT audit row (signal_type='EXIT_TSL', parent = the entry signal)
//  2. Publish Kafka trade-signals so trade-execution places the broker sell
//  3. On Kafka ACK: UPDATE dispatched_at
//  4. Publish portfolio.allocations (best-effort notification)
//
// No more manthan_positions UPDATE (was here — moves to positions svc).
// No more Redis cache mutation (moves to positions svc).
func (p *ManthanPublisher) PublishSLExit(ctx context.Context, order SLExitOrder) error {
	// 1. Audit
	//    ErrDuplicateDecision short-circuit — see PublishEntryOrder doc.
	//    Same-level SL exits (tick oscillates across trigger) dedup here.
	if p.db != nil {
		if err := p.dbInsertExitDecision(ctx, order); err != nil {
			if errors.Is(err, ErrDuplicateDecision) {
				p.logger.Info("PublishSLExit: signal already dispatched — skipping (same SL exit already sent)",
					zap.String("symbol", order.Symbol),
					zap.String("signal_id", order.OrderID),
					zap.Float64("sl_price", order.SLPrice))
				return nil
			}
			p.logger.Warn("PublishSLExit: signal_decisions INSERT failed",
				zap.String("symbol", order.Symbol),
				zap.String("signal_id", order.OrderID),
				zap.Error(err))
		}
	}

	// 2. Kafka — trade-signals (critical)
	kafkaOK := false
	if p.tradeWriter != nil {
		body, _ := json.Marshal(order)
		if err := p.tradeWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(order.OrderID),
			Value: body,
			Headers: []kafka.Header{
				{Key: "order_type", Value: []byte("MANTHAN_SL_EXIT")},
				{Key: "user_id", Value: []byte(order.UserID)},
				{Key: "symbol", Value: []byte(order.Symbol)},
				{Key: "signal_id", Value: []byte(order.OrderID)},
				{Key: "signal_type", Value: []byte("EXIT_TSL")},
			},
		}); err != nil {
			p.logger.Error("PublishSLExit: Kafka publish failed (broker NOT notified to exit)",
				zap.String("symbol", order.Symbol),
				zap.String("order_id", order.OrderID), zap.Error(err))
		} else {
			kafkaOK = true
		}
	}

	// 3. Mark DISPATCHED
	if p.db != nil && kafkaOK {
		if err := p.markDecisionDispatched(ctx, order.OrderID); err != nil {
			p.logger.Warn("PublishSLExit: mark DISPATCHED failed",
				zap.String("signal_id", order.OrderID), zap.Error(err))
		}
	}

	// 4. Kafka — portfolio.allocations
	if p.portfolioWriter != nil {
		event := map[string]any{
			"type":        "POSITION_EXITED",
			"user_id":     order.UserID,
			"strategy_id": order.StrategyID,
			"symbol":      order.Symbol,
			"exit_price":  order.ExitPrice,
			"sl_price":    order.SLPrice,
			"pnl":         order.PnL,
			"quantity":    order.Quantity,
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
		}
		body, _ := json.Marshal(event)
		if err := p.portfolioWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("%s:%s", order.StrategyID, order.Symbol)),
			Value: body,
		}); err != nil {
			p.logger.Warn("portfolio.allocations publish failed (POSITION_EXITED)",
				zap.String("symbol", order.Symbol), zap.Error(err))
		}
	}

	return nil
}

// ────────────────────────────────────────────────────────────────────
// Portfolio state (legacy — belongs to portfolio svc)
// ────────────────────────────────────────────────────────────────────

// UpdatePortfolioState syncs portfolio summary to DB + Redis.
// Called from consumer.go after every batch of entries.
//
// STAYS as legacy until portfolio svc exists (per plan §4.5). At that point
// this method + manthan_portfolio_state table become portfolio svc's job.
func (p *ManthanPublisher) UpdatePortfolioState(ctx context.Context, portfolio *types.Portfolio) {
	// Snapshot fields under a single RLock — keeps window short, no I/O held.
	portfolio.Mu.RLock()
	activeCount := 0
	for _, pos := range portfolio.Positions {
		if pos.Active {
			activeCount++
		}
	}
	strategyID := portfolio.StrategyID
	userID := portfolio.UserID
	initialCapital := portfolio.InitialCapital
	currentCapital := portfolio.CurrentCapital
	maxPositions := portfolio.MaxPositions
	perStockBase := portfolio.PerStockBase
	portfolio.Mu.RUnlock()

	if p.db != nil {
		if _, err := p.db.ExecContext(ctx, `
			INSERT INTO manthan_portfolio_state
				(strategy_id, user_id, initial_capital, current_capital, max_positions, per_stock_base, active_count, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
			ON CONFLICT (strategy_id) DO UPDATE SET
				current_capital = EXCLUDED.current_capital,
				per_stock_base  = EXCLUDED.per_stock_base,
				active_count    = EXCLUDED.active_count,
				updated_at      = NOW()`,
			strategyID, userID,
			initialCapital, currentCapital,
			maxPositions, perStockBase, activeCount); err != nil {
			p.logger.Warn("UpdatePortfolioState: DB upsert failed",
				zap.String("strategy_id", strategyID), zap.Error(err))
		}
	}

	if p.rdb != nil {
		key := fmt.Sprintf("manthan:portfolio:%s", strategyID)
		data, _ := json.Marshal(map[string]any{
			"user_id":         userID,
			"strategy_id":     strategyID,
			"initial_capital": initialCapital,
			"current_capital": currentCapital,
			"max_positions":   maxPositions,
			"per_stock_base":  perStockBase,
			"active_count":    activeCount,
			"updated_at":      time.Now().UTC().Format(time.RFC3339),
		})
		if err := p.rdb.Set(ctx, key, data, 30*24*time.Hour).Err(); err != nil {
			p.logger.Warn("UpdatePortfolioState: Redis Set failed",
				zap.String("key", key), zap.Error(err))
		}
	}
}

// ────────────────────────────────────────────────────────────────────
// signal_decisions writers (per signal_type)
// ────────────────────────────────────────────────────────────────────

// dbInsertEntryDecision writes the ENTRY_BUY row for a fresh decision.
// Returns ErrDuplicateDecision if the signal_id already exists — the
// publisher uses that signal to skip Kafka re-publish + mark-dispatched
// (both were already done on the first-time-through path).
//
// Uses RETURNING signal_id to distinguish "row newly inserted" from
// "row already existed (ON CONFLICT DO NOTHING)". With DO NOTHING,
// RETURNING emits ZERO rows on conflict — QueryRow.Scan gets
// sql.ErrNoRows which we translate to ErrDuplicateDecision.
//
// Prior to 2026-07-15 the OrderIDs were random UUIDs so the ON CONFLICT
// never fired; this insert always "succeeded" as INSERT and every retry
// re-published to Kafka → duplicate broker orders. That's now fixed at
// the OrderID layer (see order.go deterministicSignalID) and this dedup
// path is the safety net that catches any residual replay.
func (p *ManthanPublisher) dbInsertEntryDecision(ctx context.Context, order ManthanOrder) error {
	var returnedID string
	err := p.db.QueryRowContext(ctx, `
		INSERT INTO manthan_signal_decisions (
			signal_id, user_id, strategy_id, symbol, isin,
			signal_type,
			ltp_at_decision, ema_alloc_pct, intended_qty, intended_invested,
			initial_sl_target, industry, mcap_bucket, index_name, status
		) VALUES ($1,$2,$3,$4,$5,'ENTRY_BUY',$6,$7,$8,$9,$10,$11,$12,$13,'PROPOSED')
		ON CONFLICT (signal_id) DO NOTHING
		RETURNING signal_id`,
		order.OrderID, order.UserID, order.StrategyID, order.Symbol, order.ISIN,
		order.EntryPrice, order.EMAAllocPct/100, order.Quantity, order.InvestedAmt,
		order.StopLoss, order.Industry, order.MCapBucket, order.IndexName,
	).Scan(&returnedID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDuplicateDecision
	}
	return err
}

// dbInsertSLModifyDecision writes the SL_MODIFY row. Entry-specific columns
// are left NULL (allowed by migration 009's chk_msd_entry_fields_required);
// type-specific fields live in payload JSONB. Parent signal_id is resolved
// via lookupParentSignalID.
func (p *ManthanPublisher) dbInsertSLModifyDecision(ctx context.Context, order SLModifyOrder) error {
	parentID, err := p.lookupParentSignalID(ctx, order.StrategyID, order.Symbol)
	if err != nil {
		// Best-effort: log and continue with NULL parent. The audit row still
		// gets written so we have a record that we fired SL_MODIFY.
		p.logger.Debug("dbInsertSLModifyDecision: parent lookup failed",
			zap.String("symbol", order.Symbol), zap.Error(err))
	}

	payload, _ := json.Marshal(map[string]any{
		"new_sl":   order.NewSL,
		"old_sl":   order.OldSL,
		"new_high": order.NewHigh,
	})

	// Same dedup pattern as dbInsertEntryDecision — see its docstring for
	// why RETURNING + sql.ErrNoRows → ErrDuplicateDecision.
	var returnedID string
	err = p.db.QueryRowContext(ctx, `
		INSERT INTO manthan_signal_decisions (
			signal_id, user_id, strategy_id, symbol,
			signal_type, parent_signal_id, payload, status
		) VALUES ($1,$2,$3,$4,'SL_MODIFY',$5,$6,'PROPOSED')
		ON CONFLICT (signal_id) DO NOTHING
		RETURNING signal_id`,
		order.OrderID, order.UserID, order.StrategyID, order.Symbol,
		nullableID(parentID), payload,
	).Scan(&returnedID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDuplicateDecision
	}
	return err
}

// dbInsertExitDecision writes the EXIT_TSL row. Same pattern as SL_MODIFY.
// Returns ErrDuplicateDecision on ON CONFLICT — see dbInsertEntryDecision doc.
func (p *ManthanPublisher) dbInsertExitDecision(ctx context.Context, order SLExitOrder) error {
	parentID, err := p.lookupParentSignalID(ctx, order.StrategyID, order.Symbol)
	if err != nil {
		p.logger.Debug("dbInsertExitDecision: parent lookup failed",
			zap.String("symbol", order.Symbol), zap.Error(err))
	}

	payload, _ := json.Marshal(map[string]any{
		"exit_price": order.ExitPrice,
		"sl_price":   order.SLPrice,
		"pnl":        order.PnL,
		"quantity":   order.Quantity,
	})

	var returnedID string
	err = p.db.QueryRowContext(ctx, `
		INSERT INTO manthan_signal_decisions (
			signal_id, user_id, strategy_id, symbol,
			signal_type, parent_signal_id, payload, status
		) VALUES ($1,$2,$3,$4,'EXIT_TSL',$5,$6,'PROPOSED')
		ON CONFLICT (signal_id) DO NOTHING
		RETURNING signal_id`,
		order.OrderID, order.UserID, order.StrategyID, order.Symbol,
		nullableID(parentID), payload,
	).Scan(&returnedID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrDuplicateDecision
	}
	return err
}

// markDecisionDispatched flips PROPOSED → DISPATCHED after Kafka ACK.
// Guarded UPDATE — only flips from PROPOSED, so a late call after positions
// svc has moved the row further (CONFIRMED etc) won't regress.
func (p *ManthanPublisher) markDecisionDispatched(ctx context.Context, signalID string) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE manthan_signal_decisions
		SET status = 'DISPATCHED', dispatched_at = NOW()
		WHERE signal_id = $1 AND status = 'PROPOSED'`,
		signalID)
	return err
}

// lookupParentSignalID finds the entry signal for (strategy_id, symbol).
// Returns the most recent DISPATCHED / CONFIRMED / PARTIAL ENTRY_BUY row.
// Returns empty string + nil error if none found (legacy positions pre-schema
// don't have decisions rows; those SL_MODIFY / EXIT_TSL rows get NULL parent).
func (p *ManthanPublisher) lookupParentSignalID(ctx context.Context, strategyID, symbol string) (string, error) {
	var parentID string
	err := p.db.QueryRowContext(ctx, `
		SELECT signal_id
		FROM manthan_signal_decisions
		WHERE strategy_id = $1
		  AND symbol = $2
		  AND signal_type = 'ENTRY_BUY'
		  AND status IN ('DISPATCHED','CONFIRMED','PARTIAL')
		ORDER BY decided_at DESC
		LIMIT 1`, strategyID, symbol).Scan(&parentID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return parentID, err
}

// nullableID converts empty string to sql.NullString for FK-nullable columns.
func nullableID(id string) sql.NullString {
	if id == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: id, Valid: true}
}
