package manthan

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ManthanPublisher publishes to all 3 sinks: PostgreSQL, Kafka, Redis.
//
// Kafka topics:
//   - trade-signals          → entry/exit orders (consumed by trade-execution)
//   - portfolio.allocations  → portfolio state changes (consumed by frontend/monitoring)
//
// Redis keys:
//   - manthan:portfolio:{strategy_id}        → current portfolio JSON snapshot
//   - manthan:position:{strategy_id}:{symbol} → individual position state
//   - manthan:positions:active                → set of all active {strategy_id}:{symbol}
//
// PostgreSQL tables:
//   - manthan_positions        → per-position lifecycle (entry → SL trail → exit)
//   - manthan_portfolio_state  → portfolio-level summary (capital, P&L)
//   - manthan_cooldown         → re-entry cooldown tracking
//   - manthan_signal_decisions → intent log (when DecisionLogEnabled=true)
//
// Decision-log mode (DecisionLogEnabled=true):
//   On entry, writes manthan_signal_decisions (status=PROPOSED) instead of
//   manthan_positions. Status flips to DISPATCHED after Kafka publish to
//   trade-signals succeeds. The CONFIRMED/REJECTED transitions are owned by
//   FillConsumer reacting to broker events. This makes "ghost positions"
//   (DB row written but broker rejected/never placed) impossible.
//
//   When false (default), preserves legacy optimistic write to
//   manthan_positions for safe rollout.
type ManthanPublisher struct {
	db                 *sql.DB
	rdb                *redis.Client
	tradeWriter        *kafka.Writer // trade-signals topic
	portfolioWriter    *kafka.Writer // portfolio.allocations topic
	logger             *zap.Logger
	decisionLogEnabled bool
}

type ManthanPublisherConfig struct {
	DB                 *sql.DB
	Redis              *redis.Client
	KafkaBrokers       []string
	DecisionLogEnabled bool // gate for new manthan_signal_decisions write path
}

func NewManthanPublisher(cfg ManthanPublisherConfig, logger *zap.Logger) *ManthanPublisher {
	p := &ManthanPublisher{
		db:                 cfg.DB,
		rdb:                cfg.Redis,
		logger:             logger,
		decisionLogEnabled: cfg.DecisionLogEnabled,
	}
	if cfg.DecisionLogEnabled {
		logger.Info("Manthan decision-log mode ENABLED — writing manthan_signal_decisions on entry (skip optimistic manthan_positions write)")
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

// --- OrderPublisher interface implementation ---

func (p *ManthanPublisher) PublishEntryOrder(ctx context.Context, order ManthanOrder) error {
	// 1. DB write — branch by mode.
	//    decisionLogEnabled=false → legacy optimistic write to manthan_positions
	//    decisionLogEnabled=true  → write manthan_signal_decisions (PROPOSED)
	//                               manthan_positions stays empty until broker fills
	if p.db != nil {
		if p.decisionLogEnabled {
			if err := p.dbInsertDecision(ctx, order); err != nil {
				p.logger.Error("DB insert signal_decision failed",
					zap.String("symbol", order.Symbol),
					zap.String("signal_id", order.OrderID),
					zap.Error(err))
			}
		} else {
			if err := p.dbInsertPosition(ctx, order); err != nil {
				p.logger.Error("DB insert position failed", zap.String("symbol", order.Symbol), zap.Error(err))
			}
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
			},
		}); err != nil {
			p.logger.Error("Kafka trade-signals publish failed", zap.Error(err))
		} else {
			kafkaOK = true
		}
	}

	// 2b. Flip decision PROPOSED → DISPATCHED only after Kafka publish succeeds.
	//     If Kafka failed, the decision stays PROPOSED so the reaper or a retry
	//     can pick it up. We never lie about dispatch state.
	if p.decisionLogEnabled && p.db != nil && kafkaOK {
		if err := p.markDecisionDispatched(ctx, order.OrderID); err != nil {
			p.logger.Warn("Mark decision DISPATCHED failed (will reconcile on next event)",
				zap.String("signal_id", order.OrderID), zap.Error(err))
		}
	}

	// 3. Kafka — portfolio.allocations (best-effort notification topic for
	// the frontend WS; failure should NOT bubble up to the caller, but
	// silent drops during a Kafka outage mean the frontend stops updating
	// with no log signal).
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

	// 4. Redis — position state + active set
	if p.rdb != nil {
		posKey := fmt.Sprintf("manthan:position:%s:%s", order.StrategyID, order.Symbol)
		setMember := fmt.Sprintf("%s:%s", order.StrategyID, order.Symbol)
		posData, _ := json.Marshal(map[string]any{
			"symbol":       order.Symbol,
			"entry_price":  order.EntryPrice,
			"quantity":     order.Quantity,
			"invested":     order.InvestedAmt,
			"stop_loss":    order.StopLoss,
			"high":         order.EntryPrice,
			"status":       "ACTIVE",
			"entry_time":   order.Timestamp.Format(time.RFC3339),
			"industry":     order.Industry,
			"mcap_bucket":  order.MCapBucket,
			"ema_pct":      order.EMAAllocPct,
			"trading_mode": order.TradingMode,
		})
		if err := p.rdb.Set(ctx, posKey, posData, 30*24*time.Hour).Err(); err != nil {
			p.logger.Warn("PublishEntryOrder: Redis Set failed",
				zap.String("key", posKey), zap.Error(err))
		}
		if err := p.rdb.SAdd(ctx, "manthan:positions:active", setMember).Err(); err != nil {
			p.logger.Warn("PublishEntryOrder: Redis SAdd (active set) failed",
				zap.String("member", setMember), zap.Error(err))
		}
	}

	return nil
}

func (p *ManthanPublisher) PublishSLModify(ctx context.Context, order SLModifyOrder) error {
	// 1. DB — update position SL (LEGACY path; the CQRS projector owns this
	// when DecisionLogEnabled=true). Silent failure here meant DB.current_sl
	// drifted from the broker truth and the next trail recomputation could
	// LOWER the stop.
	if p.db != nil {
		if _, err := p.db.ExecContext(ctx, `
			UPDATE manthan_positions
			SET current_sl = $1, high_since_entry = $2, last_trail_level = $2, updated_at = NOW()
			WHERE strategy_id = $3 AND symbol = $4 AND status = 'ACTIVE'`,
			order.NewSL, order.NewHigh, order.StrategyID, order.Symbol); err != nil {
			p.logger.Warn("PublishSLModify: DB update failed",
				zap.String("symbol", order.Symbol),
				zap.Float64("new_sl", order.NewSL), zap.Error(err))
		}
	}

	// 2. Kafka — trade-signals (SL modify). CRITICAL outbound: this is what
	// drives trade-execution to update the broker SL. Silent failure here
	// was the worst class — rules-engine THINKS it told the broker, but didn't.
	if p.tradeWriter != nil {
		body, _ := json.Marshal(order)
		if err := p.tradeWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(order.OrderID),
			Value: body,
			Headers: []kafka.Header{
				{Key: "order_type", Value: []byte("MANTHAN_SL_MODIFY")},
				{Key: "user_id", Value: []byte(order.UserID)},
				{Key: "symbol", Value: []byte(order.Symbol)},
			},
		}); err != nil {
			p.logger.Error("PublishSLModify: Kafka trade-signals publish failed (broker SL NOT updated)",
				zap.String("symbol", order.Symbol),
				zap.String("order_id", order.OrderID),
				zap.Float64("new_sl", order.NewSL), zap.Error(err))
		}
	}

	// 3. Kafka — portfolio.allocations (best-effort notification)
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

	// 4. Redis — update position
	if p.rdb != nil {
		posKey := fmt.Sprintf("manthan:position:%s:%s", order.StrategyID, order.Symbol)
		existing, err := p.rdb.Get(ctx, posKey).Bytes()
		if err == nil {
			var pos map[string]any
			if json.Unmarshal(existing, &pos) == nil {
				pos["stop_loss"] = order.NewSL
				pos["high"] = order.NewHigh
				updated, _ := json.Marshal(pos)
				if err := p.rdb.Set(ctx, posKey, updated, 30*24*time.Hour).Err(); err != nil {
					p.logger.Warn("PublishSLModify: Redis Set failed",
						zap.String("key", posKey), zap.Error(err))
				}
			}
		}
	}

	return nil
}

func (p *ManthanPublisher) PublishSLExit(ctx context.Context, order SLExitOrder) error {
	// 1. DB — mark position exited (LEGACY path). Silent failure here meant
	// the row stayed ACTIVE in DB → next restart, rehydrate brings the
	// position back to memory as ACTIVE → ghost position the broker has
	// already closed.
	if p.db != nil {
		if _, err := p.db.ExecContext(ctx, `
			UPDATE manthan_positions
			SET status = 'EXITED', exit_price = $1, realized_pnl = $2,
			    exit_reason = 'SL_TRIGGERED', exit_time = NOW(), updated_at = NOW()
			WHERE strategy_id = $3 AND symbol = $4 AND status = 'ACTIVE'`,
			order.ExitPrice, order.PnL, order.StrategyID, order.Symbol); err != nil {
			p.logger.Warn("PublishSLExit: DB update failed",
				zap.String("symbol", order.Symbol),
				zap.Float64("exit_price", order.ExitPrice), zap.Error(err))
		}
	}

	// 2. Kafka — trade-signals (sell order). CRITICAL outbound: this is what
	// drives trade-execution to actually exit at the broker. Silent failure
	// would leave the position open with rules-engine thinking it's closed.
	if p.tradeWriter != nil {
		body, _ := json.Marshal(order)
		if err := p.tradeWriter.WriteMessages(ctx, kafka.Message{
			Key:   []byte(order.OrderID),
			Value: body,
			Headers: []kafka.Header{
				{Key: "order_type", Value: []byte("MANTHAN_SL_EXIT")},
				{Key: "user_id", Value: []byte(order.UserID)},
				{Key: "symbol", Value: []byte(order.Symbol)},
			},
		}); err != nil {
			p.logger.Error("PublishSLExit: Kafka trade-signals publish failed (broker NOT notified to exit)",
				zap.String("symbol", order.Symbol),
				zap.String("order_id", order.OrderID), zap.Error(err))
		}
	}

	// 3. Kafka — portfolio.allocations (best-effort notification)
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

	// 4. Redis — remove from active, update position
	if p.rdb != nil {
		member := fmt.Sprintf("%s:%s", order.StrategyID, order.Symbol)
		if err := p.rdb.SRem(ctx, "manthan:positions:active", member).Err(); err != nil {
			p.logger.Warn("PublishSLExit: Redis SRem failed",
				zap.String("member", member), zap.Error(err))
		}

		posKey := fmt.Sprintf("manthan:position:%s:%s", order.StrategyID, order.Symbol)
		posData, _ := json.Marshal(map[string]any{
			"symbol":     order.Symbol,
			"status":     "EXITED",
			"exit_price": order.ExitPrice,
			"pnl":        order.PnL,
			"sl_price":   order.SLPrice,
			"exit_time":  time.Now().UTC().Format(time.RFC3339),
		})
		if err := p.rdb.Set(ctx, posKey, posData, 7*24*time.Hour).Err(); err != nil {
			p.logger.Warn("PublishSLExit: Redis Set failed",
				zap.String("key", posKey), zap.Error(err))
		}
	}

	return nil
}

// UpdatePositionFill reconciles the manthan_positions row and Redis cache with
// the actual broker fill price after a FILL_CONFIRMED event. Called from
// FillConsumer AFTER PortfolioManager.ProcessFillEvent has updated in-memory
// state — this persists that update so a rules-engine restart rehydrates the
// real price (not the optimistic LTP-at-signal-time).
//
// Also recalculates current_sl from the real fill price (previously computed
// from optimistic entry). high_since_entry and last_trail_level are reset to
// the real fill price since trailing starts from there.
func (p *ManthanPublisher) UpdatePositionFill(ctx context.Context, strategyID, symbol string, fillPrice float64, filledQty int32, slPrice float64) {
	invested := fillPrice * float64(filledQty)

	if p.db != nil {
		_, err := p.db.ExecContext(ctx, `
			UPDATE manthan_positions
			SET entry_price      = $1,
			    invested_amt     = $2,
			    current_sl       = $3,
			    high_since_entry = $1,
			    last_trail_level = $1,
			    quantity         = $4,
			    updated_at       = NOW()
			WHERE strategy_id = $5 AND symbol = $6 AND status = 'ACTIVE'`,
			fillPrice, invested, slPrice, filledQty, strategyID, symbol)
		if err != nil {
			p.logger.Warn("UpdatePositionFill DB update failed",
				zap.String("strategy_id", strategyID),
				zap.String("symbol", symbol), zap.Error(err))
		}
	}

	if p.rdb != nil {
		posKey := fmt.Sprintf("manthan:position:%s:%s", strategyID, symbol)
		existing, err := p.rdb.Get(ctx, posKey).Bytes()
		if err == nil {
			var pos map[string]any
			if json.Unmarshal(existing, &pos) == nil {
				pos["entry_price"] = fillPrice
				pos["invested"] = invested
				pos["stop_loss"] = slPrice
				pos["high"] = fillPrice
				pos["quantity"] = filledQty
				updated, _ := json.Marshal(pos)
				if err := p.rdb.Set(ctx, posKey, updated, 30*24*time.Hour).Err(); err != nil {
					p.logger.Warn("UpdatePositionFill: Redis Set failed",
						zap.String("key", posKey), zap.Error(err))
				}
			}
		}
	}
}

// UpdatePositionSL writes the broker's actual SL trigger price back to
// manthan_positions and the Redis cache. Called from FillConsumer when a
// SL_PLACED or SL_MODIFIED event arrives. Only used in LEGACY mode — when
// the CQRS projector is enabled it owns the manthan_positions update via
// its transactional path (see position_projector.go SL_PLACED / SL_MODIFIED).
//
// Critical for the trailing-SL contract: without this, the broker may have
// DPR-clamped our initial SL to a tighter trigger (e.g. requested 332.56 →
// accepted 390), but manthan_positions.current_sl still shows our requested
// value. Subsequent trail math computed off the stale DB row would emit
// SL_MODIFY signals BELOW the broker's actual trigger — lowering the stop.
//
// Idempotent: same trigger arriving twice produces an identical UPDATE.
// `WHERE status='ACTIVE'` filters out racing EXIT updates.
func (p *ManthanPublisher) UpdatePositionSL(ctx context.Context, strategyID, symbol string, brokerTrigger float64) {
	if brokerTrigger <= 0 {
		return
	}
	if p.db != nil {
		if _, err := p.db.ExecContext(ctx, `
			UPDATE manthan_positions
			SET current_sl = $1, updated_at = NOW()
			WHERE strategy_id = $2 AND symbol = $3 AND status = 'ACTIVE'`,
			brokerTrigger, strategyID, symbol); err != nil {
			p.logger.Warn("UpdatePositionSL DB update failed",
				zap.String("strategy_id", strategyID),
				zap.String("symbol", symbol),
				zap.Float64("trigger", brokerTrigger),
				zap.Error(err))
		}
	}

	if p.rdb != nil {
		posKey := fmt.Sprintf("manthan:position:%s:%s", strategyID, symbol)
		existing, err := p.rdb.Get(ctx, posKey).Bytes()
		if err == nil {
			var pos map[string]any
			if json.Unmarshal(existing, &pos) == nil {
				pos["stop_loss"] = brokerTrigger
				if updated, mErr := json.Marshal(pos); mErr == nil {
					if err := p.rdb.Set(ctx, posKey, updated, 30*24*time.Hour).Err(); err != nil {
						p.logger.Warn("UpdatePositionSL: Redis Set failed",
							zap.String("key", posKey), zap.Error(err))
					}
				}
			}
		}
	}
}

// UpdatePortfolioState syncs portfolio summary to DB + Redis.
// Called after every entry/exit to keep state current.
func (p *ManthanPublisher) UpdatePortfolioState(ctx context.Context, portfolio *Portfolio) {
	// Snapshot all fields we need under a single RLock. Releasing before the
	// DB + Redis round-trips below keeps the lock window short and avoids
	// holding it across IO.
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

	// DB
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

	// Redis
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

func (p *ManthanPublisher) dbInsertPosition(ctx context.Context, order ManthanOrder) error {
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO manthan_positions (
			strategy_id, user_id, symbol, isin, industry, mcap_bucket, index_name,
			entry_price, quantity, invested_amt, ema_alloc_pct,
			high_since_entry, current_sl, last_trail_level, status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,'ACTIVE')`,
		order.StrategyID, order.UserID, order.Symbol, order.ISIN,
		order.Industry, order.MCapBucket, order.IndexName,
		order.EntryPrice, order.Quantity, order.InvestedAmt, order.EMAAllocPct/100,
		order.EntryPrice, order.StopLoss, order.EntryPrice)
	return err
}

// dbInsertDecision records the entry intent in manthan_signal_decisions with
// status='PROPOSED'. Used when DecisionLogEnabled=true. The signal_id is the
// order's server-generated UUID (also the Kafka message key, so trade-execution
// can echo it back on every broker event for idempotent reconciliation).
//
// ON CONFLICT DO NOTHING makes this safe under accidental retry: if the same
// signal_id is inserted twice (e.g. catch-up + live consumer race), the second
// INSERT is a no-op rather than a constraint violation that aborts the tx.
func (p *ManthanPublisher) dbInsertDecision(ctx context.Context, order ManthanOrder) error {
	_, err := p.db.ExecContext(ctx, `
		INSERT INTO manthan_signal_decisions (
			signal_id, user_id, strategy_id, symbol, isin,
			ltp_at_decision, ema_alloc_pct, intended_qty, intended_invested,
			initial_sl_target, industry, mcap_bucket, index_name, status
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'PROPOSED')
		ON CONFLICT (signal_id) DO NOTHING`,
		order.OrderID, order.UserID, order.StrategyID, order.Symbol, order.ISIN,
		order.EntryPrice, order.EMAAllocPct/100, order.Quantity, order.InvestedAmt,
		order.StopLoss, order.Industry, order.MCapBucket, order.IndexName)
	return err
}

// markDecisionDispatched flips PROPOSED → DISPATCHED after the Kafka publish
// to trade-signals succeeded. We use a guarded UPDATE (only flip from
// PROPOSED) so a late call after the FillConsumer has already set CONFIRMED
// won't regress the status.
func (p *ManthanPublisher) markDecisionDispatched(ctx context.Context, signalID string) error {
	_, err := p.db.ExecContext(ctx, `
		UPDATE manthan_signal_decisions
		SET status = 'DISPATCHED', dispatched_at = NOW()
		WHERE signal_id = $1 AND status = 'PROPOSED'`,
		signalID)
	return err
}
