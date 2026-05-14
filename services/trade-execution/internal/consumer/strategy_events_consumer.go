package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// PaperPriceLookup looks up the current LTP for an instrument from Redis.
// Satisfied by *paper.RedisPriceClient.
type PaperPriceLookup interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
}

// configEventType mirrors the event types published by user-config service.
type configEventType string

const (
	configPaused  configEventType = "CONFIG_PAUSED"
	configDeleted configEventType = "CONFIG_DELETED"
)

type strategyEvent struct {
	Type       configEventType `json:"type"`
	UserID     string          `json:"user_id"`
	StrategyID string          `json:"strategy_id"`
	Version    uint64          `json:"version"`
}

// OrderUnwatcher removes orders from the price monitor watch list.
// Implemented by *scheduler.PriceMonitor.
type OrderUnwatcher interface {
	UnwatchByStrategy(strategyID string) int
}

// MLGroupCanceller cancels active multi-level groups for a strategy, recording
// paper partial exits for remaining qty. Implemented by *multilevel.Manager.
type MLGroupCanceller interface {
	CancelGroupsByStrategy(ctx context.Context, userID, strategyID string)
}

// StrategyEventsConsumer listens to user-config-events and closes all open
// positions / cancels all pending orders when a strategy is deactivated or deleted.
type StrategyEventsConsumer struct {
	reader       *kafka.Reader
	orderRepo    repository.OrderRepository
	executor     *executor.OrderExecutor
	priceMonitor OrderUnwatcher    // nil-safe: may be unset if PriceMonitor is disabled
	priceClient  PaperPriceLookup  // nil-safe: used to get LTP for paper exit PnL
	mlManager    MLGroupCanceller  // nil-safe: creates paper partial exits for remaining ML qty
	logger       *zap.Logger
}

// NewStrategyEventsConsumer creates a consumer for the user-config-events topic.
func NewStrategyEventsConsumer(
	brokers []string,
	orderRepo repository.OrderRepository,
	exec *executor.OrderExecutor,
	logger *zap.Logger,
) *StrategyEventsConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          "user-config-events",
		GroupID:        "trade-execution-strategy-events",
		MinBytes:       1,
		MaxBytes:       1e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	logger.Info("Strategy events consumer initialized",
		zap.Strings("brokers", brokers),
		zap.String("topic", "user-config-events"))

	return &StrategyEventsConsumer{
		reader:    reader,
		orderRepo: orderRepo,
		executor:  exec,
		logger:    logger,
	}
}

// SetPriceMonitor wires the price monitor so orders are unwatched on strategy deactivation.
func (c *StrategyEventsConsumer) SetPriceMonitor(pm OrderUnwatcher) {
	c.priceMonitor = pm
}

// SetPriceClient wires the Redis price client so paper exits use real LTP instead of entry price.
func (c *StrategyEventsConsumer) SetPriceClient(pc PaperPriceLookup) {
	c.priceClient = pc
}

// SetMLManager wires the multi-level manager so paper partial exits are recorded
// for remaining ML qty when a strategy is deactivated.
func (c *StrategyEventsConsumer) SetMLManager(ml MLGroupCanceller) {
	c.mlManager = ml
}

// Start begins consuming user-config-events. Blocks until ctx is cancelled.
func (c *StrategyEventsConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting strategy events consumer (user-config-events)")

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Strategy events consumer shutting down")
			return nil
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.logger.Error("Failed to fetch strategy event", zap.Error(err))
				time.Sleep(time.Second)
				continue
			}

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process strategy event",
					zap.Error(err),
					zap.Int64("offset", msg.Offset))
				// Do not commit — will be retried on restart.
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Error("Failed to commit strategy event offset", zap.Error(err))
			}
		}
	}
}

func (c *StrategyEventsConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	var ev strategyEvent
	if err := json.Unmarshal(msg.Value, &ev); err != nil {
		// Malformed message — log and skip (return nil so offset is committed).
		c.logger.Warn("Skipping malformed strategy event", zap.Error(err))
		return nil
	}

	if ev.UserID == "" || ev.StrategyID == "" {
		return nil
	}

	switch ev.Type {
	case configPaused, configDeleted:
		return c.closeStrategyPositions(ctx, ev)
	default:
		return nil
	}
}

// closeStrategyPositions handles the full lifecycle of shutting down a strategy:
//
//  1. Fetch all non-terminal orders for the strategy.
//  2. For live orders at the broker (SUBMITTED / PARTIALLY_FILLED with an IndiraOrderID):
//     call executor.CancelOrder so the broker cancellation API is invoked.
//  3. Bulk-cancel everything remaining (paper orders, RECEIVED/PENDING live orders,
//     and FILLED positions) via a single DB UPDATE so the DB is always consistent.
func (c *StrategyEventsConsumer) closeStrategyPositions(ctx context.Context, ev strategyEvent) error {
	c.logger.Info("Closing all positions for strategy",
		zap.String("event_type", string(ev.Type)),
		zap.String("strategy_id", ev.StrategyID),
		zap.String("user_id", ev.UserID))

	// Step 0: immediately remove all orders for this strategy from the price
	// monitor watch list so no new triggers fire while we cancel.
	if c.priceMonitor != nil {
		removed := c.priceMonitor.UnwatchByStrategy(ev.StrategyID)
		if removed > 0 {
			c.logger.Info("Unwatched price-monitored orders for deactivated strategy",
				zap.String("strategy_id", ev.StrategyID),
				zap.Int("removed", removed))
		}
	}

	orders, err := c.orderRepo.GetActiveOrdersByStrategy(ctx, ev.StrategyID, ev.UserID)
	if err != nil {
		return fmt.Errorf("closeStrategyPositions: fetch orders: %w", err)
	}

	// Step 1: cancel broker-submitted live orders concurrently so the exchange
	// receives cancellation requests in parallel (each is an independent API call).
	var wg sync.WaitGroup
	for _, order := range orders {
		if order.IsPaperTrade {
			continue // paper orders have no broker state; handled by bulk cancel below
		}
		if order.IndiraOrderID == nil {
			continue // not yet submitted to broker; bulk cancel is sufficient
		}
		if models.IsTerminalStatus(order.Status) {
			continue // terminal status — broker API not applicable
		}

		wg.Add(1)
		go func(o *models.Order) {
			defer wg.Done()
			reason := "Strategy deactivated or deleted"
			if cancelErr := c.executor.CancelOrder(ctx, o, reason); cancelErr != nil {
				c.logger.Error("Broker cancellation failed for order",
					zap.String("order_id", o.OrderID.String()),
					zap.String("indira_order_id", *o.IndiraOrderID),
					zap.Error(cancelErr))
			}
		}(order)
	}
	wg.Wait()

	// Step 1b: cancel any active ML groups for this strategy, recording paper partial
	// exits for remaining qty so the closed-positions tab shows a row per level exit
	// (the strategy-deactivation exit appears as a distinct row alongside any SL/TP
	// levels that already fired).
	if c.mlManager != nil {
		c.mlManager.CancelGroupsByStrategy(ctx, ev.UserID, ev.StrategyID)
	}

	// Step 2: for filled paper orders, record the actual LTP as exit price so that
	// the closed positions response shows real PnL instead of zero.
	// CancelAllOrdersByStrategy (step 3) preserves already-set exit prices and acts
	// as fallback (entry price, zero PnL) when Redis is unavailable.
	if c.priceClient != nil {
		for _, order := range orders {
			if !order.IsPaperTrade {
				continue
			}
			if order.FilledPrice == nil || order.FilledQuantity == 0 {
				continue
			}
			ltp, ltpErr := c.priceClient.GetLTP(ctx, string(order.Exchange), order.StockCode)
			if ltpErr != nil {
				c.logger.Warn("LTP unavailable for paper exit; will fall back to entry price",
					zap.String("order_id", order.OrderID.String()),
					zap.String("symbol", order.Symbol),
					zap.Error(ltpErr))
				continue
			}
			entryPrice := *order.FilledPrice
			qty := float64(order.FilledQuantity)
			var pnl float64
			if order.OrderSide == models.OrderSideBuy {
				pnl = (ltp - entryPrice) * qty
			} else {
				pnl = (entryPrice - ltp) * qty
			}
			if updateErr := c.orderRepo.UpdatePaperExitPrice(ctx, order.OrderID, ltp, pnl); updateErr != nil {
				c.logger.Warn("Failed to set paper exit price",
					zap.String("order_id", order.OrderID.String()),
					zap.Error(updateErr))
			}
		}
	}

	// Step 3: bulk-cancel anything still not in a terminal state (paper orders,
	// pre-broker live orders, FILLED positions, and any broker cancel that failed above).
	// For paper orders where LTP was unavailable, the CASE expression falls back to
	// filled_price (zero PnL) so they still appear in the closed positions tab.
	if err := c.orderRepo.CancelAllOrdersByStrategy(ctx, ev.StrategyID, ev.UserID); err != nil {
		return fmt.Errorf("closeStrategyPositions: bulk cancel: %w", err)
	}

	c.logger.Info("All positions closed for strategy",
		zap.String("strategy_id", ev.StrategyID),
		zap.String("user_id", ev.UserID),
		zap.Int("orders_processed", len(orders)))

	return nil
}

// Close closes the underlying Kafka reader.
func (c *StrategyEventsConsumer) Close() error {
	c.logger.Info("Closing strategy events consumer")
	return c.reader.Close()
}
