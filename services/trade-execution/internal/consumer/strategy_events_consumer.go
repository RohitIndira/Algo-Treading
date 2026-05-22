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
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/timezone"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// isEODSquareOffWindow returns true during the EOD square-off window (15:00–15:10 IST,
// weekdays). During this window, AutoSquareOffScheduler is the sole owner of live
// position closure; StrategyEventsConsumer must NOT also place reverse broker orders
// or the same position gets closed twice → net short → SEBI penalty.
func isEODSquareOffWindow() bool {
	now := time.Now().In(timezone.IST)
	if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
		return false
	}
	totalMin := now.Hour()*60 + now.Minute()
	return totalMin >= 15*60 && totalMin <= 15*60+10
}

// PaperPriceLookup looks up the current LTP for an instrument from Redis.
// Satisfied by *paper.RedisPriceClient.
type PaperPriceLookup interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
}

// configEventType mirrors the event types published by user-config service.
type configEventType string

const (
	configPaused   configEventType = "CONFIG_PAUSED"
	configDeleted  configEventType = "CONFIG_DELETED"
	configCreated  configEventType = "CONFIG_CREATED"
	configUpdated  configEventType = "CONFIG_UPDATED"
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

// PaperOrderPurger removes paper orders from the paper monitor's in-memory cache.
// Implemented by *paper.PaperTradeMonitor.
type PaperOrderPurger interface {
	RemoveOrdersByStrategy(strategyID string)
}

// CredentialsInvalidator evicts a user's cached broker credentials so the next
// order re-reads the fresh token from DB. Implemented by *executor.CredentialsCache.
type CredentialsInvalidator interface {
	Invalidate(userID string)
}

// BrokerWSStarter opens the per-user broker order-status WebSocket for a user.
// Implemented in main.go as: load the user's broker credentials, then call
// statusservice.StartSubscription. Idempotent — safe to call repeatedly.
type BrokerWSStarter func(ctx context.Context, userID string) error

// StrategyHalter blocks new order execution for a strategy that has been deactivated.
// Implemented by *executor.SignalProcessor.
type StrategyHalter interface {
	HaltStrategy(strategyID string)
}
// MLGroupCanceller cancels active multi-level groups for a strategy, recording
// paper partial exits for remaining qty. Implemented by *multilevel.Manager.
type MLGroupCanceller interface {
	CancelGroupsByStrategy(ctx context.Context, userID, strategyID string)
}

// OpenPositionsSnapshot reports whether an order's underlying position is
// already flat (NetQty == 0) at the broker. Implemented by
// *scheduler.OpenPositions.
type OpenPositionsSnapshot interface {
	IsExited(order *models.Order) bool
}

// OpenPositionsLookup queries the broker position book for a user.
// Implemented by an adapter around *scheduler.PositionChecker. Nil-safe.
type OpenPositionsLookup interface {
	FetchOpenPositions(ctx context.Context, userID string) (OpenPositionsSnapshot, error)
}

// StrategyEventsConsumer listens to user-config-events and closes all open
// positions / cancels all pending orders when a strategy is deactivated or deleted.
// It also invalidates the credentials cache when a strategy is created or updated
// so the next order for that user picks up the latest bearer token from DB.
type StrategyEventsConsumer struct {
	reader         *kafka.Reader
	orderRepo      repository.OrderRepository
	executor       *executor.OrderExecutor
	paperMonitor   PaperOrderPurger       // nil-safe
	credsCache     CredentialsInvalidator  // nil-safe
	startBrokerWS  BrokerWSStarter         // nil-safe: opens broker WS on strategy create/update
	priceMonitor   OrderUnwatcher          // nil-safe: may be unset if PriceMonitor is disabled
	priceClient    PaperPriceLookup        // nil-safe: used to get LTP for paper exit PnL
	mlManager      MLGroupCanceller        // nil-safe: creates paper partial exits for remaining ML qty
	strategyHalter StrategyHalter          // nil-safe: halts new signals for deactivated strategies
	positions      OpenPositionsLookup     // nil-safe: skips square-off for already-flat broker positions
	logger         *zap.Logger
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

// SetPaperMonitor wires the paper trade monitor so its in-memory cache is purged
// when a strategy is deactivated, stopping stale Redis scan log lines.
func (c *StrategyEventsConsumer) SetPaperMonitor(pm PaperOrderPurger) {
	c.paperMonitor = pm
}

// SetCredentialsCache wires the credentials cache so that CONFIG_CREATED and
// CONFIG_UPDATED events evict the stale entry — forcing the next order to
// re-read the fresh bearer token from broker_accounts.
func (c *StrategyEventsConsumer) SetCredentialsCache(cc CredentialsInvalidator) {
	c.credsCache = cc
}

// SetBrokerWSStarter wires the per-user broker WebSocket starter so a
// CONFIG_CREATED / CONFIG_UPDATED event opens the user's order-status socket
// immediately — before their first order — instead of waiting for the first
// live signal. Call before Start().
func (c *StrategyEventsConsumer) SetBrokerWSStarter(fn BrokerWSStarter) {
	c.startBrokerWS = fn
}

// SetMLManager wires the multi-level manager so paper partial exits are recorded
// for remaining ML qty when a strategy is deactivated.
func (c *StrategyEventsConsumer) SetMLManager(ml MLGroupCanceller) {
	c.mlManager = ml
}

// SetStrategyHalter wires the signal processor so new trade signals for deactivated
// strategies are rejected before they reach the broker. Call before Start().
func (c *StrategyEventsConsumer) SetStrategyHalter(h StrategyHalter) {
	c.strategyHalter = h
}

// SetPositionsLookup wires the broker position-book checker. With it, strategy
// deactivation skips reverse orders for symbols already flat (NetQty == 0) at
// the broker — preventing a redundant square-off from opening a fresh short.
func (c *StrategyEventsConsumer) SetPositionsLookup(p OpenPositionsLookup) {
	c.positions = p
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
		// Immediately block new Kafka signals for this strategy so no new orders
		// are placed after deactivation, even if the rules-engine has in-flight messages.
		if c.strategyHalter != nil {
			c.strategyHalter.HaltStrategy(ev.StrategyID)
		}
		return c.closeStrategyPositions(ctx, ev)
	case configCreated, configUpdated:
		// Invalidate cached credentials so the next live order re-reads the fresh
		// bearer token that user-config just wrote to broker_accounts.
		if c.credsCache != nil && ev.UserID != "" {
			c.credsCache.Invalidate(ev.UserID)
			c.logger.Info("credentials cache invalidated on strategy event",
				zap.String("event_type", string(ev.Type)),
				zap.String("user_id", ev.UserID))
		}
		// Open this user's broker order-status socket now, so order updates stream
		// from the moment the strategy is active — not only after the first order.
		// Runs in the background: the connect makes broker network calls and must
		// not block Kafka offset commits. StartSubscription is idempotent, so a
		// CONFIG_UPDATED for an already-connected user is a cheap no-op.
		if c.startBrokerWS != nil && ev.UserID != "" {
			userID := ev.UserID
			go func() {
				bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := c.startBrokerWS(bgCtx, userID); err != nil {
					c.logger.Warn("broker WS subscription on strategy event failed",
						zap.String("user_id", userID), zap.Error(err))
				}
			}()
		}
		return nil
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
	// Purge from paper monitor in-memory cache so the Redis scan loop stops
	// logging these positions after the strategy is deactivated.
	if c.paperMonitor != nil {
		c.paperMonitor.RemoveOrdersByStrategy(ev.StrategyID)
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

	// Step 1.5: square off live positions already filled at the broker.
	// SKIP during the EOD window (15:00–15:10 IST): AutoSquareOffScheduler fires at
	// 15:05 and is the sole owner of EOD position closure. If we also place reverse
	// orders here, the same position gets closed twice → net short sell → SEBI penalty.
	// Outside the EOD window (manual deactivation during trading hours) we do need to
	// place the reverse broker order ourselves.
	if isEODSquareOffWindow() {
		c.logger.Info("Skipping live square-off on deactivation — EOD window active; AutoSquareOffScheduler owns this",
			zap.String("strategy_id", ev.StrategyID),
			zap.String("user_id", ev.UserID))
	} else if filledOrders, fetchErr := c.orderRepo.GetExitableLiveOrdersByStrategy(ctx, ev.StrategyID, ev.UserID); fetchErr != nil {
		c.logger.Error("Failed to fetch filled live orders for square-off on deactivation",
			zap.String("strategy_id", ev.StrategyID),
			zap.Error(fetchErr))
	} else if len(filledOrders) > 0 {
		// Fetch broker positions once for this user — skip orders whose
		// underlying position is already flat at the broker (NetQty == 0).
		// Fail-open: a nil snapshot bypasses the check.
		var snapshot OpenPositionsSnapshot
		if c.positions != nil {
			snap, posErr := c.positions.FetchOpenPositions(ctx, ev.UserID)
			if posErr != nil {
				c.logger.Warn("Position-book check unavailable for deactivation; proceeding without skip",
					zap.String("user_id", ev.UserID),
					zap.String("strategy_id", ev.StrategyID),
					zap.Error(posErr))
			} else {
				snapshot = snap
			}
		}

		var sqWg sync.WaitGroup
		for _, o := range filledOrders {
			if o.FilledQuantity <= 0 {
				continue
			}
			if snapshot != nil && snapshot.IsExited(o) {
				c.logger.Info("Skipping square-off on deactivation: broker NetQty=0 (already exited)",
					zap.String("order_id", o.OrderID.String()),
					zap.String("symbol", o.Symbol),
					zap.String("user_id", o.UserID))
				continue
			}
			sqWg.Add(1)
			go func(ord *models.Order) {
				defer sqWg.Done()
				if sqErr := c.squareOffLivePosition(ctx, ord); sqErr != nil {
					c.logger.Error("Square-off failed on strategy deactivation",
						zap.String("order_id", ord.OrderID.String()),
						zap.String("symbol", ord.Symbol),
						zap.Error(sqErr))
				}
			}(o)
		}
		sqWg.Wait()
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

// squareOffLivePosition places a reverse MARKET/IOC order to close a live position
// already filled at the broker. Only the FilledQuantity is reversed so partial fills
// don't over-exit.
func (c *StrategyEventsConsumer) squareOffLivePosition(ctx context.Context, original *models.Order) error {
	reverseSide := models.OrderSideSell
	if original.OrderSide == models.OrderSideSell {
		reverseSide = models.OrderSideBuy
	}

	sqOrder := &models.Order{
		OrderID:          uuid.New(),
		EventID:          uuid.New(),
		UserID:           original.UserID,
		StrategyID:       original.StrategyID,
		StrategyName:     original.StrategyName,
		StockCode:        original.StockCode,
		Exchange:         original.Exchange,
		Symbol:           original.Symbol,
		OrderType:        models.OrderTypeMarket,
		OrderSide:        reverseSide,
		Quantity:         original.FilledQuantity,
		Validity:         "IOC",
		ProductType:      original.ProductType,
		Status:           models.StatusReceived,
		IsSquareOffOrder: true,
		IsPaperTrade:     false,
		TradingMode:      "LIVE",
		RiskApproved:     true,
		BearerToken:      original.BearerToken,
		AppId:            original.AppId,
		Source:           original.Source,
		// Link back to the entry order so statusservice records the exact exit
		// price / P&L on the parent when this reverse order fills.
		ParentOrderID: &original.OrderID,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	if err := c.orderRepo.Create(ctx, sqOrder); err != nil {
		return fmt.Errorf("save square-off order: %w", err)
	}

	if err := c.executor.ExecuteOrder(ctx, sqOrder); err != nil {
		return fmt.Errorf("execute square-off order: %w", err)
	}

	c.logger.Info("Live position squared off on strategy deactivation",
		zap.String("original_order_id", original.OrderID.String()),
		zap.String("square_off_order_id", sqOrder.OrderID.String()),
		zap.String("symbol", original.Symbol),
		zap.String("reverse_side", string(reverseSide)),
		zap.Int32("qty", original.FilledQuantity))

	return nil
}

// Close closes the underlying Kafka reader.
func (c *StrategyEventsConsumer) Close() error {
	c.logger.Info("Closing strategy events consumer")
	return c.reader.Close()
}
