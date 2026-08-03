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

// configEventType mirrors the event types published by user-config service.
type configEventType string

const (
	configPaused             configEventType = "CONFIG_PAUSED"
	configDeleted            configEventType = "CONFIG_DELETED"
	userCredentialsUpdated   configEventType = "USER_CREDENTIALS_UPDATED"
)

type strategyEvent struct {
	Type       configEventType `json:"type"`
	UserID     string          `json:"user_id"`
	StrategyID string          `json:"strategy_id"`
	Version    uint64          `json:"version"`

	// PositionHandling tells this consumer whether user-config already
	// dealt with open positions (via its HTTP force-exit call) or the
	// caller explicitly wanted them left alone. Populated by user-config
	// from the Deactivate/Delete gRPC request's position_handling field.
	// See user-config events/config_event.go PositionHandling docstring
	// for the full contract.
	//
	// Values:
	//   ""                        legacy path — no explicit handling,
	//                             typically EOD auto-deactivate. Run
	//                             closeStrategyPositions as before.
	//   "KEEP_POSITIONS_OPEN"     mobile Pause modal "keep positions".
	//                             SKIP closeStrategyPositions — the user
	//                             wants to manage their lots manually.
	//   "SQUARE_OFF_AT_MARKET"    mobile picked "square off". user-config
	//                             ALREADY called our HTTP force-exit
	//                             endpoint synchronously, so the exit
	//                             orders are SUBMITTED at the broker
	//                             right now. SKIP closeStrategyPositions
	//                             — otherwise we'd immediately CancelOrder
	//                             those exits before they fill.
	PositionHandling string `json:"position_handling,omitempty"`
}

const (
	positionHandlingKeepOpen  = "KEEP_POSITIONS_OPEN"
	positionHandlingSquareOff = "SQUARE_OFF_AT_MARKET"
)

// CredentialsCacheInvalidator is the minimal surface this consumer needs from
// executor.CredentialsCache. Defined as an interface so the consumer doesn't
// have to import the executor package (avoids an import cycle if executor
// later wants to import this consumer).
type CredentialsCacheInvalidator interface {
	Invalidate(userID string)
}

// AuthGateClearer lets the consumer reset the per-user "session-expired" gate
// in the manthan AuthExpiryNotifier when a fresh JWT lands. Without this hook
// the gate would only clear after its 5-min auto-expiry, leaving every loop
// idle even after the user re-logged in. Implemented by *manthan.JWTExpiryNotifier.
type AuthGateClearer interface {
	ClearSessionExpired(userID string)
}

// CredentialsObserver lets the consumer fan out USER_CREDENTIALS_UPDATED events
// to interested components. Implemented by *manthan.ArmRetryWorker so the Layer 4
// EOD-arm retry queue drains the moment a user re-logs in — without this hook the
// queue would wait up to 5 minutes for the next ticker even though fresh creds
// are now in memory.
type CredentialsObserver interface {
	OnCredentialsUpdated(userID string)
}

// OrderUnwatcher removes orders from the price monitor watch list.
// Implemented by *scheduler.PriceMonitor.
type OrderUnwatcher interface {
	UnwatchByStrategy(strategyID string) int
}

// StrategyEventsConsumer listens to user-config-events and closes all open
// positions / cancels all pending orders when a strategy is deactivated or deleted.
type StrategyEventsConsumer struct {
	reader       *kafka.Reader
	orderRepo    repository.OrderRepository
	executor     *executor.OrderExecutor
	priceMonitor OrderUnwatcher // nil-safe: may be unset if PriceMonitor is disabled
	credsCache    CredentialsCacheInvalidator // nil-safe: invalidates JWT cache on USER_CREDENTIALS_UPDATED
	authGate      AuthGateClearer             // nil-safe: clears manthan session-expired gate on USER_CREDENTIALS_UPDATED
	credsObserver CredentialsObserver         // nil-safe: wakes Layer-4 retry queue on USER_CREDENTIALS_UPDATED
	logger        *zap.Logger
}

// SetAuthGate wires the manthan SESSION_EXPIRED gate so it clears on JWT refresh.
func (c *StrategyEventsConsumer) SetAuthGate(g AuthGateClearer) {
	c.authGate = g
}

// SetCredentialsObserver wires a side-effect observer that fires on every
// USER_CREDENTIALS_UPDATED event after the cache/gate are reset. Used by the
// Layer-4 ArmRetryWorker to wake immediately on user re-login instead of
// waiting for its 5-minute poll.
func (c *StrategyEventsConsumer) SetCredentialsObserver(o CredentialsObserver) {
	c.credsObserver = o
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

// SetCredentialsCache wires the in-memory JWT cache so we can invalidate a
// user's cached credentials when their broker JWT is refreshed via the
// /api/v1/auth/credentials endpoint. Without this, the cache's 5-min TTL
// could serve a stale JWT to the protective replayer's 16:35 IST cron for
// up to 5 minutes after a fresh login.
func (c *StrategyEventsConsumer) SetCredentialsCache(cc CredentialsCacheInvalidator) {
	c.credsCache = cc
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

	if ev.UserID == "" {
		return nil
	}

	switch ev.Type {
	case userCredentialsUpdated:
		// JWT-refresh path. No strategy_id needed; the event is per-user.
		// Invalidate the in-memory cache so the next read pulls the fresh
		// encrypted token from user_credentials. This is best-effort —
		// failure here just means the cache stays warm for up to its TTL.
		if c.credsCache != nil {
			c.credsCache.Invalidate(ev.UserID)
			c.logger.Info("Credentials cache invalidated",
				zap.String("user_id", ev.UserID))
		}
		// Clear the manthan SESSION_EXPIRED gate so safety monitor + reconciler
		// resume hitting the broker on their next tick instead of waiting up
		// to 5 min for the dedup window to expire.
		if c.authGate != nil {
			c.authGate.ClearSessionExpired(ev.UserID)
		}
		// Layer 4: wake the EOD-arm retry queue. If the user logged in
		// because they got the 14:30 IST pre-flight alert (Layer 3) or the
		// 16:35 IST EOD-skip notification, this drains their PENDING rows
		// in seconds rather than waiting for the 5-min ticker.
		if c.credsObserver != nil {
			c.credsObserver.OnCredentialsUpdated(ev.UserID)
		}
		return nil
	case configPaused, configDeleted:
		if ev.StrategyID == "" {
			return nil
		}
		// The mobile Pause/Stop modal always ships position_handling; if
		// the caller explicitly picked KEEP_POSITIONS_OPEN or already
		// squared off via HTTP, skipping closeStrategyPositions is
		// critical — otherwise it would CancelOrder the just-placed
		// exit orders or nuke lots the user meant to keep.
		//
		// Empty position_handling preserves the legacy behavior (EOD
		// auto-deactivate + any pre-mockup callers) — run the classic
		// cleanup so backwards-compat holds.
		switch ev.PositionHandling {
		case positionHandlingSquareOff:
			c.logger.Info("Skipping closeStrategyPositions — user-config already placed exit orders",
				zap.String("event_type", string(ev.Type)),
				zap.String("strategy_id", ev.StrategyID),
				zap.String("user_id", ev.UserID),
				zap.String("position_handling", ev.PositionHandling))
			// Still unhook the price monitor so no new triggers fire against
			// the paused/deleted strategy while its exit orders settle.
			if c.priceMonitor != nil {
				if removed := c.priceMonitor.UnwatchByStrategy(ev.StrategyID); removed > 0 {
					c.logger.Info("Unwatched price-monitored orders",
						zap.String("strategy_id", ev.StrategyID),
						zap.Int("removed", removed))
				}
			}
			return nil
		case positionHandlingKeepOpen:
			c.logger.Info("Skipping closeStrategyPositions — caller chose to keep positions open",
				zap.String("event_type", string(ev.Type)),
				zap.String("strategy_id", ev.StrategyID),
				zap.String("user_id", ev.UserID),
				zap.String("position_handling", ev.PositionHandling))
			// Same unwatch — no new signals should fire for a paused strategy.
			if c.priceMonitor != nil {
				if removed := c.priceMonitor.UnwatchByStrategy(ev.StrategyID); removed > 0 {
					c.logger.Info("Unwatched price-monitored orders",
						zap.String("strategy_id", ev.StrategyID),
						zap.Int("removed", removed))
				}
			}
			return nil
		default:
			return c.closeStrategyPositions(ctx, ev)
		}
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

	// Step 2: bulk-cancel anything still not in a terminal state (paper orders,
	// pre-broker live orders, FILLED positions, and any broker cancel that failed above).
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
