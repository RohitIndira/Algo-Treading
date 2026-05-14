package executor

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

// PriceLookup is a minimal interface for retrieving the current market LTP.
// *paper.RedisPriceClient satisfies this interface.
// Pass nil to disable Redis-based fill pricing.
type PriceLookup interface {
	GetLTP(ctx context.Context, exchange string, token int64) (float64, error)
}

// PaperOrderExecutor handles paper trading order execution (no real broker calls).
type PaperOrderExecutor struct {
	repo        repository.OrderRepository
	kafkaPub    *publisher.KafkaPublisher
	priceLookup   PriceLookup // optional — Redis live LTP for accurate fill price
	OnPaperFilled func(order *models.Order)
}

// NewPaperOrderExecutor creates a new paper order executor.
// priceLookup may be nil; if nil, the signal's computed price is used as fill price.
func NewPaperOrderExecutor(
	repo repository.OrderRepository,
	kafkaPub *publisher.KafkaPublisher,
	priceLookup PriceLookup,
) *PaperOrderExecutor {
	return &PaperOrderExecutor{
		repo:        repo,
		kafkaPub:    kafkaPub,
		priceLookup: priceLookup,
	}
}

// ExecuteOrder implements scheduler.OrderExecutorFunc so PaperOrderExecutor can be
// used anywhere a live executor is expected (e.g. PriceMonitor, RoutingExecutor).
func (e *PaperOrderExecutor) ExecuteOrder(ctx context.Context, order *models.Order) error {
	return e.ExecutePaperOrder(ctx, order)
}

// ExecutePaperOrder simulates an immediate order fill.
//
// Entry price resolution:
//  1. Redis live LTP  — accurate market price from the shared Redis feed
//  2. order.Price     — signal-computed price (fallback when Redis is unavailable)
func (e *PaperOrderExecutor) ExecutePaperOrder(ctx context.Context, order *models.Order) error {
	log.Printf("[paper] Executing paper order %s for user %s symbol %s",
		order.OrderID, order.UserID, order.Symbol)

	now := time.Now()

	// Resolve entry price: prefer Redis live LTP for accurate market fill
	signalPrice := safePrice(order.Price)
	entryPrice := signalPrice
	if e.priceLookup != nil {
		pCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		ltp, err := e.priceLookup.GetLTP(pCtx, string(order.Exchange), order.StockCode)
		cancel()
		if err == nil && ltp > 0 {
			log.Printf("[paper] Using Redis LTP %.2f for %s (signal price was %.2f)",
				ltp, order.Symbol, entryPrice)
			entryPrice = ltp
		} else if err != nil {
			log.Printf("[paper] Redis price unavailable for %s: %v — using signal price %.2f",
				order.Symbol, err, entryPrice)
		}
	}

	// If the fill price differs from the signal price (e.g. price monitor set a limit
	// price then paper executor refreshed with LTP), rescale SL/TP to the actual fill
	// price so the user's configured percentages are preserved exactly.
	if signalPrice > 0 && entryPrice != signalPrice {
		if order.StopLoss != nil && *order.StopLoss > 0 {
			ratio := *order.StopLoss / signalPrice
			rescaled := entryPrice * ratio
			order.StopLoss = &rescaled
		}
		if order.TakeProfit != nil && *order.TakeProfit > 0 {
			ratio := *order.TakeProfit / signalPrice
			rescaled := entryPrice * ratio
			order.TakeProfit = &rescaled
		}
	}

	// Immediately fill the paper order
	order.Status = models.StatusFilled
	order.IsPaperTrade = true
	order.TradingMode = "PAPER"
	order.FilledQuantity = order.Quantity
	order.FilledPrice = &entryPrice
	order.SubmittedAt = &now
	order.ExecutedAt = &now

	paperID := "PAPER-" + uuid.New().String()[:8]
	order.IndiraOrderID = &paperID

	if err := retryDB(3, func() error { return e.repo.Update(ctx, order) }); err != nil {
		log.Printf("[paper] DB error updating paper order %s after retries: %v", order.OrderID, err)
	}

	e.repo.RecordExecutionEvent(ctx, order.OrderID, "PAPER_FILLED", map[string]interface{}{
		"entry_price": entryPrice,
		"quantity":    order.Quantity,
		"timestamp":   now,
		"is_paper":    true,
	})

	e.publishPaperOrderUpdate(ctx, order, entryPrice)

	if e.OnPaperFilled != nil {
		e.OnPaperFilled(order)
	}

	log.Printf("[paper] ✓ Paper order %s filled for %s qty=%d @ %.2f (SL=%.2f, TP=%.2f)",
		order.OrderID, order.Symbol, order.Quantity, entryPrice,
		safePrice(order.StopLoss), safePrice(order.TakeProfit))

	return nil
}

// ExitPaperPosition closes an open paper position at the given exit price.
// Reason can be "STOP_LOSS", "TAKE_PROFIT", or "FORCE_EXIT".
func (e *PaperOrderExecutor) ExitPaperPosition(ctx context.Context, order *models.Order, exitPrice float64, reason string) error {
	log.Printf("[paper] Exiting paper position %s (%s) @ %.2f reason=%s",
		order.OrderID, order.Symbol, exitPrice, reason)

	entryPrice := safePrice(order.FilledPrice)
	qty := float64(order.FilledQuantity)
	var pnl float64
	if order.OrderSide == models.OrderSideBuy {
		pnl = (exitPrice - entryPrice) * qty
	} else {
		pnl = (entryPrice - exitPrice) * qty
	}

	now := time.Now()
	order.Status = models.StatusCancelled
	order.PaperExitPrice = &exitPrice
	order.PaperPnL = &pnl
	rejectionReason := fmt.Sprintf("Paper exit: %s", reason)
	order.RejectionReason = &rejectionReason
	order.UpdatedAt = now

	if err := retryDB(3, func() error {
		return e.repo.UpdatePaperTradeExit(ctx, order.OrderID, exitPrice, pnl)
	}); err != nil {
		log.Printf("[paper] DB error recording paper exit for order %s: %v", order.OrderID, err)
		return fmt.Errorf("failed to persist paper exit for %s: %w", order.OrderID, err)
	}

	e.repo.RecordExecutionEvent(ctx, order.OrderID, "PAPER_EXITED", map[string]interface{}{
		"exit_price":  exitPrice,
		"entry_price": entryPrice,
		"pnl":         pnl,
		"reason":      reason,
		"timestamp":   now,
	})

	// Create a reverse paper order in DB to mirror live behaviour (entry + exit records).
	// Non-fatal: if this fails the position is still correctly exited above.
	go e.createReverseExitOrder(context.Background(), order, exitPrice, now)

	log.Printf("[paper] ✓ Paper position %s exited @ %.2f pnl=%.2f reason=%s",
		order.OrderID, exitPrice, pnl, reason)

	return nil
}

// createReverseExitOrder persists a square-off paper order in DB so the trade history
// shows both entry and exit legs — mirroring live force-exit behaviour.
func (e *PaperOrderExecutor) createReverseExitOrder(ctx context.Context, original *models.Order, exitPrice float64, at time.Time) {
	reverseSide := models.OrderSideSell
	if original.OrderSide == models.OrderSideSell {
		reverseSide = models.OrderSideBuy
	}

	paperID := "PAPER-EXIT-" + uuid.New().String()[:8]
	exitOrder := &models.Order{
		OrderID:          uuid.New(),
		UserID:           original.UserID,
		StrategyID:       original.StrategyID,
		StrategyName:     original.StrategyName,
		StockCode:        original.StockCode,
		Exchange:         original.Exchange,
		Symbol:           original.Symbol,
		OrderType:        models.OrderTypeMarket,
		OrderSide:        reverseSide,
		Quantity:         original.FilledQuantity,
		Price:            original.FilledPrice, // original entry price (mirrors ML partial-exit convention)
		Validity:         "IOC",
		ProductType:      original.ProductType,
		Status:           models.StatusFilled,
		IsPaperTrade:     true,
		TradingMode:      "PAPER",
		FilledQuantity:   original.FilledQuantity,
		FilledPrice:      &exitPrice,
		IsSquareOffOrder: true,
		RiskApproved:     true,
		IndiraOrderID:    &paperID,
		SubmittedAt:      &at,
		ExecutedAt:       &at,
		CreatedAt:        at,
		UpdatedAt:        at,
	}

	if err := retryDB(3, func() error { return e.repo.Create(ctx, exitOrder) }); err != nil {
		log.Printf("[paper] Failed to create reverse exit order for %s: %v", original.OrderID, err)
	}
}

func (e *PaperOrderExecutor) publishPaperOrderUpdate(ctx context.Context, order *models.Order, entryPrice float64) {
	if e.kafkaPub == nil {
		return
	}
	now := time.Now()
	update := &models.OrderUpdate{
		UpdateID:  uuid.New().String(),
		OrderID:   order.OrderID.String(),
		UserID:    order.UserID,
		CreatedAt: now,
		ExpiresAt: now.Add(24 * time.Hour),
		UpdateType: "ORDER_PAPER_FILLED",
		Priority:   "MEDIUM",
		Title:      "📄 Paper Order Filled",
		Message: fmt.Sprintf("Paper trade for %s filled at ₹%.2f (qty=%d)",
			order.Symbol, entryPrice, order.Quantity),
		Status: string(order.Status),
		OrderSummary: models.OrderSummary{
			Stock:     order.Symbol,
			Action:    string(order.OrderSide),
			Quantity:  order.Quantity,
			Exchange:  string(order.Exchange),
			OrderType: string(order.OrderType),
			Price:     fmt.Sprintf("₹%.2f", entryPrice),
		},
		NotificationChannels: models.NotificationChannels{
			Push:  true,
			InApp: true,
		},
	}
	if err := e.kafkaPub.PublishOrderUpdate(ctx, update); err != nil {
		log.Printf("[paper] Failed to publish paper-fill Kafka event: %v", err)
	}
}

func safePrice(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

// retryDB retries fn up to maxAttempts times with linear back-off (50ms × attempt).
func retryDB(maxAttempts int, fn func() error) error {
	var err error
	for i := 0; i < maxAttempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		if i < maxAttempts-1 {
			time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
		}
	}
	return err
}
