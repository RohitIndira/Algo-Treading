package paper

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
)

// PriceProvider interface for getting live market prices
type PriceProvider interface {
	GetLivePrice(ctx context.Context, token int64, exchange string) (float64, error)
	GetBatchPrices(ctx context.Context, tokens []int64, exchange string) (map[int64]float64, error)
}

// PositionManager manages paper trading positions including SL/TP monitoring
type PositionManager struct {
	positionRepo  repository.PaperPositionRepository
	orderRepo     repository.OrderRepository
	priceProvider PriceProvider

	mu            sync.RWMutex
	monitorTicker *time.Ticker
	stopChan      chan struct{}
	isRunning     bool

	// Configuration
	checkInterval time.Duration
	enabled       bool
}

// NewPositionManager creates a new paper position manager
func NewPositionManager(
	positionRepo repository.PaperPositionRepository,
	orderRepo repository.OrderRepository,
	priceProvider PriceProvider,
	checkInterval time.Duration,
) *PositionManager {
	if checkInterval == 0 {
		checkInterval = 10 * time.Second // Default: check every 10 seconds
	}

	return &PositionManager{
		positionRepo:  positionRepo,
		orderRepo:     orderRepo,
		priceProvider: priceProvider,
		checkInterval: checkInterval,
		stopChan:      make(chan struct{}),
		enabled:       true,
	}
}

// Start begins monitoring paper positions for SL/TP triggers
func (pm *PositionManager) Start(ctx context.Context) error {
	pm.mu.Lock()
	if pm.isRunning {
		pm.mu.Unlock()
		return fmt.Errorf("position manager already running")
	}
	pm.isRunning = true
	pm.monitorTicker = time.NewTicker(pm.checkInterval)
	pm.mu.Unlock()

	log.Printf("✓ Paper Position Manager started (checking every %v)", pm.checkInterval)

	go pm.monitorLoop(ctx)

	return nil
}

// Stop stops the position monitoring
func (pm *PositionManager) Stop() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if !pm.isRunning {
		return
	}

	close(pm.stopChan)
	if pm.monitorTicker != nil {
		pm.monitorTicker.Stop()
	}
	pm.isRunning = false

	log.Println("Paper Position Manager stopped")
}

// monitorLoop continuously monitors all open positions
func (pm *PositionManager) monitorLoop(ctx context.Context) {
	for {
		select {
		case <-pm.stopChan:
			return
		case <-pm.monitorTicker.C:
			if err := pm.checkAllPositions(ctx); err != nil {
				log.Printf("Error checking paper positions: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

// checkAllPositions checks all open positions for SL/TP triggers
func (pm *PositionManager) checkAllPositions(ctx context.Context) error {
	if !pm.enabled {
		return nil
	}

	// Get all users with open paper positions
	// For now, we'll need to query all open positions
	// In production, you might want to maintain a cache of active user IDs
	positions, err := pm.getAllOpenPositions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get open positions: %w", err)
	}

	if len(positions) == 0 {
		return nil
	}

	log.Printf("Monitoring %d open paper positions for SL/TP triggers", len(positions))

	// Use batch price fetching for better performance
	return pm.checkPositionsBatch(ctx, positions)
}

// getAllOpenPositions gets all open positions across all users
func (pm *PositionManager) getAllOpenPositions(ctx context.Context) ([]*models.PaperPosition, error) {
	return pm.positionRepo.GetAllOpenPositions(ctx)
}

// checkPositionsBatch checks multiple positions using batch price fetching
func (pm *PositionManager) checkPositionsBatch(ctx context.Context, positions []*models.PaperPosition) error {
	// Group positions by exchange for efficient batch fetching
	exchangeGroups := make(map[string][]*models.PaperPosition)
	for _, pos := range positions {
		exchangeGroups[pos.Exchange] = append(exchangeGroups[pos.Exchange], pos)
	}

	// Process each exchange group
	for exchange, exchangePositions := range exchangeGroups {
		// Collect all tokens for this exchange
		tokens := make([]int64, 0, len(exchangePositions))
		tokenToPositions := make(map[int64][]*models.PaperPosition)

		for _, pos := range exchangePositions {
			tokens = append(tokens, pos.Token)
			tokenToPositions[pos.Token] = append(tokenToPositions[pos.Token], pos)
		}

		// Fetch all prices in one batch call
		prices, err := pm.priceProvider.GetBatchPrices(ctx, tokens, exchange)
		if err != nil {
			log.Printf("Error fetching batch prices for %s: %v (falling back to individual)", exchange, err)
			// Fallback to individual checking
			for _, pos := range exchangePositions {
				if err := pm.checkPosition(ctx, pos); err != nil {
					log.Printf("Error checking position %s: %v", pos.PositionID, err)
				}
			}
			continue
		}

		// Process positions with fetched prices
		for token, positionsForToken := range tokenToPositions {
			currentPrice, ok := prices[token]
			if !ok {
				log.Printf("Price not found for token %d in batch result", token)
				// Fallback to individual fetch for this token
				for _, pos := range positionsForToken {
					if err := pm.checkPosition(ctx, pos); err != nil {
						log.Printf("Error checking position %s: %v", pos.PositionID, err)
					}
				}
				continue
			}

			// Check each position for this token
			for _, pos := range positionsForToken {
				if err := pm.checkPositionWithPrice(ctx, pos, currentPrice); err != nil {
					log.Printf("Error checking position %s: %v", pos.PositionID, err)
				}
			}
		}
	}

	return nil
}

// checkPosition checks a single position for SL/TP triggers
func (pm *PositionManager) checkPosition(ctx context.Context, position *models.PaperPosition) error {
	// Get current live price
	currentPrice, err := pm.priceProvider.GetLivePrice(ctx, position.Token, position.Exchange)
	if err != nil {
		return fmt.Errorf("failed to get live price for %s: %w", position.Symbol, err)
	}

	return pm.checkPositionWithPrice(ctx, position, currentPrice)
}

// checkPositionWithPrice checks a position with a pre-fetched price (for batch optimization)
func (pm *PositionManager) checkPositionWithPrice(ctx context.Context, position *models.PaperPosition, currentPrice float64) error {
	// Calculate price change percentage
	priceChangeThreshold := 0.01 // 0.01% - only update if price changed significantly
	priceChangePct := 0.0
	if position.CurrentPrice > 0 {
		priceChangePct = ((currentPrice - position.CurrentPrice) / position.CurrentPrice) * 100
		if priceChangePct < 0 {
			priceChangePct = -priceChangePct
		}
	}

	// Check for stop loss trigger FIRST (before updating DB)
	if position.ShouldTriggerStopLoss(currentPrice) {
		log.Printf("🛑 Stop Loss triggered for %s (User: %s, Price: %.2f, SL: %.2f)",
			position.Symbol, position.UserID, currentPrice, *position.StopLoss)
		return pm.closePositionOnTrigger(ctx, position, currentPrice, models.ExitReasonStopLoss)
	}

	// Check for take profit trigger
	if position.ShouldTriggerTakeProfit(currentPrice) {
		log.Printf("🎯 Take Profit triggered for %s (User: %s, Price: %.2f, TP: %.2f)",
			position.Symbol, position.UserID, currentPrice, *position.TakeProfit)
		return pm.closePositionOnTrigger(ctx, position, currentPrice, models.ExitReasonTakeProfit)
	}

	// Only update DB if price changed significantly or this is the first price
	// This reduces unnecessary database writes
	if position.CurrentPrice == 0 || priceChangePct >= priceChangeThreshold {
		if err := pm.positionRepo.UpdatePrice(ctx, position.PositionID, currentPrice); err != nil {
			return fmt.Errorf("failed to update position price: %w", err)
		}
	}

	return nil
}

// closePositionOnTrigger closes a position when SL or TP is hit
func (pm *PositionManager) closePositionOnTrigger(
	ctx context.Context,
	position *models.PaperPosition,
	exitPrice float64,
	reason models.ExitReason,
) error {
	// Create a paper SELL order for tracking
	exitOrderID := uuid.New()
	exitOrder := &models.Order{
		OrderID:      exitOrderID,
		UserID:       position.UserID,
		StrategyID:   position.StrategyID,
		EventID:      uuid.Nil,
		StockCode:    position.StockCode,
		Token:        position.Token,
		Exchange:     models.Exchange(position.Exchange),
		Symbol:       position.Symbol,
		TradingMode:  "PAPER",
		OrderType:    models.OrderTypeMarket,
		OrderSide:    models.OrderSideSell,
		Quantity:     position.Quantity,
		Status:       models.StatusFilled,
		RiskApproved: true,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Set filled details
	exitOrder.FilledQuantity = position.Quantity
	filledPrice := exitPrice
	exitOrder.FilledPrice = &filledPrice

	// Save exit order to database
	if err := pm.orderRepo.Create(ctx, exitOrder); err != nil {
		return fmt.Errorf("failed to create exit order: %w", err)
	}

	// Close the position
	if err := pm.positionRepo.ClosePosition(ctx, position.PositionID, exitPrice, reason, exitOrderID); err != nil {
		return fmt.Errorf("failed to close position: %w", err)
	}

	// Calculate realized PnL for logging
	realizedPnL, realizedPnLPct := models.CalculateRealizedPnL(
		position.EntryPrice,
		exitPrice,
		position.Quantity,
	)

	log.Printf("✓ Position closed - Symbol: %s, User: %s, Reason: %s, PnL: ₹%.2f (%.2f%%)",
		position.Symbol, position.UserID, reason, realizedPnL, realizedPnLPct)

	return nil
}

// CreatePosition creates a new paper position from a filled order
func (pm *PositionManager) CreatePosition(ctx context.Context, order *models.Order) error {
	// Only create positions for BUY orders
	if order.OrderSide != models.OrderSideBuy {
		return nil
	}

	// Only for paper trading
	if order.TradingMode != "PAPER" {
		return nil
	}

	// Only for filled orders
	if order.Status != models.StatusFilled {
		return nil
	}

	// Check if position already exists
	existingPos, err := pm.positionRepo.GetByToken(ctx, order.UserID, order.Token, models.PositionStatusOpen)
	if err != nil {
		return fmt.Errorf("failed to check existing position: %w", err)
	}

	if existingPos != nil {
		// Position already exists, could be from averaging or duplicate processing
		log.Printf("Position already exists for %s (User: %s)", order.Symbol, order.UserID)
		return nil
	}

	// Get entry price from filled price or order price
	entryPrice := 0.0
	if order.FilledPrice != nil {
		entryPrice = *order.FilledPrice
	} else if order.Price != nil {
		entryPrice = *order.Price
	} else {
		return fmt.Errorf("no price available for position creation")
	}

	// Create new position
	position := &models.PaperPosition{
		PositionID:   uuid.New(),
		UserID:       order.UserID,
		StrategyID:   order.StrategyID,
		StockCode:    order.StockCode,
		Token:        order.Token,
		Symbol:       order.Symbol,
		Exchange:     string(order.Exchange),
		Quantity:     order.FilledQuantity,
		EntryPrice:   entryPrice,
		CurrentPrice: entryPrice,
		StopLoss:     order.StopLoss,
		TakeProfit:   order.TakeProfit,
		Status:       models.PositionStatusOpen,
		EntryOrderID: order.OrderID,
		OpenedAt:     time.Now(),
		LastUpdated:  time.Now(),
	}

	// Calculate initial PnL (should be 0)
	position.CalculatePnL(entryPrice)

	// Save position to database
	if err := pm.positionRepo.Create(ctx, position); err != nil {
		return fmt.Errorf("failed to create paper position: %w", err)
	}

	log.Printf("✓ Paper position created - Symbol: %s, User: %s, Qty: %d, Entry: ₹%.2f",
		position.Symbol, position.UserID, position.Quantity, position.EntryPrice)

	return nil
}

// ClosePosition manually closes a position
func (pm *PositionManager) ClosePosition(ctx context.Context, positionID uuid.UUID, currentPrice float64) error {
	position, err := pm.positionRepo.Get(ctx, positionID)
	if err != nil {
		return fmt.Errorf("failed to get position: %w", err)
	}

	if position.Status != models.PositionStatusOpen {
		return fmt.Errorf("position is not open")
	}

	return pm.closePositionOnTrigger(ctx, position, currentPrice, models.ExitReasonManual)
}

// UpdatePositionPrices updates all open positions with current prices
func (pm *PositionManager) UpdatePositionPrices(ctx context.Context, userID string) error {
	positions, err := pm.positionRepo.GetOpenPositions(ctx, userID)
	if err != nil {
		return fmt.Errorf("failed to get open positions: %w", err)
	}

	for _, position := range positions {
		currentPrice, err := pm.priceProvider.GetLivePrice(ctx, position.Token, position.Exchange)
		if err != nil {
			log.Printf("Failed to get price for %s: %v", position.Symbol, err)
			continue
		}

		if err := pm.positionRepo.UpdatePrice(ctx, position.PositionID, currentPrice); err != nil {
			log.Printf("Failed to update position price for %s: %v", position.Symbol, err)
		}
	}

	return nil
}

// GetUserPositions gets all positions for a user
func (pm *PositionManager) GetUserPositions(ctx context.Context, userID string, includeChlosed bool) ([]*models.PaperPosition, error) {
	if includeChlosed {
		return pm.positionRepo.GetAllPositions(ctx, userID, 100, 0)
	}
	return pm.positionRepo.GetOpenPositions(ctx, userID)
}

// GetUserPnL gets total PnL for a user
func (pm *PositionManager) GetUserPnL(ctx context.Context, userID, strategyID string) (unrealizedPnL, realizedPnL float64, err error) {
	// Get open positions for unrealized PnL
	positions, err := pm.positionRepo.GetOpenPositions(ctx, userID, strategyID)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get open positions: %w", err)
	}

	for _, pos := range positions {
		unrealizedPnL += pos.UnrealizedPnL
	}

	// Get realized PnL from history
	today := time.Now().Truncate(24 * time.Hour)
	tomorrow := today.Add(24 * time.Hour)
	realizedPnL, err = pm.positionRepo.GetTotalRealizedPnL(ctx, userID, strategyID, today, tomorrow)
	if err != nil {
		return unrealizedPnL, 0, fmt.Errorf("failed to get realized PnL: %w", err)
	}

	return unrealizedPnL, realizedPnL, nil
}
