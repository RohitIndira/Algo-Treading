package cash52w

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// ============================================================================
// PHASE 1: Multi-Level Stop-Loss & Profit Taking Exit Manager
// ============================================================================
//
// This module implements the Phase 1 enhanced exit logic with:
// - 2-level stop-loss (fixed + trailing)
// - 3-level profit taking (2 fixed + 1 trailing)
// - Partial position exits
// - Per-user configuration from ConfigStore
//
// ============================================================================

// ExitLevel represents a triggered exit level with action details
type ExitLevel struct {
	LevelType       string  // "PROFIT" or "STOPLOSS"
	LevelNumber     int     // 1, 2, 3
	TriggerPercent  float64 // The percentage that triggered exit
	ExitPercent     int     // Percentage of position to exit
	ExitQuantity    int32   // Actual quantity to exit
	Type            string  // "fixed" or "trailing"
	TrailPercent    float64 // For trailing stops
	Reason          string  // Human-readable reason
}

// PositionExitSignal represents an exit signal for a position
type PositionExitSignal struct {
	UserID        string
	Token         string
	Symbol        string
	Exchange      string
	CurrentPrice  float64
	EntryPrice    float64
	CurrentQty    int32
	PnLPercent    float64
	ExitLevels    []ExitLevel
	TotalExitQty  int32
	Timestamp     time.Time
}

// ExitManager handles multi-level exit logic for 52W positions
type ExitManager struct {
	engine *Engine
	logger *zap.Logger
}

// NewExitManager creates a new exit manager
func NewExitManager(engine *Engine, logger *zap.Logger) *ExitManager {
	return &ExitManager{
		engine: engine,
		logger: logger,
	}
}

// EvaluateExits checks all positions and generates exit signals for any
// that have hit stop-loss or profit-taking levels
func (em *ExitManager) EvaluateExits(ctx context.Context, portfolios []*models.RealtimePortfolioEvent) []*PositionExitSignal {
	if len(portfolios) == 0 {
		return nil
	}

	var exitSignals []*PositionExitSignal

	for _, portfolio := range portfolios {
		// Get user's Phase 1 config
		cfg, ok := em.engine.store.Get(portfolio.UserID)
		if !ok || !cfg.Enabled {
			continue
		}

		// Check for force exit commands
		if cfg.ForceExitAll {
			// Generate exit signals for all positions
			for _, pos := range portfolio.Positions {
				signal := &PositionExitSignal{
					UserID:       portfolio.UserID,
					Token:        pos.Token,
					Symbol:       pos.Symbol,
					Exchange:     pos.Exchange,
					CurrentPrice: pos.LTP,
					EntryPrice:   pos.EntryPrice,
					CurrentQty:   pos.Quantity,
					PnLPercent:   pos.PnLPct,
					ExitLevels: []ExitLevel{{
						LevelType:      "FORCE_EXIT",
						LevelNumber:    0,
						TriggerPercent: pos.PnLPct,
						ExitPercent:    100,
						ExitQuantity:   pos.Quantity,
						Type:           "manual",
						Reason:         "Force exit all positions",
					}},
					TotalExitQty: pos.Quantity,
					Timestamp:    time.Now(),
				}
				exitSignals = append(exitSignals, signal)
			}
			continue
		}

		// Check for force exit specific stocks
		forceExitStocks := make(map[string]bool)
		for _, symbol := range cfg.ForceExitStocks {
			forceExitStocks[symbol] = true
		}

		// Evaluate each position
		for _, pos := range portfolio.Positions {
			// Check force exit for specific stock
			if forceExitStocks[pos.Symbol] {
				signal := &PositionExitSignal{
					UserID:       portfolio.UserID,
					Token:        pos.Token,
					Symbol:       pos.Symbol,
					Exchange:     pos.Exchange,
					CurrentPrice: pos.LTP,
					EntryPrice:   pos.EntryPrice,
					CurrentQty:   pos.Quantity,
					PnLPercent:   pos.PnLPct,
					ExitLevels: []ExitLevel{{
						LevelType:      "FORCE_EXIT",
						LevelNumber:    0,
						TriggerPercent: pos.PnLPct,
						ExitPercent:    100,
						ExitQuantity:   pos.Quantity,
						Type:           "manual",
						Reason:         fmt.Sprintf("Force exit stock: %s", pos.Symbol),
					}},
					TotalExitQty: pos.Quantity,
					Timestamp:    time.Now(),
				}
				exitSignals = append(exitSignals, signal)
				continue
			}

			// Normal multi-level exit evaluation
			signal := em.evaluatePositionExits(pos, cfg)
			if signal != nil {
				exitSignals = append(exitSignals, signal)
			}
		}
	}

	return exitSignals
}

// evaluatePositionExits checks a single position against Phase 1 exit levels
func (em *ExitManager) evaluatePositionExits(pos models.RealtimePosition, cfg UserConfig) *PositionExitSignal {
	if pos.Quantity <= 0 {
		return nil
	}

	var exitLevels []ExitLevel
	remainingQty := pos.Quantity

	// Check P&L percentage
	pnlPct := pos.PnLPct

	// ========================================================================
	// STOP-LOSS LEVELS (losses are negative)
	// ========================================================================
	if pnlPct < 0 {
		// Level 2: More severe loss
		if cfg.StopLossLevels.Level2.Enabled && 
		   pnlPct <= cfg.StopLossLevels.Level2.TriggerPercent {
			exitQty := calculateExitQuantity(remainingQty, cfg.StopLossLevels.Level2.ExitQuantityPercent)
			if exitQty > 0 {
				exitLevels = append(exitLevels, ExitLevel{
					LevelType:      "STOPLOSS",
					LevelNumber:    2,
					TriggerPercent: cfg.StopLossLevels.Level2.TriggerPercent,
					ExitPercent:    cfg.StopLossLevels.Level2.ExitQuantityPercent,
					ExitQuantity:   exitQty,
					Type:           cfg.StopLossLevels.Level2.Type,
					Reason:         fmt.Sprintf("Stop-loss level 2 hit: %.2f%% loss", pnlPct),
				})
				remainingQty -= exitQty
			}
		}
		// Level 1: First line of defense
		if cfg.StopLossLevels.Level1.Enabled && 
		   pnlPct <= cfg.StopLossLevels.Level1.TriggerPercent &&
		   remainingQty > 0 {
			exitQty := calculateExitQuantity(remainingQty, cfg.StopLossLevels.Level1.ExitQuantityPercent)
			if exitQty > 0 {
				exitLevels = append(exitLevels, ExitLevel{
					LevelType:      "STOPLOSS",
					LevelNumber:    1,
					TriggerPercent: cfg.StopLossLevels.Level1.TriggerPercent,
					ExitPercent:    cfg.StopLossLevels.Level1.ExitQuantityPercent,
					ExitQuantity:   exitQty,
					Type:           cfg.StopLossLevels.Level1.Type,
					Reason:         fmt.Sprintf("Stop-loss level 1 hit: %.2f%% loss", pnlPct),
				})
				remainingQty -= exitQty
			}
		}
	}

	// ========================================================================
	// PROFIT LEVELS (gains are positive)
	// ========================================================================
	if pnlPct > 0 {
		// Level 3: Highest profit
		if cfg.ProfitLevels.Level3.Enabled && 
		   pnlPct >= cfg.ProfitLevels.Level3.TriggerPercent {
			exitQty := calculateExitQuantity(remainingQty, cfg.ProfitLevels.Level3.ExitQuantityPercent)
			if exitQty > 0 {
				exitLevels = append(exitLevels, ExitLevel{
					LevelType:      "PROFIT",
					LevelNumber:    3,
					TriggerPercent: cfg.ProfitLevels.Level3.TriggerPercent,
					ExitPercent:    cfg.ProfitLevels.Level3.ExitQuantityPercent,
					ExitQuantity:   exitQty,
					Type:           cfg.ProfitLevels.Level3.Type,
					TrailPercent:   cfg.ProfitLevels.Level3.TrailPercent,
					Reason:         fmt.Sprintf("Profit level 3 hit: %.2f%% gain", pnlPct),
				})
				remainingQty -= exitQty
			}
		}
		// Level 2: Medium profit
		if cfg.ProfitLevels.Level2.Enabled && 
		   pnlPct >= cfg.ProfitLevels.Level2.TriggerPercent &&
		   remainingQty > 0 {
			exitQty := calculateExitQuantity(remainingQty, cfg.ProfitLevels.Level2.ExitQuantityPercent)
			if exitQty > 0 {
				exitLevels = append(exitLevels, ExitLevel{
					LevelType:      "PROFIT",
					LevelNumber:    2,
					TriggerPercent: cfg.ProfitLevels.Level2.TriggerPercent,
					ExitPercent:    cfg.ProfitLevels.Level2.ExitQuantityPercent,
					ExitQuantity:   exitQty,
					Type:           cfg.ProfitLevels.Level2.Type,
					TrailPercent:   cfg.ProfitLevels.Level2.TrailPercent,
					Reason:         fmt.Sprintf("Profit level 2 hit: %.2f%% gain", pnlPct),
				})
				remainingQty -= exitQty
			}
		}
		// Level 1: First profit target
		if cfg.ProfitLevels.Level1.Enabled && 
		   pnlPct >= cfg.ProfitLevels.Level1.TriggerPercent &&
		   remainingQty > 0 {
			exitQty := calculateExitQuantity(remainingQty, cfg.ProfitLevels.Level1.ExitQuantityPercent)
			if exitQty > 0 {
				exitLevels = append(exitLevels, ExitLevel{
					LevelType:      "PROFIT",
					LevelNumber:    1,
					TriggerPercent: cfg.ProfitLevels.Level1.TriggerPercent,
					ExitPercent:    cfg.ProfitLevels.Level1.ExitQuantityPercent,
					ExitQuantity:   exitQty,
					Type:           cfg.ProfitLevels.Level1.Type,
					TrailPercent:   cfg.ProfitLevels.Level1.TrailPercent,
					Reason:         fmt.Sprintf("Profit level 1 hit: %.2f%% gain", pnlPct),
				})
				remainingQty -= exitQty
			}
		}
	}

	// Only generate signal if there are exits to execute
	if len(exitLevels) == 0 {
		return nil
	}

	totalExitQty := int32(0)
	for _, level := range exitLevels {
		totalExitQty += level.ExitQuantity
	}

	return &PositionExitSignal{
		UserID:       "", // Will be filled by caller
		Token:        pos.Token,
		Symbol:       pos.Symbol,
		Exchange:     pos.Exchange,
		CurrentPrice: pos.LTP,
		EntryPrice:   pos.EntryPrice,
		CurrentQty:   pos.Quantity,
		PnLPercent:   pnlPct,
		ExitLevels:   exitLevels,
		TotalExitQty: totalExitQty,
		Timestamp:    time.Now(),
	}
}

// ExecuteExitSignals converts exit signals into actual SELL orders
func (em *ExitManager) ExecuteExitSignals(ctx context.Context, signals []*PositionExitSignal) error {
	if len(signals) == 0 {
		return nil
	}

	em.logger.Info("Executing exit signals",
		zap.Int("signal_count", len(signals)))

	for _, signal := range signals {
		if err := em.executeExitSignal(ctx, signal); err != nil {
			em.logger.Error("Failed to execute exit signal",
				zap.Error(err),
				zap.String("user_id", signal.UserID),
				zap.String("symbol", signal.Symbol))
			// Continue with other signals
		}
	}

	return nil
}

// executeExitSignal creates and publishes SELL orders for a position exit
func (em *ExitManager) executeExitSignal(ctx context.Context, signal *PositionExitSignal) error {
	// Get user's trading mode
	mode := em.engine.effectiveModeForUser(signal.UserID)

	// Create SELL order for each exit level
	for _, level := range signal.ExitLevels {
		if level.ExitQuantity <= 0 {
			continue
		}

		orderReq := &models.OrderRequest{
			OrderID:      uuid.New().String(),
			UserID:       signal.UserID,
			StrategyID:   "CASH_52W_HIGH",
			StrategyName: "Cash 52-Week High",
			StockCode:    parseToken(signal.Token),
			Token:        parseToken(signal.Token),
			Symbol:       signal.Symbol,
			Exchange:     signal.Exchange,
			OrderType:    "MARKET",
			OrderSide:    "SELL", // EXIT
			Quantity:     level.ExitQuantity,
			Price:        signal.CurrentPrice,
			TradingMode:  mode,
			Timestamp:    time.Now(),
			MatchScore:   100.0,
			RiskApproved: true, // Auto-approve exits
		}

		// Publish to Kafka trade-signals
		if em.engine.kafkaPub != nil {
			if err := em.engine.kafkaPub.PublishTradeSignal(ctx, orderReq); err != nil {
				em.logger.Error("Failed to publish exit trade signal",
					zap.Error(err),
					zap.String("order_id", orderReq.OrderID))
			}
		}

		// Publish to RabbitMQ for execution (if LIVE mode)
		if mode == "LIVE" && em.engine.rabbitPub != nil {
			if err := em.engine.rabbitPub.PublishOrder(ctx, orderReq); err != nil {
				return fmt.Errorf("failed to publish exit order: %w", err)
			}
		}

		em.logger.Info("Exit order generated",
			zap.String("user_id", signal.UserID),
			zap.String("token", signal.Token),
			zap.String("symbol", signal.Symbol),
			zap.String("level_type", level.LevelType),
			zap.Int("level_number", level.LevelNumber),
			zap.Int32("exit_qty", level.ExitQuantity),
			zap.Float64("exit_price", signal.CurrentPrice),
			zap.String("order_id", orderReq.OrderID))
		
		// Record exit in position tracker for lifecycle tracking
		if em.engine.positionTracker != nil {
			if err := em.engine.positionTracker.RecordExit(
				signal.UserID,
				signal.Token,
				level.LevelType,
				level.LevelNumber,
				signal.CurrentPrice,
				level.ExitQuantity,
			); err != nil {
				em.logger.Error("Failed to record exit in position tracker",
					zap.Error(err),
					zap.String("user_id", signal.UserID),
					zap.String("token", signal.Token))
			}
		}
	}

	return nil
}

// calculateExitQuantity computes the quantity to exit based on percentage
func calculateExitQuantity(currentQty int32, exitPercent int) int32 {
	if currentQty <= 0 || exitPercent <= 0 {
		return 0
	}
	if exitPercent >= 100 {
		return currentQty
	}
	
	qty := float64(currentQty) * float64(exitPercent) / 100.0
	return int32(math.Floor(qty))
}
