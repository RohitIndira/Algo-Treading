package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/jobbing"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/userconfig"
	"go.uber.org/zap"
)

// loadJobbingConfigsForUsers loads active JOBBING strategies for the given
// users from the user-config service and registers per-user, per-token
// parameters in the Jobbing engine. This is called once at startup.
func loadJobbingConfigsForUsers(ctx context.Context, logger *zap.Logger, ucClient *userconfig.Client, engine *jobbing.Engine, userIDs []string) {
	if ucClient == nil || engine == nil {
		return
	}

	for _, userID := range userIDs {
		userID = strings.TrimSpace(userID)
		if userID == "" {
			continue
		}

		// Fetch all active strategies for this user
		strategies, err := ucClient.ListActiveStrategies(ctx, userID)
		if err != nil {
			logger.Warn("Failed to list strategies for jobbing user", zap.String("user_id", userID), zap.Error(err))
			continue
		}

		for _, strat := range strategies {
			if !isJobbingStrategyName(strat.StrategyName) {
				continue
			}

			if strat.TradeConfig.Quantity <= 0 {
				logger.Warn("Skipping jobbing strategy with invalid quantity", zap.String("user_id", userID), zap.String("strategy_id", strat.StrategyID))
				continue
			}
			if len(strat.Conditions.Stocks) == 0 {
				logger.Warn("Skipping jobbing strategy with no stock codes configured", zap.String("user_id", userID), zap.String("strategy_id", strat.StrategyID))
				continue
			}

			// Map fields from user-config strategy into per-user jobbing params
			cfg := jobbing.UserTokenConfig{}

			// Price range
			if strat.Conditions.PriceRange.MinPrice > 0 {
				cfg.LowerRange = strat.Conditions.PriceRange.MinPrice
			}
			if strat.Conditions.PriceRange.MaxPrice > 0 {
				cfg.HigherRange = strat.Conditions.PriceRange.MaxPrice
			}

			// Quantity per order
			cfg.QuantityPerOrder = strat.TradeConfig.Quantity

			// Interpret MaxPositionSize as MaxQuantity for jobbing (integer semantics)
			if strat.TradeConfig.MaxPositionSize > 0 {
				cfg.MaxQuantity = int32(strat.TradeConfig.MaxPositionSize)
			}

			// Reuse stop_loss_pct as initial offset (B) and take_profit_pct as distance continue (S)
			if strat.TradeConfig.StopLossPct > 0 {
				cfg.InitialBuyOffset = strat.TradeConfig.StopLossPct
			}
			if strat.TradeConfig.TakeProfitPct > 0 {
				cfg.DistanceContinue = strat.TradeConfig.TakeProfitPct
			}

			// Normalize tokens from stock codes
			tokens := make([]string, 0, len(strat.Conditions.Stocks))
			for _, code := range strat.Conditions.Stocks {
				if code <= 0 {
					continue
				}
				tokens = append(tokens, fmt.Sprintf("%d", code))
			}
			if len(tokens) == 0 {
				continue
			}

			engine.SetJobbingConfig(userID, tokens, cfg)

			logger.Info("Loaded jobbing config from user-config",
				zap.String("user_id", userID),
				zap.String("strategy_id", strat.StrategyID),
				zap.Any("tokens", tokens),
				zap.Float64("lower_range", cfg.LowerRange),
				zap.Float64("higher_range", cfg.HigherRange),
				zap.Float64("initial_offset", cfg.InitialBuyOffset),
				zap.Float64("distance_continue", cfg.DistanceContinue),
				zap.Int32("qty_per_order", cfg.QuantityPerOrder),
				zap.Int32("max_qty", cfg.MaxQuantity))
		}
	}
}

// isJobbingStrategyName implements a simple convention-based check on
// StrategyName to identify jobbing strategies on the rules-engine side.
func isJobbingStrategyName(name string) bool {
	name = strings.ToUpper(strings.TrimSpace(name))
	if name == "" {
		return false
	}
	return name == "JOBBING" || strings.HasPrefix(name, "JOBBING_")
}
