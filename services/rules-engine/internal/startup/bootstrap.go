package startup

import (
	"context"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// Bootstrapper performs startup sequence: fetch → validate → bulk load → verify.
type Bootstrapper struct {
	userConfigClient ActiveStrategiesFetcher
	configStore      *configstore.ConfigStore
	logger           *zap.Logger
}

// ActiveStrategiesFetcher is implemented by *UserConfigClient and is mockable in tests.
type ActiveStrategiesFetcher interface {
	GetAllActiveStrategies(ctx context.Context) ([]*models.StrategyConfig, error)
}

func NewBootstrapper(client ActiveStrategiesFetcher, store *configstore.ConfigStore, logger *zap.Logger) *Bootstrapper {
	return &Bootstrapper{userConfigClient: client, configStore: store, logger: logger}
}

func (b *Bootstrapper) Run(ctx context.Context) error {
	if b.userConfigClient == nil {
		return fmt.Errorf("user config client is nil")
	}
	if b.configStore == nil {
		return fmt.Errorf("config store is nil")
	}

	fetched, err := b.userConfigClient.GetAllActiveStrategies(ctx)
	if err != nil {
		return err
	}
	fetchedCount := len(fetched)

	valid := make([]*models.StrategyConfig, 0, fetchedCount)
	for _, s := range fetched {
		if s == nil {
			continue
		}
		if err := validateStrategyConfig(s); err != nil {
			b.logger.Warn("Skipping invalid strategy from bulk-load",
				zap.String("user_id", s.UserID),
				zap.String("strategy_id", s.StrategyID),
				zap.Error(err),
			)
			continue
		}
		valid = append(valid, s)
	}

	b.configStore.BulkLoad(valid)
	metrics := b.configStore.Metrics()
	if metrics.TotalStrategies == 0 {
		// If user-config returned 0 (fresh system), acceptable.
		// Only fatal if it returned >0 but all were invalid.
		if fetchedCount > 0 {
			return fmt.Errorf("FATAL: fetched %d strategies but 0 loaded — check validation logs", fetchedCount)
		}
	}

	b.logger.Info("BulkLoad complete",
		zap.Int("strategies", metrics.TotalStrategies),
		zap.Int("users", metrics.TotalUsers),
	)
	return nil
}

func validateStrategyConfig(s *models.StrategyConfig) error {
	if s.StrategyID == "" {
		return fmt.Errorf("strategy_id empty")
	}
	if s.UserID == "" {
		return fmt.Errorf("user_id empty")
	}
	if s.Conditions.ImpactScoreMin > s.Conditions.ImpactScoreMax {
		return fmt.Errorf("impact_score_min > impact_score_max")
	}
	if s.Conditions.MinPctChange > 0 && s.Conditions.MaxPctChange > 0 {
		if s.Conditions.MinPctChange > s.Conditions.MaxPctChange {
			return fmt.Errorf("min_pct_change > max_pct_change")
		}
	}
	if s.TradeConfig.Quantity <= 0 {
		return fmt.Errorf("quantity <= 0")
	}
	if s.RiskLimits.MaxDailyTrades <= 0 {
		return fmt.Errorf("max_daily_trades <= 0")
	}
	return nil
}
