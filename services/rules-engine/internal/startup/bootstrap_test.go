package startup

import (
	"context"
	"errors"
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

type fakeUserConfigClient struct {
	strategies []*models.StrategyConfig
	err        error
}

func (f *fakeUserConfigClient) GetAllActiveStrategies(ctx context.Context) ([]*models.StrategyConfig, error) {
	_ = ctx
	if f.err != nil {
		return nil, f.err
	}
	return f.strategies, nil
}

func TestBootstrapper_BulkLoadSuccess(t *testing.T) {
	logger := zap.NewNop()
	store := configstore.New()
	client := &fakeUserConfigClient{strategies: []*models.StrategyConfig{{
		StrategyID:   "s1",
		UserID:       "u1",
		StrategyName: "n",
		StrategyType: "MANTHAN",
		Active:       true,
		Version:      1,
		TradingMode:  "LIVE",
		TradeConfig: models.TradeConfig{
			OrderType:      "MARKET",
			TotalCapital:   100000,
			MaxPositions:   10,
			PerStockAmount: 10000,
		},
	}}}

	bs := NewBootstrapper(client, store, logger)
	if err := bs.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	metrics := store.Metrics()
	if metrics.TotalStrategies != 1 {
		t.Fatalf("expected 1 strategy, got %d", metrics.TotalStrategies)
	}
}

func TestBootstrapper_UserConfigDown_ReturnsError(t *testing.T) {
	logger := zap.NewNop()
	store := configstore.New()
	bs := NewBootstrapper(&fakeUserConfigClient{err: errors.New("down")}, store, logger)
	if err := bs.Run(context.Background()); err == nil {
		t.Fatalf("expected error")
	}
}

func TestBootstrapper_ZeroStrategies_AcceptedOnFreshSystem(t *testing.T) {
	store := configstore.New()
	logger := zap.NewNop()
	bs := NewBootstrapper(&fakeUserConfigClient{strategies: nil}, store, logger)
	if err := bs.Run(context.Background()); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	metrics := store.Metrics()
	if metrics.TotalStrategies != 0 {
		t.Fatalf("expected 0")
	}
}

func TestValidateStrategyConfig_InvalidCases(t *testing.T) {
	// Validation rules after the 2026-06-25 Cat B trim:
	//   - StrategyID / UserID must be non-empty
	//   - StrategyType must be "MANTHAN" (NEWS / 52W_BREAKOUT no longer accepted)
	//   - TradeConfig.TotalCapital must be > 0
	base := &models.StrategyConfig{
		StrategyID:   "s",
		UserID:       "u",
		StrategyType: "MANTHAN",
		TradeConfig:  models.TradeConfig{TotalCapital: 100000},
	}

	if err := validateStrategyConfig(&models.StrategyConfig{}); err == nil {
		t.Fatalf("expected error for empty strategy_id")
	}

	cNoUser := *base
	cNoUser.UserID = ""
	if err := validateStrategyConfig(&cNoUser); err == nil {
		t.Fatalf("expected error for empty user_id")
	}

	cWrongType := *base
	cWrongType.StrategyType = "NEWS"
	if err := validateStrategyConfig(&cWrongType); err == nil {
		t.Fatalf("expected error for non-MANTHAN strategy_type")
	}

	cNoCapital := *base
	cNoCapital.TradeConfig.TotalCapital = 0
	if err := validateStrategyConfig(&cNoCapital); err == nil {
		t.Fatalf("expected error for zero total_capital")
	}
}

var _ = errors.New
