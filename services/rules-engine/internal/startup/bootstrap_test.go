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
		Active:       true,
		Version:      1,
		Conditions: models.Conditions{
			ImpactScoreMin: 1,
			ImpactScoreMax: 2,
		},
		TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1, Exchange: "NSE"},
		RiskLimits:  models.RiskLimits{MaxDailyTrades: 1},
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
	base := &models.StrategyConfig{StrategyID: "s", UserID: "u", TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1}, RiskLimits: models.RiskLimits{MaxDailyTrades: 1}}

	if err := validateStrategyConfig(&models.StrategyConfig{}); err == nil {
		t.Fatalf("expected error")
	}

	c := *base
	c.Conditions.ImpactScoreMin = 5
	c.Conditions.ImpactScoreMax = 1
	if err := validateStrategyConfig(&c); err == nil {
		t.Fatalf("expected error")
	}

	c2 := *base
	c2.TradeConfig.Quantity = 0
	if err := validateStrategyConfig(&c2); err == nil {
		t.Fatalf("expected error")
	}

	c3 := *base
	c3.RiskLimits.MaxDailyTrades = -1
	if err := validateStrategyConfig(&c3); err == nil {
		t.Fatalf("expected error for negative max_daily_trades")
	}

	// MaxDailyTrades == 0 means "unlimited / use system default" and must be accepted.
	c4 := *base
	c4.RiskLimits.MaxDailyTrades = 0
	if err := validateStrategyConfig(&c4); err != nil {
		t.Fatalf("expected zero max_daily_trades to be accepted, got: %v", err)
	}
}

var _ = errors.New
