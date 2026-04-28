package events

import (
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
)

// ToFullConfigEvent builds a ConfigEvent with complete strategy payload.
// Use for: CONFIG_CREATED, CONFIG_UPDATED.
//
// NOTE: This publisher does NOT do enum conversion. It publishes values
// exactly as stored in the User Config DB model.
func ToFullConfigEvent(eventType ConfigEventType, s *models.Strategy) *ConfigEvent {
	return &ConfigEvent{
		Type:       eventType,
		UserID:     s.UserID,
		StrategyID: s.StrategyID.String(),
		Version:    uint64(s.Version),
		Timestamp:  time.Now().UnixNano(),
		Config:     toPayload(s),
	}
}

// ToThinConfigEvent builds a ConfigEvent with NO strategy payload.
// Use for: CONFIG_DELETED, CONFIG_PAUSED, CONFIG_RESUMED.
//
// CONFIG_RESUMED is thin (config=nil) because:
// Rule Engine keeps strategy in its byUser paused index on PAUSE.
// On RESUME it re-activates from that index — it already has full config.
// Sending full config again on RESUME is redundant.
//
// If Rule Engine does NOT have the strategy in its paused index
// (e.g. it restarted between PAUSE and RESUME), it will bulk-reload
// from DB at next startup anyway.
func ToThinConfigEvent(eventType ConfigEventType, userID string, strategyID string, version uint64) *ConfigEvent {
	return &ConfigEvent{
		Type:       eventType,
		UserID:     userID,
		StrategyID: strategyID,
		Version:    version,
		Timestamp:  time.Now().UnixNano(),
		Config:     nil,
	}
}

// ToManthanConfigEvent builds a slim ConfigEvent for MANTHAN strategies.
// Only includes fields the rules-engine needs: identity, capital, SL config.
// No news conditions, no order-level fields (backend controls everything).
func ToManthanConfigEvent(eventType ConfigEventType, s *models.Strategy) *ConfigEvent {
	return &ConfigEvent{
		Type:       eventType,
		UserID:     s.UserID,
		StrategyID: s.StrategyID.String(),
		Version:    uint64(s.Version),
		Timestamp:  time.Now().UnixNano(),
		Config:     toManthanPayload(s),
	}
}

func toManthanPayload(s *models.Strategy) *StrategyPayload {
	p := &StrategyPayload{
		StrategyID:   s.StrategyID.String(),
		UserID:       s.UserID,
		StrategyName: s.StrategyName,
		StrategyType: string(s.StrategyType),
		Active:       s.Active,
		TradingMode:  string(s.TradingMode),
		Version:      uint64(s.Version),
	}
	if s.TradeConfig != nil {
		p.TradeConfig = TradeConfigPayload{
			TotalCapital:   valueOrZeroFloat64(s.TradeConfig.TotalCapital),
			MaxPositions:   valueOrZeroInt32(s.TradeConfig.MaxPositions),
			PerStockAmount: valueOrZeroFloat64(s.TradeConfig.PerStockAmount),
			StopLossPct:    valueOrZeroFloat64(s.TradeConfig.StopLossPct),
			TrailingSLPct:  valueOrZeroFloat64(s.TradeConfig.TrailingSLPct),
			StopLossType:   s.TradeConfig.StopLossType,
			ProductType:    s.TradeConfig.ProductType,
		}
	}
	return p
}

func toPayload(s *models.Strategy) *StrategyPayload {
	p := &StrategyPayload{
		StrategyID:   s.StrategyID.String(),
		UserID:       s.UserID,
		StrategyName: s.StrategyName,
		Active:       s.Active,
		TradingMode:  string(s.TradingMode),
		Version:      uint64(s.Version),
		CreatedAt:    s.CreatedAt.UnixNano(),
		UpdatedAt:    s.UpdatedAt.UnixNano(),
		Conditions: &ConditionsPayload{
			Sentiments:     []string{},
			Categories:     []string{},
			StockCodes:     []int64{},
			MarketCapTypes: []string{},
			Exchanges:      []string{},
		},
		TradeConfig: TradeConfigPayload{},
		RiskLimits:  &RiskLimitsPayload{},
	}

	// Conditions
	if s.Conditions != nil {
		p.Conditions = &ConditionsPayload{
			MatchAllNews:      s.Conditions.MatchAllNews,
			ImpactScoreMin:    s.Conditions.ImpactScoreMin,
			ImpactScoreMax:    s.Conditions.ImpactScoreMax,
			Sentiments:        nilSafeStringSlice([]string(s.Conditions.Sentiments)),
			Categories:        nilSafeStringSlice([]string(s.Conditions.Categories)),
			StockCodes:        nilSafeInt64Slice([]int64(s.Conditions.StockCodes)),
			MarketCapTypes:    nilSafeStringSlice([]string(s.Conditions.MarketCapTypes)),
			MinMarketCap:      valueOrZeroFloat64(s.Conditions.MinMarketCap),
			MaxMarketCap:      valueOrZeroFloat64(s.Conditions.MaxMarketCap),
			MinPriceChangePct: valueOrZeroFloat64(s.Conditions.MinPriceChangePct),
			MaxPriceChangePct: valueOrZeroFloat64(s.Conditions.MaxPriceChangePct),
			MinVolume:         valueOrZeroInt64(s.Conditions.MinVolume),
			Exchanges:         nilSafeStringSlice([]string(s.Conditions.Exchanges)),
			CreatedAt:         s.Conditions.CreatedAt.UnixNano(),
		}
	}

	// Trade config
	if s.TradeConfig != nil {
		p.TradeConfig = TradeConfigPayload{
			OrderType:     s.TradeConfig.OrderType,
			ProductType:   s.TradeConfig.ProductType,
			Validity:      s.TradeConfig.Validity,
			Quantity:      s.TradeConfig.Quantity,
			Exchange:      s.TradeConfig.Exchange,
			OrderSide:     s.TradeConfig.OrderSide,
			LimitPrice:    valueOrZeroFloat64(s.TradeConfig.LimitPrice),
			StopLossPct:   valueOrZeroFloat64(s.TradeConfig.StopLossPct),
			TakeProfitPct: valueOrZeroFloat64(s.TradeConfig.TakeProfitPct),
			TrailingSLPct: valueOrZeroFloat64(s.TradeConfig.TrailingSLPct),
			StopLossType:  s.TradeConfig.StopLossType,
			CreatedAt:     s.TradeConfig.CreatedAt.UnixNano(),
		}
	}

	// Risk limits
	if s.RiskLimits != nil {
		p.RiskLimits = &RiskLimitsPayload{
			MaxDailyTrades:          valueOrZeroInt32(s.RiskLimits.MaxDailyTrades),
			MaxPerTradeRisk:         valueOrZeroFloat64(s.RiskLimits.MaxPerTradeRisk),
			MaxPortfolioExposurePct: valueOrZeroFloat64(s.RiskLimits.MaxPortfolioExposurePct),
			MaxLossPerDay:           valueOrZeroFloat64(s.RiskLimits.MaxLossPerDay),
			EnableRiskChecks:        s.RiskLimits.EnableRiskChecks,
			EnableAutoSquareOff:     s.RiskLimits.EnableAutoSquareOff,
			AutoSquareOffTime:       s.RiskLimits.AutoSquareOffTime,
			CreatedAt:               s.RiskLimits.CreatedAt.UnixNano(),
		}
	}

	return p
}

func nilSafeStringSlice(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func nilSafeInt64Slice(in []int64) []int64 {
	if in == nil {
		return []int64{}
	}
	return in
}

func valueOrZeroFloat64(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func valueOrZeroInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func valueOrZeroInt32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
