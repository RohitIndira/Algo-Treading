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
// Use for: CONFIG_DELETED, CONFIG_PAUSED.
//
// NOTE: Do NOT use for CONFIG_RESUMED / STRATEGY_ACTIVATED — a thin resume
// silently no-ops in the rules engine when the strategy is absent from its
// in-memory Paused map (e.g. after a restart). Use ToFullConfigEvent with
// ConfigUpdated for activation so ApplyUpsert runs unconditionally.
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

func toPayload(s *models.Strategy) *StrategyPayload {
	p := &StrategyPayload{
		StrategyID:             s.StrategyID.String(),
		UserID:                 s.UserID,
		StrategyName:           s.StrategyName,
		Active:                 s.Active,
		TradingMode:            string(s.TradingMode),
		Version:                uint64(s.Version),
		CreatedAt:              s.CreatedAt.UnixNano(),
		UpdatedAt:              s.UpdatedAt.UnixNano(),
		ProcessAfterMarketNews: s.ProcessAfterMarketNews,
		AMNSelectedStocks:      s.AMNSelectedStocks,
		Conditions: ConditionsPayload{
			Sentiments:     []string{},
			Categories:     []string{},
			MarketCapTypes: []string{},
			Exchanges:      []string{},
		},
		TradeConfig: TradeConfigPayload{},
		RiskLimits:  RiskLimitsPayload{},
	}

	// Conditions
	if s.Conditions != nil {
		p.Conditions = ConditionsPayload{
			MatchAllNews:      s.Conditions.MatchAllNews,
			ImpactScoreMin:    s.Conditions.ImpactScoreMin,
			ImpactScoreMax:    s.Conditions.ImpactScoreMax,
			Sentiments:        nilSafeStringSlice([]string(s.Conditions.Sentiments)),
			Categories:        nilSafeStringSlice([]string(s.Conditions.Categories)),
			MarketCapTypes:    nilSafeStringSlice([]string(s.Conditions.MarketCapTypes)),
			MinMarketCap:      valueOrZeroFloat64(s.Conditions.MinMarketCap),
			MaxMarketCap:      valueOrZeroFloat64(s.Conditions.MaxMarketCap),
			MinPriceChangePct: valueOrZeroFloat64(s.Conditions.MinPriceChangePct),
			MaxPriceChangePct: valueOrZeroFloat64(s.Conditions.MaxPriceChangePct),
			TradeValueMode:    valueOrEmptyString(s.Conditions.TradeValueMode),
			MinTradeValue:     valueOrZeroFloat64(s.Conditions.MinTradeValue),
			MaxTradeValue:     valueOrZeroFloat64(s.Conditions.MaxTradeValue),
			Exchanges:         nilSafeStringSlice([]string(s.Conditions.Exchanges)),
			CreatedAt:         s.Conditions.CreatedAt.UnixNano(),
		}
	}

	// Trade config
	if s.TradeConfig != nil {
		p.TradeConfig = TradeConfigPayload{
			OrderType:        s.TradeConfig.OrderType,
			ProductType:      s.TradeConfig.ProductType,
			Validity:         s.TradeConfig.Validity,
			Quantity:         s.TradeConfig.Quantity,
			Exchange:         s.TradeConfig.Exchange,
			OrderSide:        s.TradeConfig.OrderSide,
			LimitPrice:       valueOrZeroFloat64(s.TradeConfig.LimitPrice),
			StopLossPct:      valueOrZeroFloat64(s.TradeConfig.StopLossPct),
			TakeProfitPct:    valueOrZeroFloat64(s.TradeConfig.TakeProfitPct),
			TrailingSLPct:    valueOrZeroFloat64(s.TradeConfig.TrailingSLPct),
			StopLossType:     s.TradeConfig.StopLossType,
			TakeProfitType:   s.TradeConfig.TakeProfitType,
			MultiLevelSL:     toMultiLevelPayload(s.TradeConfig.MultiLevelSL),
			MultiLevelTP:     toMultiLevelPayload(s.TradeConfig.MultiLevelTP),
			TradeWindowStart: s.TradeConfig.TradeWindowStart,
			TradeWindowEnd:   s.TradeConfig.TradeWindowEnd,
			CreatedAt:        s.TradeConfig.CreatedAt.UnixNano(),
		}
	}

	// Risk limits
	if s.RiskLimits != nil {
		p.RiskLimits = RiskLimitsPayload{
			MaxDailyTrades:          valueOrZeroInt32(s.RiskLimits.MaxDailyTrades),
			MaxPerTradeRisk:         valueOrZeroFloat64(s.RiskLimits.MaxPerTradeRisk),
			MaxPortfolioExposurePct: valueOrZeroFloat64(s.RiskLimits.MaxPortfolioExposurePct),
			MaxLossPerDay:           valueOrZeroFloat64(s.RiskLimits.MaxLossPerDay),
			EnableRiskChecks:        s.RiskLimits.EnableRiskChecks,
			EnableAutoSquareOff:     s.RiskLimits.EnableAutoSquareOff,
			AutoSquareOffTime:       s.RiskLimits.AutoSquareOffTime,
			MaxAmountPerStock:       valueOrZeroFloat64(s.RiskLimits.MaxAmountPerStock),
			MaxTradesPerStrategy:    valueOrZeroInt32(s.RiskLimits.MaxTradesPerStrategy),
			CreatedAt:               s.RiskLimits.CreatedAt.UnixNano(),
		}
	}

	return p
}

// toMultiLevelPayload converts model multi-level levels to the Kafka payload type.
// Returns nil (omitted from JSON) when levels is empty.
func toMultiLevelPayload(levels []models.MultiLevelExitLevel) []MultiLevelExitLevelPayload {
	if len(levels) == 0 {
		return nil
	}
	out := make([]MultiLevelExitLevelPayload, len(levels))
	for i, l := range levels {
		out[i] = MultiLevelExitLevelPayload{
			LevelNum: l.LevelNum,
			PricePct: l.PricePct,
			QtyPct:   l.QtyPct,
		}
	}
	return out
}

func nilSafeStringSlice(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func valueOrZeroFloat64(v *float64) float64 {
	if v == nil {
		return 0
	}
	return *v
}

func valueOrEmptyString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

func valueOrZeroInt32(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
