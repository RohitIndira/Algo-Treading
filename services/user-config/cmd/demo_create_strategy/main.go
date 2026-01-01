package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
)

func main() {
	// Build a sample depth-only BUY strategy
	strategy := &models.Strategy{
		StrategyID:   uuid.New(),
		UserID:       "demo-user",
		StrategyName: "DemoDepthBuy",
		Description:  "Demo: Buy based on depth (best ask small, bid large)",
		Active:       true,
		Version:      1,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		Conditions: &models.StrategyCondition{
			ConditionID:    uuid.New(),
			ImpactScoreThreshold: 1,
			StockCodes:      []int64{12345},
			Exchanges:       nil,
			MinBidQuantity:  ptrInt64(1000),
			MinAskQuantity:  ptrInt64(500),
			MaxSpreadPct:    ptrFloat64(0.2),
			DepthOnly:       true,
			RequireLTPBetweenSpread: ptrBool(true),
			CreatedAt:       time.Now(),
		},
		TradeConfig: &models.TradeConfig{
			TradeConfigID: uuid.New(),
			OrderType:     "MARKET",
			Quantity:      10,
			Exchange:      "NSE",
			OrderSide:     "BUY",
			Validity:      "DAY",
			CreatedAt:     time.Now(),
		},
		RiskLimits: &models.RiskLimits{
			RiskLimitID: uuid.New(),
			PositionSizing: "FIXED",
			EnableRiskChecks: false,
			CreatedAt: time.Now(),
		},
	}

	// Build event similar to ConfigEvent used for Kafka
	event := map[string]interface{}{
		"event_type": "CREATE",
		"strategy":   strategy,
		"timestamp":  time.Now().Unix(),
	}

	b, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		panic(err)
	}

	fmt.Println(string(b))
}

func ptrInt64(v int64) *int64 { return &v }
func ptrFloat64(v float64) *float64 { return &v }
func ptrBool(v bool) *bool { return &v }
