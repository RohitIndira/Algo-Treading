package executor

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/processor"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// MockBrokerExecutor simulates broker execution for testing
type MockBrokerExecutor struct {
	successRate float64 // 0.0 to 1.0
	logger      *zap.Logger
}

// NewMockBrokerExecutor creates a new mock broker executor
func NewMockBrokerExecutor(successRate float64, logger *zap.Logger) *MockBrokerExecutor {
	if successRate < 0 || successRate > 1 {
		successRate = 0.95 // Default 95% success rate
	}

	return &MockBrokerExecutor{
		successRate: successRate,
		logger:      logger,
	}
}

// ExecuteOrder simulates order execution
func (m *MockBrokerExecutor) ExecuteOrder(ctx context.Context, signal *models.TradeSignal) (*processor.BrokerExecutionResult, error) {
	m.logger.Info("Mock broker: Executing order",
		zap.String("order_id", signal.OrderID),
		zap.String("symbol", signal.Symbol),
		zap.Float64("price", signal.Price))

	// Simulate network latency
	time.Sleep(time.Duration(50+rand.Intn(200)) * time.Millisecond)

	// Simulate occasional failures
	if rand.Float64() > m.successRate {
		return nil, fmt.Errorf("mock broker: simulated execution failure")
	}

	// Calculate executed price with slippage (±0.5%)
	slippage := (rand.Float64() - 0.5) * 0.01 // -0.5% to +0.5%
	executedPrice := signal.Price * (1 + slippage)

	// Calculate charges
	baseValue := float64(signal.Quantity) * executedPrice
	brokerage := baseValue * 0.0005       // 0.05% brokerage
	exchangeCharges := baseValue * 0.0003 // 0.03% exchange charges

	result := &processor.BrokerExecutionResult{
		BrokerOrderID:   fmt.Sprintf("MOCK_%s", uuid.New().String()[:8]),
		Status:          "EXECUTED",
		ExecutedPrice:   executedPrice,
		ExecutedQty:     signal.Quantity,
		Brokerage:       brokerage,
		ExchangeCharges: exchangeCharges,
		Timestamp:       time.Now(),
	}

	m.logger.Info("Mock broker: Order executed successfully",
		zap.String("broker_order_id", result.BrokerOrderID),
		zap.Float64("executed_price", executedPrice),
		zap.Float64("slippage", slippage*100))

	return result, nil
}
