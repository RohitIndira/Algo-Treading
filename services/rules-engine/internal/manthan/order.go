package manthan

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// OrderGenerator creates trade orders from allocation results.
// All MANTHAN orders are: DELIVERY + MARKET + BUY with trailing SL.
type OrderGenerator struct {
	logger *zap.Logger
}

func NewOrderGenerator(logger *zap.Logger) *OrderGenerator {
	return &OrderGenerator{logger: logger}
}

// ManthanOrder is the trade signal published to Kafka trade-signals topic
// and persisted to PostgreSQL.
type ManthanOrder struct {
	OrderID       string  `json:"order_id"`
	UserID        string  `json:"user_id"`
	StrategyID    string  `json:"strategy_id"`
	Symbol        string  `json:"symbol"`
	ISIN          string  `json:"isin"`
	Exchange      string  `json:"exchange"`
	OrderType     string  `json:"order_type"`     // MARKET
	OrderSide     string  `json:"order_side"`      // BUY
	ProductType   string  `json:"product_type"`    // DELIVERY
	Quantity      int32   `json:"quantity"`
	EntryPrice    float64 `json:"entry_price"`
	StopLoss      float64 `json:"stop_loss"`
	StopLossType  string  `json:"stop_loss_type"`  // TRAILING
	StopLossPct   float64 `json:"stop_loss_pct"`   // 20
	TrailingSLPct float64 `json:"trailing_sl_pct"` // 2
	InvestedAmt   float64 `json:"invested_amt"`
	TxnCostPct    float64 `json:"txn_cost_pct"`

	// Allocation context
	Industry   string  `json:"industry"`
	MCapBucket string  `json:"mcap_bucket"`
	IndexName  string  `json:"index_name"`
	EMAAllocPct float64 `json:"ema_alloc_pct"`

	// Auth fields removed 2026-06-25: broker creds are fetched at-edge by
	// trade-execution via user-config gRPC (see entry_handler.authProvider).
	// Embedding the encrypted token on the Kafka wire was a security leak.

	TradingMode string    `json:"trading_mode"` // PAPER or LIVE
	Timestamp   time.Time `json:"timestamp"`
}

// SLModifyOrder is published when trailing SL needs to be updated.
type SLModifyOrder struct {
	OrderID    string  `json:"order_id"`
	UserID     string  `json:"user_id"`
	StrategyID string  `json:"strategy_id"`
	Symbol     string  `json:"symbol"`
	Exchange   string  `json:"exchange"`
	OrderType  string  `json:"order_type"` // SL_MODIFY
	NewSL      float64 `json:"new_sl"`
	OldSL      float64 `json:"old_sl"`
	NewHigh    float64 `json:"new_high"`

	// Auth fields removed 2026-06-25 — fetched at-edge in trade-execution.
	TradingMode string    `json:"trading_mode"`
	Timestamp   time.Time `json:"timestamp"`
}

// SLExitOrder is published when trailing SL is triggered.
type SLExitOrder struct {
	OrderID    string  `json:"order_id"`
	UserID     string  `json:"user_id"`
	StrategyID string  `json:"strategy_id"`
	Symbol     string  `json:"symbol"`
	Exchange   string  `json:"exchange"`
	OrderType  string  `json:"order_type"`   // MARKET
	OrderSide  string  `json:"order_side"`    // SELL
	ProductType string `json:"product_type"`  // DELIVERY
	Quantity   int32   `json:"quantity"`
	ExitPrice  float64 `json:"exit_price"`
	SLPrice    float64 `json:"sl_price"`
	PnL        float64 `json:"pnl"`

	// Auth fields removed 2026-06-25 — fetched at-edge in trade-execution.
	TradingMode string    `json:"trading_mode"`
	Timestamp   time.Time `json:"timestamp"`
}

// GenerateEntryOrders creates entry orders from allocation results.
func (g *OrderGenerator) GenerateEntryOrders(
	strategy types.UserStrategy,
	allocations []types.AllocationResult,
) []ManthanOrder {
	orders := make([]ManthanOrder, 0, len(allocations))

	for _, alloc := range allocations {
		order := ManthanOrder{
			OrderID:       uuid.New().String(),
			UserID:        strategy.UserID,
			StrategyID:    strategy.StrategyID,
			Symbol:        alloc.Symbol,
			ISIN:          alloc.ISIN,
			Exchange:      "NSE",
			OrderType:     "MARKET",
			OrderSide:     "BUY",
			ProductType:   "DELIVERY",
			Quantity:      alloc.Quantity,
			EntryPrice:    alloc.EntryPrice,
			StopLoss:      alloc.InitialSL,
			StopLossType:  "TRAILING",
			StopLossPct:   strategy.StopLossPct,
			TrailingSLPct: strategy.TrailingSLPct,
			InvestedAmt:   float64(alloc.Quantity) * alloc.EntryPrice,
			TxnCostPct:    types.TotalTxnCostPct() * 100,
			Industry:      alloc.Industry,
			MCapBucket:    alloc.MCapBucket,
			IndexName:     alloc.IndexName,
			EMAAllocPct:   alloc.EMAAllocPct * 100,
			TradingMode:   strategy.TradingMode,
			Timestamp:     time.Now(),
		}
		orders = append(orders, order)

		g.logger.Info("Entry order generated",
			zap.String("symbol", alloc.Symbol),
			zap.Int32("qty", alloc.Quantity),
			zap.Float64("price", alloc.EntryPrice),
			zap.Float64("sl", alloc.InitialSL),
			zap.Float64("invested", order.InvestedAmt),
		)
	}
	return orders
}

// GenerateSLModify creates an SL modification order for trailing SL update.
func (g *OrderGenerator) GenerateSLModify(
	strategy types.UserStrategy,
	update SLUpdate,
) SLModifyOrder {
	return SLModifyOrder{
		OrderID:     fmt.Sprintf("slmod-%s-%s", update.Symbol, uuid.New().String()[:8]),
		UserID:      strategy.UserID,
		StrategyID:  strategy.StrategyID,
		Symbol:      update.Symbol,
		Exchange:    "NSE",
		OrderType:   "SL_MODIFY",
		NewSL:       update.NewSL,
		OldSL:       update.OldSL,
		NewHigh:     update.NewHigh,
		TradingMode: strategy.TradingMode,
		Timestamp:   time.Now(),
	}
}

// GenerateSLExit creates a sell order when trailing SL is triggered.
func (g *OrderGenerator) GenerateSLExit(
	strategy types.UserStrategy,
	pos *types.Position,
	exitPrice, pnl float64,
) SLExitOrder {
	return SLExitOrder{
		OrderID:     fmt.Sprintf("slexit-%s-%s", pos.Symbol, uuid.New().String()[:8]),
		UserID:      strategy.UserID,
		StrategyID:  strategy.StrategyID,
		Symbol:      pos.Symbol,
		Exchange:    "NSE",
		OrderType:   "MARKET",
		OrderSide:   "SELL",
		ProductType: "DELIVERY",
		Quantity:    pos.Quantity,
		ExitPrice:   exitPrice,
		SLPrice:     pos.CurrentSL,
		PnL:         pnl,
		TradingMode: strategy.TradingMode,
		Timestamp:   time.Now(),
	}
}
