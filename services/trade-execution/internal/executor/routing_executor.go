package executor

import (
	"context"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
)

// OrderExecutorFunc is a minimal interface satisfied by both OrderExecutor (live)
// and PaperOrderExecutor (paper). It is also the scheduler.OrderExecutorFunc interface —
// kept here to avoid an import cycle between executor ↔ scheduler.
type OrderExecutorFuncIface interface {
	ExecuteOrder(ctx context.Context, order *models.Order) error
}

// RoutingExecutor routes each order to the live or paper executor based on IsPaperTrade.
// It satisfies the scheduler.OrderExecutorFunc interface used by PriceMonitor and
// AutoSquareOffScheduler, allowing both paper and live orders to be handled via a
// single executor reference.
type RoutingExecutor struct {
	live  OrderExecutorFuncIface
	paper OrderExecutorFuncIface
}

// NewRoutingExecutor creates a routing executor.
// live is called for IsPaperTrade=false orders; paper for IsPaperTrade=true orders.
func NewRoutingExecutor(live, paper OrderExecutorFuncIface) *RoutingExecutor {
	return &RoutingExecutor{live: live, paper: paper}
}

// ExecuteOrder dispatches to the appropriate executor.
func (r *RoutingExecutor) ExecuteOrder(ctx context.Context, order *models.Order) error {
	if order.IsPaperTrade {
		return r.paper.ExecuteOrder(ctx, order)
	}
	return r.live.ExecuteOrder(ctx, order)
}
