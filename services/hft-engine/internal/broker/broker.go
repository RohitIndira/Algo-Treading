// Package broker defines the order-placement interface the strategy uses
// to talk to the outside world. Two implementations exist:
//
//   PaperBroker — Phase 2 default. Logs every call, generates fake order
//                 IDs, never touches Indira. Used in tests and during
//                 dev so a bug can never accidentally place real orders.
//
//   IndiraBroker — Phase 3. Wraps pkg/indira to place/modify/cancel real
//                  limit orders on the exchange.
//
// Anything that mutates the order book goes through this interface. The
// strategy layer never imports pkg/indira directly. That means:
//   - tests stay fast (no HTTP)
//   - flipping PAPER ↔ LIVE is one constructor call in main.go
//   - a future "mock broker that simulates partial fills" plugs in cleanly
package broker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// SymbolSpec carries the broker-facing identity of a stock. Built once at
// strategy Start time from state.Config (which has Symbol + ISIN +
// Exchange) plus the resolved Indira excTkn (looked up from external
// Redis in Phase 3).
type SymbolSpec struct {
	Symbol        string  // "IDEA"
	ISIN          string  // "INE669E01016"
	Exchange      string  // "NSE"
	ExchangeToken string  // numeric token Indira uses for orders, e.g. "14366"
	ProductType   string  // "INTRADAY" | "DELIVERY"
	TickSize      float64 // for rounding
}

// AuthContext is the per-request broker auth. We don't import pkg/indira's
// AuthContext directly to keep the broker package decoupled — the
// IndiraBroker implementation will translate AuthContext → indira.AuthContext
// at the boundary.
type AuthContext struct {
	UserID      string
	AppID       string
	Source      string
	BearerToken string
}

// Broker is the only thing the strategy goroutine knows about order placement.
//
// Concurrency: every call is independent; implementations must be safe
// to call from multiple goroutines. (We currently call from one goroutine
// per strategy, but the interface keeps the door open.)
type Broker interface {
	// PlaceLimit submits a new LIMIT order. Returns the broker-assigned
	// order id. Side is BUY (we're buying at ask) or SELL (selling at bid).
	PlaceLimit(ctx context.Context, auth *AuthContext, sym SymbolSpec,
		side state.Side, qty int, price float64) (brokerOrderID string, err error)

	// ModifyLimit changes the price (and optionally qty) of an existing
	// resting order. qty must be >= already-filled — passing less than
	// the broker thinks is traded triggers a rejection.
	ModifyLimit(ctx context.Context, auth *AuthContext, sym SymbolSpec,
		brokerOrderID string, qty int, newPrice float64) error

	// Cancel removes a resting order from the book. Returns nil if the
	// broker accepted the cancel OR replied "already filled / already
	// cancelled" (idempotent semantics — both are terminal-good for us).
	Cancel(ctx context.Context, auth *AuthContext, sym SymbolSpec,
		brokerOrderID string) error
}

// ─────────────────────────────────────────────────────────────────────────
// PaperBroker — the safe default for Phase 2 + tests
// ─────────────────────────────────────────────────────────────────────────

// PaperBroker satisfies Broker without touching any exchange. Every method
// logs, increments a sequence counter to manufacture an order id, and
// optionally calls OnPlace / OnModify / OnCancel callbacks (used by tests
// to drive synthetic fills back into the strategy).
type PaperBroker struct {
	logger *zap.Logger
	seq    atomic.Uint64

	// OnPlace, if set, is called from PlaceLimit BEFORE returning the
	// order id. Tests use it to immediately publish synthetic FillEvents.
	OnPlace  func(orderID string, sym SymbolSpec, side state.Side, qty int, price float64)
	OnModify func(orderID string, qty int, newPrice float64)
	OnCancel func(orderID string)
}

// NewPaperBroker builds an instance. logger is named so log lines are
// easy to grep (`hft-engine.paper-broker`).
func NewPaperBroker(logger *zap.Logger) *PaperBroker {
	return &PaperBroker{logger: logger.Named("paper-broker")}
}

// nextID returns a deterministic-ish broker order id. Real broker ids
// look like "NZWKE0001A<5"; ours look like "PAPER-1737540000-0001" so
// you can tell them apart in audit immediately.
func (b *PaperBroker) nextID() string {
	n := b.seq.Add(1)
	return fmt.Sprintf("PAPER-%d-%05d", time.Now().Unix(), n)
}

func (b *PaperBroker) PlaceLimit(ctx context.Context, auth *AuthContext, sym SymbolSpec,
	side state.Side, qty int, price float64) (string, error) {

	id := b.nextID()
	b.logger.Info("paper PLACE",
		zap.String("symbol", sym.Symbol),
		zap.String("side", string(side)),
		zap.Int("qty", qty),
		zap.Float64("price", price),
		zap.String("paper_order_id", id))

	if b.OnPlace != nil {
		b.OnPlace(id, sym, side, qty, price)
	}
	return id, nil
}

func (b *PaperBroker) ModifyLimit(ctx context.Context, auth *AuthContext, sym SymbolSpec,
	brokerOrderID string, qty int, newPrice float64) error {

	b.logger.Info("paper MODIFY",
		zap.String("symbol", sym.Symbol),
		zap.String("paper_order_id", brokerOrderID),
		zap.Int("qty", qty),
		zap.Float64("new_price", newPrice))

	if b.OnModify != nil {
		b.OnModify(brokerOrderID, qty, newPrice)
	}
	return nil
}

func (b *PaperBroker) Cancel(ctx context.Context, auth *AuthContext, sym SymbolSpec,
	brokerOrderID string) error {

	b.logger.Info("paper CANCEL",
		zap.String("symbol", sym.Symbol),
		zap.String("paper_order_id", brokerOrderID))

	if b.OnCancel != nil {
		b.OnCancel(brokerOrderID)
	}
	return nil
}
