package manthan

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/manthan/types"
)

// manthanSignalIDNamespace is a fixed, arbitrary UUID that scopes every
// deterministic signal_id we mint in this package. Using a project-owned
// namespace (not one of the well-known DNS/URL namespaces) means our
// UUIDs can never collide with anything else in the ecosystem — the
// namespace bits are unique to Manthan and stable forever.
//
// SECURITY: this value must NEVER change. Changing it re-keys every
// existing deterministic id, which would break idempotency across the
// upgrade (a signal generated pre-change vs post-change would compute
// different ids, and downstream dedup would let both through).
var manthanSignalIDNamespace = uuid.MustParse("6f4f7f2c-3b0f-4c5a-8c11-9ddaa63d8b6a")

// deterministicSignalID returns a v5 UUID derived from the inputs. Same
// inputs always produce the same UUID — that's the whole point. It lets
// us anchor idempotency at the DB constraint level (UNIQUE(signal_id))
// so no matter how many times rules-engine restarts, replays Kafka, or
// gets duplicate manthan_signals from data-ingestion, we mint the SAME
// id for the same (strategy, symbol, day[, discriminator]) tuple and
// the second INSERT to manthan_signal_decisions cleanly conflicts.
//
// key format is "field1|field2|..." — the pipe is just a readable
// separator; we don't parse it back out. Callers pass whatever
// discriminates one legitimate signal from another of the same shape:
//   entry:      strategyID|symbol|runDate
//   SL modify:  strategyID|symbol|runDate|SLMOD|<newSL:2dp>
//   SL exit:    strategyID|symbol|runDate|EXIT|<slPrice:2dp>
//
// The 2dp price truncation means "SL modify to 220.34" dedups against
// itself but not against "SL modify to 220.35" — which is the intended
// semantics for option (b): duplicates at the same level are dropped,
// legitimate ratchets to a new level go through.
func deterministicSignalID(parts ...string) string {
	key := ""
	for i, p := range parts {
		if i > 0 {
			key += "|"
		}
		key += p
	}
	return uuid.NewSHA1(manthanSignalIDNamespace, []byte(key)).String()
}

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
		// Idempotent OrderID — same (strategy, symbol, runDate) always yields
		// the same UUID. Downstream UNIQUE(signal_id) on manthan_signal_decisions
		// rejects the second attempt on any replay path (rules-engine restart,
		// Kafka rewind, manthan-live re-fire). See deterministicSignalID doc.
		//
		// alloc.RunDate is the source signal's run_date (see AllocationResult).
		// Never fall back to today() here — a cross-midnight retry would compute
		// a different id and break dedup.
		order := ManthanOrder{
			OrderID:       deterministicSignalID(strategy.StrategyID, alloc.Symbol, alloc.RunDate),
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
//
// Idempotent by (strategy, symbol, today, newSL). Two consecutive ticks that
// each produce the same new SL level dedup (only one modify goes to broker).
// A ratchet to a different SL level cleanly produces a different OrderID.
// Uses todayIST rather than a stored run_date because SL modifications are
// strictly intra-market (09:15–15:30 IST); no cross-midnight risk.
func (g *OrderGenerator) GenerateSLModify(
	strategy types.UserStrategy,
	update SLUpdate,
) SLModifyOrder {
	return SLModifyOrder{
		OrderID: deterministicSignalID(
			strategy.StrategyID, update.Symbol, todayIST(),
			"SLMOD", fmt.Sprintf("%.2f", update.NewSL),
		),
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
//
// Idempotent by (strategy, symbol, today, EXIT, slPrice). Two ticks that both
// cross the same SL level (typical: LTP oscillates around the trigger for
// a second or two before the SL fires) dedup — only one MARKET SELL goes
// out. Uses todayIST for the same reason as SL_MODIFY (intra-market only).
// slPrice as tiebreaker means: if the position rebuilds and gets a new SL
// at a different level and THAT one also triggers, it produces a distinct
// OrderID (though same-day re-entry after SL exit is not expected for
// Manthan, this keeps the id safe for future re-entry variants).
func (g *OrderGenerator) GenerateSLExit(
	strategy types.UserStrategy,
	pos *types.Position,
	exitPrice, pnl float64,
) SLExitOrder {
	return SLExitOrder{
		OrderID: deterministicSignalID(
			strategy.StrategyID, pos.Symbol, todayIST(),
			"EXIT", fmt.Sprintf("%.2f", pos.CurrentSL),
		),
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
