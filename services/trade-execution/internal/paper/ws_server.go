package paper

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/metrics"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/scheduler"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/timezone"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	// writeWait is the maximum time allowed to write a message to a client.
	writeWait = 10 * time.Second
	// pongWait is the maximum time to wait for a pong response from the client.
	pongWait = 60 * time.Second
	// pingPeriod is how often the server pings the client (must be < pongWait).
	pingPeriod = (pongWait * 9) / 10
)

func fmtIST(t time.Time) string {
	return t.In(timezone.IST).Format(time.RFC3339)
}

func fmtISTPtr(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := fmtIST(*t)
	return &s
}

// orderISTWrapper shadows the time.Time timestamp fields of models.Order with
// IST-formatted RFC3339 strings so the frontend receives IST times directly.
type orderISTWrapper struct {
	*models.Order
	CreatedAt     string  `json:"created_at"`
	UpdatedAt     string  `json:"updated_at"`
	SubmittedAt   *string `json:"submitted_at,omitempty"`
	ExecutedAt    *string `json:"executed_at,omitempty"`   // entry_time alias
	EntryTime     *string `json:"entry_time,omitempty"`    // same as executed_at, explicit label
	PaperExitTime *string `json:"paper_exit_time,omitempty"` // exit_time for paper positions
}

func wrapOrderIST(o *models.Order) *orderISTWrapper {
	return &orderISTWrapper{
		Order:         o,
		CreatedAt:     fmtIST(o.CreatedAt),
		UpdatedAt:     fmtIST(o.UpdatedAt),
		SubmittedAt:   fmtISTPtr(o.SubmittedAt),
		ExecutedAt:    fmtISTPtr(o.ExecutedAt),
		EntryTime:     fmtISTPtr(o.ExecutedAt),
		PaperExitTime: fmtISTPtr(o.PaperExitTime),
	}
}

func wrapOrdersIST(orders []*models.Order) []*orderISTWrapper {
	wrapped := make([]*orderISTWrapper, len(orders))
	for i, o := range orders {
		wrapped[i] = wrapOrderIST(o)
	}
	return wrapped
}

// PaperUpdate is the message sent to frontend clients over the paper trading WebSocket.
type PaperUpdate struct {
	Type       string          `json:"type"`                 // connected | pnl_update | position_exit | force_exit_done | initial_orders | new_order | ml_level_triggered
	OrderID    string          `json:"order_id,omitempty"`
	UserID     string          `json:"user_id,omitempty"`
	Symbol     string          `json:"symbol,omitempty"`
	LTP        float64         `json:"ltp,omitempty"`
	LivePnL    float64         `json:"live_pnl"`              // Ongoing PnL (always sent, even when 0)
	FinalPnL   float64         `json:"final_pnl,omitempty"`   // On exit
	CurrentSL  float64         `json:"current_sl,omitempty"`  // Updated trailing SL price (non-zero only when it changed)
	Reason     string          `json:"reason,omitempty"`    // STOP_LOSS | TAKE_PROFIT | FORCE_EXIT | MULTI_LEVEL_COMPLETE
	ExitPrice  float64         `json:"exit_price,omitempty"`
	EntryPrice float64         `json:"entry_price,omitempty"`
	EntryTime  *time.Time      `json:"entry_time,omitempty"`  // When the entry order was filled (executed_at)
	ExitTime   *time.Time      `json:"exit_time,omitempty"`   // When the position was closed (paper_exit_time)
	Positions  []*models.Order `json:"positions,omitempty"` // Used for initial_orders
	Order      *models.Order   `json:"order,omitempty"`     // Used for new_order
	// Fields for ml_level_triggered
	ExitType              string `json:"exit_type,omitempty"`               // "SL" | "TP"
	LevelNum              int    `json:"level_num,omitempty"`
	RemainingQty          int32  `json:"remaining_qty,omitempty"`           // qty still open after this level fires
	CancelledExitType     string `json:"cancelled_exit_type,omitempty"`     // opposite side level cancelled as side effect
	CancelledLevelNum     int    `json:"cancelled_level_num"`               // -1 = none (always serialised so frontend can read it)
}

func (u PaperUpdate) MarshalJSON() ([]byte, error) {
	type alias struct {
		Type              string             `json:"type"`
		OrderID           string             `json:"order_id,omitempty"`
		UserID            string             `json:"user_id,omitempty"`
		Symbol            string             `json:"symbol,omitempty"`
		LTP               float64            `json:"ltp,omitempty"`
		LivePnL           float64            `json:"live_pnl"`
		FinalPnL          float64            `json:"final_pnl,omitempty"`
		CurrentSL         float64            `json:"current_sl,omitempty"`
		Reason            string             `json:"reason,omitempty"`
		ExitPrice         float64            `json:"exit_price,omitempty"`
		EntryPrice        float64            `json:"entry_price,omitempty"`
		EntryTime         *string            `json:"entry_time,omitempty"`
		ExitTime          *string            `json:"exit_time,omitempty"`
		Positions         []*orderISTWrapper `json:"positions,omitempty"`
		Order             *orderISTWrapper   `json:"order,omitempty"`
		ExitType          string             `json:"exit_type,omitempty"`
		LevelNum          int                `json:"level_num,omitempty"`
		RemainingQty      int32              `json:"remaining_qty,omitempty"`
		CancelledExitType string             `json:"cancelled_exit_type,omitempty"`
		CancelledLevelNum int                `json:"cancelled_level_num"`
	}
	a := alias{
		Type:              u.Type,
		OrderID:           u.OrderID,
		UserID:            u.UserID,
		Symbol:            u.Symbol,
		LTP:               u.LTP,
		LivePnL:           u.LivePnL,
		FinalPnL:          u.FinalPnL,
		CurrentSL:         u.CurrentSL,
		Reason:            u.Reason,
		ExitPrice:         u.ExitPrice,
		EntryPrice:        u.EntryPrice,
		EntryTime:         fmtISTPtr(u.EntryTime),
		ExitTime:          fmtISTPtr(u.ExitTime),
		ExitType:          u.ExitType,
		LevelNum:          u.LevelNum,
		RemainingQty:      u.RemainingQty,
		CancelledExitType: u.CancelledExitType,
		CancelledLevelNum: u.CancelledLevelNum,
	}
	if u.Positions != nil {
		a.Positions = wrapOrdersIST(u.Positions)
	}
	if u.Order != nil {
		a.Order = wrapOrderIST(u.Order)
	}
	return json.Marshal(a)
}

// LiveOrderUpdate is the message sent to frontend clients over the live orders WebSocket.
type LiveOrderUpdate struct {
	Type         string                     `json:"type"`                    // connected | initial_orders | initial_closed_orders | order_update | closed_orders | force_exit_done | price_watches_snapshot | price_watch_cancelled | token_expired | positions
	UserID       string                     `json:"user_id,omitempty"`
	Orders       []*models.Order            `json:"orders,omitempty"`        // For initial_orders, initial_closed_orders, closed_orders
	Order        *models.Order              `json:"order,omitempty"`         // For order_update
	PriceWatches []scheduler.WatchSnapshot  `json:"price_watches,omitempty"` // For price_watches_snapshot
	Positions    []EnrichedAlgoPosition     `json:"positions,omitempty"`     // For positions (event-driven push after fills)
}

func (u LiveOrderUpdate) MarshalJSON() ([]byte, error) {
	type alias struct {
		Type         string                    `json:"type"`
		UserID       string                    `json:"user_id,omitempty"`
		Orders       []*orderISTWrapper        `json:"orders,omitempty"`
		Order        *orderISTWrapper          `json:"order,omitempty"`
		PriceWatches []scheduler.WatchSnapshot `json:"price_watches,omitempty"`
		Positions    []EnrichedAlgoPosition    `json:"positions,omitempty"`
	}
	a := alias{
		Type:         u.Type,
		UserID:       u.UserID,
		PriceWatches: u.PriceWatches,
		Positions:    u.Positions,
	}
	if u.Orders != nil {
		a.Orders = wrapOrdersIST(u.Orders)
	}
	if u.Order != nil {
		a.Order = wrapOrderIST(u.Order)
	}
	return json.Marshal(a)
}

// BrokerPosition is a position returned by the Indira broker API.
// Defined here to avoid importing pkg/indira into the paper package.
type BrokerPosition struct {
	Symbol        string  `json:"symbol"`
	Exchange      string  `json:"exchange"`
	ProductType   string  `json:"product_type"`
	NetQty        int     `json:"net_qty"`
	BuyQty        int     `json:"buy_qty"`
	SellQty       int     `json:"sell_qty"`
	BuyAvgPrice   float64 `json:"buy_avg_price"`
	SellAvgPrice  float64 `json:"sell_avg_price"`
	CurrentPrice  float64 `json:"current_price"`
	PnL           float64 `json:"pnl"`
	PnLPercentage float64 `json:"pnl_percentage"`
	ExcTkn        int     `json:"exc_tkn,omitempty"`
}

// EnrichedAlgoPosition is a broker position filtered and enriched with our algo order metadata.
type EnrichedAlgoPosition struct {
	// Broker data
	Symbol        string  `json:"symbol"`
	Exchange      string  `json:"exchange"`
	ProductType   string  `json:"product_type"`
	NetQty        int     `json:"net_qty"`
	BuyQty        int     `json:"buy_qty"`
	SellQty       int     `json:"sell_qty"`
	BuyAvgPrice   float64 `json:"buy_avg_price"`
	SellAvgPrice  float64 `json:"sell_avg_price"`
	CurrentPrice  float64 `json:"current_price"`
	BrokerPnL     float64 `json:"broker_pnl"`
	BrokerPnLPct  float64 `json:"broker_pnl_pct"`
	ExcTkn        int     `json:"exc_tkn,omitempty"`
	// Our algo order metadata
	OrderID       string  `json:"order_id,omitempty"`
	StrategyID    string  `json:"strategy_id,omitempty"`
	StrategyName  string  `json:"strategy_name,omitempty"`
	IndiraOrderID string  `json:"indira_order_id,omitempty"`
	OrderSide     string  `json:"order_side,omitempty"`
	FilledPrice   float64 `json:"filled_price,omitempty"`
	FilledQty     int32   `json:"filled_qty,omitempty"`
	TradingMode   string  `json:"trading_mode,omitempty"`
	Status        string  `json:"status,omitempty"`
	ExecutedAt    string  `json:"executed_at,omitempty"`
}

// strategyShare tracks what fraction of a broker position belongs to a strategy.
type strategyShare struct {
	StrategyID   string
	StrategyName string
	TradingMode  string
	EntryQty     int    // qty from entry orders (used to compute ratio)
	EntryOrderID string // earliest entry order_id
	EntryTime    string // earliest entry time (ISO)
	OrderSide    string // side of the entry order
}

// enrichPositions uses broker positions as the source of truth for quantities,
// prices, exchange data, and P&L. Our orders DB is used ONLY to determine which
// strategy each position belongs to.
//
// When multiple strategies trade the same symbol, the broker shows one merged
// position. This function splits it proportionally across strategies based on
// the entry order quantities in our DB.
func enrichPositions(brokerPositions []BrokerPosition, algoOrders []*models.Order) []EnrichedAlgoPosition {
	// Pre-scan: build a strategy_id → strategy_name lookup from ALL orders (including
	// SELL, square-off, etc.) so we can backfill names even when entry orders lack them.
	strategyNameLookup := make(map[string]string)
	for _, order := range algoOrders {
		if order.StrategyID != "" && order.StrategyName != "" {
			if _, exists := strategyNameLookup[order.StrategyID]; !exists {
				strategyNameLookup[order.StrategyID] = order.StrategyName
			}
		}
	}

	// Step 1: From our orders, find the FIRST BUY entry order per (symbol, strategy_id).
	// We only look at the initial entry order to determine each strategy's share.
	// Use a dedup key to avoid counting duplicate/retry orders.
	type symStratKey struct {
		Symbol     string
		StrategyID string
	}

	shares := make(map[symStratKey]*strategyShare)
	// Track (symbol, strategy_id, side, qty) to deduplicate identical orders
	type dedupKey struct {
		Symbol     string
		StrategyID string
		Side       string
		Qty        int32
	}
	seen := make(map[dedupKey]bool)

	for _, order := range algoOrders {
		if order.IsSquareOffOrder {
			continue
		}
		// Only consider entry orders (BUY side for long strategies).
		// Skip SL/TP legs which are SELL orders for the same strategy.
		if order.OrderSide != models.OrderSideBuy {
			continue
		}

		sym := strings.ToUpper(order.Symbol)
		sid := order.StrategyID
		if sid == "" {
			sid = "__manual__"
		}

		// Deduplicate: skip if we've already seen an identical (symbol, strategy, side, qty) order.
		dk := dedupKey{Symbol: sym, StrategyID: sid, Side: string(order.OrderSide), Qty: order.Quantity}
		if seen[dk] {
			continue
		}
		seen[dk] = true

		// Resolve strategy name: order itself > lookup from all orders
		stratName := order.StrategyName
		if stratName == "" {
			stratName = strategyNameLookup[order.StrategyID]
		}

		key := symStratKey{Symbol: sym, StrategyID: sid}
		share, exists := shares[key]
		if !exists {
			share = &strategyShare{
				StrategyID:   order.StrategyID,
				StrategyName: stratName,
				TradingMode:  order.TradingMode,
				OrderSide:    string(order.OrderSide),
			}
			shares[key] = share
		}
		// Backfill strategy_name if earlier orders had it empty
		if share.StrategyName == "" && stratName != "" {
			share.StrategyName = stratName
		}

		share.EntryQty += int(order.Quantity)

		// Track earliest entry order for metadata
		entryTime := ""
		if order.ExecutedAt != nil {
			entryTime = fmtIST(*order.ExecutedAt)
		} else if !order.CreatedAt.IsZero() {
			entryTime = fmtIST(order.CreatedAt)
		}
		if share.EntryOrderID == "" || (entryTime != "" && entryTime < share.EntryTime) {
			share.EntryOrderID = order.OrderID.String()
			share.EntryTime = entryTime
		}
	}

	// Step 2: Group shares by symbol so we know which strategies trade each symbol.
	symbolShares := make(map[string][]*strategyShare) // symbol → list of strategy shares
	for key, share := range shares {
		symbolShares[key.Symbol] = append(symbolShares[key.Symbol], share)
	}

	// Step 3: For each broker position, split across strategies proportionally.
	result := make([]EnrichedAlgoPosition, 0, len(brokerPositions))

	for _, bp := range brokerPositions {
		bpSym := strings.ToUpper(bp.Symbol)

		// Find matching strategies: try exact match, then substring
		stratShares, matched := symbolShares[bpSym]
		if !matched {
			for sym, ss := range symbolShares {
				if strings.Contains(bpSym, sym) || strings.Contains(sym, bpSym) {
					stratShares = ss
					matched = true
					break
				}
			}
		}

		if !matched || len(stratShares) == 0 {
			// No algo orders for this symbol — emit as broker-direct/manual position
			result = append(result, EnrichedAlgoPosition{
				Symbol:       bp.Symbol,
				Exchange:     bp.Exchange,
				ProductType:  bp.ProductType,
				NetQty:       bp.NetQty,
				BuyQty:       bp.BuyQty,
				SellQty:      bp.SellQty,
				BuyAvgPrice:  bp.BuyAvgPrice,
				SellAvgPrice: bp.SellAvgPrice,
				CurrentPrice: bp.CurrentPrice,
				BrokerPnL:    bp.PnL,
				BrokerPnLPct: bp.PnLPercentage,
				ExcTkn:       bp.ExcTkn,
			})
			continue
		}

		if len(stratShares) == 1 {
			// Single strategy owns the entire broker position — no splitting needed.
			ss := stratShares[0]
			result = append(result, EnrichedAlgoPosition{
				Symbol:       bp.Symbol,
				Exchange:     bp.Exchange,
				ProductType:  bp.ProductType,
				NetQty:       bp.NetQty,
				BuyQty:       bp.BuyQty,
				SellQty:      bp.SellQty,
				BuyAvgPrice:  bp.BuyAvgPrice,
				SellAvgPrice: bp.SellAvgPrice,
				CurrentPrice: bp.CurrentPrice,
				BrokerPnL:    bp.PnL,
				BrokerPnLPct: bp.PnLPercentage,
				ExcTkn:       bp.ExcTkn,
				// Algo metadata
				OrderID:      ss.EntryOrderID,
				StrategyID:   ss.StrategyID,
				StrategyName: ss.StrategyName,
				OrderSide:    ss.OrderSide,
				FilledPrice:  bp.BuyAvgPrice,
				FilledQty:    int32(bp.BuyQty),
				TradingMode:  ss.TradingMode,
				Status:       "FILLED",
				ExecutedAt:   ss.EntryTime,
			})
			continue
		}

		// Multiple strategies trade this symbol — split proportionally.
		totalEntryQty := 0
		for _, ss := range stratShares {
			totalEntryQty += ss.EntryQty
		}
		if totalEntryQty == 0 {
			totalEntryQty = 1 // avoid division by zero
		}

		// Track remainder for rounding — ensure quantities sum to broker total.
		assignedBuyQty := 0
		assignedSellQty := 0
		assignedNetQty := 0

		for i, ss := range stratShares {
			ratio := float64(ss.EntryQty) / float64(totalEntryQty)
			isLast := i == len(stratShares)-1

			var splitBuyQty, splitSellQty, splitNetQty int
			if isLast {
				// Last strategy gets the remainder to ensure totals match broker.
				splitBuyQty = bp.BuyQty - assignedBuyQty
				splitSellQty = bp.SellQty - assignedSellQty
				splitNetQty = bp.NetQty - assignedNetQty
			} else {
				splitBuyQty = int(math.Round(float64(bp.BuyQty) * ratio))
				splitSellQty = int(math.Round(float64(bp.SellQty) * ratio))
				splitNetQty = int(math.Round(float64(bp.NetQty) * ratio))
			}
			assignedBuyQty += splitBuyQty
			assignedSellQty += splitSellQty
			assignedNetQty += splitNetQty

			// P&L is proportional to this strategy's share.
			splitPnL := bp.PnL * ratio

			result = append(result, EnrichedAlgoPosition{
				Symbol:       bp.Symbol,
				Exchange:     bp.Exchange,
				ProductType:  bp.ProductType,
				NetQty:       splitNetQty,
				BuyQty:       splitBuyQty,
				SellQty:      splitSellQty,
				BuyAvgPrice:  bp.BuyAvgPrice,
				SellAvgPrice: bp.SellAvgPrice,
				CurrentPrice: bp.CurrentPrice,
				BrokerPnL:    splitPnL,
				BrokerPnLPct: bp.PnLPercentage,
				ExcTkn:       bp.ExcTkn,
				// Algo metadata
				OrderID:      ss.EntryOrderID,
				StrategyID:   ss.StrategyID,
				StrategyName: ss.StrategyName,
				OrderSide:    ss.OrderSide,
				FilledPrice:  bp.BuyAvgPrice,
				FilledQty:    int32(splitBuyQty),
				TradingMode:  ss.TradingMode,
				Status:       "FILLED",
				ExecutedAt:   ss.EntryTime,
			})
		}
	}

	return result
}

// PositionsFetcher fetches live positions from the broker using the provided auth credentials.
// bearerToken, appId, userId, source come from the frontend request headers.
type PositionsFetcher func(ctx context.Context, bearerToken, appId, userId, source string) ([]BrokerPosition, error)


// OCOCanceller cancels active OCO groups.
// Implemented by oco.OCOManager; defined here as an interface to avoid import cycles.
type OCOCanceller interface {
	CancelAllGroupsByUser(ctx context.Context, userID string)
	CancelGroupsByStrategy(ctx context.Context, userID, strategyID string)
}

// ExitOrderPlacer places a reverse limit order at the broker to exit a position.
// limitPrice is LTP±1% to ensure immediate fill without using a market order (compliance).
// Wired in main.go to OrderExecutor.
type ExitOrderPlacer func(ctx context.Context, originalOrder *models.Order, limitPrice float64, bearerToken, appId, source string) error

var wsUpgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  1024,
	WriteBufferSize: 4096,
}

// PaperWSServer is the WebSocket server that pushes live PnL updates (paper) and
// order status updates (live) to the frontend — both run on the same port (PAPER_WS_PORT).
type PaperWSServer struct {
	repo             repository.OrderRepository
	monitor          *PaperTradeMonitor // set after monitor is created
	positionsFetcher PositionsFetcher   // set via SetPositionsFetcher
	// startBrokerWS is wired in main.go to statusService.StartSubscription so the paper
	// package doesn't import statusservice and create an import cycle.
	startBrokerWS func(ctx context.Context, userID, bearerToken, appID, source string) error
	// resumeBrokerWS is wired in main.go to statusService.ResumeUserSubscription.
	// Called when the frontend sends a resume_token message on /ws/live-orders.
	resumeBrokerWS func(ctx context.Context, userID, bearerToken, appID, source string) error

	// priceMonitor exposes the watch snapshot for the "price watches" UI panel.
	priceMonitor       *scheduler.PriceMonitor
	lastWatchBroadcast time.Time // throttle WS broadcasts

	// ocoCanceller cancels active OCO groups on force-exit.
	ocoCanceller OCOCanceller

	// exitOrderPlacer places reverse limit orders at the broker for live position exits.
	exitOrderPlacer ExitOrderPlacer

	// Paper trade clients: sync.Map[userID string → *sync.Map[*websocket.Conn → *sync.Mutex]]
	// Lock-free reads on the hot path (Broadcast).
	clients sync.Map

	// Live order clients: sync.Map[userID string → *sync.Map[*websocket.Conn → *sync.Mutex]]
	liveClients sync.Map
}

// NewPaperWSServer creates the WebSocket server.
func NewPaperWSServer(repo repository.OrderRepository) *PaperWSServer {
	return &PaperWSServer{
		repo: repo,
	}
}

// SetMonitor wires the monitor (must be called after both are created to break circular dep).
func (s *PaperWSServer) SetMonitor(m *PaperTradeMonitor) {
	s.monitor = m
}

// SetOCOCanceller wires the OCO canceller for force-exit cleanup.
func (s *PaperWSServer) SetOCOCanceller(c OCOCanceller) {
	s.ocoCanceller = c
}

// SetExitOrderPlacer wires the callback that places reverse limit orders at the broker.
func (s *PaperWSServer) SetExitOrderPlacer(fn ExitOrderPlacer) {
	s.exitOrderPlacer = fn
}

// SetPositionsFetcher wires the Indira positions fetcher used by the indira-positions endpoint.
func (s *PaperWSServer) SetPositionsFetcher(fn PositionsFetcher) {
	s.positionsFetcher = fn
}

// SetStatusService wires the broker WS subscription starter.
// fn must call statusService.StartSubscription under the hood.
// Called from main.go to avoid import cycles between paper ↔ statusservice.
func (s *PaperWSServer) SetStatusService(fn func(ctx context.Context, userID, bearerToken, appID, source string) error) {
	s.startBrokerWS = fn
}

// SetResumeService wires the broker WS resume callback.
// fn must call statusService.ResumeUserSubscription under the hood.
// Called from main.go; invoked when the frontend sends a resume_token message.
func (s *PaperWSServer) SetResumeService(fn func(ctx context.Context, userID, bearerToken, appID, source string) error) {
	s.resumeBrokerWS = fn
}

// SetPriceMonitor wires the PriceMonitor for the price-watches endpoint.
func (s *PaperWSServer) SetPriceMonitor(pm *scheduler.PriceMonitor) {
	s.priceMonitor = pm
}

// RegisterRoutes registers all paper trading and live order HTTP/WebSocket endpoints onto a mux.
func (s *PaperWSServer) RegisterRoutes(mux *http.ServeMux) {
	// Paper trading
	mux.HandleFunc("/ws/paper-trades", s.handleWSConnection)
	mux.HandleFunc("/ws/paper-trades/positions", s.handleGetPositions)
	mux.HandleFunc("/ws/paper-trades/closed-orders", s.handleGetClosedPaperOrders)
	mux.HandleFunc("/ws/paper-trades/force-exit-all", s.handleForceExitAll)
	mux.HandleFunc("/ws/paper-trades/force-exit-strategy", s.handleForceExitStrategyPaper)
	// Live orders
	mux.HandleFunc("/ws/live-orders", s.handleLiveOrdersWS)
	mux.HandleFunc("/ws/live-orders/closed-orders", s.handleGetClosedLiveOrders)
	mux.HandleFunc("/ws/live-orders/force-exit-all", s.handleForceExitAllLive)
	mux.HandleFunc("/ws/live-orders/force-exit-strategy", s.handleForceExitStrategyLive)
	mux.HandleFunc("/ws/live-orders/indira-positions", s.handleIndiraPositions)
	mux.HandleFunc("/ws/live-orders/subscribe-broker-ws", s.handleSubscribeBrokerWS)
	mux.HandleFunc("/ws/live-orders/price-watches", s.handleGetPriceWatches)
	mux.HandleFunc("/ws/live-orders/cancel-price-watch", s.handleCancelPriceWatch)
	// Auto square-off config (trade-execution native, no risk-management dependency)
	mux.HandleFunc("/ws/auto-square-off/config", s.handleAutoSquareOffConfig)
	// Dashboard
	mux.HandleFunc("/ws/dashboard-stats", s.handleGetDashboardStats)
}

// ─────────────────────────────────────────────────────────────────────────────
// PAPER TRADING handlers
// ─────────────────────────────────────────────────────────────────────────────

// handleWSConnection upgrades an HTTP request to a WebSocket and streams PnL updates.
// GET /ws/paper-trades?user_id=xxx
func (s *PaperWSServer) handleWSConnection(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[paper-ws] Upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("[paper-ws] Client connected: user=%s remote=%s", userID, conn.RemoteAddr())

	// Keepalive: detect dead connections within pongWait (60s).
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Send welcome and initial orders before registering for broadcasts,
	// so concurrent Broadcast calls cannot race with these writes.
	conn.WriteJSON(PaperUpdate{Type: "connected", UserID: userID})

	orders, err := s.repo.GetFilledPaperOrdersByUser(r.Context(), userID)
	if err != nil {
		log.Printf("[paper-ws] Failed to fetch initial orders for user %s: %v", userID, err)
	} else {
		conn.WriteJSON(PaperUpdate{
			Type:      "initial_orders",
			UserID:    userID,
			Positions: orders,
		})
	}

	// Immediately push current PnL snapshot so the client doesn't wait for the next tick.
	if s.monitor != nil {
		for _, snap := range s.monitor.GetCurrentPnLSnapshot(r.Context(), userID) {
			conn.WriteJSON(snap)
		}
	}

	connMu := s.registerClient(userID, conn)
	defer s.unregisterClient(userID, conn)

	// Server-side ping loop: keeps the connection alive through proxies/load balancers.
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for range ticker.C {
			connMu.Lock()
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			err := conn.WriteMessage(websocket.PingMessage, nil)
			conn.SetWriteDeadline(time.Time{})
			connMu.Unlock()
			if err != nil {
				return
			}
		}
	}()

	// Keep connection alive — server pushes, client just reads (pings)
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break // client disconnected or read deadline exceeded
		}
	}

	log.Printf("[paper-ws] Client disconnected: user=%s", userID)
}

// paperPositionDTO is the JSON shape sent to the frontend for each open paper position.
// It embeds the full Order and appends multi-level exit level arrays so the UI can
// display SL/TP level chips instead of a single absolute price.
type paperPositionDTO struct {
	*models.Order
	MultiLevelSL []*models.MultiLevelExitRecord `json:"multi_level_sl,omitempty"`
	MultiLevelTP []*models.MultiLevelExitRecord `json:"multi_level_tp,omitempty"`
}

func (dto *paperPositionDTO) MarshalJSON() ([]byte, error) {
	type shadow struct {
		*orderISTWrapper
		MultiLevelSL []*models.MultiLevelExitRecord `json:"multi_level_sl,omitempty"`
		MultiLevelTP []*models.MultiLevelExitRecord `json:"multi_level_tp,omitempty"`
	}
	return json.Marshal(&shadow{
		orderISTWrapper: wrapOrderIST(dto.Order),
		MultiLevelSL:    dto.MultiLevelSL,
		MultiLevelTP:    dto.MultiLevelTP,
	})
}

// handleGetPositions returns all open paper positions as JSON.
// GET /ws/paper-trades/positions?user_id=xxx
func (s *PaperWSServer) handleGetPositions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	orders, err := s.repo.GetFilledPaperOrdersByUser(r.Context(), userID)
	if err != nil {
		http.Error(w, "failed to fetch positions: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Collect order IDs for a single batch query instead of N+1.
	orderIDs := make([]uuid.UUID, len(orders))
	for i, o := range orders {
		orderIDs[i] = o.OrderID
	}
	mlLevelsByOrder, _ := s.repo.GetMultiLevelExitLevelsBatch(r.Context(), orderIDs)

	// Build DTOs enriched with ML level data.
	dtos := make([]*paperPositionDTO, 0, len(orders))
	for _, o := range orders {
		dto := &paperPositionDTO{Order: o}
		for _, l := range mlLevelsByOrder[o.OrderID] {
			if l.ExitType == models.MLExitTypeSL {
				dto.MultiLevelSL = append(dto.MultiLevelSL, l)
			} else {
				dto.MultiLevelTP = append(dto.MultiLevelTP, l)
			}
		}
		dtos = append(dtos, dto)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"positions": dtos,
	})
}

// handleForceExitAll force-exits all open paper positions for a user.
// POST /ws/paper-trades/force-exit-all  body: {"user_id":"xxx"}
func (s *PaperWSServer) handleForceExitAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		UserID string `json:"user_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	if s.monitor == nil {
		http.Error(w, "monitor not initialized", http.StatusServiceUnavailable)
		return
	}

	if err := s.monitor.ForceExitAll(r.Context(), body.UserID); err != nil {
		http.Error(w, "force exit failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

// handleForceExitStrategyPaper force-exits all open paper positions for a specific strategy.
// POST /ws/paper-trades/force-exit-strategy  body: {"user_id":"xxx","strategy_id":"yyy"}
func (s *PaperWSServer) handleForceExitStrategyPaper(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		UserID     string `json:"user_id"`
		StrategyID string `json:"strategy_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || body.StrategyID == "" {
		http.Error(w, "user_id and strategy_id are required", http.StatusBadRequest)
		return
	}

	if s.monitor == nil {
		http.Error(w, "monitor not initialized", http.StatusServiceUnavailable)
		return
	}

	if err := s.monitor.ForceExitByStrategy(r.Context(), body.UserID, body.StrategyID); err != nil {
		http.Error(w, "force exit strategy failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "strategy_id": body.StrategyID})
}

// Broadcast sends a PaperUpdate to all WebSocket connections for a given user.
func (s *PaperWSServer) Broadcast(userID string, update PaperUpdate) {
	connMapVal, ok := s.clients.Load(userID)
	if !ok {
		return
	}
	connMap := connMapVal.(*sync.Map)

	metrics.WSMessagesSent.WithLabelValues("paper", update.Type).Inc()

	connMap.Range(func(key, value any) bool {
		conn := key.(*websocket.Conn)
		mu := value.(*sync.Mutex)
		mu.Lock()
		conn.SetWriteDeadline(time.Now().Add(writeWait))
		err := conn.WriteJSON(update)
		conn.SetWriteDeadline(time.Time{})
		mu.Unlock()
		if err != nil {
			log.Printf("[paper-ws] Failed to send to user %s: %v — removing conn", userID, err)
			s.unregisterClient(userID, conn)
		}
		return true
	})
}

func (s *PaperWSServer) registerClient(userID string, conn *websocket.Conn) *sync.Mutex {
	mu := &sync.Mutex{}
	connMapVal, _ := s.clients.LoadOrStore(userID, &sync.Map{})
	connMap := connMapVal.(*sync.Map)
	connMap.Store(conn, mu)
	metrics.WSActiveConnections.WithLabelValues("paper").Inc()
	return mu
}

func (s *PaperWSServer) unregisterClient(userID string, conn *websocket.Conn) {
	connMapVal, ok := s.clients.Load(userID)
	if !ok {
		return
	}
	connMap := connMapVal.(*sync.Map)
	connMap.Delete(conn)
	metrics.WSActiveConnections.WithLabelValues("paper").Dec()

	// Clean up empty user entry
	empty := true
	connMap.Range(func(_, _ any) bool { empty = false; return false })
	if empty {
		s.clients.Delete(userID)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// LIVE ORDERS handlers
// ─────────────────────────────────────────────────────────────────────────────

// handleLiveOrdersWS upgrades to a WebSocket and streams live order updates.
// GET /ws/live-orders?user_id=xxx
func (s *PaperWSServer) handleLiveOrdersWS(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[live-ws] Upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	log.Printf("[live-ws] Client connected: user=%s remote=%s", userID, conn.RemoteAddr())

	// Keepalive: detect dead connections within pongWait (60s).
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Send welcome and initial orders before registering for broadcasts,
	// so concurrent BroadcastLiveOrder calls cannot race with these writes.
	conn.WriteJSON(LiveOrderUpdate{Type: "connected", UserID: userID})

	orders, err := s.repo.GetLiveOrdersByUser(r.Context(), userID)
	if err != nil {
		log.Printf("[live-ws] Failed to fetch initial orders for user %s: %v", userID, err)
	} else {
		conn.WriteJSON(LiveOrderUpdate{
			Type:   "initial_orders",
			UserID: userID,
			Orders: orders,
		})
	}

	// Send today's closed orders so the frontend doesn't need a separate REST call.
	closedOrders, err := s.repo.GetClosedLiveOrdersByUser(r.Context(), userID)
	if err != nil {
		log.Printf("[live-ws] Failed to fetch initial closed orders for user %s: %v", userID, err)
	} else {
		conn.WriteJSON(LiveOrderUpdate{
			Type:   "initial_closed_orders",
			UserID: userID,
			Orders: closedOrders,
		})
	}

	// Send initial price watches snapshot
	if s.priceMonitor != nil {
		watches := s.priceMonitor.GetWatchSnapshot(userID)
		conn.WriteJSON(LiveOrderUpdate{
			Type:         "price_watches_snapshot",
			UserID:       userID,
			PriceWatches: watches,
		})
	}

	connMu := s.registerLiveClient(userID, conn)
	defer s.unregisterLiveClient(userID, conn)

	// Server-side ping loop: keeps the connection alive through proxies/load balancers.
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for range ticker.C {
			connMu.Lock()
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			err := conn.WriteMessage(websocket.PingMessage, nil)
			conn.SetWriteDeadline(time.Time{})
			connMu.Unlock()
			if err != nil {
				return
			}
		}
	}()

	// Read loop: handles inbound messages from the frontend.
	// Currently supports:
	//   resume_token — frontend sends new bearer token after user re-login so the
	//                  backend Indira WS can resume without waiting for the next order.
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break // client disconnected or read deadline exceeded
		}
		var inbound struct {
			Type        string `json:"type"`
			BearerToken string `json:"bearer_token"`
			AppID       string `json:"app_id"`
			Source      string `json:"source"`
		}
		if jsonErr := json.Unmarshal(msg, &inbound); jsonErr != nil {
			continue
		}
		if inbound.Type == "resume_token" && inbound.BearerToken != "" && s.resumeBrokerWS != nil {
			source := inbound.Source
			if source == "" {
				source = "WEB"
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := s.resumeBrokerWS(ctx, userID, inbound.BearerToken, inbound.AppID, source); err != nil {
				log.Printf("[live-ws] resume_token failed for user %s: %v", userID, err)
			} else {
				log.Printf("[live-ws] Auth resumed for user %s via WS resume_token", userID)
			}
			cancel()
		}
	}

	log.Printf("[live-ws] Client disconnected: user=%s", userID)
}

// handleForceExitAllLive square-offs all live positions for a user by placing reverse limit orders.
// Uses LTP ± 1% as the limit price (compliance: no market orders allowed).
// POST /ws/live-orders/force-exit-all  body: {"user_id":"xxx","bearer_token":"...","app_id":"...","source":"WEB"}
func (s *PaperWSServer) handleForceExitAllLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		UserID      string `json:"user_id"`
		BearerToken string `json:"bearer_token"`
		AppID       string `json:"app_id"`
		Source      string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	orders, err := s.repo.GetLiveOrdersByUser(r.Context(), body.UserID)
	if err != nil {
		http.Error(w, "failed to fetch live orders: "+err.Error(), http.StatusInternalServerError)
		return
	}

	exitCount := 0
	for _, order := range orders {
		if !models.IsFilledStatus(order.Status) || order.FilledQuantity == 0 {
			continue
		}

		exitPrice := safeF(order.FilledPrice)
		if s.monitor != nil {
			if p := s.monitor.ResolveExitPrice(r.Context(), order); p > 0 {
				exitPrice = p
			}
		}

		qty := float64(order.FilledQuantity)
		var pnl float64
		if order.OrderSide == "BUY" {
			pnl = (exitPrice - safeF(order.FilledPrice)) * qty
		} else {
			pnl = (safeF(order.FilledPrice) - exitPrice) * qty
		}

		if err := s.repo.UpdateLiveTradeExit(r.Context(), order.OrderID, exitPrice, pnl); err != nil {
			log.Printf("[live-ws] Failed to record exit for order %s: %v", order.OrderID, err)
		}

		// Place reverse limit order at LTP ± 1% for immediate fill (compliance: no market orders)
		if s.exitOrderPlacer != nil && body.BearerToken != "" {
			limitPrice := s.calcExitLimitPrice(order, exitPrice)
			if err := s.exitOrderPlacer(r.Context(), order, limitPrice, body.BearerToken, body.AppID, body.Source); err != nil {
				log.Printf("[live-ws] Failed to place exit order for %s: %v", order.OrderID, err)
			} else {
				exitCount++
			}
		}
	}

	// Cancel all active OCO groups — their SL/TP legs must be removed from exchange
	if s.ocoCanceller != nil {
		s.ocoCanceller.CancelAllGroupsByUser(r.Context(), body.UserID)
	}

	// Broadcast completion then push the updated closed orders so the frontend doesn't
	// need to REST-poll /closed-orders after receiving force_exit_done.
	s.BroadcastLiveOrder(body.UserID, LiveOrderUpdate{
		Type:   "force_exit_done",
		UserID: body.UserID,
	})
	if closedOrders, err := s.repo.GetClosedLiveOrdersByUser(r.Context(), body.UserID); err == nil {
		s.BroadcastLiveOrder(body.UserID, LiveOrderUpdate{
			Type:   "closed_orders",
			UserID: body.UserID,
			Orders: closedOrders,
		})
	}

	log.Printf("[live-ws] Force-exited all live orders for user %s (%d exit orders placed)", body.UserID, exitCount)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "exit_orders_placed": exitCount})
}

// handleForceExitStrategyLive square-offs all live positions for a specific strategy by placing
// reverse limit orders at LTP ± 1% (compliance: no market orders).
// POST /ws/live-orders/force-exit-strategy  body: {"user_id":"xxx","strategy_id":"yyy","bearer_token":"...","app_id":"...","source":"WEB"}
func (s *PaperWSServer) handleForceExitStrategyLive(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		UserID      string `json:"user_id"`
		StrategyID  string `json:"strategy_id"`
		BearerToken string `json:"bearer_token"`
		AppID       string `json:"app_id"`
		Source      string `json:"source"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || body.StrategyID == "" {
		http.Error(w, "user_id and strategy_id are required", http.StatusBadRequest)
		return
	}
	if body.BearerToken == "" {
		http.Error(w, "bearer_token is required for placing exit orders", http.StatusUnauthorized)
		return
	}
	if s.exitOrderPlacer == nil {
		http.Error(w, "exit order placer not configured", http.StatusServiceUnavailable)
		return
	}

	// Use GetExitableLiveOrdersByStrategy which includes CANCELLED orders (e.g. after strategy
	// deletion) so force-exit still works even if the strategy was deleted first.
	orders, err := s.repo.GetExitableLiveOrdersByStrategy(r.Context(), body.StrategyID, body.UserID)
	if err != nil {
		http.Error(w, "failed to fetch strategy orders: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[live-ws] Force-exiting strategy %s for user %s: %d exitable positions", body.StrategyID, body.UserID, len(orders))

	exitCount := 0
	for _, order := range orders {
		exitPrice := safeF(order.FilledPrice)
		if s.monitor != nil {
			if p := s.monitor.ResolveExitPrice(r.Context(), order); p > 0 {
				exitPrice = p
			}
		}

		// Record exit PnL on the original order
		qty := float64(order.FilledQuantity)
		var pnl float64
		if order.OrderSide == "BUY" {
			pnl = (exitPrice - safeF(order.FilledPrice)) * qty
		} else {
			pnl = (safeF(order.FilledPrice) - exitPrice) * qty
		}

		if err := s.repo.UpdateLiveTradeExit(r.Context(), order.OrderID, exitPrice, pnl); err != nil {
			log.Printf("[live-ws] Failed to record exit for order %s: %v", order.OrderID, err)
		}

		// Place reverse limit order at LTP ± 1% for immediate fill
		limitPrice := s.calcExitLimitPrice(order, exitPrice)
		if err := s.exitOrderPlacer(r.Context(), order, limitPrice, body.BearerToken, body.AppID, body.Source); err != nil {
			log.Printf("[live-ws] Failed to place exit order for %s: %v", order.OrderID, err)
		} else {
			exitCount++
		}
	}

	// Cancel OCO groups for this strategy only
	if s.ocoCanceller != nil {
		s.ocoCanceller.CancelGroupsByStrategy(r.Context(), body.UserID, body.StrategyID)
	}

	// Broadcast completion and push updated closed orders
	s.BroadcastLiveOrder(body.UserID, LiveOrderUpdate{
		Type:   "force_exit_done",
		UserID: body.UserID,
	})
	if closedOrders, err := s.repo.GetClosedLiveOrdersByUser(r.Context(), body.UserID); err == nil {
		s.BroadcastLiveOrder(body.UserID, LiveOrderUpdate{
			Type:   "closed_orders",
			UserID: body.UserID,
			Orders: closedOrders,
		})
	}

	log.Printf("[live-ws] Force-exited strategy %s for user %s: %d exit orders placed", body.StrategyID, body.UserID, exitCount)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":            true,
		"strategy_id":        body.StrategyID,
		"exit_orders_placed": exitCount,
	})
}

// calcExitLimitPrice returns an aggressive limit price for immediate fill.
// BUY positions → SELL at LTP * 0.99 (1% below); SELL positions → BUY at LTP * 1.01 (1% above).
// This avoids market orders (compliance) while ensuring the order fills instantly.
func (s *PaperWSServer) calcExitLimitPrice(order *models.Order, ltp float64) float64 {
	if order.OrderSide == models.OrderSideBuy {
		return ltp * 0.99 // selling: price 1% below LTP
	}
	return ltp * 1.01 // buying: price 1% above LTP
}

// handleGetClosedPaperOrders returns all closed (exited) paper positions for a user.
// GET /ws/paper-trades/closed-orders?user_id=xxx
func (s *PaperWSServer) handleGetClosedPaperOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}
	orders, err := s.repo.GetClosedPaperOrdersByUser(r.Context(), userID)
	if err != nil {
		http.Error(w, "failed to fetch closed paper orders: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"orders":  wrapOrdersIST(orders),
	})
}

// handleGetClosedLiveOrders returns all closed (force-exited) live orders for a user.
// GET /ws/live-orders/closed-orders?user_id=xxx
func (s *PaperWSServer) handleGetClosedLiveOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}
	orders, err := s.repo.GetClosedLiveOrdersByUser(r.Context(), userID)
	if err != nil {
		http.Error(w, "failed to fetch closed live orders: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"orders":  wrapOrdersIST(orders),
	})
}

// handleGetDashboardStats returns aggregated stats (total invested, realized PnL, counts).
// GET /ws/dashboard-stats?user_id=xxx&mode=paper  (mode: paper or live)
func (s *PaperWSServer) handleGetDashboardStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}
	mode := r.URL.Query().Get("mode")
	isPaper := mode != "live"

	stats, err := s.repo.GetDashboardStats(r.Context(), userID, isPaper)
	if err != nil {
		http.Error(w, "failed to fetch dashboard stats: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"stats":   stats,
	})
}

// handleIndiraPositions fetches ALL positions from Indira and returns only those
// that correspond to orders placed by our algo system.
// GET /ws/live-orders/indira-positions?user_id=xxx
// Requires Authorization, appId, userId, source headers (set by the Next.js proxy).
func (s *PaperWSServer) handleIndiraPositions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.positionsFetcher == nil {
		http.Error(w, "positions fetcher not configured", http.StatusServiceUnavailable)
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	// Extract Indira auth from headers set by the Next.js proxy from httpOnly cookies.
	bearerToken := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	appId := r.Header.Get("appId")
	if appId == "" {
		appId = r.Header.Get("appid")
	}
	source := r.Header.Get("source")
	if source == "" {
		source = "WEB"
	}
	if bearerToken == "" {
		http.Error(w, "Authorization header required", http.StatusUnauthorized)
		return
	}

	// 1. Fetch ALL positions from Indira.
	brokerPositions, err := s.positionsFetcher(r.Context(), bearerToken, appId, userID, source)
	if err != nil {
		log.Printf("[live-ws] Failed to fetch Indira positions for user %s: %v", userID, err)
		http.Error(w, "failed to fetch broker positions: "+err.Error(), http.StatusBadGateway)
		return
	}

	// 2. Get ALL of today's algo orders (including CANCELLED) for enrichment.
	algoOrders, err := s.repo.GetAllTodayLiveOrdersByUser(r.Context(), userID)
	if err != nil {
		log.Printf("[live-ws] Failed to fetch algo orders for user %s: %v", userID, err)
		http.Error(w, "failed to fetch algo orders", http.StatusInternalServerError)
		return
	}

	// 3. Split broker positions into per-strategy virtual positions using our orders DB.
	result := enrichPositions(brokerPositions, algoOrders)

	// 4. Backfill missing strategy names from historical orders in the DB.
	var missingIDs []string
	for i := range result {
		if result[i].StrategyID != "" && result[i].StrategyName == "" {
			missingIDs = append(missingIDs, result[i].StrategyID)
		}
	}
	if len(missingIDs) > 0 {
		if nameMap, err := s.repo.GetStrategyNamesByIDs(r.Context(), missingIDs); err == nil {
			for i := range result {
				if result[i].StrategyID != "" && result[i].StrategyName == "" {
					if name, ok := nameMap[result[i].StrategyID]; ok {
						result[i].StrategyName = name
					}
				}
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":      true,
		"positions":    result,
		"total_broker": len(brokerPositions),
	})
}

// BroadcastLiveOrder sends a LiveOrderUpdate to all connected live order WS clients for a user.
func (s *PaperWSServer) BroadcastLiveOrder(userID string, update LiveOrderUpdate) {
	connMapVal, ok := s.liveClients.Load(userID)
	if !ok {
		return
	}
	connMap := connMapVal.(*sync.Map)

	metrics.WSMessagesSent.WithLabelValues("live", update.Type).Inc()

	connMap.Range(func(key, value any) bool {
		conn := key.(*websocket.Conn)
		mu := value.(*sync.Mutex)
		mu.Lock()
		conn.SetWriteDeadline(time.Now().Add(writeWait))
		err := conn.WriteJSON(update)
		conn.SetWriteDeadline(time.Time{})
		mu.Unlock()
		if err != nil {
			log.Printf("[live-ws] Failed to send to user %s: %v — removing conn", userID, err)
			s.unregisterLiveClient(userID, conn)
		}
		return true
	})
}

// PushPositions fetches broker positions for a user and broadcasts them via /ws/live-orders.
// Called by statusservice after each order fill so the frontend receives updated positions
// without polling the REST API.
func (s *PaperWSServer) PushPositions(ctx context.Context, userID, bearerToken, appId, source string) {
	if s.positionsFetcher == nil {
		return
	}
	brokerPositions, err := s.positionsFetcher(ctx, bearerToken, appId, userID, source)
	if err != nil {
		log.Printf("[live-ws] PushPositions: failed to fetch for user %s: %v", userID, err)
		return
	}
	algoOrders, err := s.repo.GetAllTodayLiveOrdersByUser(ctx, userID)
	if err != nil {
		log.Printf("[live-ws] PushPositions: failed to fetch algo orders for user %s: %v", userID, err)
		return
	}

	result := enrichPositions(brokerPositions, algoOrders)

	// Backfill missing strategy names from historical orders in the DB.
	var missingIDs []string
	for i := range result {
		if result[i].StrategyID != "" && result[i].StrategyName == "" {
			missingIDs = append(missingIDs, result[i].StrategyID)
		}
	}
	if len(missingIDs) > 0 {
		if nameMap, err := s.repo.GetStrategyNamesByIDs(ctx, missingIDs); err == nil {
			for i := range result {
				if result[i].StrategyID != "" && result[i].StrategyName == "" {
					if name, ok := nameMap[result[i].StrategyID]; ok {
						result[i].StrategyName = name
					}
				}
			}
		}
	}

	s.BroadcastLiveOrder(userID, LiveOrderUpdate{
		Type:      "positions",
		UserID:    userID,
		Positions: result,
	})
}

func (s *PaperWSServer) registerLiveClient(userID string, conn *websocket.Conn) *sync.Mutex {
	mu := &sync.Mutex{}
	connMapVal, _ := s.liveClients.LoadOrStore(userID, &sync.Map{})
	connMap := connMapVal.(*sync.Map)
	connMap.Store(conn, mu)
	metrics.WSActiveConnections.WithLabelValues("live").Inc()
	return mu
}

func (s *PaperWSServer) unregisterLiveClient(userID string, conn *websocket.Conn) {
	connMapVal, ok := s.liveClients.Load(userID)
	if !ok {
		return
	}
	connMap := connMapVal.(*sync.Map)
	connMap.Delete(conn)
	metrics.WSActiveConnections.WithLabelValues("live").Dec()

	// Clean up empty user entry
	empty := true
	connMap.Range(func(_, _ any) bool { empty = false; return false })
	if empty {
		s.liveClients.Delete(userID)
	}
}

// handleSubscribeBrokerWS starts the per-user Indira broker WebSocket subscription immediately
// when a strategy is created/activated, so real-time order status updates begin before the
// first order is placed.
//
// POST /ws/live-orders/subscribe-broker-ws
// Headers: Authorization (Bearer token), appId, source, userId
func (s *PaperWSServer) handleSubscribeBrokerWS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.startBrokerWS == nil {
		// Not fatal — live trading WS subscription not configured (e.g. paper-only mode)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"ok":true,"note":"broker ws not configured"}`))
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		http.Error(w, `{"error":"user_id required"}`, http.StatusBadRequest)
		return
	}

	bearerToken := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	appID := r.Header.Get("appId")
	source := r.Header.Get("source")

	if bearerToken == "" || appID == "" || source == "" {
		http.Error(w, `{"error":"Authorization, appId and source headers required"}`, http.StatusUnauthorized)
		return
	}

	if err := s.startBrokerWS(r.Context(), userID, bearerToken, appID, source); err != nil {
		log.Printf("[subscribe-broker-ws] failed for user %s: %v", userID, err)
		http.Error(w, `{"error":"failed to start broker ws subscription"}`, http.StatusInternalServerError)
		return
	}

	log.Printf("[subscribe-broker-ws] started Indira WS subscription for user %s (strategy activated)", userID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"ok":true}`))
}

// BroadcastPriceWatches sends the current price watch snapshot to all connected live WS clients.
// Called by PriceMonitor after each checkPrices tick. Throttled to max once per 2 seconds
// to avoid flooding the WebSocket (checkPrices runs every 500ms).
func (s *PaperWSServer) BroadcastPriceWatches() {
	if s.priceMonitor == nil {
		return
	}

	now := time.Now()
	if now.Sub(s.lastWatchBroadcast) < 2*time.Second {
		return
	}
	s.lastWatchBroadcast = now

	var userIDs []string
	s.liveClients.Range(func(key, _ any) bool {
		userIDs = append(userIDs, key.(string))
		return true
	})

	if len(userIDs) == 0 {
		return
	}

	for _, uid := range userIDs {
		watches := s.priceMonitor.GetWatchSnapshot(uid)
		s.BroadcastLiveOrder(uid, LiveOrderUpdate{
			Type:         "price_watches_snapshot",
			UserID:       uid,
			PriceWatches: watches,
		})
	}
}

// handleGetPriceWatches returns all orders the PriceMonitor is currently watching for a user.
// GET /ws/live-orders/price-watches?user_id=xxx
func (s *PaperWSServer) handleGetPriceWatches(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.priceMonitor == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"watches": []struct{}{},
		})
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "user_id is required", http.StatusBadRequest)
		return
	}

	watches := s.priceMonitor.GetWatchSnapshot(userID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":     true,
		"watches":     watches,
		"total_count": s.priceMonitor.WatchCount(),
	})
}

// handleCancelPriceWatch cancels one or more price watches for a user.
// POST /ws/live-orders/cancel-price-watch  body: {"user_id":"xxx", "order_ids":["uuid1","uuid2"]}
func (s *PaperWSServer) handleCancelPriceWatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.priceMonitor == nil {
		http.Error(w, "price monitor not initialized", http.StatusServiceUnavailable)
		return
	}

	var body struct {
		UserID   string   `json:"user_id"`
		OrderIDs []string `json:"order_ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" || len(body.OrderIDs) == 0 {
		http.Error(w, `{"error":"user_id and order_ids are required"}`, http.StatusBadRequest)
		return
	}

	parsedIDs := make([]uuid.UUID, 0, len(body.OrderIDs))
	for _, raw := range body.OrderIDs {
		id, err := uuid.Parse(raw)
		if err != nil {
			http.Error(w, `{"error":"invalid order_id: `+raw+`"}`, http.StatusBadRequest)
			return
		}
		parsedIDs = append(parsedIDs, id)
	}

	cancelled := s.priceMonitor.CancelWatchBatch(r.Context(), parsedIDs, body.UserID)

	// Broadcast updated order list to live WS clients so UI reflects cancellation immediately
	s.BroadcastLiveOrder(body.UserID, LiveOrderUpdate{
		Type:   "price_watch_cancelled",
		UserID: body.UserID,
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":   true,
		"cancelled": cancelled,
		"requested": len(parsedIDs),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// AUTO SQUARE-OFF CONFIG handlers
// ─────────────────────────────────────────────────────────────────────────────

// handleAutoSquareOffConfig routes GET and POST for /ws/auto-square-off/config.
//
//	POST body: {"user_id":"xxx", "square_off_time":"HH:MM", "enabled":true}
//	GET  ?user_id=xxx  → {"success":true, "square_off_time":"HH:MM", "enabled":true}
func (s *PaperWSServer) handleAutoSquareOffConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.handleSetAutoSquareOffConfig(w, r)
	case http.MethodGet:
		s.handleGetAutoSquareOffConfig(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *PaperWSServer) handleSetAutoSquareOffConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		UserID         string `json:"user_id"`
		SquareOffTime  string `json:"square_off_time"`
		Enabled        bool   `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.UserID == "" {
		http.Error(w, `{"error":"user_id is required"}`, http.StatusBadRequest)
		return
	}
	if body.Enabled && body.SquareOffTime == "" {
		http.Error(w, `{"error":"square_off_time is required when enabled"}`, http.StatusBadRequest)
		return
	}
	if err := s.repo.UpsertUserSquareOffConfig(r.Context(), body.UserID, body.SquareOffTime, body.Enabled); err != nil {
		log.Printf("[auto-sq-off] Failed to upsert config for user %s: %v", body.UserID, err)
		http.Error(w, `{"error":"failed to save config"}`, http.StatusInternalServerError)
		return
	}
	log.Printf("[auto-sq-off] Config saved: user=%s time=%s enabled=%v", body.UserID, body.SquareOffTime, body.Enabled)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"success": true})
}

func (s *PaperWSServer) handleGetAutoSquareOffConfig(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		userID = r.Header.Get("userId")
	}
	if userID == "" {
		http.Error(w, `{"error":"user_id is required"}`, http.StatusBadRequest)
		return
	}
	sqTime, enabled, err := s.repo.GetUserSquareOffConfig(r.Context(), userID)
	if err != nil {
		log.Printf("[auto-sq-off] Failed to get config for user %s: %v", userID, err)
		http.Error(w, `{"error":"failed to get config"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success":         true,
		"square_off_time": sqTime,
		"enabled":         enabled,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP server
// ─────────────────────────────────────────────────────────────────────────────

// StartHTTPServer starts a standalone HTTP server for paper and live order endpoints.
// Call this in a goroutine. addr example: ":8081"
func (s *PaperWSServer) StartHTTPServer(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	log.Printf("[paper-ws] WebSocket server listening on %s (paper-trades + live-orders)", addr)
	return srv.ListenAndServe()
}
