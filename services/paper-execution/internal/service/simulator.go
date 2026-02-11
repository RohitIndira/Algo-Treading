package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	redisdb "github.com/RohitIndira/Algo-Treading/pkg/database/redis"
	"github.com/RohitIndira/Algo-Treading/services/paper-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/paper-execution/internal/publisher"
)

type Config struct {
	StrategyID          string
	TradingMode         string
	SLPct               float64
	TSLPct              float64
	PollInterval        time.Duration
	EmitPnLSnapshots    bool
	PnLSnapshotInterval time.Duration
}

// PositionState tracks a PAPER 52W position lifecycle.
type PositionState struct {
	BuyOrderID string
	UserID     string
	Token      int64
	Symbol     string
	Exchange   string

	EntryPrice float64
	QtyTotal   int32
	QtyOpen    int32

	HalfExited bool
	HighWater  float64

	CreatedAt time.Time
}

type Simulator struct {
	cfg   Config
	redis *redisdb.Client
	pub   *publisher.KafkaPublisher
	log   *zap.Logger

	mu        sync.Mutex
	positions map[string]*PositionState // key user|token
	closedPnL map[string]float64        // user -> closed pnl
}

// CloseAllPositionsForUser force-closes all open PAPER positions for the given
// user by emitting SELL execution events using the latest available LTP.
//
// This is used when the user disables/deletes the CASH_52W_HIGH strategy so
// the paper portfolio doesn't remain stuck with open positions.
func (s *Simulator) CloseAllPositionsForUser(ctx context.Context, userID string) error {
	uid := strings.TrimSpace(userID)
	if uid == "" {
		return nil
	}

	// Snapshot user's open positions.
	s.mu.Lock()
	positions := make([]*PositionState, 0)
	for _, p := range s.positions {
		if p == nil || p.QtyOpen <= 0 {
			continue
		}
		if p.UserID == uid {
			positions = append(positions, p)
		}
	}
	s.mu.Unlock()

	if len(positions) == 0 {
		return nil
	}

	for _, p := range positions {
		if p == nil || p.QtyOpen <= 0 {
			continue
		}
		ltp, ok := s.getLTP(ctx, p.Exchange, p.Token)
		if !ok || ltp <= 0 {
			// fallback to entry price if market data not available
			ltp = p.EntryPrice
		}

		// Close remaining open quantity.
		s.executeSell(
			ctx,
			p.UserID,
			p.Token,
			"FORCE_EXIT",
			"STRATEGY_DISABLED",
			p.BuyOrderID,
			p.Symbol,
			p.Exchange,
			p.QtyOpen,
			ltp,
		)
	}

	// Emit a PnL snapshot immediately so UI updates quickly.
	s.emitPnL(ctx)
	return nil
}

// OnCash52WDisabled implements consumer.Cash52WConfigHandler.
// When user disables the managed Cash52W strategy we force close all
// open positions for that user.
func (s *Simulator) OnCash52WDisabled(ctx context.Context, userID string) error {
	return s.CloseAllPositionsForUser(ctx, userID)
}

func NewSimulator(cfg Config, redisClient *redisdb.Client, pub *publisher.KafkaPublisher, logger *zap.Logger) *Simulator {
	if cfg.StrategyID == "" {
		cfg.StrategyID = "JOBBING"
	}
	if cfg.TradingMode == "" {
		cfg.TradingMode = "PAPER"
	}
	if cfg.SLPct <= 0 {
		cfg.SLPct = 10
	}
	if cfg.TSLPct <= 0 {
		cfg.TSLPct = 20
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.PnLSnapshotInterval <= 0 {
		cfg.PnLSnapshotInterval = 30 * time.Second
	}

	return &Simulator{
		cfg:       cfg,
		redis:     redisClient,
		pub:       pub,
		log:       logger,
		positions: make(map[string]*PositionState),
		closedPnL: make(map[string]float64),
	}
}

func key(userID string, token int64) string {
	return fmt.Sprintf("%s|%d", userID, token)
}

// OnTradeSignal ingests BUY PAPER signals and creates/updates in-memory position.
func (s *Simulator) OnTradeSignal(ctx context.Context, sig *models.TradeSignal) error {
	if sig == nil {
		return nil
	}
	if strings.ToUpper(strings.TrimSpace(sig.TradingMode)) != strings.ToUpper(s.cfg.TradingMode) {
		return nil
	}
	if strings.ToUpper(strings.TrimSpace(sig.StrategyID)) != strings.ToUpper(s.cfg.StrategyID) {
		return nil
	}
	if strings.ToUpper(strings.TrimSpace(sig.OrderSide)) != "BUY" {
		return nil
	}
	if sig.Quantity <= 0 || sig.Price <= 0 {
		return nil
	}

	s.mu.Lock()
	k := key(sig.UserID, sig.Token)
	if _, exists := s.positions[k]; exists {
		s.mu.Unlock()
		return nil
	}

	pos := &PositionState{
		BuyOrderID: sig.OrderID,
		UserID:     sig.UserID,
		Token:      sig.Token,
		Symbol:     sig.Symbol,
		Exchange:   sig.Exchange,
		EntryPrice: sig.Price,
		QtyTotal:   sig.Quantity,
		QtyOpen:    sig.Quantity,
		HighWater:  sig.Price,
		CreatedAt:  time.Now(),
	}
	s.positions[k] = pos
	s.mu.Unlock()

	ev := &models.PaperExecutionEvent{
		EventID:    uuid.New().String(),
		StrategyID: s.cfg.StrategyID,
		UserID:     sig.UserID,
		Token:      sig.Token,
		Symbol:     sig.Symbol,
		Exchange:   sig.Exchange,
		OrderSide:  "BUY",
		Quantity:   sig.Quantity,
		Price:      sig.Price,
		Leg:        "ENTRY",
		Reason:     "ENTRY",
		BuyOrderID: sig.OrderID,
		PnL:        0,
		CreatedAt:  time.Now(),
	}
	if s.pub != nil {
		_ = s.pub.PublishExecution(ctx, ev)
	}

	s.log.Info(
		"paper position opened",
		zap.String("user_id", sig.UserID),
		zap.Int64("token", sig.Token),
		zap.String("symbol", sig.Symbol),
		zap.String("exchange", sig.Exchange),
		zap.Int32("qty", sig.Quantity),
		zap.Float64("entry", sig.Price),
	)

	return nil
}

// Start runs the periodic simulation loop.
func (s *Simulator) Start(ctx context.Context) {
	if s.redis == nil {
		s.log.Warn("paper simulator not started: redis client is nil")
		return
	}

	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	var pnlTicker *time.Ticker
	if s.cfg.EmitPnLSnapshots {
		pnlTicker = time.NewTicker(s.cfg.PnLSnapshotInterval)
		defer pnlTicker.Stop()
	}

	s.log.Info(
		"paper simulator loop started",
		zap.Duration("poll_interval", s.cfg.PollInterval),
		zap.Bool("emit_pnl_snapshots", s.cfg.EmitPnLSnapshots),
	)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("paper simulator loop stopped")
			return
		case <-ticker.C:
			s.step(ctx)
		case <-func() <-chan time.Time {
			if pnlTicker == nil {
				return make(chan time.Time)
			}
			return pnlTicker.C
		}():
			s.emitPnL(ctx)
		}
	}
}

func (s *Simulator) step(ctx context.Context) {
	s.mu.Lock()
	positions := make([]*PositionState, 0, len(s.positions))
	for _, p := range s.positions {
		positions = append(positions, p)
	}
	s.mu.Unlock()

	for _, p := range positions {
		if p == nil || p.QtyOpen <= 0 {
			continue
		}

		ltp, ok := s.getLTP(ctx, p.Exchange, p.Token)
		if !ok || ltp <= 0 {
			continue
		}

		s.applyStops(ctx, p, ltp)
	}
}

func (s *Simulator) getLTP(ctx context.Context, exchange string, token int64) (float64, bool) {
	ex := strings.ToLower(strings.TrimSpace(exchange))
	ex = strings.TrimSuffix(ex, "_eq")
	key := fmt.Sprintf("market:%s:%d", ex, token)

	val, err := s.redis.GetString(ctx, key)
	if err != nil {
		return 0, false
	}

	var md struct {
		LTP float64 `json:"ltp"`
	}
	if err := json.Unmarshal([]byte(val), &md); err != nil {
		return 0, false
	}
	return md.LTP, md.LTP > 0
}

func (s *Simulator) applyStops(ctx context.Context, p *PositionState, ltp float64) {
	s.mu.Lock()
	pos := s.positions[key(p.UserID, p.Token)]
	if pos == nil {
		s.mu.Unlock()
		return
	}
	if ltp > pos.HighWater {
		pos.HighWater = ltp
	}
	s.mu.Unlock()

	// Check if we should exit half position on SL trigger
	if !p.HalfExited {
		slTrigger := p.EntryPrice * (1.0 - s.cfg.SLPct/100.0)
		if ltp <= slTrigger {
			exitQty := int32(math.Floor(float64(p.QtyTotal) / 2.0))
			if exitQty <= 0 {
				exitQty = 1
			}
			if exitQty > p.QtyOpen {
				exitQty = p.QtyOpen
			}
			s.executeSell(
				ctx,
				p.UserID,
				p.Token,
				"SL_HALF",
				"SL_10",
				p.BuyOrderID,
				p.Symbol,
				p.Exchange,
				exitQty,
				ltp,
			)
			s.mu.Lock()
			if st := s.positions[key(p.UserID, p.Token)]; st != nil {
				st.HalfExited = true
			}
			s.mu.Unlock()
			return
		}
	}

	// Check if we should exit rest of position on TSL trigger
	s.mu.Lock()
	current := s.positions[key(p.UserID, p.Token)]
	high := 0.0
	qtyOpen := int32(0)
	if current != nil {
		high = current.HighWater
		qtyOpen = current.QtyOpen
	}
	s.mu.Unlock()
	if current == nil || qtyOpen <= 0 {
		return
	}

	trailTrigger := high * (1.0 - s.cfg.TSLPct/100.0)
	if ltp <= trailTrigger {
		exitQty := qtyOpen
		s.executeSell(
			ctx,
			p.UserID,
			p.Token,
			"TSL_REST",
			"TSL_20",
			p.BuyOrderID,
			p.Symbol,
			p.Exchange,
			exitQty,
			ltp,
		)
	}
}

func (s *Simulator) executeSell(
	ctx context.Context,
	userID string,
	token int64,
	leg string,
	reason string,
	buyOrderID string,
	symbol string,
	exchange string,
	qty int32,
	price float64,
) {
	if qty <= 0 {
		return
	}

	// Update in-memory state first.
	var (
		realized     float64
		entry        float64
		newQtyOpen   int32
		removed      bool
		capitalFreed float64
		origQtyTotal int32
	)

	s.mu.Lock()
	pos := s.positions[key(userID, token)]
	if pos == nil {
		s.mu.Unlock()
		return
	}
	if qty > pos.QtyOpen {
		qty = pos.QtyOpen
	}

	entry = pos.EntryPrice
	origQtyTotal = pos.QtyTotal
	realized = (price - entry) * float64(qty)
	pos.QtyOpen -= qty
	newQtyOpen = pos.QtyOpen

	// accumulate closed PnL for user
	s.closedPnL[userID] += realized

	// If position fully closed, delete it and calculate capital freed.
	if pos.QtyOpen <= 0 {
		// Capital freed = entry price × original quantity
		// This represents the capital that can be reinvested
		capitalFreed = entry * float64(origQtyTotal)
		delete(s.positions, key(userID, token))
		removed = true
	}
	s.mu.Unlock()

	// Publish execution event (outside lock).
	ev := &models.PaperExecutionEvent{
		EventID:    uuid.New().String(),
		StrategyID: s.cfg.StrategyID,
		UserID:     userID,
		Token:      token,
		Symbol:     symbol,
		Exchange:   exchange,
		OrderSide:  "SELL",
		Quantity:   qty,
		Price:      price,
		Leg:        leg,
		Reason:     reason,
		BuyOrderID: buyOrderID,
		PnL:        realized,
		CreatedAt:  time.Now(),
	}
	if s.pub != nil {
		_ = s.pub.PublishExecution(ctx, ev)
	}

	// If position fully closed, emit reinvestment signal
	if removed && capitalFreed > 0 && s.pub != nil {
		reinvestSignal := &models.ReinvestmentSignal{
			UserID:           userID,
			StrategyID:       s.cfg.StrategyID,
			AvailableCapital: capitalFreed,
			ClosedToken:      token,
			ClosedSymbol:     symbol,
			Timestamp:        time.Now(),
		}
		if err := s.pub.PublishReinvestmentSignal(ctx, reinvestSignal); err != nil {
			s.log.Error("Failed to publish reinvestment signal",
				zap.String("user_id", userID),
				zap.Int64("token", token),
				zap.Float64("capital", capitalFreed),
				zap.Error(err))
		}
	}

	s.log.Info("paper position sell executed",
		zap.String("user_id", userID),
		zap.Int64("token", token),
		zap.String("symbol", symbol),
		zap.String("exchange", exchange),
		zap.String("leg", leg),
		zap.String("reason", reason),
		zap.Int32("qty", qty),
		zap.Float64("price", price),
		zap.Float64("entry", entry),
		zap.Float64("realized_pnl", realized),
		zap.Int32("qty_open", newQtyOpen),
		zap.Bool("position_closed", removed),
		zap.Float64("capital_freed", capitalFreed),
	)
}

func (s *Simulator) emitPnL(ctx context.Context) {
	if s.pub == nil {
		return
	}

	// Snapshot current state under lock
	s.mu.Lock()
	userPositions := make(map[string][]*PositionState)
	for _, p := range s.positions {
		if p == nil || p.QtyOpen <= 0 {
			continue
		}
		userPositions[p.UserID] = append(userPositions[p.UserID], p)
	}
	closedPnLCopy := make(map[string]float64, len(s.closedPnL))
	for uid, v := range s.closedPnL {
		closedPnLCopy[uid] = v
	}
	s.mu.Unlock()

	// For each user, compute comprehensive portfolio PnL
	for uid, positions := range userPositions {
		openPnLList := make([]models.OpenPositionPnL, 0, len(positions))
		totalMarketValue := 0.0
		totalUnrealizedPnL := 0.0

		for _, p := range positions {
			// Fetch current LTP from Redis
			ltp, ok := s.getLTP(ctx, p.Exchange, p.Token)
			if !ok || ltp <= 0 {
				// If we can't get LTP, use entry price as fallback
				ltp = p.EntryPrice
			}

			// Calculate unrealized P&L: (Current Price - Entry Price) × Quantity
			unrealizedPnL := (ltp - p.EntryPrice) * float64(p.QtyOpen)
			pnlPercent := 0.0
			if p.EntryPrice > 0 {
				pnlPercent = ((ltp - p.EntryPrice) / p.EntryPrice) * 100.0
			}

			// Market value of this position
			marketValue := ltp * float64(p.QtyOpen)
			totalMarketValue += marketValue
			totalUnrealizedPnL += unrealizedPnL

			openPnLList = append(openPnLList, models.OpenPositionPnL{
				UserID:        uid,
				StrategyID:    s.cfg.StrategyID,
				Token:         p.Token,
				Symbol:        p.Symbol,
				Exchange:      p.Exchange,
				Quantity:      p.QtyOpen,
				EntryPrice:    p.EntryPrice,
				CurrentPrice:  ltp,
				UnrealizedPnL: unrealizedPnL,
				PnLPercent:    pnlPercent,
				Timestamp:     time.Now(),
			})
		}

		closedPnL := closedPnLCopy[uid]

		// Portfolio Value = Market Value of Open Positions + Total Closed PnL
		// This is the correct formula - we do NOT add initial investment again
		portfolioValue := totalMarketValue + closedPnL

		// Calculate average per stock (strategy uses 25 stocks)
		avgPerStock := closedPnL / 25.0

		summary := &models.PortfolioPnLSummary{
			UserID:             uid,
			StrategyID:         s.cfg.StrategyID,
			OpenPositions:      openPnLList,
			OpenPositionsCount: len(openPnLList),
			TotalMarketValue:   totalMarketValue,
			TotalUnrealizedPnL: totalUnrealizedPnL,
			TotalClosedPnL:     closedPnL,
			PortfolioValue:     portfolioValue,
			AvgPerStock:        avgPerStock,
			AvailableCapital:   avgPerStock, // Capital available for next reinvestment
			Timestamp:          time.Now(),
		}

		// Publish comprehensive portfolio summary
		if err := s.pub.PublishPortfolioSummary(ctx, summary); err != nil {
			s.log.Error("Failed to publish portfolio summary",
				zap.String("user_id", uid),
				zap.Error(err))
		}

		// Also publish simple snapshot for backward compatibility
		snap := &models.PaperPnLSnapshot{
			UserID:        uid,
			StrategyID:    s.cfg.StrategyID,
			ClosedPnL:     closedPnL,
			OpenPositions: len(openPnLList),
			Timestamp:     time.Now(),
		}
		_ = s.pub.PublishPnLSnapshot(ctx, snap)
	}

	// Handle users with only closed positions (no open positions)
	for uid, closedPnL := range closedPnLCopy {
		if _, hasOpen := userPositions[uid]; hasOpen {
			continue
		}

		avgPerStock := closedPnL / 25.0

		// User has closed PnL but no open positions
		summary := &models.PortfolioPnLSummary{
			UserID:             uid,
			StrategyID:         s.cfg.StrategyID,
			OpenPositions:      []models.OpenPositionPnL{},
			OpenPositionsCount: 0,
			TotalMarketValue:   0,
			TotalUnrealizedPnL: 0,
			TotalClosedPnL:     closedPnL,
			PortfolioValue:     closedPnL,
			AvgPerStock:        avgPerStock,
			AvailableCapital:   avgPerStock,
			Timestamp:          time.Now(),
		}

		_ = s.pub.PublishPortfolioSummary(ctx, summary)

		snap := &models.PaperPnLSnapshot{
			UserID:        uid,
			StrategyID:    s.cfg.StrategyID,
			ClosedPnL:     closedPnL,
			OpenPositions: 0,
			Timestamp:     time.Now(),
		}
		_ = s.pub.PublishPnLSnapshot(ctx, snap)
	}
}
