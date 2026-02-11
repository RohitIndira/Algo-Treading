package jobbing

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/risk"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Config for the Jobbing strategy engine.
type Config struct {
	// List of user IDs to run this strategy for
	UserIDs []string

	// Price range limits (absolute prices)
	LowerRange  float64 // Minimum price threshold
	HigherRange float64 // Maximum price threshold

	// Initial offset from LTP for first BUY order (e.g., 0.01)
	InitialBuyOffset float64
	// Initial offset from LTP for first SELL order (e.g., 0.01)
	InitialSellOffset float64

	// Distance between consecutive orders (e.g., 0.01)
	DistanceContinue float64

	// Profit and loss thresholds
	ProfitTargetPct float64
	StopLossPct     float64
	EnableSellLogic bool

	// Quantity per order
	QuantityPerOrder int32

	// Maximum total quantity allowed
	MaxQuantity int32

	// List of tokens (stocks) to apply this strategy on
	Tokens []string
}

// UserTokenConfig holds per-user, per-token parameters for the Jobbing
// strategy. These can be loaded dynamically from the user-config service
// instead of (or in addition to) global defaults provided via env/config.
type UserTokenConfig struct {
	LowerRange        float64
	HigherRange       float64
	InitialBuyOffset  float64 // Offset below LTP for BUY orders
	InitialSellOffset float64 // Offset above LTP for SELL orders
	DistanceContinue  float64 // Distance between consecutive orders
	QuantityPerOrder  int32
	MaxQuantity       int32   // Max total position size
	ProfitTargetPct   float64 // Profit % to trigger SELL orders (default 0.5%)
	StopLossPct       float64 // Stop loss % to trigger SELL orders (default 2.0%)
	EnableSellLogic   bool    // Enable automatic SELL order generation

	// Phase B: Advanced Risk Management
	MaxDailyLoss        float64 // Max daily loss limit (default 5000.0)
	MaxPositionValue    float64 // Max position value limit (default 100000.0)
	TrailingStopPct     float64 // Trailing stop percentage (default 1.0%)
	TimeBasedExitMins   int32   // Time-based exit in minutes (default 60)
	EnableTrailingStop  bool    // Enable trailing stop functionality
	EnableTimeBasedExit bool    // Enable time-based position exit

	// Risk Multipliers
	VolatilityMultiplier float64 // Adjust quantities based on volatility (default 1.0)
	SpreadMultiplier     float64 // Adjust based on bid-ask spread (default 1.0)
}

// userTokenState tracks the state for a specific user-token combination
type userTokenState struct {
	Token    string
	Symbol   string
	Exchange string
	// Position tracking
	TotalQuantityBuy   int32   // Total quantity bought (pending + filled)
	TotalQuantitySell  int32   // Total quantity sold
	FilledQuantityBuy  int32   // Actually filled buy quantity
	FilledQuantitySell int32   // Actually filled sell quantity
	NetPosition        int32   // FilledQuantityBuy - FilledQuantitySell
	AverageEntryPrice  float64 // Average entry price of held positions
	// Order state
	LastBuyOrderPrice  float64  // Last price at which BUY order was placed
	LastSellOrderPrice float64  // Last price at which SELL order was placed
	ActiveBuyOrders    []string // List of active BUY order IDs
	ActiveSellOrders   []string // List of active SELL order IDs
	LastUpdated        time.Time

	// Phase B: Real-time P&L and Performance Tracking
	RealizedPnL   float64 // Cumulative realized profit/loss
	UnrealizedPnL float64 // Current unrealized P&L based on LTP
	TotalPnL      float64 // Total P&L (realized + unrealized)
	CurrentLTP    float64 // Current Last Traded Price
	HighWaterMark float64 // Highest profit achieved today
	MaxDrawdown   float64 // Maximum drawdown from high water mark
	DailyPnL      float64 // Daily P&L (resets daily)

	// Performance Metrics
	TotalTrades   int32   // Total number of completed trades
	WinningTrades int32   // Number of profitable trades
	LosingTrades  int32   // Number of loss-making trades
	WinRate       float64 // Winning percentage
	AvgWin        float64 // Average winning trade amount
	AvgLoss       float64 // Average losing trade amount
	ProfitFactor  float64 // Gross profit / Gross loss

	// Risk Management State
	PositionStartTime time.Time // When position was first opened
	TrailingStopPrice float64   // Current trailing stop price
	DailyLossLimit    bool      // Whether daily loss limit is breached
	RiskLevel         string    // Current risk level: LOW, MEDIUM, HIGH

	// Intraday Session Reset
	LastResetDate time.Time // Last date when daily metrics were reset
}

// userState tracks all token states for a user
type userState struct {
	TokenStates map[string]*userTokenState // key: token
}

// Engine implements the Jobbing strategy for multiple users and tokens.
type Engine struct {
	cfg        Config
	riskClient *risk.Client
	rabbitPub  *publisher.Publisher
	kafkaPub   *publisher.KafkaPublisher
	allocPub   *publisher.KafkaPublisher
	logger     *zap.Logger

	mu sync.Mutex
	// trading day tracking (for resetting intraday state)
	day string
	// userState holds per-user runtime state (filled qty, active orders, etc.).
	userState map[string]*userState // key: userID
	// userConfigs holds per-user, per-token jobbing parameters loaded from
	// user-config service. Keyed as userID -> token -> config.
	userConfigs map[string]map[string]UserTokenConfig
	// tokenUsers is the reverse index: token -> set of userIDs with configs.
	tokenUsers map[string]map[string]struct{}
}

// NewEngine creates a new Jobbing strategy engine.
func NewEngine(cfg Config, riskClient *risk.Client, rabbitPub *publisher.Publisher, kafkaPub *publisher.KafkaPublisher, allocPub *publisher.KafkaPublisher, logger *zap.Logger) *Engine {
	// defaults
	if cfg.LowerRange <= 0 {
		cfg.LowerRange = 10.0
	}
	if cfg.HigherRange <= 0 {
		cfg.HigherRange = 15.0
	}
	if cfg.InitialBuyOffset <= 0 {
		cfg.InitialBuyOffset = 0.01
	}
	if cfg.InitialSellOffset <= 0 {
		cfg.InitialSellOffset = 0.01
	}
	if cfg.DistanceContinue <= 0 {
		cfg.DistanceContinue = 0.01
	}
	if cfg.QuantityPerOrder <= 0 {
		cfg.QuantityPerOrder = 1
	}
	if cfg.MaxQuantity <= 0 {
		cfg.MaxQuantity = 10
	}
	if cfg.ProfitTargetPct <= 0 {
		cfg.ProfitTargetPct = 0.5 // 0.5% profit target
	}
	if cfg.StopLossPct <= 0 {
		cfg.StopLossPct = 2.0 // 2% stop loss
	}
	cfg.EnableSellLogic = true // Enable SELL logic by default

	// Phase B: Set advanced risk management defaults
	logger.Info("Initializing Phase B enhancements - advanced risk management and performance tracking")

	// normalize user IDs
	users := make([]string, 0, len(cfg.UserIDs))
	for _, u := range cfg.UserIDs {
		u = strings.TrimSpace(u)
		if u != "" {
			users = append(users, u)
		}
	}
	cfg.UserIDs = users

	// normalize tokens
	tokens := make([]string, 0, len(cfg.Tokens))
	for _, t := range cfg.Tokens {
		t = strings.TrimSpace(t)
		if t != "" {
			tokens = append(tokens, t)
		}
	}
	cfg.Tokens = tokens

	return &Engine{
		cfg:         cfg,
		riskClient:  riskClient,
		rabbitPub:   rabbitPub,
		kafkaPub:    kafkaPub,
		allocPub:    allocPub,
		logger:      logger,
		day:         todayStr(),
		userState:   make(map[string]*userState),
		userConfigs: make(map[string]map[string]UserTokenConfig),
		tokenUsers:  make(map[string]map[string]struct{}),
	}
}

func todayStr() string { return time.Now().Format("2006-01-02") }

func (e *Engine) resetIfNewDay() {
	e.mu.Lock()
	defer e.mu.Unlock()

	day := todayStr()
	if day != e.day {
		e.logger.Info("New trading day detected, resetting jobbing state",
			zap.String("old_day", e.day),
			zap.String("new_day", day))
		e.day = day
		// Reset only runtime state (positions, totals, etc.). Keep
		// configuration loaded from user-config service.
		e.userState = make(map[string]*userState)
	}
}

func (e *Engine) getUserState(userID string) *userState {
	e.mu.Lock()
	defer e.mu.Unlock()

	st, ok := e.userState[userID]
	if !ok {
		st = &userState{TokenStates: make(map[string]*userTokenState)}
		e.userState[userID] = st
	}
	return st
}

func (e *Engine) getTokenState(userID, token string) *userTokenState {
	st := e.getUserState(userID)

	e.mu.Lock()
	defer e.mu.Unlock()

	tokenSt, ok := st.TokenStates[token]
	if !ok {
		now := time.Now()
		tokenSt = &userTokenState{
			Token:              token,
			TotalQuantityBuy:   0,
			TotalQuantitySell:  0,
			FilledQuantityBuy:  0,
			FilledQuantitySell: 0,
			NetPosition:        0,
			AverageEntryPrice:  0,
			LastBuyOrderPrice:  0,
			LastSellOrderPrice: 0,
			ActiveBuyOrders:    make([]string, 0),
			ActiveSellOrders:   make([]string, 0),
			LastUpdated:        now,

			// Phase B: Initialize P&L and performance tracking
			RealizedPnL:       0,
			UnrealizedPnL:     0,
			TotalPnL:          0,
			CurrentLTP:        0,
			HighWaterMark:     0,
			MaxDrawdown:       0,
			DailyPnL:          0,
			TotalTrades:       0,
			WinningTrades:     0,
			LosingTrades:      0,
			WinRate:           0,
			AvgWin:            0,
			AvgLoss:           0,
			ProfitFactor:      0,
			PositionStartTime: now,
			TrailingStopPrice: 0,
			DailyLossLimit:    false,
			RiskLevel:         "LOW",
			LastResetDate:     now,
		}
		st.TokenStates[token] = tokenSt
	}
	return tokenSt
}

// SetJobbingConfig registers or updates jobbing parameters for a given user
// and a set of tokens. This is typically called from the rules-engine main
// process after loading strategies from the user-config service.
func (e *Engine) SetJobbingConfig(userID string, tokens []string, cfg UserTokenConfig) {
	// Phase B: Set defaults for advanced parameters
	e.SetUserTokenConfigDefaults(&cfg)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.userConfigs == nil {
		e.userConfigs = make(map[string]map[string]UserTokenConfig)
	}
	if e.tokenUsers == nil {
		e.tokenUsers = make(map[string]map[string]struct{})
	}

	userCfg, ok := e.userConfigs[userID]
	if !ok {
		userCfg = make(map[string]UserTokenConfig)
		e.userConfigs[userID] = userCfg
	}

	for _, t := range tokens {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		userCfg[t] = cfg

		usersForToken, ok := e.tokenUsers[t]
		if !ok {
			usersForToken = make(map[string]struct{})
			e.tokenUsers[t] = usersForToken
		}
		usersForToken[userID] = struct{}{}
	}
}

// RemoveJobbingConfig removes jobbing configuration for a specific user and token
func (e *Engine) RemoveJobbingConfig(userID string, token string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Remove from userConfigs
	if userCfg, ok := e.userConfigs[userID]; ok {
		delete(userCfg, token)
		// If user has no more configs, remove the user entry
		if len(userCfg) == 0 {
			delete(e.userConfigs, userID)
		}
	}

	// Remove from tokenUsers reverse index
	if usersForToken, ok := e.tokenUsers[token]; ok {
		delete(usersForToken, userID)
		// If no more users for this token, remove the token entry
		if len(usersForToken) == 0 {
			delete(e.tokenUsers, token)
		}
	}

	e.logger.Info("Removed jobbing config",
		zap.String("user_id", userID),
		zap.String("token", token))
}

// Phase B: Advanced Risk Management and Performance Tracking Methods

// SetUserTokenConfigDefaults ensures Phase B parameters have proper defaults
func (e *Engine) SetUserTokenConfigDefaults(cfg *UserTokenConfig) {
	if cfg.MaxDailyLoss <= 0 {
		cfg.MaxDailyLoss = 5000.0 // Default 5000 daily loss limit
	}
	if cfg.MaxPositionValue <= 0 {
		cfg.MaxPositionValue = 100000.0 // Default 100k position value limit
	}
	if cfg.TrailingStopPct <= 0 {
		cfg.TrailingStopPct = 1.0 // Default 1% trailing stop
	}
	if cfg.TimeBasedExitMins <= 0 {
		cfg.TimeBasedExitMins = 60 // Default 60 minutes
	}
	if cfg.VolatilityMultiplier <= 0 {
		cfg.VolatilityMultiplier = 1.0 // Default no adjustment
	}
	if cfg.SpreadMultiplier <= 0 {
		cfg.SpreadMultiplier = 1.0 // Default no adjustment
	}
}

// UpdatePnL calculates and updates real-time P&L for a position
func (e *Engine) UpdatePnL(tokenState *userTokenState, currentLTP float64) {
	if tokenState == nil {
		return
	}

	tokenState.CurrentLTP = currentLTP

	// Calculate unrealized P&L
	if tokenState.NetPosition != 0 && tokenState.AverageEntryPrice > 0 {
		positionValue := float64(tokenState.NetPosition) * tokenState.AverageEntryPrice
		currentValue := float64(tokenState.NetPosition) * currentLTP
		tokenState.UnrealizedPnL = currentValue - positionValue
	} else {
		tokenState.UnrealizedPnL = 0
	}

	// Calculate total P&L
	tokenState.TotalPnL = tokenState.RealizedPnL + tokenState.UnrealizedPnL

	// Update high water mark and drawdown
	if tokenState.TotalPnL > tokenState.HighWaterMark {
		tokenState.HighWaterMark = tokenState.TotalPnL
	}

	currentDrawdown := tokenState.HighWaterMark - tokenState.TotalPnL
	if currentDrawdown > tokenState.MaxDrawdown {
		tokenState.MaxDrawdown = currentDrawdown
	}

	// Update daily P&L (assuming daily reset happens elsewhere)
	tokenState.DailyPnL = tokenState.TotalPnL
}

// CheckRiskLimits validates if trading should continue based on risk parameters
func (e *Engine) CheckRiskLimits(userID, token string, cfg UserTokenConfig, tokenState *userTokenState) (bool, string) {
	// Check daily loss limit
	if tokenState.DailyPnL <= -cfg.MaxDailyLoss {
		tokenState.DailyLossLimit = true
		tokenState.RiskLevel = "HIGH"
		return false, "Daily loss limit exceeded"
	}

	// Check position value limit
	if tokenState.NetPosition > 0 && tokenState.CurrentLTP > 0 {
		positionValue := float64(tokenState.NetPosition) * tokenState.CurrentLTP
		if positionValue > cfg.MaxPositionValue {
			tokenState.RiskLevel = "HIGH"
			return false, "Position value limit exceeded"
		}
	}

	// Check maximum quantity limit
	if tokenState.NetPosition >= cfg.MaxQuantity {
		return false, "Maximum quantity limit reached"
	}

	// Update risk level based on current metrics
	if tokenState.MaxDrawdown > cfg.MaxDailyLoss*0.5 {
		tokenState.RiskLevel = "MEDIUM"
	} else if tokenState.MaxDrawdown > cfg.MaxDailyLoss*0.2 {
		tokenState.RiskLevel = "LOW"
	}

	return true, ""
}

// CheckTrailingStop checks if trailing stop should trigger a sell
func (e *Engine) CheckTrailingStop(cfg UserTokenConfig, tokenState *userTokenState) bool {
	if !cfg.EnableTrailingStop || tokenState.NetPosition <= 0 || tokenState.CurrentLTP <= 0 {
		return false
	}

	// Initialize trailing stop price on first profitable position
	if tokenState.TrailingStopPrice == 0 && tokenState.UnrealizedPnL > 0 {
		trailingOffset := tokenState.CurrentLTP * (cfg.TrailingStopPct / 100.0)
		tokenState.TrailingStopPrice = tokenState.CurrentLTP - trailingOffset
	}

	// Update trailing stop price if current price moved favorably
	if tokenState.TrailingStopPrice > 0 {
		trailingOffset := tokenState.CurrentLTP * (cfg.TrailingStopPct / 100.0)
		newTrailingPrice := tokenState.CurrentLTP - trailingOffset
		if newTrailingPrice > tokenState.TrailingStopPrice {
			tokenState.TrailingStopPrice = newTrailingPrice
		}

		// Check if current price hit trailing stop
		if tokenState.CurrentLTP <= tokenState.TrailingStopPrice {
			return true
		}
	}

	return false
}

// CheckTimeBasedExit checks if position should be closed based on time limits
func (e *Engine) CheckTimeBasedExit(cfg UserTokenConfig, tokenState *userTokenState) bool {
	if !cfg.EnableTimeBasedExit || tokenState.NetPosition <= 0 {
		return false
	}

	// Check if position has been held longer than time limit
	currentTime := time.Now()
	positionDuration := currentTime.Sub(tokenState.PositionStartTime)
	timeLimit := time.Duration(cfg.TimeBasedExitMins) * time.Minute

	return positionDuration > timeLimit
}

// UpdatePerformanceMetrics updates trading performance statistics
func (e *Engine) UpdatePerformanceMetrics(tokenState *userTokenState, tradeResult float64) {
	tokenState.TotalTrades++

	if tradeResult > 0 {
		tokenState.WinningTrades++
		tokenState.AvgWin = ((tokenState.AvgWin * float64(tokenState.WinningTrades-1)) + tradeResult) / float64(tokenState.WinningTrades)
	} else if tradeResult < 0 {
		tokenState.LosingTrades++
		tokenState.AvgLoss = ((tokenState.AvgLoss * float64(tokenState.LosingTrades-1)) + tradeResult) / float64(tokenState.LosingTrades)
	}

	// Update win rate
	if tokenState.TotalTrades > 0 {
		tokenState.WinRate = (float64(tokenState.WinningTrades) / float64(tokenState.TotalTrades)) * 100
	}

	// Update profit factor
	grossProfit := tokenState.AvgWin * float64(tokenState.WinningTrades)
	grossLoss := math.Abs(tokenState.AvgLoss * float64(tokenState.LosingTrades))
	if grossLoss > 0 {
		tokenState.ProfitFactor = grossProfit / grossLoss
	}
}

// ResetDailyMetrics resets daily P&L and performance metrics for new trading session
func (e *Engine) ResetDailyMetrics(tokenState *userTokenState) {
	now := time.Now()
	today := now.Format("2006-01-02")
	lastResetDate := tokenState.LastResetDate.Format("2006-01-02")

	// Only reset if it's a new trading day
	if today != lastResetDate {
		tokenState.DailyPnL = 0
		tokenState.HighWaterMark = 0
		tokenState.MaxDrawdown = 0
		tokenState.DailyLossLimit = false
		tokenState.TrailingStopPrice = 0
		tokenState.RiskLevel = "LOW"
		tokenState.LastResetDate = now
	}
}

// getConfig returns a per-user, per-token config if present.
func (e *Engine) getConfig(userID, token string) (UserTokenConfig, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	userCfg, ok := e.userConfigs[userID]
	if !ok {
		return UserTokenConfig{}, false
	}
	cfg, ok := userCfg[token]
	return cfg, ok
}

// getUsersForToken returns all user IDs that have a jobbing config
// for the given token.
func (e *Engine) getUsersForToken(token string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()

	usersMap, ok := e.tokenUsers[token]
	if !ok || len(usersMap) == 0 {
		return nil
	}
	users := make([]string, 0, len(usersMap))
	for u := range usersMap {
		users = append(users, u)
	}
	return users
}

// HandleJobbing processes a market depth event for the jobbing strategy.
func (e *Engine) HandleJobbing(ctx context.Context, ev *models.JobbingMarketDepthEvent) error {
	if ev == nil {
		return fmt.Errorf("nil JobbingMarketDepthEvent")
	}

	// Basic sanity checks
	if ev.MarketData.LastTradedPrice <= 0 {
		e.logger.Warn("Skipping jobbing event with invalid LTP",
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("ltp", ev.MarketData.LastTradedPrice))
		return nil
	}

	// Determine token identifier for config/state lookup
	token := fmt.Sprintf("%d", ev.StockData.StockCode)

	// If we have dynamic configs loaded (from user-config service), use those
	// to decide which users are active for this token. Otherwise, fall back
	// to static cfg.UserIDs / cfg.Tokens from env.
	users := e.getUsersForToken(token)
	if len(users) == 0 {
		// Fallback: use static configuration if no dynamic configs exist.
		if !e.isTokenEnabled(token) {
			// Not a token we're trading
			return nil
		}
		users = e.cfg.UserIDs
	}

	e.resetIfNewDay()

	// Process for all relevant users
	for _, userID := range users {
		if err := e.handleForUser(ctx, userID, ev); err != nil {
			e.logger.Error("Failed to handle jobbing for user",
				zap.Error(err),
				zap.String("user_id", userID),
				zap.String("symbol", ev.StockData.Symbol))
			// continue with other users
		}
	}

	return nil
}

func (e *Engine) isTokenEnabled(token string) bool {
	if len(e.cfg.Tokens) == 0 {
		// If no tokens configured, trade all
		return true
	}
	for _, t := range e.cfg.Tokens {
		if t == token {
			return true
		}
	}
	return false
}

func (e *Engine) handleForUser(ctx context.Context, userID string, ev *models.JobbingMarketDepthEvent) error {
	token := fmt.Sprintf("%d", ev.StockData.StockCode)
	ltp := ev.MarketData.LastTradedPrice

	// Start from global defaults and override with per-user+token config if
	// available. This allows a mix of env-based and dynamic configurations.
	cfg := UserTokenConfig{
		LowerRange:        e.cfg.LowerRange,
		HigherRange:       e.cfg.HigherRange,
		InitialBuyOffset:  e.cfg.InitialBuyOffset,
		InitialSellOffset: e.cfg.InitialSellOffset,
		DistanceContinue:  e.cfg.DistanceContinue,
		QuantityPerOrder:  e.cfg.QuantityPerOrder,
		MaxQuantity:       e.cfg.MaxQuantity,
		ProfitTargetPct:   e.cfg.ProfitTargetPct,
		StopLossPct:       e.cfg.StopLossPct,
		EnableSellLogic:   e.cfg.EnableSellLogic,
	}
	if userCfg, ok := e.getConfig(userID, token); ok {
		if userCfg.LowerRange > 0 {
			cfg.LowerRange = userCfg.LowerRange
		}
		if userCfg.HigherRange > 0 {
			cfg.HigherRange = userCfg.HigherRange
		}
		if userCfg.InitialBuyOffset > 0 {
			cfg.InitialBuyOffset = userCfg.InitialBuyOffset
		}
		if userCfg.InitialSellOffset > 0 {
			cfg.InitialSellOffset = userCfg.InitialSellOffset
		}
		if userCfg.DistanceContinue > 0 {
			cfg.DistanceContinue = userCfg.DistanceContinue
		}
		if userCfg.QuantityPerOrder > 0 {
			cfg.QuantityPerOrder = userCfg.QuantityPerOrder
		}
		if userCfg.MaxQuantity > 0 {
			cfg.MaxQuantity = userCfg.MaxQuantity
		}
		if userCfg.ProfitTargetPct > 0 {
			cfg.ProfitTargetPct = userCfg.ProfitTargetPct
		}
		if userCfg.StopLossPct > 0 {
			cfg.StopLossPct = userCfg.StopLossPct
		}
		cfg.EnableSellLogic = userCfg.EnableSellLogic

		// Phase B: Load advanced risk management parameters from user config
		if userCfg.MaxDailyLoss > 0 {
			cfg.MaxDailyLoss = userCfg.MaxDailyLoss
		}
		if userCfg.MaxPositionValue > 0 {
			cfg.MaxPositionValue = userCfg.MaxPositionValue
		}
		if userCfg.TrailingStopPct > 0 {
			cfg.TrailingStopPct = userCfg.TrailingStopPct
		}
		if userCfg.TimeBasedExitMins > 0 {
			cfg.TimeBasedExitMins = userCfg.TimeBasedExitMins
		}
		if userCfg.VolatilityMultiplier > 0 {
			cfg.VolatilityMultiplier = userCfg.VolatilityMultiplier
		}
		if userCfg.SpreadMultiplier > 0 {
			cfg.SpreadMultiplier = userCfg.SpreadMultiplier
		}
		cfg.EnableTrailingStop = userCfg.EnableTrailingStop
		cfg.EnableTimeBasedExit = userCfg.EnableTimeBasedExit
	}

	// Check if LTP is within configured range
	if ltp < cfg.LowerRange || ltp > cfg.HigherRange {
		e.logger.Debug("LTP outside jobbing range, skipping",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("ltp", ltp),
			zap.Float64("lower_range", cfg.LowerRange),
			zap.Float64("higher_range", cfg.HigherRange))
		return nil
	}

	tokenState := e.getTokenState(userID, token)

	// Phase B: Advanced Position Management and Risk Control

	// Apply Phase B defaults to config if needed
	e.SetUserTokenConfigDefaults(&cfg)

	// Reset daily metrics if new trading day
	e.ResetDailyMetrics(tokenState)

	// Update real-time P&L with current market price
	e.UpdatePnL(tokenState, ltp)

	// Check risk limits before any trading decisions
	canTrade, riskReason := e.CheckRiskLimits(userID, token, cfg, tokenState)
	if !canTrade {
		e.logger.Warn("Trading blocked due to risk limits",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.String("reason", riskReason),
			zap.String("risk_level", tokenState.RiskLevel),
			zap.Float64("daily_pnl", tokenState.DailyPnL))
		return nil
	}

	// Phase B: Check exit conditions first (trailing stop, time-based exit)
	shouldExitPosition := false
	exitReason := ""

	if tokenState.NetPosition > 0 {
		// Check trailing stop
		if e.CheckTrailingStop(cfg, tokenState) {
			shouldExitPosition = true
			exitReason = "trailing_stop"
		}

		// Check time-based exit
		if !shouldExitPosition && e.CheckTimeBasedExit(cfg, tokenState) {
			shouldExitPosition = true
			exitReason = "time_limit"
		}

		// If exit condition triggered, place SELL order for entire position
		if shouldExitPosition {
			exitQty := tokenState.NetPosition
			sellPrice := ltp // Market price exit

			e.logger.Info("Exiting position due to Phase B exit condition",
				zap.String("user_id", userID),
				zap.String("symbol", ev.StockData.Symbol),
				zap.String("exit_reason", exitReason),
				zap.Int32("exit_qty", exitQty),
				zap.Float64("exit_price", sellPrice),
				zap.Float64("unrealized_pnl", tokenState.UnrealizedPnL))

			return e.placeOrder(ctx, userID, token, ev, cfg, sellPrice, exitQty, "SELL", tokenState)
		}
	}

	// Handle BUY logic - place buy orders when conditions are right
	if err := e.handleBuyLogic(ctx, userID, token, ev, cfg, tokenState); err != nil {
		e.logger.Error("Failed to handle BUY logic", zap.Error(err))
	}

	// Handle SELL logic - exit positions when profitable or stop loss
	if cfg.EnableSellLogic {
		if err := e.handleSellLogic(ctx, userID, token, ev, cfg, tokenState); err != nil {
			e.logger.Error("Failed to handle SELL logic", zap.Error(err))
		}
	}

	return nil
}

// handleBuyLogic implements the original BUY order placement logic
func (e *Engine) handleBuyLogic(ctx context.Context, userID, token string, ev *models.JobbingMarketDepthEvent, cfg UserTokenConfig, tokenState *userTokenState) error {
	ltp := ev.MarketData.LastTradedPrice

	// Check if max quantity reached
	if tokenState.TotalQuantityBuy >= cfg.MaxQuantity {
		e.logger.Debug("Max quantity reached for token",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Int32("total_qty", tokenState.TotalQuantityBuy),
			zap.Int32("max_qty", cfg.MaxQuantity))
		return nil
	}

	// Calculate BUY order price
	var orderPrice float64
	if tokenState.LastBuyOrderPrice == 0 {
		// First order: LTP - InitialBuyOffset
		orderPrice = ltp - cfg.InitialBuyOffset
	} else {
		// Subsequent orders: LastBuyOrderPrice - DistanceContinue
		orderPrice = tokenState.LastBuyOrderPrice - cfg.DistanceContinue
	}

	// Round to 2 decimal places
	orderPrice = math.Round(orderPrice*100) / 100

	// Ensure order price is within range
	if orderPrice < cfg.LowerRange {
		e.logger.Debug("Calculated BUY order price below lower range",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("order_price", orderPrice),
			zap.Float64("lower_range", cfg.LowerRange))
		return nil
	}

	// Check market depth conditions for better execution
	if !e.validateMarketDepth(ev, orderPrice) {
		e.logger.Debug("Market depth conditions not favorable for BUY",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("spread_pct", ev.MarketData.DepthMetrics.SpreadPct))
		return nil
	}

	// Calculate remaining quantity
	remainingQty := cfg.MaxQuantity - tokenState.TotalQuantityBuy
	qty := cfg.QuantityPerOrder
	if qty > remainingQty {
		qty = remainingQty
	}

	return e.placeOrder(ctx, userID, token, ev, cfg, orderPrice, qty, "BUY", tokenState)
}

// handleSellLogic implements SELL order placement for position exit
func (e *Engine) handleSellLogic(ctx context.Context, userID, token string, ev *models.JobbingMarketDepthEvent, cfg UserTokenConfig, tokenState *userTokenState) error {
	ltp := ev.MarketData.LastTradedPrice

	// Check if we have any positions to sell
	if tokenState.NetPosition <= 0 {
		return nil // No positions to sell
	}

	// Calculate if we should exit based on profit or loss
	avgEntryPrice := tokenState.AverageEntryPrice
	currentPnLPct := ((ltp - avgEntryPrice) / avgEntryPrice) * 100

	shouldExit := false
	exitReason := ""

	// Check profit target
	if currentPnLPct >= cfg.ProfitTargetPct {
		shouldExit = true
		exitReason = "PROFIT_TARGET"
	}

	// Check stop loss
	if currentPnLPct <= -cfg.StopLossPct {
		shouldExit = true
		exitReason = "STOP_LOSS"
	}

	if !shouldExit {
		return nil // No exit condition met
	}

	// Calculate SELL order price
	var orderPrice float64
	if tokenState.LastSellOrderPrice == 0 {
		// First SELL order: LTP + InitialSellOffset
		orderPrice = ltp + cfg.InitialSellOffset
	} else {
		// Subsequent SELL orders: LastSellOrderPrice + DistanceContinue
		orderPrice = tokenState.LastSellOrderPrice + cfg.DistanceContinue
	}

	// Round to 2 decimal places
	orderPrice = math.Round(orderPrice*100) / 100

	// Ensure SELL order price is within range
	if orderPrice > cfg.HigherRange {
		e.logger.Debug("Calculated SELL order price above higher range",
			zap.String("user_id", userID),
			zap.String("symbol", ev.StockData.Symbol),
			zap.Float64("order_price", orderPrice),
			zap.Float64("higher_range", cfg.HigherRange))
		return nil
	}

	// Calculate quantity to sell (sell all position)
	qty := tokenState.NetPosition
	if qty > cfg.QuantityPerOrder {
		qty = cfg.QuantityPerOrder // Limit to max quantity per order
	}

	e.logger.Info("Placing SELL order for position exit",
		zap.String("user_id", userID),
		zap.String("symbol", ev.StockData.Symbol),
		zap.String("exit_reason", exitReason),
		zap.Float64("pnl_pct", currentPnLPct),
		zap.Float64("avg_entry_price", avgEntryPrice),
		zap.Float64("current_ltp", ltp),
		zap.Int32("position_size", tokenState.NetPosition),
		zap.Int32("sell_qty", qty))

	return e.placeOrder(ctx, userID, token, ev, cfg, orderPrice, qty, "SELL", tokenState)
}

// placeOrder handles the common order placement logic for both BUY and SELL
func (e *Engine) placeOrder(ctx context.Context, userID, token string, ev *models.JobbingMarketDepthEvent, cfg UserTokenConfig, orderPrice float64, qty int32, side string, tokenState *userTokenState) error {
	// Build order request
	orderReq := &models.OrderRequest{
		OrderID:      uuid.New().String(),
		UserID:       userID,
		StrategyID:   "JOBBING",
		StrategyName: "Jobbing Strategy",
		EventID:      ev.EventID,
		StockCode:    int64(ev.StockData.StockCode),
		Token:        int64(ev.StockData.StockCode),
		Symbol:       ev.StockData.Symbol,
		Exchange:     strings.ToUpper(ev.StockData.Exchange),
		OrderType:    "LIMIT",
		OrderSide:    side,
		Quantity:     qty,
		Price:        orderPrice,
		StopLoss:     0,
		TakeProfit:   0,
		Timestamp:    time.Now(),
		MatchScore:   100.0,
		ImpactScore:  0,
		Sentiment:    "",
		NewsCategory: "",
	}

	// Run risk check
	if e.riskClient != nil {
		strategy := &models.Strategy{
			StrategyID:   "JOBBING",
			UserID:       userID,
			StrategyName: "Jobbing Strategy",
			RiskLimits: models.RiskLimits{
				MaxDailyTrades:  100,
				MaxLossPerDay:   10000,
				MaxPositionSize: float64(cfg.MaxQuantity) * orderPrice,
				MaxPerTradeRisk: float64(qty) * orderPrice * 0.02,
				PositionSizing:  "FIXED",
			},
		}

		riskResp, err := e.riskClient.CheckPreTradeRisk(ctx, orderReq, strategy)
		if err != nil {
			e.logger.Error("Risk check failed for jobbing order",
				zap.Error(err),
				zap.String("user_id", userID),
				zap.String("symbol", ev.StockData.Symbol),
				zap.String("side", side))
			return err
		}
		orderReq.RiskApproved = riskResp.Approved
		orderReq.RiskScore = riskResp.RiskScore
		if !riskResp.Approved {
			e.logger.Warn("Jobbing order rejected by risk",
				zap.String("user_id", userID),
				zap.String("symbol", ev.StockData.Symbol),
				zap.String("side", side),
				zap.Float64("risk_score", riskResp.RiskScore))
			return nil
		}
	} else {
		orderReq.RiskApproved = true
		orderReq.RiskScore = 0
	}

	// Publish to Kafka trade signals
	if e.kafkaPub != nil {
		if err := e.kafkaPub.PublishTradeSignal(ctx, orderReq); err != nil {
			e.logger.Error("Failed to publish jobbing trade signal to Kafka",
				zap.Error(err),
				zap.String("order_id", orderReq.OrderID),
				zap.String("side", side))
		}
	}

	// Publish to RabbitMQ
	if err := e.rabbitPub.PublishOrder(ctx, orderReq); err != nil {
		return fmt.Errorf("failed to publish jobbing order: %w", err)
	}

	// Update token state based on order side
	e.mu.Lock()
	if side == "BUY" {
		tokenState.TotalQuantityBuy += qty
		tokenState.LastBuyOrderPrice = orderPrice
		tokenState.ActiveBuyOrders = append(tokenState.ActiveBuyOrders, orderReq.OrderID)
	} else if side == "SELL" {
		tokenState.TotalQuantitySell += qty
		tokenState.LastSellOrderPrice = orderPrice
		tokenState.ActiveSellOrders = append(tokenState.ActiveSellOrders, orderReq.OrderID)
	}
	tokenState.LastUpdated = time.Now()
	tokenState.Symbol = ev.StockData.Symbol
	tokenState.Exchange = orderReq.Exchange
	e.mu.Unlock()

	// Publish allocation snapshot
	e.publishAllocation(ctx, userID)

	e.logger.Info("Jobbing order published",
		zap.String("user_id", userID),
		zap.String("symbol", ev.StockData.Symbol),
		zap.String("exchange", orderReq.Exchange),
		zap.String("side", side),
		zap.Int32("quantity", qty),
		zap.Float64("order_price", orderPrice),
		zap.Float64("ltp", ev.MarketData.LastTradedPrice),
		zap.Int32("total_qty_bought", tokenState.TotalQuantityBuy),
		zap.Int32("total_qty_sold", tokenState.TotalQuantitySell),
		zap.Int32("net_position", tokenState.NetPosition))

	return nil
}

// validateMarketDepth checks if market conditions are favorable for order placement
func (e *Engine) validateMarketDepth(ev *models.JobbingMarketDepthEvent, orderPrice float64) bool {
	// Check spread - jobbing works best with tight spreads
	if ev.MarketData.DepthMetrics.SpreadPct > 0.5 { // 0.5% spread threshold
		return false
	}

	// Check if there's sufficient liquidity at bid side
	if ev.MarketData.DepthMetrics.TotalBidQty < 100 {
		return false
	}

	// Check bid-ask ratio - avoid extreme imbalances
	if ev.MarketData.DepthMetrics.BidAskRatio < 0.3 || ev.MarketData.DepthMetrics.BidAskRatio > 3.0 {
		return false
	}

	return true
}

// HandleOrderFill processes order fill notifications and updates position state
func (e *Engine) HandleOrderFill(orderID string, userID string, token string, side string, executedQty int32, executedPrice float64) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tokenState := e.getTokenState(userID, token)

	if side == "BUY" {
		// Update position for BUY fill
		oldPosition := tokenState.FilledQuantityBuy
		oldAvgPrice := tokenState.AverageEntryPrice

		// Update filled quantities
		tokenState.FilledQuantityBuy += executedQty

		// Calculate new average entry price
		if oldPosition == 0 {
			tokenState.AverageEntryPrice = executedPrice
		} else {
			totalValue := (float64(oldPosition) * oldAvgPrice) + (float64(executedQty) * executedPrice)
			tokenState.AverageEntryPrice = totalValue / float64(tokenState.FilledQuantityBuy)
		}

		// Remove from active orders
		for i, activeOrderID := range tokenState.ActiveBuyOrders {
			if activeOrderID == orderID {
				tokenState.ActiveBuyOrders = append(tokenState.ActiveBuyOrders[:i], tokenState.ActiveBuyOrders[i+1:]...)
				break
			}
		}

		e.logger.Info("BUY order filled",
			zap.String("user_id", userID),
			zap.String("token", token),
			zap.String("order_id", orderID),
			zap.Int32("executed_qty", executedQty),
			zap.Float64("executed_price", executedPrice),
			zap.Float64("avg_entry_price", tokenState.AverageEntryPrice),
			zap.Int32("total_filled_buy", tokenState.FilledQuantityBuy))

	} else if side == "SELL" {
		// Update position for SELL fill
		oldBuyQty := tokenState.FilledQuantityBuy

		tokenState.FilledQuantitySell += executedQty

		// Phase B: Calculate realized P&L for this SELL transaction
		if tokenState.AverageEntryPrice > 0 && oldBuyQty > 0 {
			tradeResult := float64(executedQty) * (executedPrice - tokenState.AverageEntryPrice)
			tokenState.RealizedPnL += tradeResult
			tokenState.DailyPnL += tradeResult

			// Update performance metrics
			e.UpdatePerformanceMetrics(tokenState, tradeResult)

			// Reset trailing stop and position start time if position closed
			if tokenState.FilledQuantityBuy == tokenState.FilledQuantitySell {
				tokenState.TrailingStopPrice = 0
				tokenState.PositionStartTime = time.Now() // Reset for next position
			}

			e.logger.Info("SELL order filled with P&L calculation",
				zap.String("user_id", userID),
				zap.String("token", token),
				zap.String("order_id", orderID),
				zap.Int32("executed_qty", executedQty),
				zap.Float64("executed_price", executedPrice),
				zap.Float64("avg_entry_price", tokenState.AverageEntryPrice),
				zap.Float64("trade_result", tradeResult),
				zap.Float64("realized_pnl", tokenState.RealizedPnL),
				zap.Int32("total_filled_sell", tokenState.FilledQuantitySell),
				zap.Float64("win_rate", tokenState.WinRate),
				zap.String("risk_level", tokenState.RiskLevel))
		}

		// Remove from active orders
		for i, activeOrderID := range tokenState.ActiveSellOrders {
			if activeOrderID == orderID {
				tokenState.ActiveSellOrders = append(tokenState.ActiveSellOrders[:i], tokenState.ActiveSellOrders[i+1:]...)
				break
			}
		}
	}

	// Update net position
	tokenState.NetPosition = tokenState.FilledQuantityBuy - tokenState.FilledQuantitySell
	tokenState.LastUpdated = time.Now()

	// Phase B: Update trailing stop price if position opened/increased
	if side == "BUY" && tokenState.NetPosition > 0 {
		tokenState.PositionStartTime = time.Now()
	}

	e.logger.Debug("Order fill processed - position updated",
		zap.String("user_id", userID),
		zap.String("token", token),
		zap.Int32("net_position", tokenState.NetPosition),
		zap.Float64("total_pnl", tokenState.TotalPnL),
		zap.Int32("total_trades", tokenState.TotalTrades))

	return nil
}

// publishAllocation emits the current allocation snapshot for jobbing strategy
func (e *Engine) publishAllocation(ctx context.Context, userID string) {
	if e.allocPub == nil {
		return
	}

	e.mu.Lock()
	st, ok := e.userState[userID]
	if !ok {
		e.mu.Unlock()
		return
	}

	positions := make([]models.AllocationPosition, 0, len(st.TokenStates))
	for _, tokenSt := range st.TokenStates {
		if tokenSt.TotalQuantityBuy > 0 {
			positions = append(positions, models.AllocationPosition{
				Token:    tokenSt.Token,
				Symbol:   tokenSt.Symbol,
				Exchange: tokenSt.Exchange,
			})
		}
	}
	e.mu.Unlock()

	ev := &models.PortfolioAllocationEvent{
		UserID:          userID,
		StrategyID:      "JOBBING",
		StrategyName:    "Jobbing Strategy",
		Date:            todayStr(),
		Positions:       positions,
		TotalPositions:  len(positions),
		MaxPositions:    len(e.cfg.Tokens),
		CapitalPerStock: float64(e.cfg.MaxQuantity) * e.cfg.LowerRange, // approximate
		Timestamp:       time.Now(),
	}

	if err := e.allocPub.PublishAllocation(ctx, ev); err != nil {
		e.logger.Error("Failed to publish jobbing allocation",
			zap.String("user_id", userID),
			zap.Error(err))
	}
}

// GetStats returns current statistics for monitoring
func (e *Engine) GetStats() map[string]interface{} {
	e.mu.Lock()
	defer e.mu.Unlock()

	stats := make(map[string]interface{})
	stats["day"] = e.day
	stats["total_users"] = len(e.userState)
	stats["config"] = map[string]interface{}{
		"lower_range":        e.cfg.LowerRange,
		"higher_range":       e.cfg.HigherRange,
		"initial_buy_offset": e.cfg.InitialBuyOffset,
		"distance_continue":  e.cfg.DistanceContinue,
		"quantity_per_order": e.cfg.QuantityPerOrder,
		"max_quantity":       e.cfg.MaxQuantity,
	}

	// Phase B: Enhanced statistics with P&L and performance metrics
	totalUsers := 0
	totalPositions := 0
	totalTrades := int32(0)
	totalWinningTrades := int32(0)
	totalRealizedPnL := 0.0
	totalUnrealizedPnL := 0.0
	totalDailyPnL := 0.0
	totalMaxDrawdown := 0.0
	highRiskUsers := 0
	mediumRiskUsers := 0
	lowRiskUsers := 0
	userStats := make(map[string]interface{})

	for userID, userState := range e.userState {
		totalUsers++
		userPnL := 0.0
		userPositions := 0
		userTotalTrades := int32(0)
		userWinRate := 0.0
		userTokens := make(map[string]interface{})

		for token, tokenState := range userState.TokenStates {
			if tokenState.NetPosition != 0 {
				totalPositions++
				userPositions++
			}

			totalTrades += tokenState.TotalTrades
			totalWinningTrades += tokenState.WinningTrades
			totalRealizedPnL += tokenState.RealizedPnL
			totalUnrealizedPnL += tokenState.UnrealizedPnL
			totalDailyPnL += tokenState.DailyPnL
			totalMaxDrawdown += tokenState.MaxDrawdown
			userPnL += tokenState.TotalPnL
			userTotalTrades += tokenState.TotalTrades
			userWinRate = tokenState.WinRate

			// Risk level counting
			switch tokenState.RiskLevel {
			case "HIGH":
				highRiskUsers++
			case "MEDIUM":
				mediumRiskUsers++
			case "LOW":
				lowRiskUsers++
			}

			userTokens[token] = map[string]interface{}{
				"symbol":              tokenState.Symbol,
				"net_position":        tokenState.NetPosition,
				"realized_pnl":        tokenState.RealizedPnL,
				"unrealized_pnl":      tokenState.UnrealizedPnL,
				"total_pnl":           tokenState.TotalPnL,
				"daily_pnl":           tokenState.DailyPnL,
				"total_trades":        tokenState.TotalTrades,
				"winning_trades":      tokenState.WinningTrades,
				"win_rate":            tokenState.WinRate,
				"avg_win":             tokenState.AvgWin,
				"avg_loss":            tokenState.AvgLoss,
				"profit_factor":       tokenState.ProfitFactor,
				"max_drawdown":        tokenState.MaxDrawdown,
				"risk_level":          tokenState.RiskLevel,
				"trailing_stop_price": tokenState.TrailingStopPrice,
				"daily_loss_limit":    tokenState.DailyLossLimit,
				"current_ltp":         tokenState.CurrentLTP,
			}
		}

		userStats[userID] = map[string]interface{}{
			"total_pnl":    userPnL,
			"positions":    userPositions,
			"total_trades": userTotalTrades,
			"win_rate":     userWinRate,
			"tokens":       userTokens,
		}
	}

	// Calculate overall win rate
	overallWinRate := 0.0
	if totalTrades > 0 {
		overallWinRate = (float64(totalWinningTrades) / float64(totalTrades)) * 100
	}

	stats["phase_b_metrics"] = map[string]interface{}{
		"total_realized_pnl":   totalRealizedPnL,
		"total_unrealized_pnl": totalUnrealizedPnL,
		"total_daily_pnl":      totalDailyPnL,
		"total_positions":      totalPositions,
		"total_trades":         totalTrades,
		"winning_trades":       totalWinningTrades,
		"overall_win_rate":     overallWinRate,
		"total_max_drawdown":   totalMaxDrawdown,
		"risk_distribution": map[string]interface{}{
			"high_risk_users":   highRiskUsers,
			"medium_risk_users": mediumRiskUsers,
			"low_risk_users":    lowRiskUsers,
		},
	}

	stats["user_details"] = userStats

	return stats
}
