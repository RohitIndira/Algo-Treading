package cash52w

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

// BuildRealtimePortfolios constructs realtime marked-to-market portfolio
// snapshots for all users of the CASH_52W_HIGH strategy by joining the
// in-memory allocation state with live prices from Redis.
//
// This function now returns snapshots for ALL active users, even those with
// zero positions, ensuring the WebSocket receives data every 5 seconds.
func (e *Engine) BuildRealtimePortfolios(ctx context.Context, redisCache *cache.RedisCache) []*models.RealtimePortfolioEvent {
	if redisCache == nil {
		return nil
	}

	// Snapshot current allocations, realized P&L, and active users under lock
	e.mu.Lock()
	activeUsers := make([]string, len(e.cfg.UserIDs))
	copy(activeUsers, e.cfg.UserIDs)

	allocSnapshot := make(map[string][]models.AllocationPosition, len(e.userState))
	realizedPnls := make(map[string]float64, len(activeUsers))
	userModes := make(map[string]string, len(activeUsers))

	for userID, st := range e.userState {
		positions := make([]models.AllocationPosition, 0, len(st.Positions))
		for _, pos := range st.Positions {
			positions = append(positions, pos)
		}
		allocSnapshot[userID] = positions
		realizedPnls[userID] = st.RealizedPnL
	}

	// Get per-user trading modes
	for _, userID := range activeUsers {
		if mode, ok := e.userTradingMode[userID]; ok {
			userModes[userID] = mode
		} else {
			userModes[userID] = e.cfg.TradingMode
		}
	}
	e.mu.Unlock()

	// Return nil if no active users configured
	if len(activeUsers) == 0 {
		return nil
	}

	events := make([]*models.RealtimePortfolioEvent, 0, len(activeUsers))

	// Iterate through ALL active users, not just those with positions
	for _, userID := range activeUsers {
		positions := allocSnapshot[userID] // may be nil or empty
		realized := realizedPnls[userID]   // may be 0
		mode := userModes[userID]

		var (
			realtimePositions []models.RealtimePosition
			totalInvested     float64
			totalCurrent      float64
		)

		// Process positions if they exist
		for _, pos := range positions {
			if pos.Quantity <= 0 || pos.EntryPrice <= 0 {
				continue
			}

			// Build Redis key: market:{exchange}:{token}
			exchange := strings.ToLower(pos.Exchange)
			exchange = strings.TrimSuffix(exchange, "_eq")
			key := fmt.Sprintf("market:%s:%s", exchange, pos.Token)

			invested := float64(pos.Quantity) * pos.EntryPrice
			totalInvested += invested

			jsonData, err := redisCache.Get(ctx, key)
			var currentPrice float64
			if err != nil {
				// If Redis fails, fallback to entry price for valuation
				// Log at Debug level to avoid spamming logs if keys are transiently missing,
				// but allows debugging when LTP is stuck.
				e.logger.Debug("LTP lookup failed, using entry price",
					zap.String("key", key),
					zap.Error(err))
				currentPrice = pos.EntryPrice
			} else {
				var md struct {
					LTP float64 `json:"ltp"`
				}
				if err := json.Unmarshal([]byte(jsonData), &md); err != nil {
					e.logger.Warn("LTP JSON parse failed, using entry price",
						zap.String("key", key),
						zap.Error(err))
					currentPrice = pos.EntryPrice
				} else if md.LTP <= 0 {
					e.logger.Warn("LTP is non-positive, using entry price",
						zap.String("key", key),
						zap.Float64("ltp", md.LTP))
					currentPrice = pos.EntryPrice
				} else {
					currentPrice = md.LTP
				}
			}

			current := float64(pos.Quantity) * currentPrice
			pnl := current - invested
			pnlPct := 0.0
			if invested > 0 {
				pnlPct = (pnl / invested) * 100.0
			}

			realtimePositions = append(realtimePositions, models.RealtimePosition{
				Token:      pos.Token,
				Symbol:     pos.Symbol,
				Exchange:   pos.Exchange,
				Quantity:   pos.Quantity,
				EntryPrice: pos.EntryPrice,
				LTP:        currentPrice,
				PnL:        pnl,
				PnLPct:     pnlPct,
			})

			totalCurrent += current
		}

		// Always create an event, even if positions are empty
		totalPnL := (totalCurrent - totalInvested) + realized
		events = append(events, &models.RealtimePortfolioEvent{
			UserID:        userID,
			StrategyID:    Cash52WStrategyID,
			StrategyName:  "Cash 52-Week High",
			Mode:          mode,
			Positions:     realtimePositions,
			TotalPnL:      totalPnL,
			RealizedPnL:   realized,
			TotalInvested: totalInvested,
			TotalCurrent:  totalCurrent,
			Timestamp:     time.Now(),
		})
	}

	return events
}
