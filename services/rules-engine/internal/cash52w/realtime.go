package cash52w

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// BuildRealtimePortfolios constructs realtime marked-to-market portfolio
// snapshots for all users of the CASH_52W_HIGH strategy by joining the
// in-memory allocation state with live prices from Redis.
func (e *Engine) BuildRealtimePortfolios(ctx context.Context, redisCache *cache.RedisCache) []*models.RealtimePortfolioEvent {
	if redisCache == nil {
		return nil
	}

	// Snapshot current allocations under lock
	e.mu.Lock()
	allocSnapshot := make(map[string][]models.AllocationPosition, len(e.userState))
	for userID, st := range e.userState {
		positions := make([]models.AllocationPosition, 0, len(st.Positions))
		for _, pos := range st.Positions {
			positions = append(positions, pos)
		}
		allocSnapshot[userID] = positions
	}
	mode := e.cfg.TradingMode
	e.mu.Unlock()

	if len(allocSnapshot) == 0 {
		return nil
	}

	events := make([]*models.RealtimePortfolioEvent, 0, len(allocSnapshot))

	for userID, positions := range allocSnapshot {
		if len(positions) == 0 {
			continue
		}

		var (
			realtimePositions []models.RealtimePosition
			totalInvested     float64
			totalCurrent      float64
		)

		for _, pos := range positions {
			if pos.Quantity <= 0 || pos.EntryPrice <= 0 {
				continue
			}

			// Build Redis key: market:{exchange}:{token}
			exchange := strings.ToLower(pos.Exchange)
			exchange = strings.TrimSuffix(exchange, "_eq")
			key := fmt.Sprintf("market:%s:%s", exchange, pos.Token)

			jsonData, err := redisCache.Get(ctx, key)
			if err != nil {
				// Ignore cache misses or errors for now; this position will
				// simply be omitted from realtime valuations.
				continue
			}

			var md struct {
				LTP float64 `json:"ltp"`
			}
			if err := json.Unmarshal([]byte(jsonData), &md); err != nil || md.LTP <= 0 {
				continue
			}

			invested := float64(pos.Quantity) * pos.EntryPrice
			current := float64(pos.Quantity) * md.LTP
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
				LTP:        md.LTP,
				PnL:        pnl,
				PnLPct:     pnlPct,
			})

			totalInvested += invested
			totalCurrent += current
		}

		if len(realtimePositions) == 0 {
			continue
		}

		totalPnL := totalCurrent - totalInvested
		events = append(events, &models.RealtimePortfolioEvent{
			UserID:        userID,
			StrategyID:    "CASH_52W_HIGH",
			StrategyName:  "Cash 52-Week High",
			Mode:          mode,
			Positions:     realtimePositions,
			TotalPnL:      totalPnL,
			TotalInvested: totalInvested,
			TotalCurrent:  totalCurrent,
			Timestamp:     time.Now(),
		})
	}

	return events
}
