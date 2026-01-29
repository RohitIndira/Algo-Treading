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
	// Build a set of users who currently have the managed 52W strategy
	// enabled. We check the ConfigStore directly to get the latest list
	// of enabled users, avoiding race conditions with the periodic refresh.
	var enabledUsers map[string]struct{}
	if e.store != nil {
		userIDs, _ := e.store.Snapshot()
		enabledUsers = make(map[string]struct{}, len(userIDs))
		for _, u := range userIDs {
			u = strings.TrimSpace(u)
			if u == "" {
				continue
			}
			enabledUsers[u] = struct{}{}
		}
	} else {
		enabledUsers = make(map[string]struct{}, len(e.cfg.UserIDs))
		for _, u := range e.cfg.UserIDs {
			u = strings.TrimSpace(u)
			if u == "" {
				continue
			}
			enabledUsers[u] = struct{}{}
		}
	}

	allocSnapshot := make(map[string][]models.AllocationPosition, len(e.userState))
	userModes := make(map[string]string, len(e.userState))
	for userID, st := range e.userState {
		// Only emit realtime portfolios for users that currently have
		// an active CASH_52W_HIGH strategy according to ES.
		if _, ok := enabledUsers[userID]; !ok {
			continue
		}

		positions := make([]models.AllocationPosition, 0, len(st.Positions))
		for _, pos := range st.Positions {
			positions = append(positions, pos)
		}
		if len(positions) == 0 {
			continue
		}

		// Determine effective trading mode for this user (LIVE/PAPER).
		mode := e.effectiveModeForUser(userID)
		if mode != "PAPER" {
			// For now we only emit realtime PnL snapshots for PAPER users.
			continue
		}

		allocSnapshot[userID] = positions
		userModes[userID] = mode
	}
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
			var ltp float64
			var prevClose float64
			if err != nil {
				// If Redis has no live price yet, fall back to entry/prev_close
				// so that we still emit a snapshot. In this case open PnL will
				// be 0, which is acceptable until real market data arrives.
				ltp = pos.EntryPrice
				prevClose = pos.EntryPrice
			} else {
				var md struct {
					LTP       float64 `json:"ltp"`
					PrevClose float64 `json:"prev_close"`
				}
				if err := json.Unmarshal([]byte(jsonData), &md); err != nil {
					ltp = pos.EntryPrice
					prevClose = pos.EntryPrice
				} else {
					// Normalise invalid/missing values by falling back to
					// entry price so that PnL remains well-defined.
					if md.LTP > 0 {
						ltp = md.LTP
					} else {
						ltp = pos.EntryPrice
					}
					if md.PrevClose > 0 {
						prevClose = md.PrevClose
					} else {
						prevClose = pos.EntryPrice
					}
				}
			}

			// Open Position PnL per your spec:
			// (Current Price – Previous Close Price) × Quantity
			invested := float64(pos.Quantity) * prevClose
			current := float64(pos.Quantity) * ltp
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
				LTP:        ltp,
				PnL:        pnl,
				PnLPct:     pnlPct,
			})

			totalInvested += invested
			totalCurrent += current
		}

		if len(realtimePositions) == 0 {
			continue
		}

		mode, ok := userModes[userID]
		if !ok || mode == "" {
			mode = "LIVE"
		}

		// Open PnL for current positions
		totalPnL := totalCurrent - totalInvested
		// For now we do not track realized (closed) PnL at the engine
		// level, so ClosedPnL is reported as 0. This still lets us
		// compute a sensible portfolio value for open positions.
		closedPnL := 0.0
		portfolioValue := totalCurrent + closedPnL
		// Per your strategy design, average per-stock return is
		// PortfolioValue / 25 (25-stock basket).
		averagePerStock := 0.0
		if 25 > 0 {
			averagePerStock = portfolioValue / 25.0
		}

		events = append(events, &models.RealtimePortfolioEvent{
			UserID:          userID,
			StrategyID:      "CASH_52W_HIGH",
			StrategyName:    "Cash 52-Week High",
			Mode:            mode,
			Positions:       realtimePositions,
			TotalPnL:        totalPnL,
			TotalInvested:   totalInvested,
			TotalCurrent:    totalCurrent,
			ClosedPnL:       closedPnL,
			PortfolioValue:  portfolioValue,
			AveragePerStock: averagePerStock,
			Timestamp:       time.Now(),
		})
	}

	return events
}
