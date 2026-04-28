// Test: Verify broker adapter resolves symbols, fetches LTP, checks circuit.
// Does NOT place real orders — just validates data path.
// Usage: EXT_REDIS_ADDR=15.207.203.46:6379 EXT_REDIS_PASSWORD='R3d1s@Prod#2026' go run ./services/trade-execution/cmd/manthan-broker-test/
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/manthan"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	extAddr := os.Getenv("EXT_REDIS_ADDR")
	extPass := os.Getenv("EXT_REDIS_PASSWORD")
	if extAddr == "" {
		fmt.Println("Set EXT_REDIS_ADDR and EXT_REDIS_PASSWORD")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	extRedis := redis.NewClient(&redis.Options{
		Addr:     extAddr,
		Password: extPass,
		DB:       0,
	})
	if err := extRedis.Ping(ctx).Err(); err != nil {
		logger.Fatal("External Redis ping failed", zap.Error(err))
	}
	fmt.Println("✅ External Redis connected")

	adapter := manthan.NewBrokerAdapter(nil, extRedis, logger) // nil client = no real orders

	// Test stocks from our portfolio
	stocks := []struct {
		Symbol string
		ISIN   string
	}{
		{"GALLANTT", "INE297H01019"},
		{"INOXINDIA", "INE616N01034"},
		{"KRISHANA", "INE506W01012"},
		{"NATIONALUM", "INE139A01034"},
		{"GMDCLTD", "INE131A01031"},
		{"PRECWIRE", "INE372C01037"},
		{"VISHNU", "INE270I01022"},
	}

	fmt.Printf("\n%-12s %-8s %-20s %-10s %-8s %-10s %-10s %-8s %-8s\n",
		"SYMBOL", "TOKEN", "INDIRA_SYMBOL", "LTP", "TICK", "DPR_LOW", "DPR_HIGH", "UC?", "LC?")
	fmt.Println("────────────────────────────────────────────────────────────────────────────────────────────────")

	for _, s := range stocks {
		info, err := adapter.ResolveSymbol(ctx, s.Symbol, s.ISIN)
		if err != nil {
			fmt.Printf("%-12s ERROR: %v\n", s.Symbol, err)
			continue
		}

		ltp, ltpErr := adapter.FetchLTP(ctx, info.ExchangeToken)
		atUpper, atLower, circErr := adapter.CheckCircuit(ctx, info.ExchangeToken)

		ltpStr := "ERR"
		if ltpErr == nil {
			ltpStr = fmt.Sprintf("₹%.2f", ltp)
		}
		circStr := ""
		if circErr == nil {
			if atUpper {
				circStr = "UC!"
			} else if atLower {
				circStr = "LC!"
			} else {
				circStr = "OK"
			}
		}

		fmt.Printf("%-12s %-8s %-20s %-10s %-8.2f %-10.2f %-10.2f %-8s\n",
			s.Symbol,
			info.ExchangeToken,
			info.IndiraSymbol[:20],
			ltpStr,
			info.TickSize,
			info.DPRLower,
			info.DPRUpper,
			circStr,
		)

		// Test: what would the SL order look like?
		if ltpErr == nil {
			slTrigger := ltp * 0.80
			slLimit := slTrigger - (info.TickSize * 5)
			fmt.Printf("             └─ Entry=₹%.2f → SL trigger=₹%.2f limit=₹%.2f (20%% below)\n",
				ltp, slTrigger, slLimit)
		}
	}

	// Test: what if ISIN doesn't exist?
	fmt.Println("\n=== EDGE CASE: Invalid ISIN ===")
	_, err := adapter.ResolveSymbol(ctx, "FAKE", "INE000000000")
	if err != nil {
		fmt.Printf("✅ Invalid ISIN correctly rejected: %v\n", err)
	} else {
		fmt.Println("❌ BUG: invalid ISIN should have failed")
	}
}
