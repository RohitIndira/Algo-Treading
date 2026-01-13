package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/joho/godotenv"

	redispkg "github.com/RohitIndira/Algo-Treading/pkg/database/redis"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/config"
)

// MarketSnapshot mirrors the structure used by the Redis52WWatcher so we can
// debug which stocks in Redis satisfy the 52-week breakout criteria.
type MarketSnapshot struct {
	Symbol          string  `json:"symbol"`
	Token           string  `json:"token"`
	Exchange        string  `json:"exchange"`
	LTP             float64 `json:"ltp"`
	Week52High      float64 `json:"week_52_high"`
	Week52Low       float64 `json:"week_52_low"`
	Week52HighDate  string  `json:"week_52_high_date"`
	IsNewWeek52High bool    `json:"is_new_week_52_high"`
	IsNewWeek52Low  bool    `json:"is_new_week_52_low"`
	Timestamp       int64   `json:"timestamp"`
	LastUpdated     string  `json:"last_updated"`
}

func todayStr() string {
	return time.Now().Format("2006-01-02")
}

// updatedDate extracts the local date (YYYY-MM-DD) from last_updated or,
// as a fallback, from timestamp. Returns the date string and true on
// success, or "" and false if no valid date is available. This matches
// the logic in Redis52WWatcher.updatedDate.
func updatedDate(snap MarketSnapshot) (string, bool) {
	if snap.LastUpdated != "" {
		if t, err := time.Parse(time.RFC3339Nano, snap.LastUpdated); err == nil {
			local := t.Local()
			return local.Format("2006-01-02"), true
		}
	}

	if snap.Timestamp != 0 {
		t := time.Unix(0, snap.Timestamp*int64(time.Millisecond)).Local()
		return t.Format("2006-01-02"), true
	}

	return "", false
}

// matches52WCriteria returns true if this snapshot should be treated as a
// 52-week high breakout candidate according to the same rules as
// Redis52WWatcher.isToday52WBreakout: updatedDate == today AND
// week_52_high_date == updatedDate.
func matches52WCriteria(snap MarketSnapshot) bool {
	updateDay, ok := updatedDate(snap)
	if !ok {
		return false
	}
	if updateDay != todayStr() {
		return false
	}
	if snap.Week52HighDate == "" {
		return false
	}
	return snap.Week52HighDate == updateDay
}

func scanPattern(ctx context.Context, client *redispkg.Client, pattern string) (int, error) {
	iter := client.Scan(ctx, 0, pattern, 1000).Iterator()
	count := 0
	scanned := 0

	for iter.Next(ctx) {
		scanned++
		key := iter.Val()
		raw, err := client.GetString(ctx, key)
		if err != nil {
			fmt.Printf("[WARN] redis get failure for key=%s err=%v\n", key, err)
			continue
		}

		var snap MarketSnapshot
		if err := json.Unmarshal([]byte(raw), &snap); err != nil {
			fmt.Printf("[WARN] failed to unmarshal key=%s err=%v\n", key, err)
			continue
		}

		if !matches52WCriteria(snap) {
			continue
		}

		count++
		fmt.Printf("MATCH %d | key=%s token=%s symbol=%s exch=%s ltp=%.2f week_52_high=%.2f week_52_high_date=%s last_updated=%s timestamp=%d\n",
			count, key, snap.Token, snap.Symbol, snap.Exchange,
			snap.LTP, snap.Week52High, snap.Week52HighDate, snap.LastUpdated, snap.Timestamp)
	}

	if err := iter.Err(); err != nil {
		return count, fmt.Errorf("redis scan error for pattern %s: %w", pattern, err)
	}

	fmt.Printf("[INFO] Pattern %s: scanned %d keys, matched %d\n", pattern, scanned, count)
	return count, nil
}

func main() {
	// Load .env to pick up MARKET_REDIS_* values
	fmt.Println("Loading .env file (if any)... scan_redis_52w")
	_ = godotenv.Load()

	cfg := config.Load()

	fmt.Println("[scan_redis_52w] Using Redis:", cfg.MarketRedisAddr)

	client, err := redispkg.New(redispkg.Config{
		Address:  cfg.MarketRedisAddr,
		Password: cfg.MarketRedisPassword,
		DB:       cfg.MarketRedisDB,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to Redis: %v", err))
	}
	defer client.Close()

	ctx := context.Background()

	fmt.Printf("[scan_redis_52w] Scanning for 52-week highs updated today (%s) with matching week_52_high_date\n", todayStr())
	fmt.Println("[scan_redis_52w] ================================================")

	total := 0
	for _, pattern := range []string{"market:nse:*", "market:bse:*"} {
		fmt.Printf("\n[scan_redis_52w] Scanning pattern: %s\n", pattern)
		c, err := scanPattern(ctx, client, pattern)
		if err != nil {
			fmt.Println("[ERROR]", err)
		}
		total += c
	}

	fmt.Println("\n[scan_redis_52w] ================================================")
	fmt.Printf("[scan_redis_52w] Total 52-week high matches for today (%s): %d\n", todayStr(), total)
}
