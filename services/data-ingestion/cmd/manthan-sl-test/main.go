// End-to-end SL + TSL test via production services.
//
// Publishes ONE synthetic Manthan signal to Kafka `manthan.signals` for a
// user-chosen stock. Rules-engine picks it up → runs allocator for every
// active strategy → publishes MANTHAN_ENTRY to `trade-signals` → trade-execution
// places the broker order → SL placed on fill → rules-engine trails SL as LTP
// ticks on external Redis. You watch the full flow in the service logs and on
// the Manthan dashboard.
//
// Usage:
//
//	EXT_REDIS_ADDR=15.207.203.46:6379 EXT_REDIS_PASSWORD='R3d1s@Prod#2026' \
//	KAFKA_BROKERS=localhost:9092 \
//	SYMBOL=IDEA ISIN=INE669E01016 \
//	go run ./services/data-ingestion/cmd/manthan-sl-test/
//
// Required env:
//
//	SYMBOL                 NSE trading symbol (e.g. IDEA, RVNL)
//	ISIN                   ISIN of the same stock — required for rules-engine
//	                       LTP resolution (ISIN → token → market:nse:{token})
//
// Optional env (sensible defaults so synthetic signal passes all filters):
//
//	INDEX_NAME             default NIFTY50 (so EMA allocation is non-zero)
//	MCAP_BUCKET            LARGE | MID | SMALL (default LARGE)
//	INDUSTRY               default "Test Industry"
//	MCAP                   market cap in Cr, default 1000 (passes 500–150000)
//	PE                     default 30 (passes ≤60)
//	FSCORE                 default 65 (passes ≥60)
//	PAT                    default 100 (passes >0)
//	LATEST_PRICE           overrides live LTP (otherwise fetched from ext Redis)
//	ATH_CLOSE / WEEK52_HIGH default: LATEST_PRICE × 1.1 (not critical for test)
//	KAFKA_BROKERS          default localhost:9092
//	KAFKA_TOPIC            default manthan.signals
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
)

type manthanSignal struct {
	RunDate     string  `json:"run_date"`
	Symbol      string  `json:"symbol"`
	ISIN        string  `json:"isin"`
	Industry    string  `json:"industry"`
	MCapBucket  string  `json:"mcap_bucket"`
	IndexName   string  `json:"index_name"`
	MarketCap   float64 `json:"market_cap"`
	PE          float64 `json:"pe"`
	FScore      float64 `json:"fscore"`
	PAT         float64 `json:"pat"`
	LatestPrice float64 `json:"latest_price"`
	ATHClose    float64 `json:"ath_close"`
	Week52High  float64 `json:"week52_high"`
	EmittedAt   string  `json:"emitted_at"`
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

// fetchLiveLTP resolves ISIN → NSE token via external Redis, then reads
// market:nse:{token} for the current LTP. Returns 0 if resolution fails.
func fetchLiveLTP(ctx context.Context, rdb *redis.Client, isin string) float64 {
	raw, err := rdb.Get(ctx, "isin:"+isin).Result()
	if err != nil {
		return 0
	}
	var id struct {
		NSECode string `json:"nsecode"`
	}
	if json.Unmarshal([]byte(raw), &id) != nil || id.NSECode == "" {
		return 0
	}
	raw2, err := rdb.Get(ctx, "market:nse:"+id.NSECode).Result()
	if err != nil {
		return 0
	}
	var mk struct {
		LTP float64 `json:"ltp"`
	}
	if json.Unmarshal([]byte(raw2), &mk) != nil {
		return 0
	}
	return mk.LTP
}

func main() {
	symbol := strings.ToUpper(strings.TrimSpace(os.Getenv("SYMBOL")))
	isin := strings.ToUpper(strings.TrimSpace(os.Getenv("ISIN")))
	if symbol == "" || isin == "" {
		fmt.Fprintln(os.Stderr, "❌ SYMBOL and ISIN are required env vars.")
		fmt.Fprintln(os.Stderr, "   Example: SYMBOL=IDEA ISIN=INE669E01016")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Optional LTP override from env; else try external Redis.
	ltp := envFloat("LATEST_PRICE", 0)
	if ltp <= 0 {
		extAddr := os.Getenv("EXT_REDIS_ADDR")
		if extAddr == "" {
			fmt.Fprintln(os.Stderr, "❌ LATEST_PRICE not set and EXT_REDIS_ADDR empty — can't resolve LTP.")
			os.Exit(1)
		}
		rdb := redis.NewClient(&redis.Options{
			Addr:     extAddr,
			Password: os.Getenv("EXT_REDIS_PASSWORD"),
		})
		defer rdb.Close()
		if err := rdb.Ping(ctx).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "❌ External Redis ping failed: %v\n", err)
			os.Exit(1)
		}
		ltp = fetchLiveLTP(ctx, rdb, isin)
		if ltp <= 0 {
			fmt.Fprintf(os.Stderr, "❌ Couldn't resolve live LTP for ISIN=%s via ext Redis.\n", isin)
			fmt.Fprintln(os.Stderr, "   Either set LATEST_PRICE manually or check that the ISIN is correct.")
			os.Exit(1)
		}
	}

	athClose := envFloat("ATH_CLOSE", ltp*1.10)
	week52 := envFloat("WEEK52_HIGH", athClose)

	sig := manthanSignal{
		RunDate:     time.Now().Format("2006-01-02"),
		Symbol:      symbol,
		ISIN:        isin,
		Industry:    envOr("INDUSTRY", "Test Industry"),
		MCapBucket:  envOr("MCAP_BUCKET", "LARGE"),
		IndexName:   envOr("INDEX_NAME", "NIFTY50"),
		MarketCap:   envFloat("MCAP", 1000),
		PE:          envFloat("PE", 30),
		FScore:      envFloat("FSCORE", 65),
		PAT:         envFloat("PAT", 100),
		LatestPrice: ltp,
		ATHClose:    athClose,
		Week52High:  week52,
		EmittedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	body, err := json.MarshalIndent(sig, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ marshal failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("=== SIGNAL PAYLOAD ===")
	fmt.Println(string(body))
	fmt.Println()

	// Kafka publish
	brokers := strings.Split(envOr("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := envOr("KAFKA_TOPIC", "manthan.signals")

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireAll,
	}
	defer w.Close()

	compact, _ := json.Marshal(sig)
	pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pubCancel()
	if err := w.WriteMessages(pubCtx, kafka.Message{
		Key:   []byte(symbol),
		Value: compact,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Kafka publish failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Signal published to Kafka  topic=%s  brokers=%v\n", topic, brokers)
	fmt.Println()
	fmt.Println("=== WHAT TO WATCH ===")
	fmt.Println()
	fmt.Println("1. Rules-engine terminal — within 1-2s you should see:")
	fmt.Printf("     Using live LTP for allocation  symbol=%s  live_ltp=%.2f\n", symbol, ltp)
	fmt.Printf("     Manthan entry order published  symbol=%s  qty=N  price=%.2f  sl=%.2f\n", symbol, ltp, ltp*0.98)
	fmt.Println()
	fmt.Println("     If you see 'Signal skipped' — check reason (qty=0 / sector cap / etc).")
	fmt.Println()
	fmt.Println("2. Trade-execution terminal:")
	fmt.Println("     Manthan order received  order_type=MANTHAN_ENTRY")
	fmt.Println("     [indira] → POST /order-services/api/order/v1/place-order ...")
	fmt.Println("     LIMIT BUY placed  broker_order_id=...")
	fmt.Println("     (on fill) LIVE BUY filled  avg_price=...")
	fmt.Println("     (SL then placed automatically at avg_price × 0.98 — TEST MODE)")
	fmt.Println()
	fmt.Println("3. As LTP ticks up on external Redis:")
	fmt.Println("     Rules-engine LTPFeed polls → when price > last_trail × 1.02:")
	fmt.Println("       Trailing SL updated  old_sl=X  new_sl=Y  high=Z")
	fmt.Println("     Rules-engine publishes MANTHAN_SL_MODIFY → trade-execution modifies broker SL.")
	fmt.Println()
	fmt.Println("4. Dashboard: http://localhost:3100/stockkask/algo-trading/manthan")
	fmt.Printf("     %s should appear in Positions with live LTP pulse + updated SL.\n", symbol)
	fmt.Println()
	fmt.Println("Prerequisites:")
	fmt.Println("  - An ACTIVE Manthan strategy with total_capital ≥ (stock_price × ~2) must exist.")
	fmt.Println("  - rules-engine + trade-execution + api-gateway must be running.")
	fmt.Println("  - Indira account must have sufficient margin for 1 share.")
}
