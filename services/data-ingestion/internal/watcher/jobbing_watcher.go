package watcher

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/transformer"

	"go.uber.org/zap"
)

// B2CMarketData represents live market price data from B2C API bridge This is the exact JSON structure the Python bridge prints per line
type B2CMarketData struct {
	Symbol        string    `json:"symbol"`
	Token         string    `json:"token"`
	LTP           float64   `json:"ltp"`
	High          float64   `json:"high"`
	Low           float64   `json:"low"`
	Open          float64   `json:"open"`
	Close         float64   `json:"close"`
	Volume        int64     `json:"volume"`
	Change        float64   `json:"change"`
	PrevClose     float64   `json:"prev_close"`
	Timestamp     int64     `json:"timestamp"`
	Week52High    float64   `json:"week_52_high"`
	Week52Low     float64   `json:"week_52_low"`
	AvgVolume5D   int64     `json:"avg_volume_5d"`
	BidPrices     []float64 `json:"bid_prices"`
	BidQuantities []int     `json:"bid_quantities"`
	AskPrices     []float64 `json:"ask_prices"`
	AskQuantities []int     `json:"ask_quantities"`
}

// B2CWatcher listens to B2C API bridge output and forwards market data to a publisher
type B2CWatcher struct {
	b2cBridgePath  string
	b2cTokens      []string
	stocksDBPath   string
	pub            publisher.Publisher
	lgr            *logger.Logger
	cmd            *exec.Cmd
	processedCount int64
	mu             sync.RWMutex
}

// NewB2CWatcher creates a new B2C watcher
func NewB2CWatcher(b2cBridgePath string, b2cTokens []string, stocksDBPath string, pub publisher.Publisher, lgr *logger.Logger) (*B2CWatcher, error) {
	if b2cBridgePath == "" {
		return nil, fmt.Errorf("b2c bridge path is empty")
	}
	if pub == nil {
		return nil, fmt.Errorf("publisher is nil")
	}
	return &B2CWatcher{
		b2cBridgePath:  b2cBridgePath,
		b2cTokens:      b2cTokens,
		stocksDBPath:   stocksDBPath,
		pub:            pub,
		lgr:            lgr,
		processedCount: 0,
	}, nil
}

// Run starts the B2C bridge and processes market data
func (w *B2CWatcher) Run(ctx context.Context) error {
	// Start B2C bridge process
	if err := w.startB2CBridge(ctx); err != nil {
		return fmt.Errorf("failed to start B2C bridge: %w", err)
	}
	defer w.stopB2CBridge()

	// Create stdout pipe to read data
	stdout, err := w.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdout pipe: %w", err)
	}

	// Start the process
	if err := w.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start B2C bridge process: %w", err)
	}

	w.lgr.Info("started B2C bridge process", zap.String("path", w.b2cBridgePath), zap.Strings("tokens", w.b2cTokens))

	// Read and process market data. We use a bufio.Scanner but explicitly
	// increase the maximum token size because B2C BESTFIVE payloads can be
	// large (deep bid/ask arrays). The default 64KB limit would cause
	// "bufio.Scanner: token too long" and stop the watcher silently.
	scanner := bufio.NewScanner(stdout)
	buf := make([]byte, 0, 64*1024)
	// Allow up to 5MB per line which is plenty for our JSON payloads.
	scanner.Buffer(buf, 5*1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			w.lgr.Info("shutting down B2C watcher", zap.Int64("processed_count", w.processedCount))
			return nil
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Parse market data
		var marketData B2CMarketData
		if err := json.Unmarshal(line, &marketData); err != nil {
			w.lgr.Error("failed to unmarshal market data", zap.Error(err), zap.ByteString("line", line))
			continue
		}

		// Validate market data
		if !w.validateMarketData(&marketData) {
			w.lgr.Warn("invalid market data", zap.String("token", marketData.Token))
			continue
		}

		// Publish to Kafka
		if err := w.publishMarketData(ctx, &marketData); err != nil {
			w.lgr.Error("failed to publish market data", zap.Error(err), zap.String("token", marketData.Token))
			continue
		}

		w.mu.Lock()
		w.processedCount++
		w.mu.Unlock()

		w.lgr.Debug("processed market data",
			zap.String("token", marketData.Token),
			zap.Float64("ltp", marketData.LTP),
			zap.Int64("timestamp", marketData.Timestamp),
		)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanner error: %w", err)
	}

	// Wait for process to exit
	if err := w.cmd.Wait(); err != nil {
		w.lgr.Error("B2C bridge process exited with error", zap.Error(err))
		return fmt.Errorf("B2C bridge process error: %w", err)
	}

	return nil
}

// startB2CBridge starts the B2C bridge Python process
func (w *B2CWatcher) startB2CBridge(ctx context.Context) error {
	// Determine subscription tokens. Prefer dynamic list from stocks.db if
	// configured; fall back to static B2C_TOKENS from environment.
	args := w.getSubscriptionTokens()
	if len(args) == 0 {
		// As an ultimate fallback, do not start the bridge without any tokens.
		return fmt.Errorf("no tokens available for B2C subscription (stocks.db and B2C_TOKENS both empty)")
	}

	w.cmd = exec.CommandContext(ctx, "python", append([]string{w.b2cBridgePath}, args...)...)

	// Redirect stderr to logger
	stderr, err := w.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to create stderr pipe: %w", err)
	}

	// Log stderr from bridge in a separate goroutine
	go w.logBridgeStderr(stderr)

	return nil
}

// stopB2CBridge stops the B2C bridge process
func (w *B2CWatcher) stopB2CBridge() {
	if w.cmd != nil && w.cmd.Process != nil {
		_ = w.cmd.Process.Kill()
	}
}

// getSubscriptionTokens builds the list of token arguments to pass to the
// Python B2C bridge. The bridge expects arguments in the form:
//
//	token            -> defaults to NSE
//	token:EXCHANGE   -> explicit exchange (NSE/BSE), used to derive market
//	                     segment IDs inside the Python script.
//
// We first try to read all ACTIVE rows from the stock_subscriptions table
// in the stocks.db SQLite database. If that succeeds and yields tokens,
// we return a list like ["476:NSE", "500410:BSE", ...]. If anything fails
// (missing sqlite3 binary, DB not found, query error, or empty result), we
// fall back to the static B2C_TOKENS list provided via environment.
func (w *B2CWatcher) getSubscriptionTokens() []string {
	// If no DB path is configured, just use static tokens.
	if strings.TrimSpace(w.stocksDBPath) == "" {
		w.lgr.Info("Stocks DB path not set; using static B2C_TOKENS from env",
			zap.Strings("tokens", w.b2cTokens))
		return w.b2cTokens
	}

	// Optional: cap the number of tokens we subscribe to from stocks.db.
	// This prevents trying to stream thousands of symbols at once and
	// overwhelming the B2C infra.
	maxTokensStr := os.Getenv("B2C_MAX_TOKENS")
	maxTokens := 0
	if maxTokensStr != "" {
		if v, err := strconv.Atoi(maxTokensStr); err == nil && v > 0 {
			maxTokens = v
		}
	}

	// Use sqlite3 CLI to query tokens and exchanges from stock_subscriptions.
	// We rely on the sqlite3 binary being available on the host (which is
	// already used in local tooling/scripts).
	sql := "SELECT token, exchange FROM stock_subscriptions WHERE status = 'ACTIVE'"
	if maxTokens > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, maxTokens)
		w.lgr.Info("Applying B2C_MAX_TOKENS limit for subscriptions",
			zap.Int("max_tokens", maxTokens))
	}

	cmd := exec.Command("sqlite3", "-separator", "|", w.stocksDBPath, sql)
	out, err := cmd.Output()
	if err != nil {
		w.lgr.Warn("failed to query stocks.db for subscriptions; falling back to static B2C_TOKENS",
			zap.String("db_path", w.stocksDBPath),
			zap.Error(err))
		return w.b2cTokens
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	var tokens []string
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.SplitN(line, "|", 2)
		token := strings.TrimSpace(parts[0])
		if token == "" {
			continue
		}
		exchange := "NSE"
		if len(parts) > 1 {
			exchange = strings.ToUpper(strings.TrimSpace(parts[1]))
			if exchange == "" {
				exchange = "NSE"
			}
		}

		tokens = append(tokens, fmt.Sprintf("%s:%s", token, exchange))
	}

	if len(tokens) == 0 {
		w.lgr.Warn("no ACTIVE stock_subscriptions found in stocks.db; using static B2C_TOKENS",
			zap.String("db_path", w.stocksDBPath))
		return w.b2cTokens
	}

	w.lgr.Info("Loaded subscription tokens from stocks.db",
		zap.String("db_path", w.stocksDBPath),
		zap.Int("count", len(tokens)))
	return tokens
}

// logBridgeStderr logs stderr from B2C bridge
func (w *B2CWatcher) logBridgeStderr(stderr io.ReadCloser) {
	scanner := bufio.NewScanner(stderr)
	buf := make([]byte, 0, 16*1024)
	// Stderr lines are usually small log messages, but in case of large
	// stack traces we still allow up to 1MB so we don't silently stop
	// consuming error logs.
	scanner.Buffer(buf, 1*1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		w.lgr.Info("B2C bridge log", zap.String("message", line))
	}
	if err := scanner.Err(); err != nil {
		w.lgr.Error("error reading B2C bridge stderr", zap.Error(err))
	}
}

// validateMarketData checks if market data is valid
func (w *B2CWatcher) validateMarketData(data *B2CMarketData) bool {
	// Check token is not empty
	if data.Token == "" {
		return false
	}

	// Check LTP is valid
	if data.LTP <= 0 {
		return false
	}

	// We intentionally do NOT reject ticks based on timestamp age anymore.
	// Some symbols are illiquid and may trade rarely; we still want to keep
	// their last known tick. Kafka compaction (by token key) ensures that for
	// each token only the most recent tick is retained in storage, and
	// downstream consumers naturally see the latest event.
	//
	// If you ever want to monitor extremely old data, you can add a log here:
	//   now := time.Now().UnixMilli()
	//   age := now - data.Timestamp
	//   if age > N { log.Warn(...) }
	// but without returning false.

	return true
}

// publishMarketData publishes market data to Kafka, transforming B2C format to MarketEvent
func (w *B2CWatcher) publishMarketData(ctx context.Context, data *B2CMarketData) error {
	// Transform B2C market data to MarketEvent with depth metrics
	// Extract stock code from token (token is typically stock code as string)
	stockCode := int64(0)
	if code, err := strconv.ParseInt(data.Token, 10, 64); err == nil {
		stockCode = code
	} else {
		w.lgr.Warn("failed to parse token as stock code, using 0", zap.String("token", data.Token))
		stockCode = 0
	}

	// Determine exchange from environment or default to NSE
	exchange := os.Getenv("DEFAULT_EXCHANGE")
	if exchange == "" {
		exchange = "NSE" // default to NSE
	}

	// Create transformer instance and transform data
	marketEvent, err := transformer.TransformB2CToMarketEvent(
		&transformer.B2CMarketData{
			Symbol:        data.Symbol,
			Token:         data.Token,
			LTP:           data.LTP,
			High:          data.High,
			Low:           data.Low,
			Open:          data.Open,
			Close:         data.Close,
			Volume:        data.Volume,
			Change:        data.Change,
			PrevClose:     data.PrevClose,
			Timestamp:     data.Timestamp,
			Week52High:    data.Week52High,
			Week52Low:     data.Week52Low,
			AvgVolume5D:   data.AvgVolume5D,
			BidPrices:     data.BidPrices,
			BidQuantities: data.BidQuantities,
			AskPrices:     data.AskPrices,
			AskQuantities: data.AskQuantities,
		},
		stockCode,
		exchange,
	)

	if err != nil {
		return fmt.Errorf("failed to transform market data: %w", err)
	}

	// Marshal transformed event to JSON
	payload, err := json.Marshal(marketEvent)
	if err != nil {
		return fmt.Errorf("failed to marshal market event: %w", err)
	}

	// Log the exact JSON payload that will be published for debugging
	// This helps verify what is being sent to Kafka via docker logs
	w.lgr.Info("B2C market event JSON prepared",
		zap.String("token", data.Token),
		zap.String("symbol", data.Symbol),
		zap.String("json", string(payload)),
	)

	// Use token as key
	key := []byte(data.Token)

	// Publish with timeout
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := w.pub.Publish(pctx, key, payload); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	w.lgr.Info("published B2C market event to kafka",
		zap.String("token", data.Token),
		zap.String("symbol", data.Symbol),
		zap.Float64("ltp", data.LTP),
		zap.Float64("spread_pct", marketEvent.MarketData.DepthMetrics.SpreadPct),
		zap.Float64("bid_ask_ratio", marketEvent.MarketData.DepthMetrics.BidAskRatio),
		zap.String("ltp_position", marketEvent.MarketData.DepthMetrics.LTPPositionType),
	)

	return nil
}

// GetProcessedCount returns the number of market data records processed
func (w *B2CWatcher) GetProcessedCount() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.processedCount
}
