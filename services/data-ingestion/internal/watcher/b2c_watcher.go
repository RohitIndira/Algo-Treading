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
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/data-ingestion/internal/transformer"

	"go.uber.org/zap"
)

// B2CMarketData represents live market price data from B2C API bridge
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
	pub            publisher.Publisher
	lgr            *logger.Logger
	cmd            *exec.Cmd
	processedCount int64
	mu             sync.RWMutex
}

// NewB2CWatcher creates a new B2C watcher
func NewB2CWatcher(b2cBridgePath string, b2cTokens []string, pub publisher.Publisher, lgr *logger.Logger) (*B2CWatcher, error) {
	if b2cBridgePath == "" {
		return nil, fmt.Errorf("b2c bridge path is empty")
	}
	if pub == nil {
		return nil, fmt.Errorf("publisher is nil")
	}
	return &B2CWatcher{
		b2cBridgePath:  b2cBridgePath,
		b2cTokens:      b2cTokens,
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

	// Read and process market data
	scanner := bufio.NewScanner(stdout)
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
	// Build command with tokens as arguments
	args := w.b2cTokens
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

// logBridgeStderr logs stderr from B2C bridge
func (w *B2CWatcher) logBridgeStderr(stderr io.ReadCloser) {
	scanner := bufio.NewScanner(stderr)
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

	// Check timestamp is recent (not more than 1 minute old)
	now := time.Now().UnixMilli()
	if now-data.Timestamp > 60000 { // 60 seconds
		w.lgr.Warn("stale market data", zap.String("token", data.Token), zap.Int64("age_ms", now-data.Timestamp))
		return false
	}

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

	// Use token as key
	key := []byte(data.Token)

	// Publish with timeout
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := w.pub.Publish(pctx, key, payload); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	w.lgr.Debug("published market event to kafka",
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
