package transformer

import (
	"fmt"
	"math"
	"time"
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

// DepthMetrics represents calculated market depth metrics
type DepthMetrics struct {
	SpreadPct       float64 `json:"spread_pct"`
	BidAskRatio     float64 `json:"bid_ask_ratio"`
	TotalBidQty     int64   `json:"total_bid_qty"`
	TotalAskQty     int64   `json:"total_ask_qty"`
	ImbalanceRatio  float64 `json:"imbalance_ratio"`   // (bid_qty - ask_qty) / (bid_qty + ask_qty)
	LTPPositionType string  `json:"ltp_position_type"` // "between_spread", "above_ask", "below_bid"
}

// MarketData represents market depth data
type MarketData struct {
	LastTradedPrice float64      `json:"last_traded_price"`
	PctChange       float64      `json:"pct_change"`
	PriceMap        PriceMap     `json:"price_map"`
	BidPrices       []float64    `json:"bid_prices"`
	BidQuantities   []int        `json:"bid_quantities"`
	AskPrices       []float64    `json:"ask_prices"`
	AskQuantities   []int        `json:"ask_quantities"`
	DepthMetrics    DepthMetrics `json:"depth_metrics"`
}

// PriceMap contains OHLCV data
type PriceMap struct {
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Volume int64   `json:"volume"`
}

// StockData contains stock information (matches rules-engine model)
type StockData struct {
	StockCode   int64  `json:"stock_code"`
	NSECode     int64  `json:"nse_code"`
	BSECode     int64  `json:"bse_code"`
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	ISIN        string `json:"isin"`
	CompanyName string `json:"company_name"`
}

// MarketEvent represents a market event from Kafka (market depth based)
type MarketEvent struct {
	EventID    string     `json:"event_id"`
	EventType  string     `json:"event_type"`
	Timestamp  time.Time  `json:"timestamp"`
	StockData  StockData  `json:"stock_data"`
	MarketData MarketData `json:"market_data"`
}

// TransformB2CToMarketEvent transforms B2C market data to MarketEvent with depth metrics
func TransformB2CToMarketEvent(b2cData *B2CMarketData, stockCode int64, exchange string) (*MarketEvent, error) {
	if b2cData == nil {
		return nil, fmt.Errorf("b2c market data is nil")
	}

	if stockCode <= 0 {
		return nil, fmt.Errorf("invalid stock code: %d", stockCode)
	}

	// Calculate depth metrics
	depthMetrics := calculateDepthMetrics(b2cData)

	// Create market event
	event := &MarketEvent{
		EventID:   fmt.Sprintf("%s_%d", b2cData.Token, b2cData.Timestamp),
		EventType: "market_depth",
		Timestamp: time.Unix(0, b2cData.Timestamp*int64(time.Millisecond)),
		StockData: StockData{
			StockCode: stockCode,
			Exchange:  exchange,
			Symbol:    b2cData.Symbol,
		},
		MarketData: MarketData{
			LastTradedPrice: b2cData.LTP,
			PctChange:       b2cData.Change,
			PriceMap: PriceMap{
				Open:   b2cData.Open,
				High:   b2cData.High,
				Low:    b2cData.Low,
				Volume: b2cData.Volume,
			},
			BidPrices:     b2cData.BidPrices,
			BidQuantities: b2cData.BidQuantities,
			AskPrices:     b2cData.AskPrices,
			AskQuantities: b2cData.AskQuantities,
			DepthMetrics:  depthMetrics,
		},
	}

	return event, nil
}

// calculateDepthMetrics calculates depth-related metrics from market data
func calculateDepthMetrics(b2cData *B2CMarketData) DepthMetrics {
	metrics := DepthMetrics{}

	// Extract best bid/ask
	bestBid := 0.0
	bestAsk := 0.0

	if len(b2cData.BidPrices) > 0 {
		bestBid = b2cData.BidPrices[0]
	}

	if len(b2cData.AskPrices) > 0 {
		bestAsk = b2cData.AskPrices[0]
	}

	// Calculate total bid/ask quantities
	metrics.TotalBidQty = sumInt(b2cData.BidQuantities)
	metrics.TotalAskQty = sumInt(b2cData.AskQuantities)

	// Calculate spread percentage
	if bestBid > 0 && bestAsk > 0 && b2cData.LTP > 0 {
		spread := bestAsk - bestBid
		metrics.SpreadPct = (spread / b2cData.LTP) * 100.0
	}

	// Calculate bid-ask ratio
	if metrics.TotalAskQty > 0 {
		metrics.BidAskRatio = float64(metrics.TotalBidQty) / float64(metrics.TotalAskQty)
	}

	// Calculate imbalance ratio
	totalDepth := metrics.TotalBidQty + metrics.TotalAskQty
	if totalDepth > 0 {
		imbalance := float64(metrics.TotalBidQty-metrics.TotalAskQty) / float64(totalDepth)
		metrics.ImbalanceRatio = imbalance
	}

	// Determine LTP position relative to spread
	ltp := b2cData.LTP
	if ltp > 0 && bestBid > 0 && bestAsk > 0 {
		if ltp >= bestBid && ltp <= bestAsk {
			metrics.LTPPositionType = "between_spread"
		} else if ltp > bestAsk {
			metrics.LTPPositionType = "above_ask"
		} else if ltp < bestBid {
			metrics.LTPPositionType = "below_bid"
		}
	}

	return metrics
}

// Helper functions

// sumInt calculates sum of integer slice
func sumInt(values []int) int64 {
	sum := int64(0)
	for _, v := range values {
		sum += int64(v)
	}
	return sum
}

// CalculateSpreadPct calculates spread percentage
func CalculateSpreadPct(bestBid, bestAsk, ltp float64) float64 {
	if bestBid <= 0 || bestAsk <= 0 || ltp <= 0 {
		return 0
	}
	spread := bestAsk - bestBid
	return (spread / ltp) * 100.0
}

// CalculateBidAskRatio calculates ratio of bid quantity to ask quantity
func CalculateBidAskRatio(bidQty, askQty int64) float64 {
	if askQty == 0 {
		return 0
	}
	return float64(bidQty) / float64(askQty)
}

// CalculateImbalanceRatio calculates order imbalance ratio
func CalculateImbalanceRatio(bidQty, askQty int64) float64 {
	total := bidQty + askQty
	if total == 0 {
		return 0
	}
	return float64(bidQty-askQty) / float64(total)
}

// DetermineLTPPosition determines if LTP is between spread or outside
func DetermineLTPPosition(ltp, bestBid, bestAsk float64) string {
	if ltp <= 0 || bestBid <= 0 || bestAsk <= 0 {
		return "unknown"
	}

	if math.IsNaN(ltp) || math.IsNaN(bestBid) || math.IsNaN(bestAsk) {
		return "unknown"
	}

	if ltp >= bestBid && ltp <= bestAsk {
		return "between_spread"
	} else if ltp > bestAsk {
		return "above_ask"
	} else if ltp < bestBid {
		return "below_bid"
	}
	return "unknown"
}
