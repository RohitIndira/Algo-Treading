// package models

// // MarketDepthEvent represents real-time market depth data coming from
// // the `market.data.live` Kafka topic. This mirrors the payload published
// // by the B2C market data watcher.
// type JobbingMarketDepthEvent struct {
// 	EventID    string     `json:"event_id"`
// 	EventType  string     `json:"event_type"`
// 	Timestamp  string     `json:"timestamp"`
// 	StockData  StockData  `json:"stock_data"`
// 	MarketData MarketData `json:"market_data"`
// }

// // StockData contains basic stock identification information
// type JobbingStockData struct {
// 	StockCode   int    `json:"stock_code"`
// 	NSECode     int    `json:"nse_code"`
// 	BSECode     int    `json:"bse_code"`
// 	Exchange    string `json:"exchange"`
// 	Symbol      string `json:"symbol"`
// 	ISIN        string `json:"isin"`
// 	CompanyName string `json:"company_name"`
// }

// // MarketData contains real-time trading and depth information
// type JobbingMarketData struct {
// 	LastTradedPrice float64             `json:"last_traded_price"`
// 	PctChange       float64             `json:"pct_change"`
// 	PriceMap        PriceMap            `json:"price_map"`
// 	BidPrices       []float64           `json:"bid_prices"`
// 	BidQuantities   []int               `json:"bid_quantities"`
// 	AskPrices       []float64           `json:"ask_prices"`
// 	AskQuantities   []int               `json:"ask_quantities"`
// 	DepthMetrics    JobbingDepthMetrics `json:"depth_metrics"`
// }

// // PriceMap contains OHLCV data
// type JobbingPriceMap struct {
// 	Open   float64 `json:"open"`
// 	High   float64 `json:"high"`
// 	Low    float64 `json:"low"`
// 	Volume int     `json:"volume"`
// }

// // DepthMetrics contains calculated market depth metrics
// type JobbingDepthMetrics struct {
// 	SpreadPct       float64 `json:"spread_pct"`
// 	BidAskRatio     float64 `json:"bid_ask_ratio"`
// 	TotalBidQty     int     `json:"total_bid_qty"`
// 	TotalAskQty     int     `json:"total_ask_qty"`
// 	ImbalanceRatio  float64 `json:"imbalance_ratio"`
// 	LTPPositionType string  `json:"ltp_position_type"`
// }

// // Breakout52WEvent represents a 52-week high breakout signal coming from
// // the `market.data.52w_breakouts` Kafka topic. This mirrors the payload
// // published by the data-ingestion Redis watcher.
// // type Breakout52WEvent struct {
// // 	Symbol         string  `json:"symbol"`
// // 	Token          string  `json:"token"`
// // 	Exchange       string  `json:"exchange"`
// // 	LTP            float64 `json:"ltp"`
// // 	Week52High     float64 `json:"week_52_high"`
// // 	Week52Low      float64 `json:"week_52_low"`
// // 	Week52HighDate string  `json:"week_52_high_date"`
// // 	Timestamp      int64   `json:"timestamp"`
// // 	LastUpdated    string  `json:"last_updated"`
// // }

package models

// JobbingMarketDepthEvent represents real-time market depth data coming from
// the `market.data.live` Kafka topic. This mirrors the payload published
// by the B2C market data watcher.
type JobbingMarketDepthEvent struct {
	EventID    string            `json:"event_id"`
	EventType  string            `json:"event_type"`
	Timestamp  string            `json:"timestamp"`
	StockData  JobbingStockData  `json:"stock_data"`
	MarketData JobbingMarketData `json:"market_data"`
}

// JobbingStockData contains basic stock identification information
type JobbingStockData struct {
	StockCode   int    `json:"stock_code"`
	NSECode     int    `json:"nse_code"`
	BSECode     int    `json:"bse_code"`
	Exchange    string `json:"exchange"`
	Symbol      string `json:"symbol"`
	ISIN        string `json:"isin"`
	CompanyName string `json:"company_name"`
}

// JobbingMarketData contains real-time trading and depth information
type JobbingMarketData struct {
	LastTradedPrice float64             `json:"last_traded_price"`
	PctChange       float64             `json:"pct_change"`
	PriceMap        JobbingPriceMap     `json:"price_map"`
	BidPrices       []float64           `json:"bid_prices"`
	BidQuantities   []int               `json:"bid_quantities"`
	AskPrices       []float64           `json:"ask_prices"`
	AskQuantities   []int               `json:"ask_quantities"`
	DepthMetrics    JobbingDepthMetrics `json:"depth_metrics"`
}

// JobbingPriceMap contains OHLCV data
type JobbingPriceMap struct {
	Open   float64 `json:"open"`
	High   float64 `json:"high"`
	Low    float64 `json:"low"`
	Volume int     `json:"volume"`
}

// JobbingDepthMetrics contains calculated market depth metrics
type JobbingDepthMetrics struct {
	SpreadPct       float64 `json:"spread_pct"`
	BidAskRatio     float64 `json:"bid_ask_ratio"`
	TotalBidQty     int     `json:"total_bid_qty"`
	TotalAskQty     int     `json:"total_ask_qty"`
	ImbalanceRatio  float64 `json:"imbalance_ratio"`
	LTPPositionType string  `json:"ltp_position_type"`
}

// Breakout52WEvent represents a 52-week high breakout signal coming from
// the `market.data.52w_breakouts` Kafka topic. This mirrors the payload
// published by the data-ingestion Redis watcher.
// type Breakout52WEvent struct {
// 	Symbol         string  `json:"symbol"`
// 	Token          string  `json:"token"`
// 	Exchange       string  `json:"exchange"`
// 	LTP            float64 `json:"ltp"`
// 	Week52High     float64 `json:"week_52_high"`
// 	Week52Low      float64 `json:"week_52_low"`
// 	Week52HighDate string  `json:"week_52_high_date"`
// 	Timestamp      int64   `json:"timestamp"`
// 	LastUpdated    string  `json:"last_updated"`
// }
