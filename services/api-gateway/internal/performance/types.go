// Package performance owns the "algo performance data" concept for the
// api-gateway. The Performance tab on the Manthan Strategy detail
// screen (`GET /api/v1/algos/{id}/performance`) is served entirely out
// of this package.
//
// Data source: two tables in the stockk_market Postgres database,
// populated by a separate ETL job (services/perf-etl/, currently the
// Python one-off at /tmp/perf_etl.py):
//
//   algo_performance_daily  — sourced from the "Date wise data" tab
//                             of the Manthan 2.0 Google Sheet, filtered
//                             to reference client A844
//   benchmark_daily         — sourced from Yahoo Finance ^NSEI daily
//                             close values
//
// The types in this file describe the SHAPE of the JSON response —
// they are what the mobile app sees. See handlers/performance_handler.go
// for how these get assembled + returned.
package performance

// Response is the full "data" payload for GET /api/v1/algos/{id}/performance.
// The envelope (infoID, infoMsg, timestamp) is added by the HTTP handler.
type Response struct {
	AlgoID            string         `json:"algoId"`
	ReferenceClientID string         `json:"referenceClientId"`
	AsOf              string         `json:"asOf"`        // ISO date of most recent row
	CapitalBase       int64          `json:"capitalBase"` // rupees — matches the reference client's actual investment
	VsBenchmark       VsBenchmark    `json:"vsBenchmark"`
	Chart             Chart          `json:"chart"`
	Returns           Returns        `json:"returns"`
	DailyPnL          []DailyPoint   `json:"dailyPnL"`
	MonthlyPnL        []MonthlyPoint `json:"monthlyPnL"`
	Disclaimer        string         `json:"disclaimer"`
}

// VsBenchmark is the small "Manthan +X.X% vs NIFTY 50 +Y.Y%" comparison
// shown above the chart. Uses the most recent day's percentages.
type VsBenchmark struct {
	Algo      Series `json:"algo"`
	Benchmark Series `json:"benchmark"`
}

// Series is one line on the chart / one label in the vs-benchmark row.
type Series struct {
	Label     string  `json:"label"`
	ReturnPct float64 `json:"returnPct"`
}

// Chart is the "Manthan Vs Benchmark" line chart data. Each ChartPoint
// carries the algo's and benchmark's return-since-start values indexed
// at 100 (so 108.5 = +8.5% since start of the series).
type Chart struct {
	Points []ChartPoint `json:"points"`
}

// ChartPoint is one day on the line chart. Nifty is nil-able because
// the Nifty series may not extend as far back as the algo series (or
// vice versa). Frontend renders whichever line has a value per date.
type ChartPoint struct {
	Date         string   `json:"date"`                   // ISO YYYY-MM-DD
	AlgoPct      *float64 `json:"algoPct,omitempty"`      // indexed at 100
	BenchmarkPct *float64 `json:"benchmarkPct,omitempty"` // indexed at 100
}

// Returns is the 4-tile summary row at the bottom of the screen —
// 1M / 6M / 1Y / Since Deployment. Each entry has both the absolute
// rupee P&L and the percentage.
type Returns struct {
	Month1          ReturnEntry `json:"1M"`
	Month6          ReturnEntry `json:"6M"`
	Year1           ReturnEntry `json:"1Y"`
	SinceDeployment ReturnEntry `json:"sinceDeployment"`
}

// ReturnEntry is one tile — "1M RETURN: ₹1,05,300 (-3.2%)".
type ReturnEntry struct {
	Amount  int64   `json:"amount"`  // rupees, can be negative
	Percent float64 `json:"percent"` // e.g. -3.2
}

// DailyPoint is one cell in the "Day wise P&L" heat-map calendar.
type DailyPoint struct {
	Date    string  `json:"date"`    // ISO YYYY-MM-DD
	Amount  int64   `json:"amount"`  // rupees for that day
	Percent float64 `json:"percent"` // % that day
}

// MonthlyPoint is one cell in the "Month wise P&L" calendar view.
type MonthlyPoint struct {
	Year    int     `json:"year"`
	Month   int     `json:"month"` // 1..12
	Amount  int64   `json:"amount"`
	Percent float64 `json:"percent"`
}
