package models

// CompanyDetails represents company information stored in Redis
type CompanyDetails struct {
	ISIN     string  `json:"isin"`
	BSECode  string  `json:"bsecode"`
	NSECode  string  `json:"nsecode"`
	MCap     float64 `json:"mcap"`
	MCapType string  `json:"mcaptype"`
	Exchange string  `json:"exchange"` // NSE, BSE, DELISTED, UNLISTED
}
