package refdata

import (
	"strings"
	"testing"
	"time"
)

func d(s string) time.Time {
	t, _ := time.Parse("2006-01-02", s)
	return t
}

// A 1:10 split: pre-split closes are divided by 10 before taking the max,
// so the ATH is the post-split peak, not the unreachable pre-split print.
func TestComputeLevels_SplitAdjusted(t *testing.T) {
	asOf := d("2026-10-29")
	rows := []closeRow{
		{d("2024-01-02"), 2000, 1e7}, // pre-split: 200 adjusted
		{d("2024-06-03"), 2500, 1e7}, // pre-split peak: 250 adjusted
		{d("2024-06-04"), 255, 1e8},  // ex-date: shares ×10
		{d("2025-03-10"), 300, 1e8},  // true ATH close
		{d("2026-10-29"), 280, 1e8},
	}
	r := computeLevels("SPLT", rows, asOf)
	if r.SplitsDetected != 1 || r.Source != "roll:eod+split" {
		t.Fatalf("split detection: %+v", r)
	}
	if r.ATHClose != 300 || !r.ATHDate.Equal(d("2025-03-10")) {
		t.Fatalf("ATH must be post-split peak 300 on 2025-03-10: %+v", r)
	}
	// 52w window (asOf−365d = 2025-10-29) holds only the last close.
	if r.Week52High != 280 || r.Week52Low != 280 {
		t.Fatalf("52w: %+v", r)
	}
}

// Small share-count moves (QIP, ESOP) are not splits.
func TestComputeLevels_AllotmentIgnored(t *testing.T) {
	rows := []closeRow{
		{d("2026-01-05"), 100, 1.00e8},
		{d("2026-02-05"), 110, 1.05e8}, // +5%: allotment
		{d("2026-03-05"), 105, 1.05e8},
	}
	r := computeLevels("QIP", rows, d("2026-10-29"))
	if r.SplitsDetected != 0 || r.ATHClose != 110 || r.Source != "roll:eod" {
		t.Fatalf("%+v", r)
	}
	if r.Week52High != 110 || r.Week52Low != 100 {
		t.Fatalf("52w: %+v", r)
	}
}

func TestParseNSEIndexCSV(t *testing.T) {
	csvText := `Index Name,Index Date,Open Index Value,High Index Value,Low Index Value,Closing Index Value,Points Change,Change(%),Volume,Turnover (Rs. Cr.),P/E,P/B,Div Yield
Nifty 50,29-10-2026,22690.65,22961.95,22656.75,22936.75,237.9,1.05,344482095,29658.93,19.19,2.82,1.42
Nifty 50,29-10-2026,22690.65,22961.95,22656.75,22936.75,237.9,1.05,344482095,29658.93,19.19,2.82,1.42
Nifty Next 50,29-10-2026,1,2,3,67000.1,0,0,0,0,0,0,0
Nifty Smallcap 250,29-10-2026,17007.35,17120.05,16906.3,17101.1,111.7,.66,896501506,21340.15,32.06,3.29,.76
Nifty Midcap 150,29-10-2026,21084.65,21245.35,21048.05,21245.3,180.5,.86,566947329,18437.86,26.79,3.72,.99
`
	rows, err := parseNSEIndexCSV(strings.NewReader(csvText), d("2026-10-29"))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, r := range rows {
		got[r.Name] = r.Close
		if !r.Date.Equal(d("2026-10-29")) {
			t.Fatalf("date parsed from file: %+v", r)
		}
	}
	if len(rows) != 3 || got["NIFTY50"] != 22936.75 || got["NFTYMCP150"] != 21245.3 || got["NTYSLCP250"] != 17101.1 {
		t.Fatalf("rows: %v", got)
	}

	// A file missing one bucket index is an error naming it.
	_, err = parseNSEIndexCSV(strings.NewReader(strings.Replace(csvText, "Nifty Midcap 150", "Nifty Midcap 100", 1)), d("2026-10-29"))
	if err == nil || !strings.Contains(err.Error(), "Nifty Midcap 150") {
		t.Fatalf("want missing-index error, got %v", err)
	}
}
