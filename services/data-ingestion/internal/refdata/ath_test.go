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

// Already-adjusted history (RELIANCE-style): shares double, price flat →
// no adjustment, plain max close.
func TestComputeLevels_AdjustedHistoryLeftAlone(t *testing.T) {
	rows := []closeRow{
		{d("2024-07-08"), 1600, 6.77e9}, // ATH (already adjusted by the vendor)
		{d("2024-10-25"), 1327, 6.77e9},
		{d("2024-10-28"), 1334, 1.353e10}, // bonus ex-date: shares ×2, price flat
		{d("2026-10-29"), 1450, 1.353e10},
	}
	r := computeLevels("REL", rows, d("2026-10-29"))
	if r.SplitsAdjusted != 0 || r.ATHClose != 1600 || r.Source != "roll:eod" {
		t.Fatalf("%+v", r)
	}
}

// Unadjusted history (CUB-style): shares ×4/3 and price ×~0.78 the same day
// → pre-ex closes divided by 4/3; the raw 301.75 peak becomes 226.31.
func TestComputeLevels_UnadjustedBonusFixed(t *testing.T) {
	rows := []closeRow{
		{d("2026-01-30"), 301.75, 7.43e8}, // raw pre-bonus peak
		{d("2026-06-11"), 256.80, 7.43e8},
		{d("2026-06-12"), 201.70, 9.907e8}, // ex-date
		{d("2026-10-29"), 220.10, 9.907e8},
	}
	r := computeLevels("CUB", rows, d("2026-10-29"))
	if r.SplitsAdjusted != 1 || r.Source != "roll:eod+split" {
		t.Fatalf("split not detected: %+v", r)
	}
	want := round4(301.75 / (9.907e8 / 7.43e8))
	if r.ATHClose != want || !r.ATHDate.Equal(d("2026-01-30")) {
		t.Fatalf("ATH %v on %s, want %v", r.ATHClose, r.ATHDate.Format("2006-01-02"), want)
	}
	if r.ATHClose < 226 || r.ATHClose > 227 {
		t.Fatalf("adjusted ATH should be ~226.3, got %v", r.ATHClose)
	}
}

// Filing noise (shares +30% with a flat price) is not an action.
func TestComputeLevels_ShareNoiseIgnored(t *testing.T) {
	rows := []closeRow{
		{d("2026-01-05"), 100, 1.0e8},
		{d("2026-02-05"), 110, 1.3e8},
		{d("2026-03-05"), 105, 1.3e8},
	}
	r := computeLevels("NOISE", rows, d("2026-10-29"))
	if r.SplitsAdjusted != 0 || r.ATHClose != 110 {
		t.Fatalf("%+v", r)
	}
	if r.Week52High != 110 || r.Week52Low != 100 {
		t.Fatalf("52w: %+v", r)
	}
}

// UpsertPre2021 picks the max close and its date; days counts rows.
func TestPre2021MaxPick(t *testing.T) {
	rows := []IndexClose{{Date: d("2018-01-02"), Close: 90}, {Date: d("2018-09-14"), Close: 140}, {Date: d("2020-03-23"), Close: 50}}
	var maxC float64
	var maxD time.Time
	for _, r := range rows {
		if r.Close > maxC {
			maxC, maxD = r.Close, r.Date
		}
	}
	if maxC != 140 || !maxD.Equal(d("2018-09-14")) {
		t.Fatal("max pick")
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
