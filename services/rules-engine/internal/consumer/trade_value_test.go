package consumer

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// The ACC row from the live Redis feed, used as the reference case throughout:
//
//	volume 167223 × ltp 1359.1 = ₹227,272,779.3 → 22.72727793 Cr
const (
	accVolume = int64(167223)
	accLTP    = 1359.1
	accCr     = 22.72727793
)

func TestTradeValueCr(t *testing.T) {
	cases := []struct {
		name   string
		volume int64
		ltp    float64
		want   float64
	}{
		{"ACC reference row", accVolume, accLTP, accCr},
		{"exactly one crore", 10000, 1000, 1},
		{"zero volume", 0, 1359.1, 0},
		{"zero ltp", 167223, 0, 0},
		{"both zero", 0, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := TradeValueCr(tc.volume, tc.ltp)
			if math.Abs(got-tc.want) > 1e-6 {
				t.Fatalf("TradeValueCr(%d, %v) = %v, want %v", tc.volume, tc.ltp, got, tc.want)
			}
		})
	}
}

// Every mode against the boundary and either side of it. The filter is enforced
// at three call sites (live handler, AMN preview, AMN backfill) through this one
// helper, so these cases define the behaviour for all of them.
func TestPassesTradeValue(t *testing.T) {
	// A volume that yields exactly 20 Cr at ltp 1000, for clean boundary cases.
	const volFor20Cr = int64(200000)
	const ltp = 1000.0

	cases := []struct {
		name     string
		mode     string
		min, max float64
		volume   int64
		ltp      float64
		want     bool
	}{
		// Filter off — bounds are irrelevant, even nonsensical ones.
		{"off ignores bounds", models.TradeValueModeOff, 999, 1, volFor20Cr, ltp, true},
		{"off with zero volume", models.TradeValueModeOff, 0, 0, 0, ltp, true},

		// ABOVE
		{"above: comfortably over", models.TradeValueModeAbove, 10, 0, volFor20Cr, ltp, true},
		{"above: exactly at bound (inclusive)", models.TradeValueModeAbove, 20, 0, volFor20Cr, ltp, true},
		{"above: just under", models.TradeValueModeAbove, 20.01, 0, volFor20Cr, ltp, false},
		{"above: ACC passes 10Cr floor", models.TradeValueModeAbove, 10, 0, accVolume, accLTP, true},
		{"above: ACC fails 50Cr floor", models.TradeValueModeAbove, 50, 0, accVolume, accLTP, false},
		{"above: zero volume fails the floor", models.TradeValueModeAbove, 10, 0, 0, ltp, false},

		// BELOW
		{"below: comfortably under", models.TradeValueModeBelow, 0, 50, volFor20Cr, ltp, true},
		{"below: exactly at bound (inclusive)", models.TradeValueModeBelow, 0, 20, volFor20Cr, ltp, true},
		{"below: just over", models.TradeValueModeBelow, 0, 19.99, volFor20Cr, ltp, false},
		{"below: ACC fails 10Cr ceiling", models.TradeValueModeBelow, 0, 10, accVolume, accLTP, false},
		{"below: zero volume passes a ceiling", models.TradeValueModeBelow, 0, 10, 0, ltp, true},

		// RANGE
		{"range: inside", models.TradeValueModeRange, 10, 100, volFor20Cr, ltp, true},
		{"range: at lower bound", models.TradeValueModeRange, 20, 100, volFor20Cr, ltp, true},
		{"range: at upper bound", models.TradeValueModeRange, 1, 20, volFor20Cr, ltp, true},
		{"range: below lower", models.TradeValueModeRange, 25, 100, volFor20Cr, ltp, false},
		{"range: above upper", models.TradeValueModeRange, 1, 19, volFor20Cr, ltp, false},
		{"range: ACC inside 10-100Cr", models.TradeValueModeRange, 10, 100, accVolume, accLTP, true},
		{"range: zero volume fails", models.TradeValueModeRange, 10, 100, 0, ltp, false},

		// Unknown mode must fail OPEN — a typo or a newer producer's mode must not
		// silently block every trade for the strategy.
		{"unknown mode fails open", "GREATER_THAN", 999, 1, volFor20Cr, ltp, true},
		{"lowercase mode is unknown, fails open", "above", 999, 0, volFor20Cr, ltp, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := models.TradeValueFilter{Mode: tc.mode, Min: tc.min, Max: tc.max}
			got, tv := PassesTradeValue(f, tc.volume, tc.ltp)
			if got != tc.want {
				t.Fatalf("mode=%q min=%v max=%v vol=%d ltp=%v → tradeValue=%.4f Cr, pass=%v, want %v",
					tc.mode, tc.min, tc.max, tc.volume, tc.ltp, tv, got, tc.want)
			}
			// The returned value must always be the real trade value, even when the
			// filter is off or unknown — call sites log it.
			if want := TradeValueCr(tc.volume, tc.ltp); math.Abs(tv-want) > 1e-9 {
				t.Errorf("returned tradeValue = %v, want %v", tv, want)
			}
		})
	}
}

func TestIsKnownTradeValueMode(t *testing.T) {
	known := []string{
		models.TradeValueModeOff,
		models.TradeValueModeAbove,
		models.TradeValueModeBelow,
		models.TradeValueModeRange,
	}
	for _, m := range known {
		if !IsKnownTradeValueMode(m) {
			t.Errorf("mode %q should be known", m)
		}
	}
	for _, m := range []string{"above", "GT", "BETWEEN", "OFF", "1"} {
		if IsKnownTradeValueMode(m) {
			t.Errorf("mode %q should NOT be known", m)
		}
	}
}

// IsActive gates whether call sites invoke the filter at all.
func TestTradeValueFilter_IsActive(t *testing.T) {
	if (models.TradeValueFilter{}).IsActive() {
		t.Error("zero-value filter must be inactive")
	}
	if (models.TradeValueFilter{Min: 10, Max: 20}).IsActive() {
		t.Error("bounds without a mode must stay inactive — mode is the only switch")
	}
	if !(models.TradeValueFilter{Mode: models.TradeValueModeAbove, Min: 10}).IsActive() {
		t.Error("ABOVE must be active")
	}
}

// volume must survive the Redis decode. The key name is the only thing tying
// MarketDataResult to the feed's payload, so pin it with a real message body
// rather than constructing the struct directly.
func TestMarketDataRedisDecode_Volume(t *testing.T) {
	raw := `{"symbol":"ACC","token":"22","exchange":"NSE","ltp":1359.1,"open":1343,` +
		`"high":1360.5,"low":1332,"close":1340.9,"prev_close":1340.9,"volume":167223,` +
		`"percent_change":1.3572973376090547,"avg_volume_5d":167223,` +
		`"tick_size":0.1,"dpr_lower":1072.8,"dpr_upper":1609}`

	// Mirrors the anonymous struct in getMarketDataFromRedis.
	var got struct {
		LTP           float64 `json:"ltp"`
		PrevClose     float64 `json:"prev_close"`
		TickSize      float64 `json:"tick_size"`
		PercentChange float64 `json:"percent_change"`
		Volume        int64   `json:"volume"`
		DPRLower      float64 `json:"dpr_lower"`
		DPRUpper      float64 `json:"dpr_upper"`
	}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("unmarshal market data: %v", err)
	}

	if got.Volume != accVolume {
		t.Fatalf("volume = %d, want %d — check the \"volume\" JSON key against the feed payload",
			got.Volume, accVolume)
	}
	if got.LTP != accLTP {
		t.Fatalf("ltp = %v, want %v", got.LTP, accLTP)
	}

	// And the pair must produce the documented trade value.
	if tv := TradeValueCr(got.Volume, got.LTP); math.Abs(tv-accCr) > 1e-6 {
		t.Fatalf("trade value = %v Cr, want %v Cr", tv, accCr)
	}
}
