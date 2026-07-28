package handlers

import (
	"testing"

	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/dto"
)

// validateConditions is the single authority for cross-field condition rules and
// is called from both CreateStrategy and UpdateStrategy. The market cap range
// (₹ crore) is a STRICT bounded range: both zero means unset, otherwise a
// complete min<=max pair is required. A min with no max is a user error, not
// an open-ended "and above".
func TestValidateConditions_MarketCapRange(t *testing.T) {
	cases := []struct {
		name    string
		min     float64
		max     float64
		wantMsg string
	}{
		// Valid
		{"both zero means filter unset", 0, 0, ""},
		{"ordinary range", 1000, 10000, ""},
		{"zero min is a real lower bound", 0, 10000, ""},
		{"exact-cap range", 5000, 5000, ""},
		{"fractional bounds", 1000.5, 10000.25, ""},

		// Invalid
		{"max below min", 10000, 1000,
			"Maximum market cap must be greater than or equal to minimum market cap"},
		{"min set with no max", 1000, 0,
			"Maximum market cap is required when a minimum is set"},
		{"negative min", -1, 10000,
			"Market cap values cannot be negative"},
		{"negative max", 1000, -5,
			"Market cap values cannot be negative"},
		{"both negative", -1000, -10000,
			"Market cap values cannot be negative"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateConditions(&dto.StrategyConditions{
				MinMarketCap: tc.min,
				MaxMarketCap: tc.max,
			})
			if got != tc.wantMsg {
				t.Fatalf("min=%v max=%v: got %q, want %q", tc.min, tc.max, got, tc.wantMsg)
			}
		})
	}
}

// Negatives are checked before the min/max comparison so the user gets the
// specific message rather than a confusing ordering complaint.
func TestValidateConditions_NegativeTakesPrecedenceOverOrdering(t *testing.T) {
	got := validateConditions(&dto.StrategyConditions{MinMarketCap: 1000, MaxMarketCap: -5})
	if got != "Market cap values cannot be negative" {
		t.Fatalf("got %q, want the negative-value message", got)
	}
}

// A nil Conditions block is legal — update requests may omit it entirely.
func TestValidateConditions_NilIsValid(t *testing.T) {
	if got := validateConditions(nil); got != "" {
		t.Fatalf("nil conditions should be valid, got %q", got)
	}
}

// The trade value filter carries an explicit mode, so each mode requires exactly
// the bounds it uses. The unused bound is deliberately NOT rejected: switching
// RANGE → ABOVE must not force the user to clear a stale max first.
func TestValidateConditions_TradeValue(t *testing.T) {
	cases := []struct {
		name    string
		mode    string
		min     float64
		max     float64
		wantMsg string
	}{
		// Valid
		{"empty mode is off", "", 0, 0, ""},
		{"empty mode ignores stale bounds", "", 999, 1, ""},
		{"above with min", "ABOVE", 10, 0, ""},
		{"above ignores a stale max", "ABOVE", 10, 5, ""},
		{"below with max", "BELOW", 0, 5, ""},
		{"below ignores a stale min", "BELOW", 999, 5, ""},
		{"range with both", "RANGE", 10, 100, ""},
		{"range equal bounds", "RANGE", 10, 10, ""},
		{"fractional bounds", "RANGE", 0.5, 1.25, ""},

		// Invalid mode
		{"unknown mode", "GREATER_THAN", 10, 0,
			"Trade value mode must be one of: ABOVE, BELOW, RANGE"},
		{"lowercase mode is rejected", "above", 10, 0,
			"Trade value mode must be one of: ABOVE, BELOW, RANGE"},

		// Negatives
		{"negative min", "ABOVE", -1, 0, "Trade value cannot be negative"},
		{"negative max", "BELOW", 0, -1, "Trade value cannot be negative"},

		// Missing required bounds
		{"above without min", "ABOVE", 0, 100,
			"Minimum trade value is required when mode is ABOVE"},
		{"below without max", "BELOW", 10, 0,
			"Maximum trade value is required when mode is BELOW"},
		{"range without min", "RANGE", 0, 100,
			"Both minimum and maximum trade value are required when mode is RANGE"},
		{"range without max", "RANGE", 10, 0,
			"Both minimum and maximum trade value are required when mode is RANGE"},

		// Ordering
		{"range max below min", "RANGE", 100, 10,
			"Maximum trade value must be greater than or equal to minimum trade value"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := validateConditions(&dto.StrategyConditions{
				TradeValueMode: tc.mode,
				MinTradeValue:  tc.min,
				MaxTradeValue:  tc.max,
			})
			if got != tc.wantMsg {
				t.Fatalf("mode=%q min=%v max=%v: got %q, want %q",
					tc.mode, tc.min, tc.max, got, tc.wantMsg)
			}
		})
	}
}

// Market cap is checked before trade value, so a request invalid in both ways
// reports the market cap problem first. Pinned so the order stays deterministic.
func TestValidateConditions_MarketCapCheckedBeforeTradeValue(t *testing.T) {
	got := validateConditions(&dto.StrategyConditions{
		MinMarketCap:   10000,
		MaxMarketCap:   1000, // invalid
		TradeValueMode: "BOGUS",
	})
	want := "Maximum market cap must be greater than or equal to minimum market cap"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// Conditions unrelated to market cap must not be rejected by this helper.
func TestValidateConditions_IgnoresUnrelatedFields(t *testing.T) {
	got := validateConditions(&dto.StrategyConditions{
		MatchAllNews:      true,
		ImpactScoreMin:    1,
		ImpactScoreMax:    10,
		Sentiments:        []string{"POSITIVE"},
		MarketCapTypes:    []string{"MID"},
		MinPriceChangePct: 5,
		MaxPriceChangePct: 1, // intentionally out of order; not this helper's concern
		Exchanges:         []string{"NSE"},
	})
	if got != "" {
		t.Fatalf("expected valid, got %q", got)
	}
}
