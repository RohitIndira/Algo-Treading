package configsync

import (
	"encoding/json"
	"strconv"
	"testing"
)

// The market cap range crosses the service boundary as raw JSON on the config
// topic: user-config's events.ConditionsPayload → this package's
// ConditionsPayload → models.Conditions.MarketCapRange. Nothing but the JSON key
// names ties those two structs together, so a rename on either side would
// silently drop the filter and every strategy would quietly stop filtering by
// market cap. This test pins the wire format by decoding a literal payload
// rather than constructing the Go struct directly.
func TestToModelStrategy_MarketCapRangeFromKafkaJSON(t *testing.T) {
	raw := []byte(`{
		"strategy_id": "s1",
		"user_id": "u1",
		"strategy_name": "mid-cap band",
		"active": true,
		"version": 3,
		"conditions": {
			"match_all_news": false,
			"impact_score_min": 5,
			"impact_score_max": 10,
			"sentiments": ["POSITIVE"],
			"categories": ["Order Wins"],
			"market_cap_types": ["MID"],
			"min_market_cap": 1000,
			"max_market_cap": 10000,
			"min_price_change_pct": 0,
			"max_price_change_pct": 0,
			"trade_value_mode": "RANGE",
			"min_trade_value": 10,
			"max_trade_value": 100,
			"exchanges": ["NSE"]
		},
		"trade_config": {"order_type": "MARKET", "quantity": 1},
		"risk_limits": {}
	}`)

	var p StrategyPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal config event: %v", err)
	}

	// Guard the decode itself — if the JSON keys ever drift, these are 0 and the
	// mapper assertions below would pass against a silently empty filter.
	if p.Conditions.MinMarketCap != 1000 || p.Conditions.MaxMarketCap != 10000 {
		t.Fatalf("payload decode: got min=%v max=%v, want 1000/10000 — check the "+
			"min_market_cap/max_market_cap JSON keys against user-config's ConditionsPayload",
			p.Conditions.MinMarketCap, p.Conditions.MaxMarketCap)
	}

	s, err := ToModelStrategy(&p)
	if err != nil {
		t.Fatalf("ToModelStrategy: %v", err)
	}

	if s.Conditions.MarketCapRange.MinMcap != 1000 {
		t.Errorf("MinMcap = %v, want 1000", s.Conditions.MarketCapRange.MinMcap)
	}
	if s.Conditions.MarketCapRange.MaxMcap != 10000 {
		t.Errorf("MaxMcap = %v, want 10000", s.Conditions.MarketCapRange.MaxMcap)
	}

	// The bucket-type filter is independent and must survive alongside the range.
	if len(s.Conditions.MarketCapTypes) != 1 || s.Conditions.MarketCapTypes[0] != "MID" {
		t.Errorf("MarketCapTypes = %v, want [MID]", s.Conditions.MarketCapTypes)
	}
}

// Same rationale as the market-cap test: the trade value filter crosses the
// service boundary as three JSON keys with nothing but their names holding the
// contract together. A rename would silently disable the liquidity filter, and
// because it fails OPEN on an unknown mode the strategy would start trading
// illiquid stocks rather than visibly breaking.
func TestToModelStrategy_TradeValueFilterFromKafkaJSON(t *testing.T) {
	cases := []struct {
		name     string
		mode     string
		min, max float64
	}{
		{"range", "RANGE", 10, 100},
		{"above", "ABOVE", 25, 0},
		{"below", "BELOW", 0, 5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(`{
				"strategy_id": "s1",
				"user_id": "u1",
				"conditions": {
					"impact_score_min": 1,
					"impact_score_max": 10,
					"trade_value_mode": "` + tc.mode + `",
					"min_trade_value": ` + jsonNum(tc.min) + `,
					"max_trade_value": ` + jsonNum(tc.max) + `
				},
				"trade_config": {"order_type": "MARKET", "quantity": 1},
				"risk_limits": {}
			}`)

			var p StrategyPayload
			if err := json.Unmarshal(raw, &p); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			// Guard the decode so the mapper assertions cannot pass against an
			// empty filter caused by drifted JSON keys.
			if p.Conditions.TradeValueMode != tc.mode {
				t.Fatalf("payload decode: mode = %q, want %q — check the trade_value_mode "+
					"key against user-config's ConditionsPayload", p.Conditions.TradeValueMode, tc.mode)
			}

			s, err := ToModelStrategy(&p)
			if err != nil {
				t.Fatalf("ToModelStrategy: %v", err)
			}

			got := s.Conditions.TradeValueFilter
			if got.Mode != tc.mode || got.Min != tc.min || got.Max != tc.max {
				t.Fatalf("TradeValueFilter = %+v, want {Mode:%s Min:%v Max:%v}",
					got, tc.mode, tc.min, tc.max)
			}
			if !got.IsActive() {
				t.Error("filter with a mode set must report IsActive")
			}
		})
	}
}

// An absent trade value filter must stay inactive — never a 0-to-0 band that
// would reject every stock.
func TestToModelStrategy_AbsentTradeValueFilterIsInactive(t *testing.T) {
	raw := []byte(`{
		"strategy_id": "s1",
		"user_id": "u1",
		"conditions": {"impact_score_min": 1, "impact_score_max": 10},
		"trade_config": {"order_type": "MARKET", "quantity": 1},
		"risk_limits": {}
	}`)

	var p StrategyPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	s, err := ToModelStrategy(&p)
	if err != nil {
		t.Fatalf("ToModelStrategy: %v", err)
	}
	if s.Conditions.TradeValueFilter.IsActive() {
		t.Fatalf("absent filter must be inactive, got %+v", s.Conditions.TradeValueFilter)
	}
}

// jsonNum renders a float for embedding in a JSON literal.
func jsonNum(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// An absent range (older producer, or a strategy that never set one) must decode
// to 0/0 — which evaluateMarketCapRange treats as "filter not set" — and must
// never be mistaken for a real 0-to-0 band that matches nothing.
func TestToModelStrategy_AbsentMarketCapRangeIsUnset(t *testing.T) {
	raw := []byte(`{
		"strategy_id": "s1",
		"user_id": "u1",
		"conditions": {"impact_score_min": 1, "impact_score_max": 10},
		"trade_config": {"order_type": "MARKET", "quantity": 1},
		"risk_limits": {}
	}`)

	var p StrategyPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	s, err := ToModelStrategy(&p)
	if err != nil {
		t.Fatalf("ToModelStrategy: %v", err)
	}

	if s.Conditions.MarketCapRange.MinMcap != 0 || s.Conditions.MarketCapRange.MaxMcap != 0 {
		t.Fatalf("absent range should be 0/0, got min=%v max=%v",
			s.Conditions.MarketCapRange.MinMcap, s.Conditions.MarketCapRange.MaxMcap)
	}
}
