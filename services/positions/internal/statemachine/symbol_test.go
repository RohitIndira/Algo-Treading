package statemachine

import "testing"

func TestNormalizeSymbol(t *testing.T) {
	tests := []struct {
		name         string
		raw          string
		wantSymbol   string
		wantExchange string
	}{
		// Wire format (REST_ORDERBOOK) — the common case we're fixing.
		{"NSE stock, numeric token", "STK_AADHARHFC_EQ_NSE_23729", "AADHARHFC", "NSE"},
		{"NSE stock, alphanumeric symbol", "STK_NIFTYBANK_EQ_NSE_26009", "NIFTYBANK", "NSE"},
		{"BSE stock", "STK_SONACOMS_EQ_BSE_543300", "SONACOMS", "BSE"},
		{"NSE stock with digits in symbol", "STK_M4M_EQ_NSE_1234", "M4M", "NSE"},

		// Multi-underscore symbol (Mahindra-style hypothetical) — non-greedy
		// regex must match the shortest string up to `_EQ_`.
		{"multi-underscore symbol", "STK_M_M_EQ_NSE_2031", "M_M", "NSE"},

		// Non-EQ series (2026-08-19 incident): trade-to-trade BE, SME SM,
		// BZ etc. must normalize exactly like EQ — the series is not part
		// of the canonical symbol.
		{"NSE trade-to-trade BE series", "STK_MODISONLTD_BE_NSE_3325", "MODISONLTD", "NSE"},
		{"NSE BZ series", "STK_XYZLTD_BZ_NSE_99", "XYZLTD", "NSE"},
		{"NSE SME series", "STK_SMECO_SM_NSE_77", "SMECO", "NSE"},
		{"BSE non-EQ series (A group code)", "STK_ABCD_A_BSE_500001", "ABCD", "BSE"},
		{"multi-underscore symbol, BE series", "STK_M_M_BE_NSE_2031", "M_M", "NSE"},

		// Already-short form (WSS numeric-buy_sell path emits these).
		// Pass through unchanged; exchange returned empty so caller can
		// fall back to ev.Exchange.
		{"already short", "AADHARHFC", "AADHARHFC", ""},
		{"already short, symbol has digits", "M4M", "M4M", ""},

		// Non-STK instrument types — safe pass-through, don't misparse.
		// Options / futures / indices fall here.
		{"option not parsed", "OPT_NIFTY_2026JUL25_CE_25000", "OPT_NIFTY_2026JUL25_CE_25000", ""},
		{"future not parsed", "FUT_NIFTY_2026JUL25", "FUT_NIFTY_2026JUL25", ""},
		{"index not parsed", "IDX_NIFTY50_NSE_26000", "IDX_NIFTY50_NSE_26000", ""},

		// Malformed wire (missing token, unknown exchange) — pass through.
		{"missing token", "STK_AADHARHFC_EQ_NSE", "STK_AADHARHFC_EQ_NSE", ""},
		{"unknown exchange", "STK_AADHARHFC_EQ_MCX_1234", "STK_AADHARHFC_EQ_MCX_1234", ""},

		// Empty / weird inputs — pass through, no crash.
		{"empty", "", "", ""},
		{"garbage", "??", "??", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSymbol, gotExchange := normalizeSymbol(tc.raw)
			if gotSymbol != tc.wantSymbol {
				t.Errorf("normalizeSymbol(%q) symbol = %q, want %q",
					tc.raw, gotSymbol, tc.wantSymbol)
			}
			if gotExchange != tc.wantExchange {
				t.Errorf("normalizeSymbol(%q) exchange = %q, want %q",
					tc.raw, gotExchange, tc.wantExchange)
			}
		})
	}
}
