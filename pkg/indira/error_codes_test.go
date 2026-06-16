package indira

import "testing"

// All test fixtures below are REAL Codifi/Indira responses captured during
// the 2026-06-06 NSE Contingency Drill (10:00-15:30 IST), against test
// account ND03920. Each test case is one observed (rejReason / infoMsg)
// string; we assert it parses to the correct canonical NSE tag + category.
//
// Drill artifacts:
//   /tmp/drill_results.json         — round 1, 10 tests
//   /tmp/drill_round2_out.txt       — round 2, 10 tests + order trail
//   /tmp/drill_round3_results.json  — round 3, 30 tests
//   /tmp/drill_round3_out.txt       — round 3 full transcript

func TestParseExchangeRejection_DrillCaptures(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		wantTag   string
		wantCat   ExchangeRejectCategory
		wantRetry bool
	}{
		// ── Codifi pre-trade (EG003 infoMsg) ──
		{
			name:      "T02 tick mismatch — SBIN @100.03 on tick=0.05",
			input:     "Price Not in multiple of PriceTick UserId[ND03920]",
			wantTag:   "INVALID_PRICE_TICK_MISMATCH",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T39 zero limit on Limit order",
			input:     "for RL order price should not be Zero",
			wantTag:   "INVALID_PRICE_ZERO_ON_LIMIT",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T41 Market with trigger set",
			input:     "Trigger price should not be allowed when order Type is RL-MKT",
			wantTag:   "TRIGGER_NOT_ALLOWED_ON_MARKET",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T04 SL with zero trigger",
			input:     "Trigger price must be provided when order Type is SL",
			wantTag:   "ERR_INVALID_TRIGGER_PRICE_MISSING",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T42 SL-M with zero trigger",
			input:     "Trigger price must be provided when order Type is SL-MKT",
			wantTag:   "ERR_INVALID_TRIGGER_PRICE_MISSING_SLM",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T05 SELL SL limit > trigger",
			input:     "Trigger price should be greater than or equal to Limit price",
			wantTag:   "ERROR_SL_LMT_RSNBLTY_CHECK_SELL",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T11 BUY SL trigger > limit",
			input:     "Trigger price should be less than or equal to Limit price",
			wantTag:   "ERROR_SL_LMT_RSNBLTY_CHECK_BUY",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T14/T46 zero/negative qty",
			input:     "Incorrect Quantity , Quantity could not be zero or negative",
			wantTag:   "INVALID_QUANTITY_NON_POSITIVE",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T37 invalid exchange",
			input:     "Scrip Information Not Found !!",
			wantTag:   "INVALID_SCRIP_LOOKUP",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T31 excToken=0",
			input:     "Scrip details not found.",
			wantTag:   "INVALID_SCRIP_LOOKUP",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "T58 Order Trail empty ordId",
			input:     "limit must be provided",
			wantTag:   "ORDER_TRAIL_LIMIT_MISSING",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},

		// ── Codifi structural (EG001 infoMsg) ──
		{
			name:      "T32-36 invalid enum field → generic Invalid request",
			input:     "Invalid request",
			wantTag:   "INVALID_REQUEST_STRUCTURE",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},

		// ── Exchange-side rejections (Order Book rejReason) ──
		{
			name:      "T43/T48/T55 SELL with no free qty (DDPI gap)",
			input:     "Limit exceeded Set= 0, N.Used= 1 ,Prev= 0, T.Free Qty= 0,Today Free Qty=0  across all securities",
			wantTag:   "RMS_FREE_QTY_EXCEEDED_LIKELY_DDPI",
			wantCat:   CategoryAuth,
			wantRetry: false,
		},
		{
			name:      "T07 margin insufficient on huge market order",
			input:     "Margin Limit exceeded.Set/OptCFS/=13215.55,0.00,Used=2849927.29(M)(Prev=127.29,EM=0.00),ShortFall=2836711.74 across all securities of All Exchange Combined (Expiry/Settlement)  of  ND03920",
			wantTag:   "MARGIN_INSUFFICIENT",
			wantCat:   CategoryMargin,
			wantRetry: false,
		},
		{
			name:      "Round-2 T13 negative price post-acceptance reject",
			input:     "Order failed Minimum. Single Transaction Value check where the limit is 0.01 and",
			wantTag:   "BELOW_MINIMUM_TRANSACTION_VALUE",
			wantCat:   CategoryRetryable,
			wantRetry: true,
		},
		{
			name:      "Round-2 T19 modify after cancel",
			input:     "Order Time has changed or is incorrect for modification/cancellation request",
			wantTag:   "OE_ORD_CANNOT_MODIFY_STALE",
			wantCat:   CategoryStaleOrder,
			wantRetry: false,
		},
		{
			name:      "2026-06-06 DPR round: re-cancel terminal order",
			input:     "Duplicate Modification.(CFIXBusinessServer:: ProcessCancelModifyOrderRequest)",
			wantTag:   "OE_ORD_DUPLICATE_MOD_CANCEL",
			wantCat:   CategoryStaleOrder,
			wantRetry: false,
		},
		{
			name:      "T06 qty above exchange freeze limit",
			input:     " Quantity  is more than Maximum Quantity (101536) allowed by the exchange. ND03920",
			wantTag:   "ERR_QUANTITY_FREEZE_CANCELLED",
			wantCat:   CategoryTerminal,
			wantRetry: false,
		},
		{
			name:      "2026-06-04: yesterday's 10 AMO conversions DPR breach",
			input:     "Order entered has invalid data.",
			wantTag:   "INVALID_DATA_GENERIC_LIKELY_DPR",
			wantCat:   CategoryDPRBreach,
			wantRetry: true,
		},

		// ── Documented patterns not seen in drill but pre-catalogued ──
		{
			name:      "documented DPR: outside revised price range",
			input:     "Order price is outside the revised price range",
			wantTag:   "ERR_PRICE_OUTSIDE_REVISED_PRICE_RANGE",
			wantCat:   CategoryDPRBreach,
			wantRetry: true,
		},
		{
			name:      "documented SEBI: invalid algo id",
			input:     "Invalid Algo Id",
			wantTag:   "ERR_INVALID_ALGO_ID",
			wantCat:   CategoryTerminal,
			wantRetry: false,
		},

		// ── Edges ──
		{
			name:    "uncatalogued — must fall through cleanly",
			input:   "Some never-before-seen broker error code Z9999",
			wantTag: "UNCATALOGUED",
			wantCat: CategoryUnknown,
		},
		{
			name: "empty rejReason — empty record (no false-positive match)",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ParseExchangeRejection(c.input)
			if got.Tag != c.wantTag {
				t.Fatalf("Tag: got %q want %q (raw=%q)", got.Tag, c.wantTag, got.Raw)
			}
			if got.Category != c.wantCat {
				t.Fatalf("Category: got %q want %q", got.Category, c.wantCat)
			}
			if got.Retryable != c.wantRetry {
				t.Fatalf("Retryable: got %v want %v", got.Retryable, c.wantRetry)
			}
		})
	}
}

func TestCodifiInfoID_Classification(t *testing.T) {
	if !CodifiInfoID("0").IsSuccess() {
		t.Fatal("infoID=0 should be success")
	}
	if !CodifiInfoID("").IsSuccess() {
		t.Fatal("empty infoID should be treated as success")
	}
	if !CodifiInfoID("AU004").IsAuthError() {
		t.Fatal("AU004 should be auth error")
	}
	if !CodifiInfoID("EG003").IsPreTradeReject() {
		t.Fatal("EG003 should be pre-trade reject")
	}
	if !CodifiInfoID("EG001").IsPreTradeReject() {
		t.Fatal("EG001 should be pre-trade reject")
	}
	if CodifiInfoID("EG003").IsAuthError() {
		t.Fatal("EG003 should NOT be auth error")
	}
	if CodifiInfoID("0").IsPreTradeReject() {
		t.Fatal("0 should NOT be pre-trade reject")
	}
	// EG004 = no data found — must not be treated as auth error or pre-trade reject.
	if CodifiInfoID("EG004").IsAuthError() {
		t.Fatal("EG004 should NOT be auth error (it's empty-result, not auth)")
	}
	if CodifiInfoID("EG004").IsPreTradeReject() {
		t.Fatal("EG004 should NOT be pre-trade reject")
	}
}

func TestParseCodifiResponse_Combined(t *testing.T) {
	// PlaceOrder pre-trade reject: EG003 + rule in infoMsg, no rejReason yet.
	r := ParseCodifiResponse("EG003", "Price Not in multiple of PriceTick UserId[ND03920]", "")
	if r.Tag != "INVALID_PRICE_TICK_MISMATCH" {
		t.Fatalf("PreTrade fallthrough: got tag=%q", r.Tag)
	}

	// Order Book row: infoID=0 success but rejReason populated for THIS order.
	r = ParseCodifiResponse("0", "success",
		" Quantity  is more than Maximum Quantity (101536) allowed by the exchange.")
	if r.Tag != "ERR_QUANTITY_FREEZE_CANCELLED" {
		t.Fatalf("OrderBook rejReason: got tag=%q", r.Tag)
	}

	// AU004 with "Session expired" → SESSION_EXPIRED.
	r = ParseCodifiResponse("AU004", "Session expired", "")
	if r.Tag != "SESSION_EXPIRED" || r.Category != CategoryAuth {
		t.Fatalf("AU004 session: got tag=%q cat=%q", r.Tag, r.Category)
	}

	// AU004 with "No data found ..." → ORDER_NOT_FOUND, not auth error.
	r = ParseCodifiResponse("AU004", "No data found for Order History", "")
	if r.Tag != "ORDER_NOT_FOUND" || r.Category != CategoryStaleOrder {
		t.Fatalf("AU004 not-found: got tag=%q cat=%q", r.Tag, r.Category)
	}

	// Pure success: empty record.
	r = ParseCodifiResponse("0", "success", "")
	if r.Tag != "" || r.Category != "" {
		t.Fatalf("success: got %+v want empty", r)
	}
}

// TestCatalogCoverage ensures every drill-captured response we know about
// resolves to something other than UNCATALOGUED. If a drill capture starts
// returning UNCATALOGUED we want this test to fail loudly so we can extend
// the catalog before the SEBI submission.
func TestCatalogCoverage_DrillCaptures(t *testing.T) {
	drillFixtures := []string{
		"Price Not in multiple of PriceTick UserId[ND03920]",
		"for RL order price should not be Zero",
		"Trigger price should not be allowed when order Type is RL-MKT",
		"Trigger price must be provided when order Type is SL",
		"Trigger price must be provided when order Type is SL-MKT",
		"Trigger price should be greater than or equal to Limit price",
		"Trigger price should be less than or equal to Limit price",
		"Incorrect Quantity , Quantity could not be zero or negative",
		"Scrip Information Not Found !!",
		"Scrip details not found.",
		"limit must be provided",
		"Invalid request",
		"Limit exceeded Set= 0, N.Used= 1 ,Prev= 0, T.Free Qty= 0,Today Free Qty=0  across",
		"Margin Limit exceeded.Set/OptCFS/=...,ShortFall=... across",
		"Order failed Minimum. Single Transaction Value check where the limit is 0.01",
		"Order Time has changed or is incorrect for modification/cancellation request",
		" Quantity  is more than Maximum Quantity (101536) allowed by the exchange.",
		"Order entered has invalid data.",
		"Duplicate Modification.(CFIXBusinessServer:: ProcessCancelModifyOrderRequest)",
	}
	for _, raw := range drillFixtures {
		got := ParseExchangeRejection(raw)
		if got.Tag == "UNCATALOGUED" {
			t.Errorf("UNCATALOGUED: %q", raw)
		}
		if got.Category == "" || got.Category == CategoryUnknown {
			t.Errorf("Empty/Unknown category for %q (tag=%q)", raw, got.Tag)
		}
	}
}
