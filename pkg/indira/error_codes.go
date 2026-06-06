// Package indira — error_codes.go
//
// Structured error-code classification for Codifi/Indira broker responses.
//
// Two layers of error surface to the caller:
//
//   1. infoID — Codifi's own response code at the API envelope level.
//      Captured directly from PlaceOrder / ModifyOrder / CancelOrder /
//      Order-Trail responses. Alphanumeric (e.g. "EG003"), NOT the NSE
//      numeric protocol codes (16115, 16247, …).
//
//   2. rejReason — free-text rejection from the exchange (or Codifi's RMS),
//      visible in Order Book and Order Trail rows. Carries the actual NSE
//      OE failure semantics — but as English prose, not the documented
//      numeric code. We pattern-match the prose back to the canonical NSE
//      tag so downstream code can categorize without hand-coded regexes.
//
// All patterns in this file were captured from real Codifi responses
// during the 2026-06-06 NSE Contingency Drill (10:00-15:30 IST window,
// account ND03920, ~50 deliberately-crafted orders).
//
// See:
//   - /tmp/drill_results.json, /tmp/drill_round3_results.json — raw responses
//   - internal/Indira Securities - API Documentation (6).pdf — broker spec
//   - SEBI Circular SEBI/HO/MIRSD/DOP/CIR/P/2022/119 — algo audit reqs
package indira

import (
	"regexp"
	"strings"
)

// ─── Codifi infoID codes ────────────────────────────────────────────────

// CodifiInfoID is the alphanumeric code Codifi returns in the API response
// envelope. NOT the NSE numeric protocol code — those only appear later in
// rejReason text from the exchange (and even then, only as English prose).
type CodifiInfoID string

const (
	// InfoIDSuccess — order accepted by Codifi. Order may still be Rejected
	// at the exchange; check Order Book / Order Trail rejReason.
	InfoIDSuccess CodifiInfoID = "0"

	// InfoIDInvalidRequest — Codifi rejected at JSON-structure level. Caused
	// by missing/unknown enum values: invalid ordType, ordValidity, ordAction,
	// prdType, instrument; wrong Order Trail instrument lookup. Drill samples
	// in tests 32-36 + 59.
	InfoIDInvalidRequest CodifiInfoID = "EG001"

	// InfoIDPreTradeReject — Codifi's pre-trade business-rule check failed.
	// The infoMsg carries the specific rule violated (tick alignment, SL
	// trigger/limit ordering, qty positivity, etc.). Most common rejection.
	// Drill samples in tests 02, 04, 05, 11, 14, 37, 39, 41, 42, 46.
	InfoIDPreTradeReject CodifiInfoID = "EG003"

	// InfoIDAuthOrNotFound — session/JWT expired OR "no data found" for
	// Order Trail / Modify on missing order. Overloaded by Codifi.
	// Drill samples in tests 08, 16, 31.
	InfoIDAuthOrNotFound CodifiInfoID = "AU004"
)

// IsSuccess reports whether the Codifi envelope indicates the API call
// succeeded. The order may still be Rejected at the exchange.
func (c CodifiInfoID) IsSuccess() bool { return c == InfoIDSuccess || c == "" }

// IsAuthError reports whether the Codifi response indicates the user's
// session/JWT is invalid. Note AU004 is overloaded — also returned when
// fetching trail for an order that doesn't exist, so callers should
// cross-check infoMsg before triggering re-login.
func (c CodifiInfoID) IsAuthError() bool { return c == InfoIDAuthOrNotFound }

// IsPreTradeReject reports whether Codifi rejected the order before
// forwarding to the exchange. Caller should consult infoMsg for the
// specific rule and apply the appropriate auto-recover.
func (c CodifiInfoID) IsPreTradeReject() bool {
	return c == InfoIDPreTradeReject || c == InfoIDInvalidRequest
}

// ─── Exchange/RMS rejection categories ──────────────────────────────────

// ExchangeRejectCategory groups rejection reasons by how the system should
// respond. Drives retry / alert / terminal-fail decisions.
type ExchangeRejectCategory string

const (
	// CategoryRetryable — fix the input + retry. Tick alignment, missing
	// trigger, SL limit/trigger ordering. Cheap auto-recover possible.
	CategoryRetryable ExchangeRejectCategory = "PRE_TRADE_RETRYABLE"

	// CategoryTerminal — order is dead; do not retry. Quantity freeze,
	// margin insufficient, order value beyond user's limit. Operator/user
	// must act before re-attempt.
	CategoryTerminal ExchangeRejectCategory = "PRE_TRADE_TERMINAL"

	// CategoryDPRBreach — trigger or limit price outside today's daily
	// price range. Retryable only after clamping price within the band.
	CategoryDPRBreach ExchangeRejectCategory = "DPR_BREACH"

	// CategoryAuth — broker/RMS rejected because the user's TPIN/DDPI
	// authorization is missing, or session is expired. Retry only after
	// user re-authorizes (DDPI eSign or TPIN OTP).
	CategoryAuth ExchangeRejectCategory = "AUTH"

	// CategoryMargin — insufficient funds/margin. Cannot retry without
	// either reducing qty or topping up margin.
	CategoryMargin ExchangeRejectCategory = "MARGIN_INSUFFICIENT"

	// CategoryStaleOrder — modify/cancel against an order that's no longer
	// in a modifiable state (already Cancelled/Filled/Rejected).
	CategoryStaleOrder ExchangeRejectCategory = "STALE_ORDER"

	// CategoryUnknown — exchange returned a reason we haven't catalogued
	// yet. Treat as terminal pending investigation.
	CategoryUnknown ExchangeRejectCategory = "UNKNOWN"
)

// ExchangeRejection is the canonical view of a broker/exchange rejection
// after parsing the free-text rejReason or infoMsg. Code maps to the NSE OE
// protocol code (e.g. 16247 INVALID_PRICE) when known; 0 when only the
// English prose was matched without a documented NSE code.
type ExchangeRejection struct {
	Code      int                    `json:"code,omitempty"`     // NSE OE numeric code (0 if unknown)
	Tag       string                 `json:"tag"`                // canonical tag (INVALID_PRICE, MARGIN_INSUFFICIENT, …)
	Category  ExchangeRejectCategory `json:"category"`           // drives retry policy
	Retryable bool                   `json:"retryable"`          // shortcut: Category in {Retryable, DPRBreach}
	Raw       string                 `json:"raw"`                // original rejReason / infoMsg text
}

// String formats the rejection for log + event fields.
func (e ExchangeRejection) String() string {
	if e.Code > 0 {
		return e.Tag + " (NSE " + itoa(e.Code) + ")"
	}
	return e.Tag
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// ─── Pattern catalog ────────────────────────────────────────────────────
//
// Each entry maps a substring/regex pattern observed in rejReason/infoMsg
// back to the canonical NSE OE tag + category. Match order matters: most
// specific patterns first to avoid loose substring shadowing.

type rejPattern struct {
	re       *regexp.Regexp         // nil → use Contains (case-insensitive)
	contains string                 // case-insensitive substring
	code     int                    // NSE OE protocol code (0 if not documented)
	tag      string                 // canonical tag
	category ExchangeRejectCategory // drives downstream behavior
}

// Catalog. Patterns 1-26 are all live drill captures from 2026-06-06.
// Patterns 27-36 are documented NSE OE codes we haven't seen yet but
// expect to handle (rare circuit/value/algo scenarios).
var rejectPatterns = []rejPattern{
	// ── Codifi structural validation (EG001 path) — drill rounds 30-36 ──
	// "Invalid request" — generic Codifi structural reject. Match on full
	// phrase to avoid catching the word "invalid" everywhere.
	{contains: "Invalid request", code: 0,
		tag: "INVALID_REQUEST_STRUCTURE", category: CategoryRetryable},

	// ── Codifi pre-trade business rules (EG003) — drill rounds 02-46 ──

	// T02 — "Price Not in multiple of PriceTick UserId[ND03920]"
	{contains: "Price Not in multiple of PriceTick", code: 16247,
		tag: "INVALID_PRICE_TICK_MISMATCH", category: CategoryRetryable},

	// T39 — "for RL order price should not be Zero"
	{contains: "for RL order price should not be Zero", code: 16247,
		tag: "INVALID_PRICE_ZERO_ON_LIMIT", category: CategoryRetryable},

	// T41 — "Trigger price should not be allowed when order Type is RL-MKT"
	{contains: "Trigger price should not be allowed", code: 16423,
		tag: "TRIGGER_NOT_ALLOWED_ON_MARKET", category: CategoryRetryable},

	// T42 — "Trigger price must be provided when order Type is SL-MKT"
	// (must be matched BEFORE the broader "SL" pattern below).
	{contains: "Trigger price must be provided when order Type is SL-MKT", code: 16423,
		tag: "ERR_INVALID_TRIGGER_PRICE_MISSING_SLM", category: CategoryRetryable},

	// T04 — "Trigger price must be provided when order Type is SL"
	{contains: "Trigger price must be provided when order Type is SL", code: 16423,
		tag: "ERR_INVALID_TRIGGER_PRICE_MISSING", category: CategoryRetryable},

	// T05 SELL — "Trigger price should be greater than or equal to Limit price"
	{contains: "Trigger price should be greater than or equal to Limit price", code: 16448,
		tag: "ERROR_SL_LMT_RSNBLTY_CHECK_SELL", category: CategoryRetryable},

	// T11 BUY — "Trigger price should be less than or equal to Limit price"
	{contains: "Trigger price should be less than or equal to Limit price", code: 16448,
		tag: "ERROR_SL_LMT_RSNBLTY_CHECK_BUY", category: CategoryRetryable},

	// T14/T46 — "Incorrect Quantity , Quantity could not be zero or negative"
	{contains: "Quantity could not be zero or negative", code: 16307,
		tag: "INVALID_QUANTITY_NON_POSITIVE", category: CategoryRetryable},

	// T37/T38 — "Scrip Information Not Found !!" (invalid exchange / wrong token)
	{contains: "Scrip Information Not Found", code: 0,
		tag: "INVALID_SCRIP_LOOKUP", category: CategoryRetryable},

	// T31 — "Scrip details not found." (excToken=0)
	{contains: "Scrip details not found", code: 0,
		tag: "INVALID_SCRIP_LOOKUP", category: CategoryRetryable},

	// T58 — "limit must be provided" (Order Trail validation)
	{contains: "limit must be provided", code: 0,
		tag: "ORDER_TRAIL_LIMIT_MISSING", category: CategoryRetryable},

	// ── Exchange-side RMS / margin (in Order Book rejReason) ────────────

	// #43/#48/#55 — SELL with no free qty (DDPI/TPIN gap)
	// "Limit exceeded Set= 0, N.Used= 1 ,Prev= 0, T.Free Qty= 0,Today Free Qty=0 across..."
	{contains: "Limit exceeded Set=", code: 0,
		tag: "RMS_FREE_QTY_EXCEEDED_LIKELY_DDPI", category: CategoryAuth},

	// #07/#44 — margin insufficient
	// "Margin Limit exceeded.Set/OptCFS/=13215.55,0.00,Used=...,ShortFall=..."
	{contains: "Margin Limit exceeded", code: 0,
		tag: "MARGIN_INSUFFICIENT", category: CategoryMargin},

	// T13 — negative price, rejected at exchange
	// "Order failed Minimum. Single Transaction Value check where the limit is 0.01 and..."
	{contains: "Single Transaction Value check", code: 0,
		tag: "BELOW_MINIMUM_TRANSACTION_VALUE", category: CategoryRetryable},

	// T19 — modify a cancelled order
	// "Order Time has changed or is incorrect for modification/cancellation request"
	{contains: "Order Time has changed", code: 16346,
		tag: "OE_ORD_CANNOT_MODIFY_STALE", category: CategoryStaleOrder},

	// 2026-06-06 IDEA DPR re-cancel — cancel/modify against terminal order
	// "Duplicate Modification.(CFIXBusinessServer:: ProcessCancelModifyOrderRequest)"
	{contains: "Duplicate Modification", code: 16346,
		tag: "OE_ORD_DUPLICATE_MOD_CANCEL", category: CategoryStaleOrder},
	{contains: "ProcessCancelModifyOrderRequest", code: 16346,
		tag: "OE_ORD_DUPLICATE_MOD_CANCEL", category: CategoryStaleOrder},

	// #06 — exchange qty freeze
	// " Quantity  is more than Maximum Quantity (101536) allowed by the exchange. ND03920"
	{re: regexp.MustCompile(`(?i)Quantity\s+is more than Maximum Quantity\s*\((\d+)\)\s*allowed`),
		code: 16307, tag: "ERR_QUANTITY_FREEZE_CANCELLED", category: CategoryTerminal},

	// 2026-06-04 yesterday's 10 AMO conversions rejected
	// "Order entered has invalid data" — generic catch-all, very likely DPR
	{contains: "Order entered has invalid data", code: 0,
		tag: "INVALID_DATA_GENERIC_LIKELY_DPR", category: CategoryDPRBreach},

	// ── NSE OE codes documented but not yet captured live ──────────────

	// DPR (circuit-band) breach — explicit forms
	{contains: "outside the revised price range", code: 16521,
		tag: "ERR_PRICE_OUTSIDE_REVISED_PRICE_RANGE", category: CategoryDPRBreach},
	{contains: "outside the price range", code: 16521,
		tag: "ERR_PRICE_OUTSIDE_REVISED_PRICE_RANGE", category: CategoryDPRBreach},
	{contains: "price freeze", code: 16308,
		tag: "ERR_PRICE_FREEZE_CANCELLED", category: CategoryTerminal},

	// Modify / Cancel rejections
	{contains: "order cannot be modified", code: 16346,
		tag: "OE_ORD_CANNOT_MODIFY", category: CategoryStaleOrder},
	{contains: "Order Modification", code: 16115,
		tag: "ERR_MOD_CAN_REJECT", category: CategoryStaleOrder},
	{contains: "Order Cancellation", code: 16115,
		tag: "ERR_MOD_CAN_REJECT", category: CategoryStaleOrder},

	// Value / qty limits (NSE OE 16436 / 16442 / 16530 / 16531)
	{contains: "buy order value limit", code: 16530,
		tag: "BUY_ORDER_VALUE_LIMIT_EXCEEDED", category: CategoryTerminal},
	{contains: "sell quantity has exceeded", code: 16531,
		tag: "SELL_ORDER_VALUE_LIMIT_EXCEEDED", category: CategoryTerminal},
	{contains: "order value limit has exceeded", code: 16436,
		tag: "ORDER_VALUE_LIMIT_EXCEEDED", category: CategoryTerminal},
	{contains: "order's value is more than", code: 16442,
		tag: "ORDER_VALUE_EXCEEDS_ORDER_VALUE_LIMIT", category: CategoryTerminal},

	// Pre-open
	{contains: "Order Entry is not allowed in preopen", code: 16440,
		tag: "SERIES_NOT_ALLOWED_IN_PREOPEN", category: CategoryRetryable},

	// AlgoID (SEBI compliance — error fires post-approval if algoID missing)
	{contains: "Invalid Algo Id", code: 17179,
		tag: "ERR_INVALID_ALGO_ID", category: CategoryTerminal},
	{contains: "Invalid Algo ID", code: 17179,
		tag: "ERR_INVALID_ALGO_ID", category: CategoryTerminal},

	// CDSL / DDPI / TPIN (additional patterns beyond RMS_FREE_QTY)
	{contains: "CDSL TPIN", code: 0,
		tag: "TPIN_AUTH_REQUIRED", category: CategoryAuth},
	{contains: "DDPI", code: 0,
		tag: "DDPI_AUTH_REQUIRED", category: CategoryAuth},

	// Session-expired prose forms (rejReason path)
	{contains: "session has expired", code: 0,
		tag: "SESSION_EXPIRED", category: CategoryAuth},
	{contains: "Re-login", code: 0,
		tag: "SESSION_EXPIRED", category: CategoryAuth},

	// RMS catch-all
	{contains: "rms_order_reject", code: 16597,
		tag: "RMS_ORDER_REJECT", category: CategoryTerminal},

	// Codifi internal "Invalid Msg" (returned from #19/#20/#08 modify+cancel
	// on non-existent or already-cancelled orders before the order-book
	// settles its state) — uninformative on its own.
	{contains: "Invalid Msg", code: 0,
		tag: "INVALID_MSG_GENERIC", category: CategoryUnknown},
}

// ParseExchangeRejection maps a free-text rejection from broker/exchange
// back to a structured rejection record. Tries each catalog entry in
// declaration order; first match wins.
//
// Returns Tag="UNCATALOGUED" + Category=CategoryUnknown when nothing
// matches — caller should log+alert these so the catalog can grow.
func ParseExchangeRejection(rejReason string) ExchangeRejection {
	if rejReason == "" {
		return ExchangeRejection{}
	}
	trimmed := strings.TrimSpace(rejReason)
	lower := strings.ToLower(trimmed)

	for _, p := range rejectPatterns {
		matched := false
		if p.re != nil {
			matched = p.re.MatchString(trimmed)
		} else if p.contains != "" {
			matched = strings.Contains(lower, strings.ToLower(p.contains))
		}
		if matched {
			return ExchangeRejection{
				Code:      p.code,
				Tag:       p.tag,
				Category:  p.category,
				Retryable: p.category == CategoryRetryable || p.category == CategoryDPRBreach,
				Raw:       rejReason,
			}
		}
	}

	return ExchangeRejection{
		Tag:       "UNCATALOGUED",
		Category:  CategoryUnknown,
		Retryable: false,
		Raw:       rejReason,
	}
}

// ParseCodifiResponse classifies a top-level Codifi API response by
// infoID + infoMsg + rejReason. Convenience wrapper combining the two
// layers (envelope + exchange).
//
//   * AU004 short-circuits to SESSION_EXPIRED (but check infoMsg first —
//     it's also returned when an order isn't found).
//   * If rejReason is populated (Order Book / Order Trail row), parse it.
//   * Else if infoMsg is populated (pre-trade reject), parse it.
//   * Else empty rejection (success).
func ParseCodifiResponse(infoID, infoMsg, rejReason string) ExchangeRejection {
	if CodifiInfoID(infoID).IsAuthError() {
		lc := strings.ToLower(infoMsg)
		if strings.Contains(lc, "not found") || strings.Contains(lc, "no data found") {
			return ExchangeRejection{
				Tag: "ORDER_NOT_FOUND", Category: CategoryStaleOrder, Raw: infoMsg,
			}
		}
		return ExchangeRejection{
			Tag: "SESSION_EXPIRED", Category: CategoryAuth, Raw: infoMsg,
		}
	}
	if rejReason != "" {
		return ParseExchangeRejection(rejReason)
	}
	if infoMsg != "" && infoMsg != "success" {
		return ParseExchangeRejection(infoMsg)
	}
	return ExchangeRejection{}
}
