package statemachine

import "regexp"

// symbolWirePattern parses Indira's REST_ORDERBOOK raw wire format for
// stock symbols:
//
//	STK_AADHARHFC_EQ_NSE_23729    → symbol="AADHARHFC" exchange="NSE"
//	STK_NIFTYBANK_EQ_NSE_26009    → symbol="NIFTYBANK" exchange="NSE"
//	STK_SONACOMS_EQ_BSE_543300    → symbol="SONACOMS"  exchange="BSE"
//
// Uses regex (not strings.Split) so multi-underscore symbols still parse.
// `(.+?)` is non-greedy: matches the shortest string up to `_EQ_` so a
// hypothetical `STK_M_M_EQ_NSE_2031` (Mahindra) correctly extracts `M_M`.
//
// Only stock instruments (STK / EQ) are recognized. Options (OPT_XXX)
// futures (FUT_XXX), indices (IDX_XXX) fall through to the identity
// branch — Manthan is stock-only today and expanding to F&O is a bigger
// change than this file needs to anticipate.
//
// The `\d+` tail is the numeric exchange token which we DON'T store
// on positions (positions.exchange holds the exchange name; the token
// only matters for the LTP Redis key format and lives on manthan_orders
// via the join back through entry_broker_order_id).
//
// 2026-08-19: the series is NOT always EQ. NSE trade-to-trade stocks carry
// BE (MODISONLTD arrived as STK_MODISONLTD_BE_NSE_3325), and BZ/BL/SM/ST/
// GC… exist too. With the old `_EQ_`-only pattern the raw wire string
// passed straight through: the position was stored and published as
// "STK_MODISONLTD_BE_NSE_3325", the UI showed that string with no LTP,
// and rules-engine's fill confirmation never matched its pending position.
// The series is now any 1–3 char alphanumeric group; the non-greedy symbol
// group still yields "M_M" for STK_M_M_EQ_NSE_2031.
var symbolWirePattern = regexp.MustCompile(`^STK_(.+?)_([A-Z0-9]{1,3})_(NSE|BSE)_\d+$`)

// normalizeSymbol maps an OrderEvent's raw symbol (whatever the wire
// carried) to the canonical (symbol, exchange) pair used throughout
// positions_db and downstream consumers.
//
// Contract:
//   - Input matches the STK wire pattern     → returns parsed (symbol, exchange).
//   - Input is already short ("AADHARHFC")   → returns (input, "") — caller
//     falls back to ev.Exchange (which for WSS events is a numeric enum,
//     for REST events is "NSE"/"BSE" already).
//   - Input is an unrecognized shape         → returns (input, "") — safe
//     pass-through; caller decides whether to reject.
//
// This is the ONLY normalization point for symbols in positions svc. Every
// INSERT into positions.symbol and every WHERE symbol=... lookup routes
// through here so a row written today with the short form still matches
// a query issued yesterday with the wire form (and vice versa).
func normalizeSymbol(raw string) (symbol, exchange string) {
	m := symbolWirePattern.FindStringSubmatch(raw)
	if m == nil {
		return raw, ""
	}
	return m[1], m[3]
}
