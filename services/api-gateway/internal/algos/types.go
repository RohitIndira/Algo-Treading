// Package algos owns the "algo catalog" concept for api-gateway.
//
// What is an "algo"?
//   An algo is an automated trading strategy users can browse, subscribe to,
//   and run on their account. Right now we have one (Manthan); more will be
//   added later. The data shown on the frontend Explore screen comes from
//   this package.
//
// What's in this file?
//   Only the data shapes (Go structs) — no logic, no Redis, no HTTP. Keeping
//   types in their own file makes them easy to find and to share with other
//   parts of the codebase that may need to read or write algo data later.
package algos

import "errors"

// ErrAlgoNotFound is returned by Catalog.ByID when the requested id
// does not exist in the catalog. Handlers use errors.Is to map this
// to a 404 response with a stable infoID the frontend can key off.
var ErrAlgoNotFound = errors.New("algos: algo not found")

// Algo is one row in the Explore screen — everything shown on a single card.
//
// Field-by-field map back to the screen:
//
//   ID            stable identifier the frontend uses to link this card to
//                 a future detail page or subscribe action. Never shown to
//                 the user directly.
//   Name          "Manthan" (big text on the card)
//   Type          "Equity" (left of the dot separator)
//   Style         "Positional" (right of the dot separator)
//   Logo          URL to the green icon shown on the card
//   Description   the paragraph under the title
//   Badge         "Most subscribed" — small pill in the top-right
//   MinInvestment 1500000 in rupees. The frontend formats it as "15 Lac".
//                 We send raw rupees so the API stays stable even if the
//                 display format ("15 Lac" / "15 Lakhs" / "₹15,00,000") changes.
//   MaxDrawdown   -12.6 (a percentage as a plain number). Frontend prefixes
//                 the "%" sign.
//   PrimaryReturn flexible map of {"3Y Return": 28.4, "2Y Return": 32.9}.
//                 Using a map (instead of fixed fields like Return3Y, Return2Y)
//                 means we can add "5Y Return" / "1Y Return" later without
//                 changing this struct or breaking the frontend.
//
// The `json:"..."` tags tell Go how to convert this struct into JSON. The
// JSON keys are lowerCamelCase because that's what the frontend payload spec
// shows. The Go field names use UpperCamelCase because Go requires exported
// fields to start with a capital letter.
type Algo struct {
	ID            string             `json:"id"`
	Name          string             `json:"name"`
	Type          string             `json:"type"`
	Style         string             `json:"style"`
	Logo          string             `json:"logo"`
	Description   string             `json:"description"`
	Badge         string             `json:"badge"`
	MinInvestment int64              `json:"minInvestment"`
	MaxDrawdown   float64            `json:"maxDrawdown"`
	PrimaryReturn map[string]float64 `json:"primaryReturn"`
}

// ListResponse is the shape of the "data" field in the final JSON response
// for GET /api/v1/algos. Wrapping the array in an object (instead of returning
// a bare JSON array) is a small but important habit:
//
//   - Adds future room to grow ("data": { "algos": [...], "pageInfo": {...} })
//     without changing the response type.
//   - Some legacy HTTP clients refuse top-level arrays for security reasons
//     (JSON hijacking, an old browser issue). Wrapping in an object sidesteps it.
//
// The full final response (with the Indira envelope around this) will look like:
//
//   {
//     "infoID": "0",
//     "infoMsg": "success",
//     "timestamp": 1781843472610,
//     "data": {
//       "algos": [ {...}, {...} ]
//     }
//   }
//
// The envelope ({infoID, infoMsg, timestamp}) is added by a separate helper
// (envelope.go). That separation keeps this package focused only on what an
// "algo" actually IS, not on HTTP delivery details.
type ListResponse struct {
	Algos []Algo `json:"algos"`
}

// KeyStats is the "Key Stats" grid on the algo detail page — six
// performance metrics shown as a 2×3 tile grid.
//
// Field notes:
//   WinRatePct     — 62 means "62%". Frontend adds the % sign.
//   ProfitFactor   — 24.1. Dimensionless ratio, no % suffix in the UI.
//   TotalTradesPct — 68 means "68%". Confusingly named on the mockup
//                    ("Total Trades") but it IS shown with a % sign,
//                    so it's a percentage. Kept the "Pct" suffix so
//                    future readers of this file don't wonder.
//   AvgHoldingDays — a signed number (screen shows -7.4). Frontend
//                    prefixes appropriately.
//   Sortino        — the Sortino ratio. Dimensionless.
//   VolatilityDays — displayed as "12 days" on the mockup. Integer.
//
// Every field defaults to zero-value when omitted; the frontend
// should treat all-zeros as "no data yet" (helpful when we add a
// new algo before we've computed its metrics).
type KeyStats struct {
	WinRatePct     float64 `json:"winRatePct"`
	ProfitFactor   float64 `json:"profitFactor"`
	TotalTradesPct float64 `json:"totalTradesPct"`
	AvgHoldingDays float64 `json:"avgHoldingDays"`
	Sortino        float64 `json:"sortino"`
	VolatilityDays int     `json:"volatilityDays"`
}

// WhatYouGetItem is one row of the "What you get" bullet list on the
// detail page. Each row is an icon + a title + a short description.
//
// Icon is a semantic name (e.g. "automation", "shield", "bell", "chart")
// that the Flutter app maps to a local SVG asset — NOT a URL. Keeping
// icons app-local means: smaller responses, no image-fetch race with
// the rest of the screen, offline-safe. When we add a new icon type,
// the mobile release is what enables it.
type WhatYouGetItem struct {
	Icon        string `json:"icon"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// AlsoWorthKnowingItem is one row of the "Also Worth Knowing" list on
// the detail page. Same icon convention as WhatYouGetItem but only a
// single line of text (no separate title/description split, matching
// the mockup).
type AlsoWorthKnowingItem struct {
	Icon string `json:"icon"`
	Text string `json:"text"`
}

// AlgoDetail is the full response payload for GET /api/v1/algos/{id}.
//
// Design decision — we EMBED Algo (rather than duplicating its fields)
// so the base Explore-card data (name, badge, minInvestment, etc.)
// stays in ONE place. Go's JSON encoder "promotes" embedded struct
// fields to the parent level automatically, so the output JSON is flat:
//
//   { "id": "...", "name": "...", ..., "keyStats": {...}, ... }
//
// not:
//
//   { "algo": { "id": "...", "name": "..." }, "keyStats": {...} }
//
// which is what the frontend expects.
//
// The extra detail-page fields (description length differs from list,
// keyStats, whatYouGet, alsoWorthKnowing, disclaimer) live only here.
// The list endpoint keeps returning the smaller Algo — no extra bytes
// on the Explore screen just because we added detail fields.
type AlgoDetail struct {
	Algo
	KeyStats         KeyStats               `json:"keyStats"`
	WhatYouGet       []WhatYouGetItem       `json:"whatYouGet"`
	AlsoWorthKnowing []AlsoWorthKnowingItem `json:"alsoWorthKnowing"`
	Disclaimer       string                 `json:"disclaimer"`
}
