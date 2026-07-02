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
