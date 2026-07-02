package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// envelope.go defines the "Indira-style" JSON response shape that NEW endpoints
// use. The frontend expects every response to look like:
//
//	{
//	  "infoID":    "0",
//	  "infoMsg":   "success",
//	  "timestamp": 1781843472610,
//	  "data":      { ... whatever the endpoint actually returns ... }
//	}
//
// For errors, infoID is non-zero (e.g. "E400"), infoMsg explains the failure,
// and data is omitted entirely.
//
// Why have a helper at all?
//
//	Without one, every handler would copy-paste the same 6 lines to wrap its
//	payload — and the moment one handler forgot the timestamp or used a
//	different key name, the frontend would silently break for that endpoint.
//	A single helper guarantees every response is shaped identically.
//
// Why have a SEPARATE helper instead of changing the existing respondWithJSON?
//
//	The 9 existing handlers in this package already use respondWithJSON, which
//	writes a different (looser) shape. Changing that helper would force a
//	migration of every existing endpoint at once — risky. By introducing a
//	new helper with a different name (respondIndiraOK / respondIndiraError),
//	the new contract co-exists with the old one. New endpoints adopt the
//	envelope, old endpoints keep working. We can migrate the legacy ones
//	one at a time later if we want.

// indiraEnvelope is the JSON shape every response uses.
//
// `omitempty` on Data means: if the caller passes nil (e.g. error responses),
// the "data" key is omitted from the JSON entirely rather than appearing as
// "data": null. The frontend prefers absent keys for error responses.
type indiraEnvelope struct {
	InfoID    string `json:"infoID"`
	InfoMsg   string `json:"infoMsg"`
	Timestamp int64  `json:"timestamp"`
	// interface{} (a.k.a. `any` in modern Go) means "any kind of value."
	// It lets us pass an algos.ListResponse here for one endpoint, a User
	// struct for another, etc. — the helper doesn't need to know the
	// specific type.
	Data interface{} `json:"data,omitempty"`
}

// respondIndiraOK writes a 200 OK response with the standard success
// envelope around the given payload.
//
// Usage from a handler:
//
//	respondIndiraOK(w, algos.ListResponse{Algos: list})
//
// Internals:
//   - infoID:    "0" — the magic value the frontend treats as success.
//   - infoMsg:   "success" — short, machine-readable status word.
//   - timestamp: current time in milliseconds since the Unix epoch. The
//     frontend prefers milliseconds (matches the Indira broker convention)
//     over the more common Go time.Now().Unix() which is seconds.
//   - data:      the caller's payload, encoded as-is. Whatever type was
//     passed in will be turned into the right JSON by its own json tags.
//
// If JSON encoding ever fails (extremely rare — would require the payload
// to contain unsupported values like channels or functions), we log it and
// give up. There's nothing useful to send to the client at that point.
func respondIndiraOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	body := indiraEnvelope{
		InfoID:    "0",
		InfoMsg:   "success",
		Timestamp: time.Now().UnixMilli(),
		Data:      data,
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// We've already called WriteHeader, so we can't change the status
		// code now. Just log and move on; the client will see a truncated
		// or malformed response. This is the canonical Go pattern for
		// "this should never happen" mid-response failures.
		log.Printf("respondIndiraOK: JSON encode failed: %v", err)
	}
}

// respondIndiraError writes an error response with the standard envelope.
// The HTTP status and the envelope's infoID/infoMsg together let the
// frontend distinguish error categories.
//
//   - httpStatus: the HTTP status code (400, 401, 404, 500, ...).
//     This is what the browser's `fetch().status` will see.
//   - infoID:     a short app-level error code ("E400", "E_RATE_LIMIT", etc.)
//     that's stable even if the human message changes. The frontend keys off
//     this to decide which error UI to show.
//   - message:    human-readable reason. Safe to log; safe to show in a UI
//     (don't include sensitive details like SQL fragments).
//
// Example error response:
//
//	{
//	  "infoID":    "E400",
//	  "infoMsg":   "invalid query parameter: limit must be a positive integer",
//	  "timestamp": 1781843472610
//	}
//
// Note the absence of "data" — that's the omitempty tag doing its job.
func respondIndiraError(w http.ResponseWriter, httpStatus int, infoID, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	body := indiraEnvelope{
		InfoID:    infoID,
		InfoMsg:   message,
		Timestamp: time.Now().UnixMilli(),
		// Data left nil → omitempty drops it from the JSON output entirely.
	}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("respondIndiraError: JSON encode failed: %v", err)
	}
}
