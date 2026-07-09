package handlers

import (
	"errors"
	"net/http"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/algos"
	"github.com/gorilla/mux"
)

// AlgosHandler is the HTTP-layer entry point for the Explore screen.
//
// Why a struct (not just a function)?
//
//	The handler depends on the algos.Catalog (where the data comes from).
//	That dependency must be remembered between requests — we can't make a
//	new catalog on every request, and we shouldn't keep it as a package-
//	level global (which would make tests painful and prevent multiple
//	configs from co-existing).
//
//	A struct holds the dependency once at startup, and every method on the
//	struct has access to it via the receiver. This is the "constructor
//	injection" pattern — same idea as how the existing HealthHandler holds
//	its DB and Redis clients.
//
// What's stored on the struct?
//
//	Just `catalog` — the interface. NOT a *StaticCatalog. The handler does
//	not know or care which implementation is plugged in. Tomorrow we swap
//	StaticCatalog for DBCatalog, and not a single line of this file changes.
type AlgosHandler struct {
	catalog algos.Catalog
}

// NewAlgosHandler wires the handler to a Catalog. main.go calls this once
// at startup and passes the returned pointer to the router.
//
// Returning a *AlgosHandler (concrete pointer, not an interface) is fine
// here because:
//   - The router needs to know which methods are available on the
//     handler (ListAlgos), so an interface wouldn't help.
//   - We're inside the api-gateway service, so the consumer (the router)
//     is part of the same codebase — no abstraction barrier needed.
//
// Contrast with the catalog: we DO return Catalog (interface) from
// NewStaticCatalog because the consumer (this handler) genuinely benefits
// from not knowing which implementation it received. The "return concrete
// vs. interface" decision turns on whether the consumer should be insulated
// from implementation details. For handlers consumed by their own service's
// router, return concrete.
func NewAlgosHandler(catalog algos.Catalog) *AlgosHandler {
	return &AlgosHandler{catalog: catalog}
}

// ListAlgos handles GET /api/v1/algos.
//
// This is the actual function the HTTP server calls when a request arrives
// at that URL. The mux router registered with this method (see router.go)
// matches the incoming URL and forwards the request here.
//
// Signature notes:
//
//	`w http.ResponseWriter`  — the empty reply envelope. We write our JSON
//	                           output here. See envelope.go for what writing
//	                           actually means.
//	`r *http.Request`        — the incoming request. URL, headers, query
//	                           params, body — everything the client sent.
//	                           We use it to extract a Context.
//
// What this handler does, line by line:
//
//	1. Pulls the request's Context (a cancellation + deadline carrier).
//	2. Asks the catalog for every algo.
//	3. On error, returns a 500 with an Indira-style error envelope.
//	4. Wraps the slice in algos.ListResponse (the documented response shape).
//	5. Writes a 200 with the success envelope around the response.
//
// What this handler does NOT do:
//
//   - Talk to a database directly. The catalog handles that.
//   - Build JSON by hand. respondIndiraOK / respondIndiraError do that.
//   - Validate input. There IS no input — GET with no query params, no body.
//
// This minimalism is on purpose. A handler that doesn't do too much is a
// handler that's easy to read, easy to test, and unlikely to break.
func (h *AlgosHandler) ListAlgos(w http.ResponseWriter, r *http.Request) {
	// Why r.Context()?
	//
	// Go's `context.Context` carries deadlines + cancellation signals
	// down through the call stack. r.Context() is the context attached
	// to THIS specific HTTP request — Go's net/http server creates it
	// for us. If the client closes the connection mid-flight, this
	// context becomes "done" and any DB/network call we made with it
	// gets the abort signal automatically. This prevents wasted work
	// on requests nobody is waiting for anymore.
	ctx := r.Context()

	// Call the catalog. The static catalog never returns an error,
	// but the contract allows for it — so we handle it as if it could,
	// keeping the handler future-proof for the day we plug in a DB.
	items, err := h.catalog.All(ctx)
	if err != nil {
		// "E500" is our app-level error code for a generic server failure.
		// The HTTP status (500) tells the browser/proxy layer about
		// retry semantics; the infoID gives the frontend a stable key
		// to switch on for messaging.
		//
		// We deliberately don't leak err.Error() into the response —
		// internal error messages can contain sensitive details (SQL
		// fragments, file paths). The client gets a generic message;
		// the real error lives in our logs (when we add request-level
		// logging in the security middleware later).
		respondIndiraError(w, http.StatusInternalServerError,
			"E500", "failed to load algos")
		return
	}

	// Wrap the slice in the documented response shape. The Algos field
	// is what the frontend reads. Tomorrow if we add pagination, we
	// add a PageInfo field here — the handler is the natural place to
	// assemble the final response.
	resp := algos.ListResponse{
		Algos: items,
	}

	// Hand off to the envelope helper. It writes the 200 status, the
	// Content-Type header, the full {infoID, infoMsg, timestamp, data}
	// JSON, and we're done.
	respondIndiraOK(w, resp)
}

// GetAlgo handles GET /api/v1/algos/{id}.
//
// This is the detail-page endpoint — powers the "Manthan Strategy"
// screen the user sees after tapping an algo card on the Explore list.
// Response is the full AlgoDetail struct wrapped in the Indira envelope.
//
// URL path parameter:
//   {id} — the algo's stable ID (e.g., "algo_manthan_v1"). Extracted
//          via gorilla/mux's Vars helper.
//
// Response codes:
//   200 — algo found, returns AlgoDetail as the envelope's "data"
//   400 — malformed request (path variable missing, extremely rare
//         because the router only matches when {id} is present, but
//         defensive check is cheap)
//   404 — infoID "E_NOT_FOUND", infoMsg "algo not found"
//         (when catalog.ByID returns ErrAlgoNotFound)
//   500 — infoID "E500" (any other catalog error — future DB down,
//         network blip, etc. StaticCatalog never returns one today.)
func (h *AlgosHandler) GetAlgo(w http.ResponseWriter, r *http.Request) {
	// Extract the {id} path variable via gorilla/mux.
	// Vars returns a map[string]string of every path parameter
	// captured in the route pattern (e.g. "/algos/{id}" captures "id").
	// If the router matched us at all, "id" is guaranteed to be
	// present — but a defensive check catches a silly mis-registered
	// route earlier, and costs nothing.
	id := mux.Vars(r)["id"]
	if id == "" {
		respondIndiraError(w, http.StatusBadRequest,
			"E_BAD_REQUEST", "algo id is required")
		return
	}

	// Delegate to the catalog. errors.Is walks any %w wrap chain so
	// this keeps working even when a future implementation wraps
	// ErrAlgoNotFound with extra context.
	detail, err := h.catalog.ByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, algos.ErrAlgoNotFound) {
			respondIndiraError(w, http.StatusNotFound,
				"E_NOT_FOUND", "algo not found")
			return
		}
		respondIndiraError(w, http.StatusInternalServerError,
			"E500", "failed to load algo")
		return
	}

	// AlgoDetail already has JSON tags for every field including the
	// embedded Algo — the envelope helper marshals it directly.
	respondIndiraOK(w, detail)
}
