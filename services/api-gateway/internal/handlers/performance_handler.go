package handlers

import (
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/performance"
	"github.com/gorilla/mux"
)

// PerformanceHandler serves GET /api/v1/algos/{id}/performance.
//
// Data flow:
//   1. URL {id} → algo id
//   2. Fetch every DailyRow from the store for that algo + reference client
//   3. Fetch every BenchmarkRow (Nifty 50) since the algo's first date
//   4. Build the response struct via performance.Build (pure function)
//   5. Wrap in the Indira envelope + write
//
// This handler holds ONE dependency: a performance.Store. Everything
// else (algo ID → reference client mapping, benchmark ID, disclaimer
// text) is in the aggregator or hard-coded here.
type PerformanceHandler struct {
	store performance.Store

	// Currently there's only ONE algo → one reference client mapping.
	// When we add more algos, this becomes a map or a config field.
	// Hard-coded works fine for the launch scope.
	algoToReferenceClient map[string]string
}

// NewPerformanceHandler wires the handler to a store. The map argument
// is `algo_id → reference_client_id` — e.g., {"algo_manthan_v1": "A844"}.
// Any algo id not in the map returns 404.
func NewPerformanceHandler(store performance.Store, algoToReferenceClient map[string]string) *PerformanceHandler {
	return &PerformanceHandler{
		store:                 store,
		algoToReferenceClient: algoToReferenceClient,
	}
}

// GetPerformance handles GET /api/v1/algos/{id}/performance.
func (h *PerformanceHandler) GetPerformance(w http.ResponseWriter, r *http.Request) {
	algoID := mux.Vars(r)["id"]
	if algoID == "" {
		respondIndiraError(w, http.StatusBadRequest,
			"E_BAD_REQUEST", "algo id is required")
		return
	}

	refClientID, ok := h.algoToReferenceClient[algoID]
	if !ok {
		respondIndiraError(w, http.StatusNotFound,
			"E_NOT_FOUND", "algo not found")
		return
	}

	// Fetch algo daily rows first — need the first date to bound the
	// benchmark fetch (avoid pulling 3 years of Nifty when we only
	// need to overlay it on 16 months of Manthan data).
	daily, err := h.store.FetchDaily(r.Context(), algoID, refClientID)
	if err != nil {
		if errors.Is(err, performance.ErrNoData) {
			// Algo is known but has no data yet — treat as 404 so the
			// frontend can show an empty state.
			respondIndiraError(w, http.StatusNotFound,
				"E_NO_DATA", "no performance data for this algo yet")
			return
		}
		log.Printf("performance: fetch daily failed for %s/%s: %v", algoID, refClientID, err)
		respondIndiraError(w, http.StatusInternalServerError,
			"E500", "failed to load performance data")
		return
	}

	// Bench fetch: since the algo's first date. Best-effort — if Nifty
	// is temporarily unavailable, the chart just renders one line and
	// the vs-benchmark row shows 0. Better UX than a hard 500.
	var bench []performance.BenchmarkRow
	if len(daily) > 0 {
		bench, err = h.store.FetchBenchmark(r.Context(), "nifty50", daily[0].Date)
		if err != nil {
			log.Printf("performance: fetch benchmark failed (rendering without it): %v", err)
			bench = nil // fall through — chart will just have algo line
		}
	}

	resp := performance.Build(algoID, refClientID, "NIFTY 50", daily, bench, r.URL.Query().Get("period"))
	respondIndiraOK(w, resp)
}

// Compile-time no-op to keep time import wired even during builds that
// prune unused; the file may grow to use it soon.
var _ = time.Now
