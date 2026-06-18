package handlers

import (
	"io"
	"log"
	"net/http"
	"time"
)

// AMNPreviewHandler proxies POST /api/v1/amn-preview to the rules-engine
// preview HTTP server. The rules-engine owns the MongoDB connection and all
// backfill logic — the gateway stays stateless.
type AMNPreviewHandler struct {
	rulesEngineBaseURL string
	client             *http.Client
}

func NewAMNPreviewHandler(rulesEngineURL string) *AMNPreviewHandler {
	// Tuned transport: the default http.Transport caps idle conns per host at 2,
	// which causes connection churn under concurrent load. Keep a warm pool to
	// the rules-engine so 1000+ concurrent previews reuse connections.
	transport := &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		MaxConnsPerHost:     300,
		IdleConnTimeout:     90 * time.Second,
	}
	return &AMNPreviewHandler{
		rulesEngineBaseURL: rulesEngineURL,
		client:             &http.Client{Timeout: 30 * time.Second, Transport: transport},
	}
}

// Preview forwards the request body to rules-engine and streams the response back.
func (h *AMNPreviewHandler) Preview(w http.ResponseWriter, r *http.Request) {
	target := h.rulesEngineBaseURL + "/internal/amn-preview"

	req, err := http.NewRequestWithContext(r.Context(), http.MethodPost, target, r.Body)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "Failed to build upstream request")
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		log.Printf("amn-preview: rules-engine unreachable: %v", err)
		respondWithError(w, http.StatusBadGateway, "AMN preview service unavailable")
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(body)
}
