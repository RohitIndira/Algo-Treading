package handlers

import (
	"bytes"
	"io"
	"net/http"
	"strings"
)

type AuthProxyHandler struct {
	loginServiceURL string
	client          *http.Client
}

func NewAuthProxyHandler(loginServiceURL string) *AuthProxyHandler {
	return &AuthProxyHandler{
		loginServiceURL: loginServiceURL,
		client:          &http.Client{},
	}
}

// ProxyRequest forwards the request to the user-login service
func (h *AuthProxyHandler) ProxyRequest(w http.ResponseWriter, r *http.Request) {
	// Build target URL
	targetURL := h.loginServiceURL + r.URL.Path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	// Read request body
	var bodyBytes []byte
	if r.Body != nil {
		bodyBytes, _ = io.ReadAll(r.Body)
		r.Body.Close()
	}

	// Create new request
	proxyReq, err := http.NewRequest(r.Method, targetURL, bytes.NewBuffer(bodyBytes))
	if err != nil {
		http.Error(w, "Failed to create proxy request", http.StatusInternalServerError)
		return
	}

	// Copy headers
	for key, values := range r.Header {
		// Skip hop-by-hop headers
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// Execute request
	resp, err := h.client.Do(proxyReq)
	if err != nil {
		http.Error(w, "Failed to proxy request: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Set status code
	w.WriteHeader(resp.StatusCode)

	// Copy response body
	io.Copy(w, resp.Body)
}

// isHopByHopHeader checks if a header is hop-by-hop
func isHopByHopHeader(header string) bool {
	hopByHopHeaders := []string{
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailers",
		"Transfer-Encoding",
		"Upgrade",
	}

	headerLower := strings.ToLower(header)
	for _, h := range hopByHopHeaders {
		if strings.ToLower(h) == headerLower {
			return true
		}
	}
	return false
}
