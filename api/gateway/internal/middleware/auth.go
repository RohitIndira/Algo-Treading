package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type authContextKey string

// AuthUserKey is the context key under which *AuthClaims is stored after successful verification.
const AuthUserKey authContextKey = "auth_user"

// AuthClaims holds the verified user info returned by the auth service.
type AuthClaims struct {
	UCC         string `json:"ucc"`
	Timestamp   int64  `json:"timestamp"`
	CallbackURL string `json:"callback-url"`
}

// AuthConfig holds the external auth service settings.
type AuthConfig struct {
	VerifyURL string
	Timeout   time.Duration
	Bypass    bool // skip external verify call (UAT / dev)
}

// authVerifyResponse mirrors the auth service response schema.
type authVerifyResponse struct {
	Code         string     `json:"code"`
	Message      string     `json:"message"`
	PlainMessage AuthClaims `json:"plain-message"`
}

// publicPaths are skipped by auth verification.
var publicPaths = map[string]bool{
	"/api/v1/health": true,
}

// Auth returns middleware that verifies every request's JWT token against the
// external Indira auth service. Requests without a valid token are rejected
// with a 401 before they reach any handler.
func Auth(cfg AuthConfig) func(http.Handler) http.Handler {
	client := &http.Client{Timeout: cfg.Timeout}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Always pass OPTIONS through — CORS middleware handles it.
			if r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)
				return
			}

			// Skip public paths.
			if publicPaths[r.URL.Path] {
				next.ServeHTTP(w, r)
				return
			}

			// Extract credentials from the incoming request.
			token := extractBearerToken(r)
			userID := r.Header.Get("userId")
			appID := r.Header.Get("appId")

			if token == "" || userID == "" || appID == "" {
				writeAuthJSON(w, http.StatusUnauthorized, "UNAUTHORIZED", "Missing required auth credentials: Authorization (Bearer token), userId, appId")
				return
			}

			var claims *AuthClaims
			if cfg.Bypass {
				// UAT / dev: skip the external verify call; trust headers.
				claims = &AuthClaims{UCC: userID}
			} else {
				// Call the external auth service.
				var httpStatus string
				var err error
				claims, httpStatus, err = callAuthService(client, cfg.VerifyURL, token, userID, appID)
				if err != nil {
					writeAuthJSON(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "Authentication service unavailable, try again shortly")
					return
				}
				if httpStatus != "200" {
					writeAuthJSON(w, http.StatusUnauthorized, "UNAUTHORIZED", "Invalid or expired token")
					return
				}
			}

			// Token is valid — propagate claims into context.
			ctx := context.WithValue(r.Context(), AuthUserKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// extractBearerToken reads the JWT from the Authorization header ("Bearer <token>"),
// a plain "token" header, or the "token" query parameter — in that priority order.
func extractBearerToken(r *http.Request) string {
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
	}
	if t := r.Header.Get("token"); t != "" {
		return t
	}
	return r.URL.Query().Get("token")
}

// callAuthService contacts the Indira token-verify endpoint and returns the
// response code string ("200"/"401"), the parsed claims, and any transport error.
func callAuthService(client *http.Client, verifyURL, token, userID, appID string) (*AuthClaims, string, error) {
	url := fmt.Sprintf("%s?token=%s", verifyURL, token)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("userId", userID)
	req.Header.Set("appId", appID)

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()

	var authResp authVerifyResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return nil, "", err
	}

	return &authResp.PlainMessage, authResp.Code, nil
}

// writeAuthJSON writes a standardised JSON error response matching the gateway's
// existing error envelope: {"success":false,"error":{"code":"...","message":"..."}}.
func writeAuthJSON(w http.ResponseWriter, status int, code, message string) {
	body, _ := json.Marshal(map[string]interface{}{
		"success": false,
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}
