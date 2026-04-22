package middleware

import (
	"net/http"
	"time"

	"github.com/RohitIndira/Algo-Treading/pkg/correlation"
	"go.uber.org/zap"
)

// auditResponseWriter wraps http.ResponseWriter to capture the response status code.
type auditResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *auditResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// AuditLog records every mutating request (POST, PUT, DELETE) with the user ID,
// client IP, path, method, response status, and latency. Read-only requests are
// not logged to keep audit output focused on state-changing operations.
func AuditLog() func(http.Handler) http.Handler {
	logger, _ := zap.NewProduction()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost && r.Method != http.MethodPut && r.Method != http.MethodDelete {
				next.ServeHTTP(w, r)
				return
			}

			rw := &auditResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			start := time.Now()

			userID := r.Header.Get("userId")
			ip := r.RemoteAddr
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				ip = xff
			}

			next.ServeHTTP(rw, r)

			logger.Info("AUDIT",
				zap.String("correlation_id", correlation.FromContext(r.Context())),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.String("user_id", userID),
				zap.String("ip", ip),
				zap.Int("status", rw.statusCode),
				zap.Duration("duration", time.Since(start).Round(time.Millisecond)),
			)
		})
	}
}
