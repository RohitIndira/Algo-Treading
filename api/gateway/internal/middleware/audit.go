package middleware

import (
	"bufio"
	"fmt"
	"net"
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

// Hijack delegates to the underlying ResponseWriter so WebSocket upgrades keep
// working through this wrapper (gorilla/websocket requires http.Hijacker).
// The old AuditLog skipped GET requests entirely, so the WS handshake was never
// wrapped; AccessLog wraps every method, so this passthrough is required.
func (rw *auditResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("audit: underlying ResponseWriter does not implement http.Hijacker")
	}
	return hj.Hijack()
}

// Flush delegates to the underlying ResponseWriter so streaming/SSE responses
// keep flushing through this wrapper.
func (rw *auditResponseWriter) Flush() {
	if fl, ok := rw.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

// AccessLog records every request (method, path, user ID, client IP, response
// status, latency) so any crash or error has surrounding context to trace
// what was in flight. Mutating requests (POST/PUT/DELETE) are additionally
// tagged "mutating" for audit filtering. Severity escalates with status: 5xx
// logs as Error, 4xx as Warn, everything else as Info, so failures are easy
// to grep/alert on without drowning in successful GETs.
func AccessLog(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := &auditResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
			start := time.Now()

			userID := r.Header.Get("userId")
			ip := r.RemoteAddr
			if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
				ip = xff
			}
			mutating := r.Method == http.MethodPost || r.Method == http.MethodPut || r.Method == http.MethodDelete

			next.ServeHTTP(rw, r)

			fields := []zap.Field{
				zap.String("correlation_id", correlation.FromContext(r.Context())),
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.String("user_id", userID),
				zap.String("ip", ip),
				zap.Int("status", rw.statusCode),
				zap.Bool("mutating", mutating),
				zap.Duration("duration", time.Since(start).Round(time.Millisecond)),
			}

			switch {
			case rw.statusCode >= 500:
				logger.Error("REQUEST", fields...)
			case rw.statusCode >= 400:
				logger.Warn("REQUEST", fields...)
			default:
				logger.Info("REQUEST", fields...)
			}
		})
	}
}
