package middleware

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"runtime/debug"

	"github.com/RohitIndira/Algo-Treading/pkg/correlation"
	"go.uber.org/zap"
)

// recoveryResponseWriter tracks whether headers were already sent, so the
// recover handler never issues a second WriteHeader on a response the
// wrapped handler had already started streaming.
type recoveryResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
}

func (rw *recoveryResponseWriter) WriteHeader(code int) {
	rw.wroteHeader = true
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *recoveryResponseWriter) Write(b []byte) (int, error) {
	rw.wroteHeader = true
	return rw.ResponseWriter.Write(b)
}

// Hijack delegates to the underlying ResponseWriter so WebSocket upgrades keep
// working through this wrapper (gorilla/websocket requires http.Hijacker).
// Without this, the /ws/* match-feed upgrade would fail. Marking wroteHeader
// prevents the panic handler from writing to a connection now owned by the caller.
func (rw *recoveryResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("recovery: underlying ResponseWriter does not implement http.Hijacker")
	}
	rw.wroteHeader = true
	return hj.Hijack()
}

// Flush delegates to the underlying ResponseWriter so streaming/SSE responses
// keep flushing through this wrapper.
func (rw *recoveryResponseWriter) Flush() {
	if fl, ok := rw.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

// Recovery must be the outermost middleware. It catches any panic from every
// later middleware and handler, logs the full stack trace with request
// context (correlation id, method, path, user id, remote addr) so a crash is
// traceable to exactly what triggered it, and returns a clean 500 instead of
// the connection just resetting.
func Recovery(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := &recoveryResponseWriter{ResponseWriter: w}

			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("PANIC RECOVERED",
						zap.String("correlation_id", correlation.FromContext(r.Context())),
						zap.String("method", r.Method),
						zap.String("path", r.URL.Path),
						zap.String("user_id", r.Header.Get("userId")),
						zap.String("remote_addr", r.RemoteAddr),
						zap.Any("panic", rec),
						zap.String("stacktrace", string(debug.Stack())),
					)

					if !rw.wroteHeader {
						body, _ := json.Marshal(map[string]interface{}{
							"success": false,
							"error": map[string]string{
								"code":    "INTERNAL_ERROR",
								"message": "Internal server error",
							},
						})
						rw.Header().Set("Content-Type", "application/json")
						rw.WriteHeader(http.StatusInternalServerError)
						rw.Write(body)
					}
				}
			}()

			next.ServeHTTP(rw, r)
		})
	}
}
