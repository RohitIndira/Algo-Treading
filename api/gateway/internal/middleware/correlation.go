package middleware

import (
	"net/http"

	"github.com/RohitIndira/Algo-Treading/pkg/correlation"
)

// CorrelationID middleware extracts X-Correlation-ID from the incoming request
// (or generates a new one when absent) and stores it in the request context.
// The ID is also echoed back in the response header so clients can correlate
// their requests with server-side log entries.
func CorrelationID() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(correlation.HTTPHeader)
			if id == "" {
				id = correlation.NewID()
			}
			w.Header().Set(correlation.HTTPHeader, id)
			next.ServeHTTP(w, r.WithContext(correlation.WithContext(r.Context(), id)))
		})
	}
}
