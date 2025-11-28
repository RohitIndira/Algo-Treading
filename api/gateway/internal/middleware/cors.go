// package middleware

// import (
// 	"net/http"
// 	"strings"
// )

// type CORSConfig struct {
// 	AllowedOrigins []string
// 	AllowedMethods []string
// 	AllowedHeaders []string
// }

// func CORS(config CORSConfig) func(http.Handler) http.Handler {
// 	return func(next http.Handler) http.Handler {
// 		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
// 			origin := r.Header.Get("Origin")

// 			// Check if origin is allowed
// 			allowed := false
// 			for _, allowedOrigin := range config.AllowedOrigins {
// 				if allowedOrigin == "*" || allowedOrigin == origin {
// 					allowed = true
// 					break
// 				}
// 			}

// 			if allowed {
// 				w.Header().Set("Access-Control-Allow-Origin", origin)
// 			}

// 			w.Header().Set("Access-Control-Allow-Methods", strings.Join(config.AllowedMethods, ", "))
// 			w.Header().Set("Access-Control-Allow-Headers", strings.Join(config.AllowedHeaders, ", "))
// 			w.Header().Set("Access-Control-Allow-Credentials", "true")
// 			w.Header().Set("Access-Control-Max-Age", "3600")

// 			// Handle preflight request
// 			if r.Method == http.MethodOptions {
// 				w.WriteHeader(http.StatusNoContent)
// 				return
// 			}

// 			next.ServeHTTP(w, r)
// 		})
// 	}
// }

package middleware

import (
	"net/http"
	"strings"
)

type CORSConfig struct {
	AllowedOrigins []string
	AllowedMethods []string
	AllowedHeaders []string
}

func CORS(config CORSConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")

			// Determine allowed origin
			allowedOrigin := ""
			for _, o := range config.AllowedOrigins {
				if o == "*" {
					allowedOrigin = "*" // Allow any origin
					break
				}
				if o == origin {
					allowedOrigin = origin
					break
				}
			}

			if allowedOrigin != "" {
				w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			}

			w.Header().Set("Access-Control-Allow-Methods", strings.Join(config.AllowedMethods, ", "))
			w.Header().Set("Access-Control-Allow-Headers", strings.Join(config.AllowedHeaders, ", "))
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Max-Age", "3600")

			// Preflight request (OPTIONS)
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
