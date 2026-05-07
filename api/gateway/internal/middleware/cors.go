package middleware

import (
	"log"
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

			// Log CORS request for debugging
			log.Printf("CORS: method=%s origin=%s path=%s", r.Method, origin, r.URL.Path)

			// Determine allowed origin
			// If wildcard (*) is configured, reflect the requesting origin
			// This is necessary because Access-Control-Allow-Credentials cannot be used with wildcard
			allowedOrigin := ""
			for _, o := range config.AllowedOrigins {
				if o == "*" {
					// For wildcard, reflect the requesting origin
					if origin != "" {
						allowedOrigin = origin
					} else {
						allowedOrigin = "*"
					}
					log.Printf("CORS: wildcard match, allowing origin: %s", allowedOrigin)
					break
				}
				// Normalize both origins by trimming trailing slashes for comparison
				normalizedConfigOrigin := strings.TrimSuffix(o, "/")
				normalizedRequestOrigin := strings.TrimSuffix(origin, "/")

				// Check for exact match
				if normalizedConfigOrigin == normalizedRequestOrigin {
					allowedOrigin = origin
					log.Printf("CORS: origin matched, allowing: %s", allowedOrigin)
					break
				}

				// Check if the origin starts with the configured origin (to handle subpaths)
				// This is not standard but some proxies/CDNs might send it this way
				if strings.HasPrefix(normalizedRequestOrigin, normalizedConfigOrigin) {
					allowedOrigin = origin
					log.Printf("CORS: origin prefix matched, allowing: %s", allowedOrigin)
					break
				}
			}

			if allowedOrigin == "" && origin != "" {
				log.Printf("CORS: origin not allowed: %s (configured origins: %v)", origin, config.AllowedOrigins)
			}

			// Always set CORS headers for proper preflight handling
			if allowedOrigin != "" {
				w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}

			w.Header().Set("Access-Control-Allow-Methods", strings.Join(config.AllowedMethods, ", "))
			w.Header().Set("Access-Control-Allow-Headers", strings.Join(config.AllowedHeaders, ", "))
			w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Type")
			w.Header().Set("Access-Control-Max-Age", "86400")

			// Handle preflight OPTIONS request
			if r.Method == http.MethodOptions {
				if allowedOrigin != "" {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusForbidden)
				}
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
