package handlers

import (
	"net/http"
)

// maperrorCodeToHTTPStatus maps internal error codes to HTTP status codes
func mapErrorCodeToHTTPStatus(code string) int {
	switch code {
	case "NOT_FOUND":
		return http.StatusNotFound
	case "INVALID_ARGUMENT", "VALIDATION_FAILED":
		return http.StatusBadRequest
	case "UNAUTHENTICATED":
		return http.StatusUnauthorized
	case "PERMISSION_DENIED":
		return http.StatusForbidden
	case "ALREADY_EXISTS":
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
