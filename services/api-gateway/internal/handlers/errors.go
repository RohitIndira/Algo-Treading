package handlers

import (
	"net/http"

	"github.com/RohitIndira/Algo-Treading/api/proto/common"
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
	case "FAILED_PRECONDITION":
		return http.StatusPreconditionFailed
	case "UNAVAILABLE":
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// respondBackendError renders a backend (gRPC) application-level failure
// (resp.Success == false) using the Indira envelope, translating the backend
// error code to an HTTP status via mapErrorCodeToHTTPStatus so a NOT_FOUND
// surfaces as 404 (not a blanket 400) and an ALREADY_EXISTS as 409.
//
// Nil-safe: a Success=false response whose Error is nil (or has an empty code)
// yields a well-formed 500 instead of panicking on a nil dereference.
func respondBackendError(w http.ResponseWriter, e *common.Error) {
	if e == nil {
		respondIndiraError(w, http.StatusInternalServerError, "INTERNAL",
			"backend returned failure without error detail")
		return
	}
	code := e.Code
	if code == "" {
		code = "INTERNAL"
	}
	respondIndiraError(w, mapErrorCodeToHTTPStatus(code), code, e.Message)
}
