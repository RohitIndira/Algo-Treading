// Package apperr defines the sentinel error values used to classify failures
// across the user-config service. It is a leaf package (no internal imports) so
// both the repository and service layers can wrap errors with these sentinels
// without an import cycle, and the gRPC server can classify them with
// errors.Is to pick the right response code.
//
// Wrap at the call site with %w (NOT %v/%s) so errors.Is keeps working:
//
//	return fmt.Errorf("%w: strategy_name is required", apperr.ErrValidation)
package apperr

import "errors"

var (
	// ErrValidation — bad client input. → INVALID_ARGUMENT → HTTP 400.
	ErrValidation = errors.New("validation failed")
	// ErrNotFound — the requested strategy/row does not exist. → NOT_FOUND → 404.
	ErrNotFound = errors.New("strategy not found")
	// ErrVersionConflict — optimistic-lock version mismatch on update. → FAILED_PRECONDITION → 412.
	ErrVersionConflict = errors.New("version mismatch")
	// ErrTerminalState — operation invalid because the strategy is stopped/terminal. → FAILED_PRECONDITION → 412.
	ErrTerminalState = errors.New("strategy is in terminal state")
	// ErrDuplicate — a conflicting strategy already exists. → ALREADY_EXISTS → 409.
	ErrDuplicate = errors.New("strategy already exists")
	// ErrUnauthorized — caller does not own the strategy. → PERMISSION_DENIED → 403.
	ErrUnauthorized = errors.New("user does not own this strategy")
)
