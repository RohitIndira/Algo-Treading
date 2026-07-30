package server

import (
	"errors"
	"fmt"
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/apperr"
)

// TestErrorCodeFor verifies each sentinel maps to the semantic code string the
// api-gateway translates to an HTTP status, including through a %w wrap.
func TestErrorCodeFor(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"validation", fmt.Errorf("%w: strategy_name is required", apperr.ErrValidation), "INVALID_ARGUMENT"},
		{"not found", fmt.Errorf("%w (id=abc)", apperr.ErrNotFound), "NOT_FOUND"},
		{"version conflict", fmt.Errorf("%w: mismatch", apperr.ErrVersionConflict), "FAILED_PRECONDITION"},
		{"terminal state", fmt.Errorf("%w: already stopped", apperr.ErrTerminalState), "FAILED_PRECONDITION"},
		{"duplicate", fmt.Errorf("%w: dup", apperr.ErrDuplicate), "ALREADY_EXISTS"},
		{"unauthorized", fmt.Errorf("%w: not owner", apperr.ErrUnauthorized), "PERMISSION_DENIED"},
		{"unclassified → internal", errors.New("some DB error"), "INTERNAL"},
		{"nested wrap still classifies", fmt.Errorf("outer: %w", fmt.Errorf("%w: inner", apperr.ErrValidation)), "INVALID_ARGUMENT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errorCodeFor(tc.err); got != tc.want {
				t.Fatalf("errorCodeFor(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}
