// Package correlation provides helpers for generating and propagating
// correlation IDs across HTTP, gRPC, and Kafka boundaries.
package correlation

import (
	"context"
	"crypto/rand"
	"fmt"
)

// Header/metadata key constants shared by all transports.
const (
	HTTPHeader      = "X-Correlation-ID"
	GRPCMetadataKey = "x-correlation-id"
	KafkaHeaderKey  = "correlation-id"
)

type contextKey struct{}

// NewID generates a new random UUID v4-format correlation ID.
// Uses crypto/rand so no external dependency is required.
func NewID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// WithContext returns a derived context carrying the given correlation ID.
func WithContext(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// FromContext extracts the correlation ID stored in ctx.
// Returns an empty string when none is present.
func FromContext(ctx context.Context) string {
	if id, ok := ctx.Value(contextKey{}).(string); ok {
		return id
	}
	return ""
}
