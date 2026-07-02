package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// ParsePayload decodes the middle segment of a JWT and returns its
// claims as a generic map[string]interface{}.
//
// This function performs NO signature verification — it is the shared
// low-level parser that every Verifier implementation reuses BEFORE
// it does its actual cryptographic work. Concretely:
//
//   - NoopVerifier uses it and skips verification entirely.
//   - IntrospectionVerifier uses it to extract userId + appId, which
//     Codifi's /verify/token endpoint requires as HTTP headers, then
//     calls Codifi to check the signature.
//   - LocalKeyVerifier (future) will use it to read claims, and
//     separately verify the signature with the HS512 shared secret.
//
// All three verifiers need the SAME parsing logic. Rather than
// duplicating the split-decode-unmarshal dance three times, this
// function centralizes it. When Codifi changes token format or we
// need to handle a new base64 variant, one file changes.
//
// Errors are wrapped with %w around ErrTokenMalformed, so callers can
// do errors.Is(err, ErrTokenMalformed) and get true. The specific
// reason (bad segments / bad base64 / bad JSON) is preserved in the
// %v part of the wrapped message for logs.
//
// SECURITY NOTE: because this function performs NO signature check,
// the returned map is UNVERIFIED — it contains whatever the sender
// put there. Callers must treat it as untrusted until a real Verifier
// implementation confirms it. In particular, do NOT use the returned
// userId for authorization decisions unless you're inside a Verifier
// implementation that will subsequently prove the token is genuine.
func ParsePayload(jwt string) (map[string]interface{}, error) {
	// Step 1 — a JWT is exactly three dot-separated segments.
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: expected 3 segments, got %d",
			ErrTokenMalformed, len(parts))
	}

	// Step 2 — decode the payload (middle segment). JWTs use base64url
	// with NO padding (RawURLEncoding), which is different from the
	// standard base64 encoding (StdEncoding). Using the wrong one here
	// would fail on perfectly valid tokens.
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: payload base64 decode failed: %v",
			ErrTokenMalformed, err)
	}

	// Step 3 — unmarshal into a generic map. Using map[string]any
	// (not a typed struct) means we preserve every claim Codifi
	// sends, including ones we don't yet know about. Verifier
	// implementations then read the specific fields they care about.
	var raw map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &raw); err != nil {
		return nil, fmt.Errorf("%w: payload JSON parse failed: %v",
			ErrTokenMalformed, err)
	}

	return raw, nil
}
