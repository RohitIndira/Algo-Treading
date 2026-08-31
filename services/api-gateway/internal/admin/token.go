// Package admin implements the Admin Console foundation (spec M1):
// elevation-based admin identity, opaque server-side session tokens,
// tiered confirmations, and an append-only audit trail.
//
// Security model (why opaque tokens, not JWTs):
//
//	An admin token is 32 bytes from crypto/rand, handed to the client
//	exactly once and stored server-side only as hex(sha256(token)).
//	There is no signing secret to leak and nothing offline-forgeable —
//	validity is a DB row (not expired, not revoked, admin still active),
//	so revocation is immediate and total. This deliberately trades a DB
//	read per admin request (cheap; admin traffic is tiny) for the
//	absence of an entire class of token-crypto bugs.
package admin

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
)

// tokenBytes is the raw entropy per token. 32 bytes = 256 bits, far beyond
// brute-force reach; hex-encoded to 64 chars on the wire.
const tokenBytes = 32

// NewToken returns (rawToken, tokenHash). The raw token goes to the client
// once and is never persisted or logged; the hash is what admin_sessions
// stores. Callers must treat rawToken as a secret.
func NewToken() (raw string, hash string, err error) {
	b := make([]byte, tokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", "", fmt.Errorf("admin token entropy: %w", err)
	}
	raw = hex.EncodeToString(b)
	return raw, HashToken(raw), nil
}

// HashToken maps a presented raw token to its storage form.
func HashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// HashesEqual compares two token hashes in constant time. The DB lookup is
// already by exact hash (an index probe leaks nothing useful), but every
// in-process comparison goes through here so timing side-channels are ruled
// out by construction rather than by argument.
func HashesEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
