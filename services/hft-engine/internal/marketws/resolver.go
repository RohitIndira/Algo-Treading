// Symbol → ODIN security_code resolver.
//
// The external Indira market-data Redis (the same one data-ingestion
// populates daily) already stores ISIN → nsecode mappings under
// `isin:{ISIN}` keys. We reuse that as the single source of truth so
// HFT and Manthan can never disagree on what "ARVIND" means.
//
// Value shape (JSON):
//   {"isin":"INE034A01011","bsecode":"500101","nsecode":"193","mcap":...,
//    "mcaptype":"Small Cap","exchange":"NSE"}
//
// `nsecode` is the ODIN security_code. We assume segment_id = 1 (NSE
// Cash) for now; extending this to other segments is one extra field.
//
// Caching: every resolved ISIN is held in an in-memory map for the
// process lifetime. Stocks don't change tokens, so cache invalidation
// isn't a concern; missed mappings (Redis down) bubble up as errors and
// the caller can decide whether to fail Entry or retry.
package marketws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	goredis "github.com/go-redis/redis/v8"
)

// ErrSymbolNotFound is returned when the ISIN isn't in external Redis.
// Likely causes: ISIN typo on the strategy form, or data-ingestion
// hasn't run today yet. Either way, refuse Entry — don't guess.
var ErrSymbolNotFound = errors.New("marketws: ISIN not found in external Redis")

// ResolvedSymbol is what the resolver hands back. SegmentID is the ODIN
// market segment (1=NSE Cash today; extend the resolver to look at the
// `exchange` field if we ever subscribe to BSE / derivatives).
type ResolvedSymbol struct {
	ISIN         string
	Symbol       string // display sym, derived from nsecode if needed
	SegmentID    int    // ODIN market segment
	SecurityCode int64  // nsecode parsed as int64
	Exchange     string // "NSE" | "BSE" — informational
}

// Resolver looks up tokens. One per process; safe for concurrent use.
type Resolver struct {
	rdb *goredis.Client

	// In-memory cache. ISIN → ResolvedSymbol. Once an ISIN is in here
	// we never go back to Redis for it during this process lifetime.
	mu    sync.RWMutex
	cache map[string]ResolvedSymbol

	// hits/misses counters for /readyz dashboards.
	hits   uint64
	misses uint64
}

// NewResolver wires an external Redis client. rdb may be nil — in that
// case every call returns ErrSymbolNotFound, which lets a paper-mode
// dev environment boot without external Redis at all (Entry will fail
// gracefully).
func NewResolver(rdb *goredis.Client) *Resolver {
	return &Resolver{
		rdb:   rdb,
		cache: make(map[string]ResolvedSymbol),
	}
}

// Resolve returns the ODIN security_code for an ISIN. Bounded by ctx.
//
// Lookup order:
//   1. In-memory cache (lock-free fast path)
//   2. Redis GET isin:{ISIN}
//   3. Parse JSON; nsecode → SecurityCode; cache + return
//
// Missing nsecode in the row is treated the same as missing row —
// returns ErrSymbolNotFound. We never make up a token.
func (r *Resolver) Resolve(ctx context.Context, isin string) (ResolvedSymbol, error) {
	if isin == "" {
		return ResolvedSymbol{}, fmt.Errorf("marketws: empty ISIN")
	}

	// Cache fast path.
	r.mu.RLock()
	if v, ok := r.cache[isin]; ok {
		r.mu.RUnlock()
		r.bumpHits()
		return v, nil
	}
	r.mu.RUnlock()

	if r.rdb == nil {
		return ResolvedSymbol{}, fmt.Errorf("%w: no external Redis configured", ErrSymbolNotFound)
	}

	// Bound the Redis call so a slow network doesn't hang Entry.
	rctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	raw, err := r.rdb.Get(rctx, "isin:"+isin).Result()
	switch {
	case errors.Is(err, goredis.Nil):
		r.bumpMisses()
		return ResolvedSymbol{}, fmt.Errorf("%w: %s", ErrSymbolNotFound, isin)
	case err != nil:
		return ResolvedSymbol{}, fmt.Errorf("redis get isin:%s: %w", isin, err)
	}

	var blob struct {
		ISIN     string `json:"isin"`
		BSECode  string `json:"bsecode"`
		NSECode  string `json:"nsecode"`
		Exchange string `json:"exchange"`
	}
	if err := json.Unmarshal([]byte(raw), &blob); err != nil {
		return ResolvedSymbol{}, fmt.Errorf("parse isin:%s value: %w", isin, err)
	}
	if blob.NSECode == "" {
		r.bumpMisses()
		return ResolvedSymbol{}, fmt.Errorf("%w: isin:%s has no nsecode", ErrSymbolNotFound, isin)
	}
	nse, err := strconv.ParseInt(blob.NSECode, 10, 64)
	if err != nil {
		return ResolvedSymbol{}, fmt.Errorf("isin:%s nsecode=%q is not an int: %w", isin, blob.NSECode, err)
	}

	rs := ResolvedSymbol{
		ISIN:         isin,
		SegmentID:    1, // NSE Cash; future: branch on blob.Exchange
		SecurityCode: nse,
		Exchange:     blob.Exchange,
	}
	r.mu.Lock()
	r.cache[isin] = rs
	r.mu.Unlock()
	r.bumpHits()
	return rs, nil
}

// Stats returns cache hits/misses for ops dashboards.
func (r *Resolver) Stats() (hits, misses uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.hits, r.misses
}

func (r *Resolver) bumpHits()   { r.mu.Lock(); r.hits++; r.mu.Unlock() }
func (r *Resolver) bumpMisses() { r.mu.Lock(); r.misses++; r.mu.Unlock() }
