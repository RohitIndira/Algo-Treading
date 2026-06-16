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

var ErrSymbolNotFound = errors.New("marketws: ISIN not found in external Redis")

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

// gracefully).
func NewResolver(rdb *goredis.Client) *Resolver {
	return &Resolver{
		rdb:   rdb,
		cache: make(map[string]ResolvedSymbol),
	}
}

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
