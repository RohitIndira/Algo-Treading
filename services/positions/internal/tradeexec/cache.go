// Package tradeexec is positions svc's gRPC client for trade-execution's
// LookupOrderMeta RPC (§6 of docs/positions_service_design.md).
package tradeexec

import (
	"math/rand"
	"sync"
	"time"
)

// entry is one cached OrderMeta with an expiration.
type entry struct {
	meta      OrderMeta // zero value + found=false represents a cached NOT_FOUND
	expiresAt time.Time
}

// Cache is a bounded, TTL-based cache for LookupOrderMeta results.
//
// NOT full LRU — bound is enforced by random eviction when over capacity.
// For our access pattern (each broker_order_id read ~1-3 times per lifecycle,
// then never again after position closes) the simpler design is sufficient
// and avoids a dep. Cache misses just mean an extra gRPC call to trade-exec.
//
// Two TTLs per §6 of the design doc:
//
//	FoundTTL     = 24h    — Manthan orders are immutable once persisted
//	NotFoundTTL  = 60s    — a NOT_FOUND may be racing a pending INSERT
//
// Thread-safe. A background sweeper (Start) evicts expired entries every 5min.
type Cache struct {
	mu       sync.RWMutex
	items    map[string]entry
	maxItems int

	foundTTL    time.Duration
	notFoundTTL time.Duration

	stopCh chan struct{}
}

// Config tunes the two TTLs and the max entry count.
// Zero values fall back to sensible defaults matching §6 of the design doc.
type Config struct {
	MaxItems    int
	FoundTTL    time.Duration
	NotFoundTTL time.Duration
}

// NewCache constructs a cache. Start the sweeper via cache.Start(ctx).
func NewCache(cfg Config) *Cache {
	if cfg.MaxItems <= 0 {
		cfg.MaxItems = 10_000
	}
	if cfg.FoundTTL <= 0 {
		cfg.FoundTTL = 24 * time.Hour
	}
	if cfg.NotFoundTTL <= 0 {
		cfg.NotFoundTTL = 60 * time.Second
	}
	return &Cache{
		items:       make(map[string]entry, cfg.MaxItems),
		maxItems:    cfg.MaxItems,
		foundTTL:    cfg.FoundTTL,
		notFoundTTL: cfg.NotFoundTTL,
		stopCh:      make(chan struct{}),
	}
}

// Start launches the background sweeper. Call Stop to end it.
func (c *Cache) Start() {
	go c.sweepLoop()
}

// Stop halts the background sweeper. Safe to call multiple times.
func (c *Cache) Stop() {
	select {
	case <-c.stopCh:
		// already stopped
	default:
		close(c.stopCh)
	}
}

// Get returns (meta, true) on hit, (_, false) on miss/expired.
// Callers must handle miss with an RPC call and then Put the result.
func (c *Cache) Get(brokerOrderID string) (OrderMeta, bool) {
	c.mu.RLock()
	e, ok := c.items[brokerOrderID]
	c.mu.RUnlock()
	if !ok {
		return OrderMeta{}, false
	}
	if time.Now().After(e.expiresAt) {
		// Expired — treat as miss. The sweeper will remove it later; we
		// don't remove here to keep Get lock-free on the write side.
		return OrderMeta{}, false
	}
	return e.meta, true
}

// Put stores a result. TTL depends on meta.Found:
//
//	true  → foundTTL    (24h default; Manthan orders are immutable)
//	false → notFoundTTL (60s default; may be racing a pending INSERT)
//
// If the cache is at capacity, one existing entry is randomly evicted first.
func (c *Cache) Put(brokerOrderID string, meta OrderMeta) {
	ttl := c.foundTTL
	if !meta.Found {
		ttl = c.notFoundTTL
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[brokerOrderID]; !ok && len(c.items) >= c.maxItems {
		c.evictOneLocked()
	}

	c.items[brokerOrderID] = entry{
		meta:      meta,
		expiresAt: time.Now().Add(ttl),
	}
}

// Len returns the current number of entries (including expired-but-not-swept).
func (c *Cache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.items)
}

// evictOneLocked removes one random entry. Caller must hold the write lock.
func (c *Cache) evictOneLocked() {
	// Grab any key. Map iteration order is randomised by the runtime, so
	// this is effectively random eviction.
	n := rand.Intn(len(c.items))
	for k := range c.items {
		if n == 0 {
			delete(c.items, k)
			return
		}
		n--
	}
}

// sweepLoop removes expired entries every 5min.
func (c *Cache) sweepLoop() {
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-t.C:
			c.sweepExpired()
		}
	}
}

// sweepExpired scans + removes entries past their expiresAt.
// Takes the write lock; over 10k entries the sweep is O(N) but only every 5min.
func (c *Cache) sweepExpired() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.items {
		if now.After(e.expiresAt) {
			delete(c.items, k)
		}
	}
}
