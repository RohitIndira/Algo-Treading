package tradeexec

import (
	"testing"
	"time"
)

// Cache behaviour we care about for P.B.5:
//   1. Hit within TTL returns the stored meta.
//   2. Expired entries look like a miss.
//   3. NOT_FOUND uses the SHORT TTL (60s default) — pending INSERTs may
//      resolve quickly, we shouldn't cache "no such order" for a day.
//   4. FOUND uses the LONG TTL (24h default) — Manthan orders are immutable.
//   5. Over-capacity Put evicts something to make room.

func TestCache_HitAndMiss(t *testing.T) {
	c := NewCache(Config{
		MaxItems:    10,
		FoundTTL:    1 * time.Hour,
		NotFoundTTL: 1 * time.Hour,
	})

	if _, ok := c.Get("NZ-A"); ok {
		t.Fatal("empty cache should miss")
	}

	want := OrderMeta{Found: true, SignalID: "sig-1", OrderType: "ENTRY"}
	c.Put("NZ-A", want)

	got, ok := c.Get("NZ-A")
	if !ok {
		t.Fatal("expected hit after Put")
	}
	if got.SignalID != want.SignalID || got.OrderType != want.OrderType {
		t.Errorf("hit returned wrong meta: got %+v, want %+v", got, want)
	}
}

func TestCache_ExpiryLooksLikeMiss(t *testing.T) {
	c := NewCache(Config{
		MaxItems:    10,
		FoundTTL:    5 * time.Millisecond,
		NotFoundTTL: 5 * time.Millisecond,
	})
	c.Put("NZ-A", OrderMeta{Found: true, SignalID: "sig-1"})

	// Sleep past TTL
	time.Sleep(20 * time.Millisecond)

	if _, ok := c.Get("NZ-A"); ok {
		t.Fatal("expired entry should miss")
	}
}

func TestCache_NotFoundUsesShortTTL(t *testing.T) {
	c := NewCache(Config{
		MaxItems:    10,
		FoundTTL:    1 * time.Hour,          // long
		NotFoundTTL: 5 * time.Millisecond,   // short
	})

	// Store a NOT_FOUND
	c.Put("BOGUS", OrderMeta{Found: false})

	// Immediately after Put — hit
	if _, ok := c.Get("BOGUS"); !ok {
		t.Fatal("NOT_FOUND should be cached at least briefly")
	}

	// After short TTL — miss (racing INSERTs get to retry)
	time.Sleep(20 * time.Millisecond)
	if _, ok := c.Get("BOGUS"); ok {
		t.Fatal("NOT_FOUND should expire after short TTL")
	}
}

func TestCache_FoundUsesLongTTL(t *testing.T) {
	c := NewCache(Config{
		MaxItems:    10,
		FoundTTL:    100 * time.Millisecond, // long-ish for the test
		NotFoundTTL: 5 * time.Millisecond,   // short
	})

	c.Put("NZ-A", OrderMeta{Found: true, SignalID: "sig-1"})

	// Wait past the SHORT TTL but before the LONG TTL — should still hit
	time.Sleep(30 * time.Millisecond)
	if _, ok := c.Get("NZ-A"); !ok {
		t.Fatal("FOUND should still be cached past NotFoundTTL")
	}
}

func TestCache_CapacityEviction(t *testing.T) {
	c := NewCache(Config{
		MaxItems:    2,
		FoundTTL:    1 * time.Hour,
		NotFoundTTL: 1 * time.Hour,
	})

	c.Put("K1", OrderMeta{Found: true})
	c.Put("K2", OrderMeta{Found: true})
	if c.Len() != 2 {
		t.Fatalf("cache should hold 2 entries, got %d", c.Len())
	}

	c.Put("K3", OrderMeta{Found: true}) // over capacity — should evict one
	if c.Len() != 2 {
		t.Fatalf("cache should stay at capacity 2 after over-cap Put, got %d", c.Len())
	}

	// K3 must be present; K1 or K2 was evicted (random)
	if _, ok := c.Get("K3"); !ok {
		t.Fatal("K3 should be present after Put")
	}
}

// Same-key Put should REPLACE, not add a new entry (no eviction needed).
func TestCache_SameKeyReplaces(t *testing.T) {
	c := NewCache(Config{
		MaxItems:    2,
		FoundTTL:    1 * time.Hour,
		NotFoundTTL: 1 * time.Hour,
	})
	c.Put("K", OrderMeta{Found: true, SignalID: "old"})
	c.Put("K", OrderMeta{Found: true, SignalID: "new"})
	if c.Len() != 1 {
		t.Fatalf("same-key Put should not grow cache; got %d entries", c.Len())
	}
	got, _ := c.Get("K")
	if got.SignalID != "new" {
		t.Errorf("same-key Put should replace value; got %q, want %q", got.SignalID, "new")
	}
}
