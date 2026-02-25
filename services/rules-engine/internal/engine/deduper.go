package engine

import (
	"sync"
	"time"
)

// Deduper is a time-window based deduplicator.
// It stores event IDs seen in the last window duration.
//
// Implementation is intentionally simple and lock-based. It is not on the
// critical hot path compared to strategy evaluation.
type Deduper struct {
	mu         sync.Mutex
	window     time.Duration
	maxEntries int
	items      map[string]time.Time
}

func NewDeduper(window time.Duration, maxEntries int) *Deduper {
	if window <= 0 {
		window = 2 * time.Minute
	}
	if maxEntries <= 0 {
		maxEntries = 100_000
	}
	return &Deduper{window: window, maxEntries: maxEntries, items: make(map[string]time.Time)}
}

// Allow returns true if this id hasn't been seen in the window.
// If it is a duplicate, it returns false.
func (d *Deduper) Allow(id string) bool {
	if id == "" {
		// can't dedup empty; allow to proceed
		return true
	}

	now := time.Now()
	cut := now.Add(-d.window)

	d.mu.Lock()
	defer d.mu.Unlock()

	// evict on growth
	if len(d.items) > d.maxEntries {
		for k, t := range d.items {
			if t.Before(cut) {
				delete(d.items, k)
			}
		}
		// still large? do a bounded random-ish purge (map iteration order)
		for k := range d.items {
			delete(d.items, k)
			if len(d.items) <= d.maxEntries {
				break
			}
		}
	}

	if t, ok := d.items[id]; ok {
		if t.After(cut) {
			return false
		}
	}

	d.items[id] = now
	return true
}
