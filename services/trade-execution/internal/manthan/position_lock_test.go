package manthan

// FIX 1 — per-position advisory lock (SL-regress race closer).
// Runs against local execution_db; skips if unreachable.
//
//   TestPositionLock_SerializesSameKey     — two goroutines on the same
//       (strategy,symbol) never run their critical sections concurrently
//   TestPositionLock_DifferentKeysDontBlock — different positions don't block
//       each other (no false contention / deadlock)

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPositionLock_SerializesSameKey(t *testing.T) {
	db := openExecTestDB(t) // from naked_position_test.go
	r := NewRepository(db)

	var active int32   // how many goroutines are in the critical section right now
	var maxSeen int32  // high-water mark — must never exceed 1 for the same key
	var wg sync.WaitGroup

	crit := func() error {
		n := atomic.AddInt32(&active, 1)
		for {
			m := atomic.LoadInt32(&maxSeen)
			if n <= m || atomic.CompareAndSwapInt32(&maxSeen, m, n) {
				break
			}
		}
		time.Sleep(80 * time.Millisecond) // hold the section so an overlap would show
		atomic.AddInt32(&active, -1)
		return nil
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = r.WithPositionLock(context.Background(), "STRAT-X", "SYMLOCK", func(ctx context.Context) error {
				return crit()
			})
		}()
	}
	wg.Wait()

	if maxSeen > 1 {
		t.Fatalf("same-position critical sections overlapped (maxSeen=%d) — SL-regress race NOT closed", maxSeen)
	}
}

func TestPositionLock_DifferentKeysDontBlock(t *testing.T) {
	db := openExecTestDB(t)
	r := NewRepository(db)

	done := make(chan struct{}, 2)
	run := func(sym string) {
		_ = r.WithPositionLock(context.Background(), "STRAT-Y", sym, func(ctx context.Context) error {
			time.Sleep(150 * time.Millisecond)
			return nil
		})
		done <- struct{}{}
	}
	start := time.Now()
	go run("SYM_A")
	go run("SYM_B")
	<-done
	<-done
	elapsed := time.Since(start)

	// Serialized would be ~300ms; concurrent ~150ms. Allow slack — assert they
	// did NOT serialize (proves different positions don't block each other).
	if elapsed > 280*time.Millisecond {
		t.Fatalf("different positions blocked each other (%v) — false contention", elapsed)
	}
}
