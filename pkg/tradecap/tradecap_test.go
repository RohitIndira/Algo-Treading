package tradecap

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func TestKey(t *testing.T) {
	// 2026-07-15 23:00 UTC = 2026-07-16 04:30 IST → key must use the IST date.
	utcLateNight := time.Date(2026, 7, 15, 23, 0, 0, 0, time.UTC)
	got := Key("strat-abc", utcLateNight)
	want := "strat:trades:strat-abc:2026-07-16"
	if got != want {
		t.Fatalf("Key across IST midnight = %q, want %q", got, want)
	}

	// Same IST day → same key regardless of the instant within the day.
	morningIST := time.Date(2026, 7, 16, 9, 15, 0, 0, IST)
	if k := Key("strat-abc", morningIST); k != want {
		t.Fatalf("Key(same IST day) = %q, want %q", k, want)
	}
}

// testReserver dials localhost Redis and skips the test when it is unavailable,
// so the suite passes in environments without Redis while still exercising the
// real Lua atomicity when Redis is present.
func testReserver(t *testing.T) (*Reserver, *redis.Client) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		t.Skipf("redis not available, skipping integration test: %v", err)
	}
	return New(rdb), rdb
}

func TestReserve_HardCapAndSeedAndRelease(t *testing.T) {
	r, rdb := testReserver(t)
	defer rdb.Close()
	ctx := context.Background()

	strat := fmt.Sprintf("test-%d", time.Now().UnixNano())
	now := time.Now()
	key := Key(strat, now)
	defer rdb.Del(ctx, key)

	// First two reserves under a cap of 2 succeed; the third is rejected.
	if n, err := r.Reserve(ctx, strat, 2, 0, now); err != nil || n != 1 {
		t.Fatalf("reserve#1 = (%d,%v), want (1,nil)", n, err)
	}
	if n, err := r.Reserve(ctx, strat, 2, 0, now); err != nil || n != 2 {
		t.Fatalf("reserve#2 = (%d,%v), want (2,nil)", n, err)
	}
	if n, err := r.Reserve(ctx, strat, 2, 0, now); err != nil || n != CapReached {
		t.Fatalf("reserve#3 = (%d,%v), want (%d,nil)", n, err, CapReached)
	}

	// Releasing one frees exactly one slot; the next reserve then succeeds.
	if err := r.Release(ctx, strat, now); err != nil {
		t.Fatalf("release: %v", err)
	}
	if n, err := r.Reserve(ctx, strat, 2, 0, now); err != nil || n != 2 {
		t.Fatalf("reserve after release = (%d,%v), want (2,nil)", n, err)
	}

	// TTL is set so the key self-cleans.
	if ttl, err := rdb.TTL(ctx, key).Result(); err != nil || ttl <= 0 {
		t.Fatalf("expected positive TTL, got (%v,%v)", ttl, err)
	}
}

func TestReserve_Unlimited(t *testing.T) {
	r, rdb := testReserver(t)
	defer rdb.Close()
	ctx := context.Background()

	strat := fmt.Sprintf("test-unl-%d", time.Now().UnixNano())
	now := time.Now()
	defer rdb.Del(ctx, Key(strat, now))

	for i := int64(1); i <= 100; i++ {
		if n, err := r.Reserve(ctx, strat, 0 /* unlimited */, 0, now); err != nil || n != i {
			t.Fatalf("unlimited reserve #%d = (%d,%v)", i, n, err)
		}
	}
}

func TestReserve_SeedAppliedOnlyWhenAbsent(t *testing.T) {
	r, rdb := testReserver(t)
	defer rdb.Close()
	ctx := context.Background()

	strat := fmt.Sprintf("test-seed-%d", time.Now().UnixNano())
	now := time.Now()
	defer rdb.Del(ctx, Key(strat, now))

	// Seed of 3 on an absent key → first reserve returns 4 and, with cap 4, the
	// next is rejected (proves the durable base was honoured).
	if n, err := r.Reserve(ctx, strat, 4, 3, now); err != nil || n != 4 {
		t.Fatalf("seeded reserve = (%d,%v), want (4,nil)", n, err)
	}
	if n, err := r.Reserve(ctx, strat, 4, 3, now); err != nil || n != CapReached {
		t.Fatalf("post-seed reserve = (%d,%v), want (%d,nil) — seed must not re-apply", n, err, CapReached)
	}

	// Reset clears the counter; trading restarts from scratch.
	if err := r.Reset(ctx, strat, now); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if n, err := r.Reserve(ctx, strat, 4, 0, now); err != nil || n != 1 {
		t.Fatalf("reserve after reset = (%d,%v), want (1,nil)", n, err)
	}
}

// TestReserve_FCFSConcurrent is the slot=1, many-simultaneous-triggers race:
// with a cap of 1 and N goroutines reserving at once, exactly one must win and
// the rest must be rejected. Run under -race. This is the core guarantee the
// user asked about ("place on FCFS basis").
func TestReserve_FCFSConcurrent(t *testing.T) {
	r, rdb := testReserver(t)
	defer rdb.Close()
	ctx := context.Background()

	strat := fmt.Sprintf("test-fcfs-%d", time.Now().UnixNano())
	now := time.Now()
	defer rdb.Del(ctx, Key(strat, now))

	const goroutines = 50
	const cap = 1
	var winners int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release all at once to maximize contention
			n, err := r.Reserve(ctx, strat, cap, 0, now)
			if err != nil {
				t.Errorf("reserve error: %v", err)
				return
			}
			if n != CapReached {
				atomic.AddInt64(&winners, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if winners != cap {
		t.Fatalf("FCFS: %d winners, want exactly %d (cap must be a hard ceiling)", winners, cap)
	}
	if got, _ := rdb.Get(ctx, Key(strat, now)).Int64(); got != cap {
		t.Fatalf("final counter = %d, want %d", got, cap)
	}
}
