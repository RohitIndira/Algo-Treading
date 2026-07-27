package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/google/uuid"
)

// slowLTPProvider imitates a Redis that has become slow. GetWatchSnapshot must
// not hold any shard lock across these calls, or Watch/Unwatch (which need the
// write lock) would stall behind an observability read.
type slowLTPProvider struct {
	delay time.Duration
	calls int32
	mu    sync.Mutex
	batch []int // sizes of each GetLTPs batch, to assert batching
}

func (s *slowLTPProvider) GetLTP(context.Context, string, int64) (float64, error) {
	return 0, nil
}

func (s *slowLTPProvider) GetLTPs(_ context.Context, keys []string) (map[string]float64, error) {
	s.mu.Lock()
	s.calls++
	s.batch = append(s.batch, len(keys))
	s.mu.Unlock()

	time.Sleep(s.delay)

	out := make(map[string]float64, len(keys))
	for _, k := range keys {
		out[k] = 100
	}
	return out, nil
}

func (s *slowLTPProvider) GetTickSize(context.Context, string, int64) (float64, error) {
	return 0.05, nil
}

func testOrder(sym string, code int64) *models.Order {
	return &models.Order{
		OrderID:    uuid.New(),
		UserID:     "IS19094",
		StrategyID: "2d0bc80a-e9a8-4106-9fbd-217530b5dc66",
		Symbol:     sym,
		StockCode:  code,
		Exchange:   models.ExchangeNSE,
		Quantity:   1,
		CreatedAt:  time.Now(),
	}
}

// The regression guard: a slow price backend must not block Watch(). Before the
// fix, prices were fetched inline while holding shard.mu.RLock(), so a snapshot
// held that lock across one Redis round-trip per watch and blocked new order
// registration for the whole window during market hours.
//
// Every order deliberately uses the SAME stock code, so all watches — and the
// late Watch() — land in one shard. Spreading them across the 32 shards makes
// the contention probabilistic and the test passes even against the old code.
func TestGetWatchSnapshotDoesNotBlockWatch(t *testing.T) {
	const sharedCode = 1234 // one stock key ⇒ one shard ⇒ guaranteed contention
	provider := &slowLTPProvider{delay: 150 * time.Millisecond}
	pm := NewPriceMonitor(provider, nil, nil, nil, time.Second)

	for i := 0; i < 20; i++ {
		pm.Watch(testOrder("SYM", sharedCode), 100+float64(i), nil)
	}

	snapDone := make(chan struct{})
	go func() {
		defer close(snapDone)
		pm.GetWatchSnapshot("IS19094")
	}()

	// Let the snapshot get inside the shard walk before we contend for the lock.
	time.Sleep(30 * time.Millisecond)

	start := time.Now()
	pm.Watch(testOrder("LATE", sharedCode), 500, nil)
	elapsed := time.Since(start)

	<-snapDone

	// Watch touches only in-memory maps; it must return promptly even while a
	// snapshot is waiting on a slow price backend.
	if elapsed > 50*time.Millisecond {
		t.Fatalf("Watch blocked for %v while a snapshot was in flight — "+
			"price lookups are holding a shard lock", elapsed)
	}
}

// Prices must be resolved in one batched call, not one call per watch.
func TestGetWatchSnapshotBatchesPriceLookups(t *testing.T) {
	provider := &slowLTPProvider{}
	pm := NewPriceMonitor(provider, nil, nil, nil, time.Second)

	// 12 watches over 4 distinct stocks.
	for i := 0; i < 12; i++ {
		pm.Watch(testOrder("SYM", int64(2000+(i%4))), 100, nil)
	}

	snaps := pm.GetWatchSnapshot("IS19094")
	if len(snaps) != 12 {
		t.Fatalf("snapshot len = %d, want 12", len(snaps))
	}

	provider.mu.Lock()
	calls, batches := provider.calls, provider.batch
	provider.mu.Unlock()

	if calls != 1 {
		t.Fatalf("GetLTPs called %d times, want 1 batched call", calls)
	}
	// Deduplicated to the 4 distinct stocks, not all 12 watches.
	if len(batches) != 1 || batches[0] != 4 {
		t.Fatalf("batch sizes = %v, want [4] (unique stock keys only)", batches)
	}
}

func TestGetWatchSnapshotComputesDistance(t *testing.T) {
	provider := &slowLTPProvider{} // always returns 100
	pm := NewPriceMonitor(provider, nil, nil, nil, time.Second)

	pm.Watch(testOrder("SYM", 3001), 125, nil) // target 125, ltp 100 → 20% away

	snaps := pm.GetWatchSnapshot("IS19094")
	if len(snaps) != 1 {
		t.Fatalf("len = %d", len(snaps))
	}
	s := snaps[0]
	if s.CurrentLTP != 100 {
		t.Fatalf("CurrentLTP = %v, want 100", s.CurrentLTP)
	}
	if s.PriceSource != "redis" {
		t.Fatalf("PriceSource = %q, want redis", s.PriceSource)
	}
	if got := s.DistancePct; got < 19.99 || got > 20.01 {
		t.Fatalf("DistancePct = %v, want ~20", got)
	}
}

// With no price backend at all the snapshot must still return every watch,
// flagged as having no price rather than failing.
func TestGetWatchSnapshotWithoutPriceBackend(t *testing.T) {
	pm := NewPriceMonitor(nil, nil, nil, nil, time.Second)
	pm.Watch(testOrder("SYM", 4001), 100, nil)

	snaps := pm.GetWatchSnapshot("")
	if len(snaps) != 1 {
		t.Fatalf("len = %d, want 1", len(snaps))
	}
	if snaps[0].PriceSource != "none" || snaps[0].CurrentLTP != 0 {
		t.Fatalf("got source=%q ltp=%v, want none/0", snaps[0].PriceSource, snaps[0].CurrentLTP)
	}
}

// Concurrent snapshot + watch + unwatch, for the race detector.
func TestGetWatchSnapshotConcurrent(t *testing.T) {
	pm := NewPriceMonitor(&slowLTPProvider{}, nil, nil, nil, time.Second)

	var wg sync.WaitGroup
	ids := make([]uuid.UUID, 0, 50)
	for i := 0; i < 50; i++ {
		o := testOrder("SYM", int64(5000+i))
		ids = append(ids, o.OrderID)
		pm.Watch(o, 100, nil)
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				pm.GetWatchSnapshot("IS19094")
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for _, id := range ids {
			pm.Unwatch(id)
		}
	}()
	wg.Wait()
}
