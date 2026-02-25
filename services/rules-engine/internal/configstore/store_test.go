package configstore

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

func mkStrategy(userID, strategyID string, version uint64, active bool) *models.Strategy {
	return &models.Strategy{
		UserID:       userID,
		StrategyID:   strategyID,
		Version:      version,
		Active:       active,
		StrategyName: "n",
		Conditions: models.Conditions{
			MatchAllNews:   true,
			ImpactScoreMin: 1,
			ImpactScoreMax: 2,
		},
		TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1, Exchange: "NSE"},
		RiskLimits:  models.RiskLimits{MaxDailyTrades: 1},
	}
}

func TestWriter_ApplyUpsert_AddsToActive(t *testing.T) {
	st := NewStore()
	w := NewWriter(st)

	applied, err := w.ApplyUpsert(mkStrategy("u1", "s1", 1, true), 1)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !applied {
		t.Fatalf("expected applied")
	}

	snap := st.Snapshot()
	if len(snap.AllActive) != 1 {
		t.Fatalf("expected 1 active, got %d", len(snap.AllActive))
	}
	if snap.AllActive[0].StrategyID != "s1" {
		t.Fatalf("unexpected strategy")
	}
}

func TestWriter_ApplyUpsert_VersionCheck_StaleRejected(t *testing.T) {
	st := NewStore()
	w := NewWriter(st)

	_, _ = w.ApplyUpsert(mkStrategy("u1", "s1", 5, true), 5)
	applied, err := w.ApplyUpsert(mkStrategy("u1", "s1", 4, true), 4)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if applied {
		t.Fatalf("expected stale rejected")
	}
	if st.Snapshot().AllActive[0].Version != 5 {
		t.Fatalf("expected version 5")
	}
}

func TestWriter_ApplyUpsert_TombstoneCheck_ResurrectionPrevented(t *testing.T) {
	st := NewStore()
	w := NewWriter(st)

	_, _ = w.ApplyUpsert(mkStrategy("u1", "s1", 10, true), 10)
	_, _ = w.ApplyDelete("u1", "s1", 11)

	// attempt resurrection with older version
	applied, err := w.ApplyUpsert(mkStrategy("u1", "s1", 10, true), 10)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if applied {
		t.Fatalf("expected resurrection prevented")
	}
}

func TestWriter_ApplyPause_MovesActiveToPaused(t *testing.T) {
	st := NewStore()
	w := NewWriter(st)
	_, _ = w.ApplyUpsert(mkStrategy("u1", "s1", 1, true), 1)

	applied, err := w.ApplyPause("u1", "s1", 2)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !applied {
		t.Fatalf("expected applied")
	}

	snap := st.Snapshot()
	if len(snap.AllActive) != 0 {
		t.Fatalf("expected 0 active")
	}
	if _, ok := snap.ByUser["u1"].Paused["s1"]; !ok {
		t.Fatalf("expected paused")
	}
}

func TestWriter_ApplyResume_MovesPausedToActive(t *testing.T) {
	st := NewStore()
	w := NewWriter(st)
	_, _ = w.ApplyUpsert(mkStrategy("u1", "s1", 1, true), 1)
	_, _ = w.ApplyPause("u1", "s1", 2)

	applied, err := w.ApplyResume("u1", "s1", 3)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !applied {
		t.Fatalf("expected applied")
	}
	if len(st.Snapshot().AllActive) != 1 {
		t.Fatalf("expected active")
	}
}

func TestWriter_ApplyResume_NotInPaused_NoOp(t *testing.T) {
	st := NewStore()
	w := NewWriter(st)

	applied, err := w.ApplyResume("u1", "s1", 1)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if applied {
		t.Fatalf("expected no-op")
	}
}

func TestWriter_ApplyDelete_RemovesFromBoth_SetsTombstone(t *testing.T) {
	st := NewStore()
	w := NewWriter(st)
	_, _ = w.ApplyUpsert(mkStrategy("u1", "s1", 1, true), 1)
	_, _ = w.ApplyPause("u1", "s1", 2)

	applied, err := w.ApplyDelete("u1", "s1", 3)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !applied {
		t.Fatalf("expected applied")
	}

	snap := st.Snapshot()
	if len(snap.ByUser["u1"].Active)+len(snap.ByUser["u1"].Paused) != 0 {
		t.Fatalf("expected removed")
	}

	// older upsert should be rejected
	applied, err = w.ApplyUpsert(mkStrategy("u1", "s1", 2, true), 2)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if applied {
		t.Fatalf("expected rejected by tombstone")
	}
}

func TestSnapshot_AllActive_DeterministicOrder(t *testing.T) {
	s := &Snapshot{ByUser: map[string]UserView{}}
	s.ByUser["u2"] = UserView{Active: map[string]*models.Strategy{"b": mkStrategy("u2", "b", 1, true), "a": mkStrategy("u2", "a", 1, true)}}
	s.ByUser["u1"] = UserView{Active: map[string]*models.Strategy{"d": mkStrategy("u1", "d", 1, true), "c": mkStrategy("u1", "c", 1, true)}}
	Finalize(s)

	got := ""
	for _, st := range s.AllActive {
		got += st.UserID + ":" + st.StrategyID + ","
	}
	// userIDs sorted u1 then u2; strategyIDs sorted within user
	want := "u1:c,u1:d,u2:a,u2:b,"
	if got != want {
		t.Fatalf("want %q got %q", want, got)
	}
}

func TestStore_ConcurrentReads_NeverBlock(t *testing.T) {
	st := NewStore()
	w := NewWriter(st)
	_, _ = w.ApplyUpsert(mkStrategy("u1", "s1", 1, true), 1)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_ = st.Snapshot()
				}
			}
		}()
	}

	// meanwhile write repeatedly
	for v := uint64(2); v < 100; v++ {
		_, _ = w.ApplyUpsert(mkStrategy("u1", "s1", v, true), v)
	}

	wg.Wait()
}

func TestBulkLoad_LoadsAllActive_SkipsInactive(t *testing.T) {
	cs := New()

	cs.BulkLoad([]*models.StrategyConfig{
		{UserID: "u1", StrategyID: "a", Active: true, Version: 1, TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1}, RiskLimits: models.RiskLimits{MaxDailyTrades: 1}},
		{UserID: "u1", StrategyID: "b", Active: false, Version: 1, TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1}, RiskLimits: models.RiskLimits{MaxDailyTrades: 1}},
	})

	snap := cs.GetSnapshot()
	if len(snap.AllActive) != 1 {
		t.Fatalf("expected 1 active, got %d", len(snap.AllActive))
	}
	if snap.AllActive[0].StrategyID != "a" {
		t.Fatalf("expected strategy a")
	}
}
