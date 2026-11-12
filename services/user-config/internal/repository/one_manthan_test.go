package repository

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/apperr"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
)

// DB-backed: a client can hold only one live Manthan strategy. Needs the
// local trading_db (skips otherwise).
func TestCreate_OneLiveManthanPerClient(t *testing.T) {
	db, err := sqlx.Open("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=trading_db sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("trading_db not reachable — skipping (%v)", err)
	}
	t.Cleanup(func() { db.Close() })

	const user = "TUC_ONE_MANTHAN"
	clean := func() {
		_, _ = db.Exec(`DELETE FROM execution_outbox WHERE aggregate_id IN (SELECT strategy_id FROM strategies WHERE user_id = $1)`, user)
		_, _ = db.Exec(`DELETE FROM strategies WHERE user_id = $1`, user)
	}
	clean()
	t.Cleanup(clean)

	repo := NewStrategyRepository(db)
	ctx := context.Background()
	req := func() *models.CreateStrategyRequest {
		return &models.CreateStrategyRequest{UserID: user, StrategyName: "My Manthan Deploy",
			StrategyType: models.StrategyTypeManthan, TradingMode: models.TradingModeLive, ActivateImmediately: true}
	}

	first, err := repo.Create(ctx, req())
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	if _, err := repo.Create(ctx, req()); !errors.Is(err, apperr.ErrDuplicate) {
		t.Fatalf("second create while first is live: want ErrDuplicate, got %v", err)
	}

	// Paused still counts.
	if err := repo.Deactivate(ctx, first.StrategyID, user, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Create(ctx, req()); !errors.Is(err, apperr.ErrDuplicate) {
		t.Fatalf("create while first is paused: want ErrDuplicate, got %v", err)
	}

	// Stopped frees the slot.
	if err := repo.Delete(ctx, first.StrategyID, user, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Create(ctx, req()); err != nil {
		t.Fatalf("create after stop: %v", err)
	}

	// Concurrent double-submit after a stop: exactly one wins.
	var live int
	if err := db.Get(&live, `SELECT count(*) FROM strategies WHERE user_id=$1 AND deleted_at IS NULL AND stopped_at IS NULL`, user); err != nil || live != 1 {
		t.Fatalf("live=%d err=%v", live, err)
	}
	_, _ = db.Exec(`UPDATE strategies SET stopped_at = now(), active = false WHERE user_id = $1`, user)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _, errs[i] = repo.Create(ctx, req()) }(i)
	}
	wg.Wait()
	ok, dup := 0, 0
	for _, e := range errs {
		switch {
		case e == nil:
			ok++
		case errors.Is(e, apperr.ErrDuplicate):
			dup++
		default:
			t.Fatalf("unexpected error: %v", e)
		}
	}
	if ok != 1 || dup != 7 {
		t.Fatalf("concurrent creates: ok=%d dup=%d, want 1/7", ok, dup)
	}
}
