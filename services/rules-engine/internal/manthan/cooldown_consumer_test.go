package manthan

// Runs against real local trading_db. Proves:
//
//   TestCooldown_UpsertOnManthanSLTrigger     — the primary happy path
//   TestCooldown_SkipsUserManualExit          — filter honours origin
//   TestCooldown_SkipsManualExitReason        — filter honours exit_reason
//   TestCooldown_UpsertIsIdempotent           — re-delivery = no dup row
//   TestCooldown_UpsertResetsClearedFlag      — re-exit reactivates the row

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// posExitedEvent builds a POSITION_EXITED JSON envelope for tests.
func posExitedEvent(strategyID, symbol, origin, exitReason string, exitPrice float64) []byte {
	env := positionEventEnvelope{
		EventID:      strategyID + "-" + symbol + "-x",
		EventType:    "POSITION_EXITED",
		ProducedAtMs: time.Now().UnixMilli(),
		PositionID:   "00000000-0000-0000-0000-000000000001",
		Origin:       origin,
		UserID:       "S4450",
		StrategyID:   strategyID,
		Symbol:       symbol,
		Action:       "EXIT",
		Price:        exitPrice,
		Quantity:     100,
		ExitReason:   exitReason,
		RealizedPnL:  (exitPrice - 500) * 100,
	}
	b, _ := json.Marshal(env)
	return b
}

// openCooldownTestDB opens the trading_db + TRUNCATEs manthan_cooldown.
func openCooldownTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres",
		"host=localhost port=5432 user=postgres password=postgres dbname=trading_db sslmode=disable")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("trading_db not reachable — skipping DB tests (%v)", err)
	}
	if _, err := db.Exec(`TRUNCATE manthan_cooldown RESTART IDENTITY`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return db
}

func newConsumer(t *testing.T, db *sql.DB) *CooldownConsumer {
	t.Helper()
	logger, _ := zap.NewDevelopment()
	return &CooldownConsumer{db: db, logger: logger}
}

// runOne is a shim that dispatches one Kafka message body into handleMessage
// without opening a real reader.
func (c *CooldownConsumer) runOne(ctx context.Context, body []byte) error {
	return c.handleMessage(ctx, kafka.Message{Value: body, Offset: 1})
}

// -- tests ---------------------------------------------------------------

func TestCooldown_UpsertOnManthanSLTrigger(t *testing.T) {
	db := openCooldownTestDB(t)
	defer db.Close()
	c := newConsumer(t, db)

	sid := "22222222-2222-2222-2222-222222222222"
	body := posExitedEvent(sid, "IDEA", "MANTHAN", "SL_TRIGGER", 13.50)
	if err := c.runOne(context.Background(), body); err != nil {
		t.Fatalf("handle: %v", err)
	}

	var (
		count      int
		exitPrice  float64
		reentryBel float64
		cleared    bool
	)
	if err := db.QueryRow(`
		SELECT COUNT(*), MAX(exit_price), MAX(reentry_below), bool_or(cleared)
		FROM manthan_cooldown
		WHERE strategy_id=$1::uuid AND symbol=$2`, sid, "IDEA").
		Scan(&count, &exitPrice, &reentryBel, &cleared); err != nil {
		t.Fatalf("select: %v", err)
	}
	if count != 1 {
		t.Errorf("count: got %d, want 1", count)
	}
	if exitPrice != 13.50 {
		t.Errorf("exit_price: got %v, want 13.50", exitPrice)
	}
	// reentry_below = exit_price × 0.80 = 13.50 × 0.80 = 10.80
	if diff := reentryBel - 10.80; diff > 0.01 || diff < -0.01 {
		t.Errorf("reentry_below: got %v, want 10.80", reentryBel)
	}
	if cleared {
		t.Errorf("cleared: got true, want false (fresh cooldown must be un-cleared)")
	}
}

func TestCooldown_SkipsUserManualExit(t *testing.T) {
	db := openCooldownTestDB(t)
	defer db.Close()
	c := newConsumer(t, db)

	body := posExitedEvent(
		"22222222-2222-2222-2222-222222222222", "IDEA", "USER_MANUAL", "SL_TRIGGER", 13.50,
	)
	if err := c.runOne(context.Background(), body); err != nil {
		t.Fatalf("handle: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM manthan_cooldown`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("USER_MANUAL exit created a cooldown row (n=%d) — filter is broken", n)
	}
}

func TestCooldown_SkipsManualExitReason(t *testing.T) {
	db := openCooldownTestDB(t)
	defer db.Close()
	c := newConsumer(t, db)

	body := posExitedEvent(
		"22222222-2222-2222-2222-222222222222", "IDEA", "MANTHAN", "MANUAL_EXIT", 13.50,
	)
	if err := c.runOne(context.Background(), body); err != nil {
		t.Fatalf("handle: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM manthan_cooldown`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 0 {
		t.Errorf("MANUAL_EXIT reason created cooldown row (n=%d) — should be skipped per Manthan spec", n)
	}
}

func TestCooldown_UpsertIsIdempotent(t *testing.T) {
	db := openCooldownTestDB(t)
	defer db.Close()
	c := newConsumer(t, db)

	sid := "22222222-2222-2222-2222-222222222222"
	body := posExitedEvent(sid, "IDEA", "MANTHAN", "SL_TRIGGER", 13.50)
	for i := 0; i < 3; i++ {
		if err := c.runOne(context.Background(), body); err != nil {
			t.Fatalf("iter %d: %v", i, err)
		}
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM manthan_cooldown WHERE strategy_id=$1::uuid`, sid).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 row after 3 identical events, got %d — UPSERT is broken", n)
	}
}

func TestCooldown_UpsertResetsClearedFlag(t *testing.T) {
	db := openCooldownTestDB(t)
	defer db.Close()
	c := newConsumer(t, db)

	sid := "22222222-2222-2222-2222-222222222222"
	// Insert an already-cleared row
	if _, err := db.Exec(`
		INSERT INTO manthan_cooldown (strategy_id, symbol, ath_at_exit, exit_price, reentry_below, cleared, cleared_at)
		VALUES ($1::uuid, 'IDEA', 20.00, 15.00, 12.00, TRUE, NOW())`, sid); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A fresh SL_TRIGGER should reactivate the cooldown
	body := posExitedEvent(sid, "IDEA", "MANTHAN", "SL_TRIGGER", 13.50)
	if err := c.runOne(context.Background(), body); err != nil {
		t.Fatalf("handle: %v", err)
	}

	var cleared bool
	var reentryBel float64
	if err := db.QueryRow(`SELECT cleared, reentry_below FROM manthan_cooldown WHERE strategy_id=$1::uuid AND symbol=$2`,
		sid, "IDEA").Scan(&cleared, &reentryBel); err != nil {
		t.Fatalf("select: %v", err)
	}
	if cleared {
		t.Errorf("cleared: got true after fresh SL_TRIGGER — must be reset to false")
	}
	if diff := reentryBel - 10.80; diff > 0.01 || diff < -0.01 {
		t.Errorf("reentry_below: got %v, want 10.80 (from fresh 13.50 × 0.80)", reentryBel)
	}
}
