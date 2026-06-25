package projector

// Test harness for PositionProjector.Apply across every event type.
//
// Approach: one throwaway Postgres database per `go test` invocation
// (TestMain). All projector tests share it; each test calls resetTables()
// at the start to wipe rows back to a known state — ~5ms/test vs ~250ms
// for create-db-per-test, while still giving full isolation between cases.
//
// The harness expects a live Postgres on localhost:5432 with superuser
// privileges to CREATE/DROP databases. Local dev already has it.
//
// To skip the projector tests (e.g. CI without Postgres):
//   SKIP_PROJECTOR_DB_TESTS=1 go test ./services/rules-engine/...

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

var testDB *sql.DB

func TestMain(m *testing.M) {
	if os.Getenv("SKIP_PROJECTOR_DB_TESTS") == "1" {
		os.Exit(m.Run())
	}

	adminDSN := getEnvOr("RULES_TEST_ADMIN_DSN",
		"host=localhost port=5432 user=postgres password=postgres dbname=postgres sslmode=disable")
	admin, err := sql.Open("postgres", adminDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "projector tests SKIPPED — admin DB open failed: %v\n", err)
		os.Exit(m.Run())
	}
	if err := admin.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "projector tests SKIPPED — admin DB unreachable: %v\n", err)
		_ = admin.Close()
		os.Exit(m.Run())
	}
	defer admin.Close()

	dbName := fmt.Sprintf("rules_proj_test_%d", os.Getpid())
	_, _ = admin.Exec("DROP DATABASE IF EXISTS " + dbName)
	if _, err := admin.Exec("CREATE DATABASE " + dbName); err != nil {
		fmt.Fprintf(os.Stderr, "projector tests SKIPPED — CREATE DATABASE failed: %v\n", err)
		os.Exit(m.Run())
	}

	dsn := fmt.Sprintf("host=localhost port=5432 user=postgres password=postgres dbname=%s sslmode=disable", dbName)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "projector tests FAILED to open test DB: %v\n", err)
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		os.Exit(1)
	}
	if err := applyMigrations(db); err != nil {
		fmt.Fprintf(os.Stderr, "projector tests FAILED to apply migrations: %v\n", err)
		_ = db.Close()
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		os.Exit(1)
	}

	testDB = db
	code := m.Run()

	_ = db.Close()
	_, _ = admin.Exec("DROP DATABASE IF EXISTS " + dbName)
	os.Exit(code)
}

// applyMigrations runs every active migration in order. 004 is skipped
// (one-time backfill that needs pre-existing rows) and 008 is skipped
// (drops a table that wouldn't exist on a fresh DB).
func applyMigrations(db *sql.DB) error {
	migDir, err := findMigrationsDir()
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(migDir)
	if err != nil {
		return fmt.Errorf("read migrations dir %q: %w", migDir, err)
	}
	var files []string
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		if strings.HasPrefix(name, "004_") || strings.HasPrefix(name, "008_") {
			continue
		}
		files = append(files, name)
	}
	sort.Strings(files)
	for _, f := range files {
		sqlBytes, err := os.ReadFile(filepath.Join(migDir, f))
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		if _, err := db.Exec(string(sqlBytes)); err != nil {
			return fmt.Errorf("apply %s: %w", f, err)
		}
	}
	return nil
}

func findMigrationsDir() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	dir := wd
	for i := 0; i < 6; i++ {
		c := filepath.Join(dir, "services", "rules-engine", "migrations")
		if isDir(c) {
			return c, nil
		}
		c = filepath.Join(dir, "..", "migrations")
		if isDir(c) {
			if _, err := os.Stat(filepath.Join(c, "002_manthan_portfolio.sql")); err == nil {
				abs, _ := filepath.Abs(c)
				return abs, nil
			}
		}
		dir = filepath.Dir(dir)
	}
	return "", fmt.Errorf("could not locate services/rules-engine/migrations from %s", wd)
}

func isDir(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

// resetTables truncates the projector-owned tables so a test starts from
// empty. ~5ms on local Postgres.
func resetTables(t *testing.T) {
	t.Helper()
	if testDB == nil {
		t.Skip("projector test DB not initialised (no Postgres at localhost:5432)")
	}
	_, err := testDB.Exec(`
		TRUNCATE TABLE
			manthan_position_events,
			manthan_positions,
			manthan_signal_decisions,
			manthan_cooldown,
			manthan_portfolio_state
		RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("resetTables: %v", err)
	}
}

// proposedDecision is the input shape for insertProposedDecision. Fields
// left blank become NULL in columns that allow it.
type proposedDecision struct {
	SignalID         string
	UserID           string
	StrategyID       string
	Symbol           string
	ISIN             string
	LTP              float64
	EMAAllocPct      float64
	IntendedQty      int
	IntendedInvested float64
	InitialSLTarget  float64
	Industry         string
	MCapBucket       string
	IndexName        string
}

// insertProposedDecision inserts a manthan_signal_decisions row in
// status=PROPOSED — the precondition the projector expects before any
// ENTRY_FILLED arrives.
func insertProposedDecision(t *testing.T, d proposedDecision) {
	t.Helper()
	if d.SignalID == "" {
		t.Fatal("insertProposedDecision: SignalID required")
	}
	_, err := testDB.Exec(`
		INSERT INTO manthan_signal_decisions (
			signal_id, user_id, strategy_id, symbol, isin,
			ltp_at_decision, ema_alloc_pct, intended_qty, intended_invested,
			initial_sl_target, industry, mcap_bucket, index_name,
			status
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, $11, $12, $13,
			'PROPOSED'
		)`,
		d.SignalID, d.UserID, d.StrategyID, d.Symbol, nilIfEmpty(d.ISIN),
		d.LTP, d.EMAAllocPct, d.IntendedQty, d.IntendedInvested,
		d.InitialSLTarget, nilIfEmpty(d.Industry), nilIfEmpty(d.MCapBucket), nilIfEmpty(d.IndexName))
	if err != nil {
		t.Fatalf("insertProposedDecision: %v", err)
	}
}

type decisionRow struct {
	SignalID      string
	Status        string
	FinalStatusAt sql.NullTime
}

func fetchDecision(t *testing.T, signalID string) decisionRow {
	t.Helper()
	var r decisionRow
	err := testDB.QueryRow(`
		SELECT signal_id::text, status, final_status_at
		FROM manthan_signal_decisions WHERE signal_id = $1`, signalID).
		Scan(&r.SignalID, &r.Status, &r.FinalStatusAt)
	if err != nil {
		t.Fatalf("fetchDecision: %v", err)
	}
	return r
}

type positionRow struct {
	SignalID       sql.NullString
	Symbol         string
	Quantity       int
	EntryPrice     float64
	InvestedAmt    float64
	CurrentSL      sql.NullFloat64
	HighSinceEntry sql.NullFloat64
	Status         string
	EventSeq       sql.NullInt64
	BrokerOrderID  sql.NullString
	ExitPrice      sql.NullFloat64
	RealizedPnL    sql.NullFloat64
	ExitReason     sql.NullString
	ExitTime       sql.NullTime
}

func fetchPosition(t *testing.T, signalID string) (positionRow, bool) {
	t.Helper()
	var r positionRow
	err := testDB.QueryRow(`
		SELECT signal_id::text, symbol, quantity, entry_price, invested_amt,
		       current_sl, high_since_entry, status, event_seq, broker_order_id,
		       exit_price, realized_pnl, exit_reason, exit_time
		FROM manthan_positions WHERE signal_id = $1`, signalID).
		Scan(&r.SignalID, &r.Symbol, &r.Quantity, &r.EntryPrice, &r.InvestedAmt,
			&r.CurrentSL, &r.HighSinceEntry, &r.Status, &r.EventSeq, &r.BrokerOrderID,
			&r.ExitPrice, &r.RealizedPnL, &r.ExitReason, &r.ExitTime)
	if err == sql.ErrNoRows {
		return positionRow{}, false
	}
	if err != nil {
		t.Fatalf("fetchPosition: %v", err)
	}
	return r, true
}

// fetchDecisionUserOverride returns the user_override_until column so
// MANUAL_EXIT tests can assert the 3-day allocator cooldown is set.
func fetchDecisionUserOverride(t *testing.T, signalID string) sql.NullTime {
	t.Helper()
	var v sql.NullTime
	err := testDB.QueryRow(`
		SELECT user_override_until FROM manthan_signal_decisions WHERE signal_id = $1`, signalID).Scan(&v)
	if err != nil {
		t.Fatalf("fetchDecisionUserOverride: %v", err)
	}
	return v
}

func countPositionEvents(t *testing.T, signalID string) int {
	t.Helper()
	var n int
	err := testDB.QueryRow(`
		SELECT count(*) FROM manthan_position_events WHERE signal_id = $1`, signalID).
		Scan(&n)
	if err != nil {
		t.Fatalf("countPositionEvents: %v", err)
	}
	return n
}

// newTestUUID returns a deterministic v4-shaped UUID derived from the test
// name + a seed. Deterministic so failures reproduce identically;
// per-test-unique so parallel tests don't collide on PKs.
func newTestUUID(t *testing.T, seed string) string {
	t.Helper()
	src := strings.ReplaceAll(t.Name(), "/", "_") + "_" + seed
	hex := ""
	for _, c := range src {
		hex += fmt.Sprintf("%02x", byte(c))
	}
	for len(hex) < 32 {
		hex += "0"
	}
	hex = hex[:32]
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		hex[0:8], hex[8:12], hex[12:16], hex[16:20], hex[20:32])
}

// newProjector returns a PositionProjector wired to the shared test DB
// with a discard logger so test output stays clean.
func newProjector(t *testing.T) *PositionProjector {
	t.Helper()
	if testDB == nil {
		t.Skip("projector test DB not initialised")
	}
	return NewPositionProjector(testDB, zap.NewNop())
}

// fakeNotifier captures Notify* calls so tests can assert MANUAL_*
// projections fire the right notification.
type fakeNotifier struct {
	manualExits         []string // signal IDs
	manualPartialExits  []string
	manualSLCancelleds  []string
}

func (f *fakeNotifier) NotifyManualExit(_ context.Context, _, _, signalID, _ string, _ int) {
	f.manualExits = append(f.manualExits, signalID)
}
func (f *fakeNotifier) NotifyManualPartialExit(_ context.Context, _, _, signalID, _ string, _, _ int) {
	f.manualPartialExits = append(f.manualPartialExits, signalID)
}
func (f *fakeNotifier) NotifyManualSLCancelled(_ context.Context, _, _, signalID, _ string) {
	f.manualSLCancelleds = append(f.manualSLCancelleds, signalID)
}

// ----------------- utility helpers -----------------

func getEnvOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
