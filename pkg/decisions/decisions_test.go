package decisions

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func validRecord() *Record {
	return &Record{
		EventID:    "evt-1",
		UserID:     "IS19094",
		StrategyID: "2d0bc80a-e9a8-4106-9fbd-217530b5dc66",
		Outcome:    OutcomeRejected,
		Stage:      StageMatch,
		ReasonCode: ReasonConditionsNotMet,
	}
}

// A nil Recorder is the disabled-feature path; every method must tolerate it so
// call sites don't need nil checks.
func TestNilRecorderIsSafe(t *testing.T) {
	var r *Recorder
	r.Record(validRecord())
	r.Close()
	if w, d, f, i := r.Stats(); w != 0 || d != 0 || f != 0 || i != 0 {
		t.Fatalf("nil recorder should report zero stats, got %d %d %d %d", w, d, f, i)
	}
}

// New(nil) must degrade to a no-op rather than panicking, so a service that
// could not reach Postgres still boots.
func TestNewWithNilDBReturnsNil(t *testing.T) {
	if got := New(nil, nil); got != nil {
		t.Fatalf("New(nil) = %v, want nil", got)
	}
}

func TestRecordRejectsInvalid(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Record)
	}{
		{"missing strategy id", func(r *Record) { r.StrategyID = "" }},
		{"missing event id", func(r *Record) { r.EventID = "" }},
		{"missing user id", func(r *Record) { r.UserID = "" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &Recorder{ch: make(chan *Record, 4), done: make(chan struct{})}
			rec := validRecord()
			tc.mutate(rec)
			r.Record(rec)

			if _, _, _, invalid := r.Stats(); invalid != 1 {
				t.Fatalf("invalid counter = %d, want 1", invalid)
			}
			if len(r.ch) != 0 {
				t.Fatalf("invalid record was queued; buffer len = %d", len(r.ch))
			}
		})
	}
}

// The core hot-path guarantee: a full buffer drops rather than blocking the
// caller. If this regresses, news evaluation stalls behind the database.
func TestRecordDropsWhenBufferFullAndNeverBlocks(t *testing.T) {
	r := &Recorder{ch: make(chan *Record, 2), done: make(chan struct{})}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			r.Record(validRecord())
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Record blocked on a full buffer")
	}

	_, dropped, _, _ := r.Stats()
	if dropped != 48 {
		t.Fatalf("dropped = %d, want 48 (50 sent, buffer 2)", dropped)
	}
}

func TestRecordStampsCreatedAt(t *testing.T) {
	r := &Recorder{ch: make(chan *Record, 1), done: make(chan struct{})}
	r.Record(validRecord())

	got := <-r.ch
	if got.CreatedAt.IsZero() {
		t.Fatal("CreatedAt was not stamped")
	}
}

// An explicit CreatedAt must survive, so a caller can record the true decision
// time even if it queues the record slightly later.
func TestRecordPreservesExplicitCreatedAt(t *testing.T) {
	want := time.Date(2026, 7, 23, 12, 38, 47, 0, time.UTC)
	r := &Recorder{ch: make(chan *Record, 1), done: make(chan struct{})}

	rec := validRecord()
	rec.CreatedAt = want
	r.Record(rec)

	if got := <-r.ch; !got.CreatedAt.Equal(want) {
		t.Fatalf("CreatedAt = %v, want %v", got.CreatedAt, want)
	}
}

func TestBuildInsertPlaceholdersAndArgs(t *testing.T) {
	batch := []*Record{validRecord(), validRecord()}

	query, args := buildInsert(batch)

	if len(args) != len(batch)*numCols {
		t.Fatalf("args = %d, want %d", len(args), len(batch)*numCols)
	}
	// Last placeholder must be the final argument index — off-by-one here would
	// produce a runtime "bind message supplies N parameters" error.
	last := len(batch) * numCols
	if !strings.Contains(query, "$"+itoa(last)) {
		t.Fatalf("query missing final placeholder $%d", last)
	}
	if strings.Contains(query, "$"+itoa(last+1)) {
		t.Fatalf("query has placeholder beyond arg count ($%d)", last+1)
	}
	if !strings.HasPrefix(query, "INSERT INTO signal_decisions") {
		t.Fatalf("unexpected query prefix: %q", query[:40])
	}
}

// Optional fields must reach the driver as NULL, not 0 — the UI distinguishes
// "no limit applies" from "limit is zero".
//
// Unset pointer fields arrive as a typed nil (*float64)(nil) rather than an
// untyped nil interface. database/sql's DefaultParameterConverter dereferences
// pointers and converts a nil one to NULL, so that is correct; the assertion
// below checks for "nil, however it is typed".
func TestBuildInsertNilsOptionalFields(t *testing.T) {
	_, args := buildInsert([]*Record{validRecord()})

	// limit_value and actual_value are positions 12 and 13 (1-indexed).
	assertNilArg(t, "limit_value", args[11])
	assertNilArg(t, "actual_value", args[12])
	// symbol/stock_code unset on the fixture → explicit untyped nil.
	assertNilArg(t, "symbol", args[4])
	assertNilArg(t, "stock_code", args[5])
}

// assertNilArg fails unless v is nil, whether that is an untyped nil interface
// or a nil pointer boxed in one. Both become SQL NULL.
func assertNilArg(t *testing.T, name string, v interface{}) {
	t.Helper()
	if v == nil {
		return
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr && rv.IsNil() {
		return
	}
	t.Fatalf("%s = %#v, want nil", name, v)
}

func TestBuildInsertPopulatesValues(t *testing.T) {
	rec := validRecord()
	rec.Symbol = "PVRINOX"
	rec.StockCode = 13147
	rec.Exchange = "NSE"
	rec.LimitValue = F(15)
	rec.ActualValue = F(18.5)
	rec.ImpactScore = I(7)
	rec.CondScores = map[string]float64{"pct_change": 50}

	_, args := buildInsert([]*Record{rec})

	if args[4] != "PVRINOX" {
		t.Fatalf("symbol = %v", args[4])
	}
	if args[5] != int64(13147) {
		t.Fatalf("stock_code = %v", args[5])
	}
	if got, ok := args[11].(*float64); !ok || *got != 15 {
		t.Fatalf("limit_value = %v", args[11])
	}
	// condition_scores is marshalled to a JSON string for the JSONB column.
	s, ok := args[15].(string)
	if !ok || !strings.Contains(s, "pct_change") {
		t.Fatalf("condition_scores = %v, want JSON containing pct_change", args[15])
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
