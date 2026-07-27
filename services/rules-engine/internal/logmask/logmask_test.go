package logmask

import (
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestWrapHidesIDAndForcesName(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	base := zap.New(core)

	if got := Wrap(base, ""); got != base {
		t.Fatalf("empty name must return the same logger (no-op)")
	}

	masked := Wrap(base, "High_Impact_Score")

	// 1. id + name on the same line: id dropped, name forced, no duplicate.
	masked.Info("match",
		zap.String("strategy_id", "REAL-123"),
		zap.String("strategy_name", "Aggressive Momentum"),
		zap.String("user_id", "U1"))

	// 2. id only (no name): id dropped, name injected.
	masked.Info("skip", zap.String("strategy_id", "REAL-999"), zap.String("event_id", "E9"))

	// 3. no strategy fields: untouched.
	masked.Info("startup", zap.String("port", "9003"))

	entries := logs.All()
	if len(entries) != 3 {
		t.Fatalf("want 3 entries, got %d", len(entries))
	}

	m0 := entries[0].ContextMap()
	if _, ok := m0["strategy_id"]; ok {
		t.Errorf("entry0: strategy_id should be removed, got %v", m0["strategy_id"])
	}
	if m0["strategy_name"] != "High_Impact_Score" {
		t.Errorf("entry0: strategy_name = %v, want High_Impact_Score", m0["strategy_name"])
	}
	if m0["user_id"] != "U1" {
		t.Errorf("entry0: user_id should be preserved, got %v", m0["user_id"])
	}
	if n := countKey(entries[0], "strategy_name"); n != 1 {
		t.Errorf("entry0: want exactly 1 strategy_name field, got %d", n)
	}

	m1 := entries[1].ContextMap()
	if _, ok := m1["strategy_id"]; ok {
		t.Errorf("entry1: strategy_id should be removed")
	}
	if m1["strategy_name"] != "High_Impact_Score" {
		t.Errorf("entry1: strategy_name = %v, want injected High_Impact_Score", m1["strategy_name"])
	}
	if m1["event_id"] != "E9" {
		t.Errorf("entry1: event_id should be preserved, got %v", m1["event_id"])
	}

	m2 := entries[2].ContextMap()
	if _, ok := m2["strategy_name"]; ok {
		t.Errorf("entry2: no strategy field expected, got strategy_name=%v", m2["strategy_name"])
	}
	if m2["port"] != "9003" {
		t.Errorf("entry2: port should be preserved, got %v", m2["port"])
	}
}

// Masking OFF is the normal production configuration (STRATEGY_NAME_LOG_OVERRIDE
// empty), and it is what makes logs traceable: real strategy_id and real
// strategy_name must both survive untouched, including on child loggers built
// with .With(). Without this, an operator grepping for a strategy would silently
// get the placeholder instead — which is exactly what happened while masking was
// left switched on.
func TestWrapDisabledPreservesRealIdentity(t *testing.T) {
	core, logs := observer.New(zapcore.InfoLevel)
	base := zap.New(core)

	lg := Wrap(base, "") // disabled
	if lg != base {
		t.Fatalf("empty name must return the very same logger, got a wrapper")
	}

	lg.Info("match",
		zap.String("strategy_id", "2d0bc80a-e9a8-4106-9fbd-217530b5dc66"),
		zap.String("strategy_name", "July 22 Fin"))

	// Child loggers must not start masking either.
	lg.With(zap.String("strategy_id", "REAL-ID")).
		Info("child", zap.String("strategy_name", "Real Name"))

	entries := logs.All()
	if len(entries) != 2 {
		t.Fatalf("want 2 entries, got %d", len(entries))
	}

	m0 := entries[0].ContextMap()
	if m0["strategy_id"] != "2d0bc80a-e9a8-4106-9fbd-217530b5dc66" {
		t.Errorf("strategy_id = %v, want the real id", m0["strategy_id"])
	}
	if m0["strategy_name"] != "July 22 Fin" {
		t.Errorf("strategy_name = %v, want the real name", m0["strategy_name"])
	}

	m1 := entries[1].ContextMap()
	if m1["strategy_id"] != "REAL-ID" || m1["strategy_name"] != "Real Name" {
		t.Errorf("child logger masked identity: id=%v name=%v", m1["strategy_id"], m1["strategy_name"])
	}
}

func countKey(e observer.LoggedEntry, key string) int {
	n := 0
	for _, f := range e.Context {
		if f.Key == key {
			n++
		}
	}
	return n
}
