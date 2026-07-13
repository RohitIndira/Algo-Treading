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

func countKey(e observer.LoggedEntry, key string) int {
	n := 0
	for _, f := range e.Context {
		if f.Key == key {
			n++
		}
	}
	return n
}
