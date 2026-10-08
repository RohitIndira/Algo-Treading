package main

import (
	"testing"
	"time"
)

func TestFullRunEvery(t *testing.T) {
	cases := map[string]time.Duration{
		"": 600 * time.Second, "600": 600 * time.Second, "0": 0, "60": 120 * time.Second,
		"900": 900 * time.Second, "junk": 600 * time.Second, "-5": 600 * time.Second,
	}
	for in, want := range cases {
		if got := fullRunEvery(in); got != want {
			t.Fatalf("fullRunEvery(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestDueForFullRun(t *testing.T) {
	now := time.Date(2026, 10, 8, 13, 0, 0, 0, time.UTC)
	if dueForFullRun(now, now.Add(-time.Hour), 0) {
		t.Fatal("disabled must never be due")
	}
	if !dueForFullRun(now, time.Time{}, 10*time.Minute) {
		t.Fatal("no completed run yet must be due")
	}
	if dueForFullRun(now, now.Add(-9*time.Minute), 10*time.Minute) {
		t.Fatal("9 min after a run must not be due with every=10m")
	}
	if !dueForFullRun(now, now.Add(-10*time.Minute), 10*time.Minute) {
		t.Fatal("10 min after a run must be due with every=10m")
	}
}
