package models

import "testing"

func TestOrderRequestSignalKind(t *testing.T) {
	cases := []struct {
		pctStatus string
		want      string
	}{
		{"below_min", SignalKindMonitoring},   // price-monitor watch → must NOT eat the cap at signal time
		{"within_range", SignalKindImmediate}, // immediate trade → reserves a slot now
		{"", SignalKindImmediate},             // no pct filter → treated as immediate
		{"anything_else", SignalKindImmediate},
	}
	for _, c := range cases {
		o := &OrderRequest{PctChangeStatus: c.pctStatus}
		if got := o.SignalKind(); got != c.want {
			t.Fatalf("SignalKind(pct=%q) = %q, want %q", c.pctStatus, got, c.want)
		}
	}
}
