package manthan

import "testing"

// The bucket-aware stop distance (2026-09-28 product rule): LARGE trails
// at 10%, everything else keeps the configured pct with the zero-guard.
func TestBucketStopLossPct(t *testing.T) {
	cases := []struct {
		bucket string
		conf   float64
		want   float64
	}{
		{"LARGE", 20, 10},  // the new rule
		{"large", 20, 10},  // case-insensitive
		{" LARGE ", 20, 10},
		{"MID", 20, 20},
		{"SMALL", 20, 20},
		{"", 20, 20},        // unknown bucket → configured
		{"LARGE", 0, 10},    // LARGE wins even with broken config
		{"SMALL", 0, 20},    // zero-guard falls back to default
		{"SMALL", 150, 20},  // implausible config → default
	}
	for _, c := range cases {
		if got := bucketStopLossPct(c.bucket, c.conf); got != c.want {
			t.Errorf("bucketStopLossPct(%q, %v) = %v, want %v", c.bucket, c.conf, got, c.want)
		}
	}
}
