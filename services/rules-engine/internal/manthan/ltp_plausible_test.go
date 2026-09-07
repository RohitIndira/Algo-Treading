package manthan

import "testing"

func TestPlausibleTick(t *testing.T) {
	cases := []struct {
		name      string
		ltp, high float64
		want      bool
	}{
		{"normal below high", 5238, 5289, true},
		{"exactly at high (new high)", 5289, 5289, true},
		{"within epsilon above", 5289.5, 5289, true},
		{"KEI poison 6094 vs 5289", 6094, 5289, false},
		{"IIFL poison 781.35 vs 705", 781.35, 705, false},
		{"high absent — accept", 6094, 0, true},
		{"tiny over-epsilon rejected", 100.2, 100, false},
	}
	for _, c := range cases {
		if got := plausibleTick(c.ltp, c.high); got != c.want {
			t.Errorf("%s: plausibleTick(%.2f,%.2f)=%v want %v", c.name, c.ltp, c.high, got, c.want)
		}
	}
}
