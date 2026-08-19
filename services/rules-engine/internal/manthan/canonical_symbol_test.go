package manthan

import "testing"

func TestCanonicalSymbol(t *testing.T) {
	for raw, want := range map[string]string{
		"STK_MODISONLTD_BE_NSE_3325": "MODISONLTD", // 2026-08-19: BE series slipped through positions svc
		"STK_MPSLTD_EQ_NSE_10578":    "MPSLTD",
		"STK_M_M_EQ_NSE_2031":        "M_M",
		"STK_SONACOMS_EQ_BSE_543300": "SONACOMS",
		"MODISONLTD":                 "MODISONLTD",
		"":                           "",
	} {
		if got := canonicalSymbol(raw); got != want {
			t.Errorf("canonicalSymbol(%q) = %q, want %q", raw, got, want)
		}
	}
}
