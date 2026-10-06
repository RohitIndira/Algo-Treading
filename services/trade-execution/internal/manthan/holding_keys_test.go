package manthan

import (
	"testing"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
)

// 2026-10-06: a BE/T2T-segment holding is reported as dispSym "BODALCHEM-BE";
// our positions carry "BODALCHEM". Keying only by dispSym made three live
// positions read as "not in holdings — likely manually exited" for weeks.
func TestHoldingKeys_BESeriesResolvesToBaseSymbol(t *testing.T) {
	legs := []indiraClient.HoldingSymbol{
		{Exc: "NSE", DispSym: "BODALCHEM-BE", BaseSym: "BODALCHEM", Series: "BE"},
		{Exc: "BSE", DispSym: "BODALCHEM", BaseSym: "BODALCHEM", Series: "EQ"},
	}
	keys := holdingKeys(legs)
	has := func(k string) bool {
		for _, x := range keys {
			if x == k {
				return true
			}
		}
		return false
	}
	if !has("BODALCHEM") {
		t.Fatalf("base symbol must be a key, got %v", keys)
	}
	if !has("BODALCHEM-BE") {
		t.Fatalf("dispSym must still be a key, got %v", keys)
	}
	// Plain EQ holding: unchanged behaviour.
	if k := holdingKeys([]indiraClient.HoldingSymbol{{Exc: "NSE", DispSym: "AEGISLOG", BaseSym: "AEGISLOG"}}); len(k) == 0 || k[0] != "AEGISLOG" {
		t.Fatalf("EQ holding keys = %v", k)
	}
	// No baseSym from broker: suffix strip still yields the base name.
	if k := holdingKeys([]indiraClient.HoldingSymbol{{Exc: "NSE", DispSym: "LOTUSDEV-BE"}}); !contains(k, "LOTUSDEV") {
		t.Fatalf("suffix strip failed: %v", k)
	}
	// Non-NSE only: falls back to the first leg.
	if k := holdingKeys([]indiraClient.HoldingSymbol{{Exc: "BSE", DispSym: "XYZ", BaseSym: "XYZ"}}); !contains(k, "XYZ") {
		t.Fatalf("fallback failed: %v", k)
	}
	if holdingKeys(nil) != nil {
		t.Fatalf("nil legs must give nil keys")
	}
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}
