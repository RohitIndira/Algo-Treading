package main

import (
	"reflect"
	"testing"
)

// The watch trigger is the BUY SYMBOL SET, not a content hash — price
// formulas recalc continuously and must never fire the pipeline. Only an
// operator adding/removing a Buy row is an event.
func TestDiffSymbols(t *testing.T) {
	added, removed := diffSymbols(
		[]string{"AEGISLOG", "KEI", "MPSLTD"},
		[]string{"AEGISLOG", "MANORAMA", "MPSLTD", "SHANTIGOLD"},
	)
	if !reflect.DeepEqual(added, []string{"MANORAMA", "SHANTIGOLD"}) {
		t.Errorf("added = %v", added)
	}
	if !reflect.DeepEqual(removed, []string{"KEI"}) {
		t.Errorf("removed = %v", removed)
	}

	// identical sets → no event
	a, r := diffSymbols([]string{"X", "Y"}, []string{"X", "Y"})
	if len(a) != 0 || len(r) != 0 {
		t.Errorf("identical sets must not diff: %v %v", a, r)
	}
	// empty → first population is all-added
	a, _ = diffSymbols(nil, []string{"NEW"})
	if !reflect.DeepEqual(a, []string{"NEW"}) {
		t.Errorf("nil baseline: %v", a)
	}
}
