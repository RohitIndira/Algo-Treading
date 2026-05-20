package multilevel

import (
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

func TestComputeInitialExitQtys(t *testing.T) {
	tests := []struct {
		name      string
		pcts      []float64
		filledQty int32
		want      []int32
	}{
		{
			name:      "exact split two levels",
			pcts:      []float64{50, 50},
			filledQty: 10,
			want:      []int32{5, 5},
		},
		{
			name:      "exact split three levels",
			pcts:      []float64{30, 30, 40},
			filledQty: 10,
			want:      []int32{3, 3, 4},
		},
		{
			name:      "bug repro: 25/25/50 of 10 must total 10",
			pcts:      []float64{25, 25, 50},
			filledQty: 10,
			want:      []int32{2, 2, 6},
		},
		{
			name:      "33/33/34 of 10",
			pcts:      []float64{33, 33, 34},
			filledQty: 10,
			want:      []int32{3, 3, 4},
		},
		{
			name:      "tiny qty",
			pcts:      []float64{50, 50},
			filledQty: 1,
			want:      []int32{0, 1},
		},
		{
			name:      "single level full exit",
			pcts:      []float64{100},
			filledQty: 7,
			want:      []int32{7},
		},
		{
			name:      "sum < 100 misconfig: last absorbs remainder",
			pcts:      []float64{25, 25},
			filledQty: 10,
			want:      []int32{2, 8},
		},
		{
			// Two non-last levels already consume the full filledQty, so the
			// last level is clamped to 0 and a warn is emitted. Sum still == filledQty.
			name:      "sum > 100 misconfig three levels: last clamps to 0",
			pcts:      []float64{60, 60, 60},
			filledQty: 10,
			want:      []int32{6, 4, 0},
		},
		{
			name:      "zero qty",
			pcts:      []float64{25, 25, 50},
			filledQty: 0,
			want:      []int32{0, 0, 0},
		},
		{
			name:      "four-level uneven split",
			pcts:      []float64{20, 20, 20, 40},
			filledQty: 11,
			want:      []int32{2, 2, 2, 5},
		},
	}

	logger := zap.NewNop()
	entryID := uuid.New()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configs := make([]SLTPLevelConfig, len(tc.pcts))
			for i, p := range tc.pcts {
				configs[i] = SLTPLevelConfig{LevelNum: i + 1, PricePct: 1, QtyPct: p}
			}
			got := computeInitialExitQtys(configs, tc.filledQty, logger, "SL", entryID)

			if len(got) != len(tc.want) {
				t.Fatalf("len mismatch: got %d, want %d (got=%v)", len(got), len(tc.want), got)
			}
			var sum int32
			for i, q := range got {
				if q != tc.want[i] {
					t.Errorf("idx %d: got %d, want %d (full got=%v)", i, q, tc.want[i], got)
				}
				sum += q
			}

			// Sum invariant: must always equal filledQty. Even in the overflow
			// case, clamping happens on intermediate levels so the last level
			// still absorbs whatever's left to keep Σ == filledQty.
			if sum != tc.filledQty {
				t.Errorf("sum invariant: got %d, want %d", sum, tc.filledQty)
			}
		})
	}
}

func TestComputeInitialExitQtysEmpty(t *testing.T) {
	got := computeInitialExitQtys(nil, 10, zap.NewNop(), "SL", uuid.New())
	if got != nil {
		t.Errorf("expected nil for empty configs, got %v", got)
	}
}
