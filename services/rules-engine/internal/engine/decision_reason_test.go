package engine

import (
	"strings"
	"testing"

	"github.com/RohitIndira/Algo-Treading/pkg/decisions"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/matcher"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

func strategyWithMaxPct(max float64) *models.Strategy {
	s := &models.Strategy{}
	s.Conditions.MaxPctChange = max
	return s
}

// A stock that had already run past the ceiling is a distinct, actionable
// rejection — not a generic "some condition failed" — so it gets its own code.
func TestMatchRejectReasonAboveMax(t *testing.T) {
	res := &matcher.EvaluationResult{
		FailedConditions: []string{"pct_change"},
		PctChangeStatus:  matcher.PctChangeAboveMax,
	}

	code, detail := matchRejectReason(res, strategyWithMaxPct(15))

	if code != decisions.ReasonPctChangeAboveMax {
		t.Fatalf("code = %q, want %q", code, decisions.ReasonPctChangeAboveMax)
	}
	if !strings.Contains(detail, "15.00") {
		t.Fatalf("detail should quote the ceiling, got %q", detail)
	}
}

func TestMatchRejectReasonGeneric(t *testing.T) {
	res := &matcher.EvaluationResult{
		FailedConditions: []string{"impact_score", "category"},
	}

	code, detail := matchRejectReason(res, strategyWithMaxPct(0))

	if code != decisions.ReasonConditionsNotMet {
		t.Fatalf("code = %q, want %q", code, decisions.ReasonConditionsNotMet)
	}
	// The failing conditions must appear — that list is the whole reason an
	// admin opens this row.
	if !strings.Contains(detail, "impact_score") || !strings.Contains(detail, "category") {
		t.Fatalf("detail should list failed conditions, got %q", detail)
	}
}

// below_min is a match, not a rejection (it becomes a price-monitor watch), so
// it must never be reported with the above-max code.
func TestMatchRejectReasonBelowMinIsNotAboveMax(t *testing.T) {
	res := &matcher.EvaluationResult{
		FailedConditions: []string{"impact_score"},
		PctChangeStatus:  matcher.PctChangeBelowMin,
	}

	code, _ := matchRejectReason(res, strategyWithMaxPct(15))

	if code == decisions.ReasonPctChangeAboveMax {
		t.Fatal("below_min must not be reported as PCT_CHANGE_ABOVE_MAX")
	}
}

func TestFailedConditionsDetailEmpty(t *testing.T) {
	if got := failedConditionsDetail(nil); got != "no conditions matched" {
		t.Fatalf("got %q", got)
	}
}

// A nil recorder is the disabled-feature path; recordDecision must tolerate it
// so the engine can run without the audit trail.
func TestRecordDecisionNilRecorderIsSafe(t *testing.T) {
	e := &Engine{}
	e.recordDecision(&models.MarketEvent{EventID: "e1"}, &models.Strategy{}, nil,
		decisions.OutcomeRejected, decisions.ReasonConditionsNotMet, "detail", nil)
}
