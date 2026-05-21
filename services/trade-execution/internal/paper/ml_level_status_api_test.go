package paper

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
)

// TestApiLevelStatus_MapsSupersededToCancelled verifies the API-boundary mapping:
// the backend-only SUPERSEDED status is exposed to the frontend as CANCELLED, and
// every other status passes through unchanged.
func TestApiLevelStatus_MapsSupersededToCancelled(t *testing.T) {
	cases := map[string]string{
		models.MLStatusSuperseded: models.MLStatusCancelled, // mapped
		models.MLStatusCancelled:  models.MLStatusCancelled, // unchanged
		models.MLStatusTriggered:  models.MLStatusTriggered, // unchanged
		models.MLStatusPending:    models.MLStatusPending,   // unchanged
		models.MLStatusActive:     models.MLStatusActive,    // unchanged
	}
	for in, want := range cases {
		if got := apiLevelStatus(in); got != want {
			t.Errorf("apiLevelStatus(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestMLLevelWrapper_SerialisesSupersededAsCancelled verifies the full JSON shape
// sent to the frontend: a SUPERSEDED level row must serialise with
// "status":"CANCELLED" and must never leak the literal "SUPERSEDED" — so the UI,
// which only knows the four original statuses, keeps working without changes.
func TestMLLevelWrapper_SerialisesSupersededAsCancelled(t *testing.T) {
	rec := &models.MultiLevelExitRecord{
		EntryOrderID: uuid.New(),
		ExitType:     models.MLExitTypeTP,
		LevelNum:     2,
		Status:       models.MLStatusSuperseded,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	out, err := json.Marshal(wrapMLLevelIST(rec))
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	js := string(out)

	if strings.Contains(js, "SUPERSEDED") {
		t.Fatalf("API payload leaked SUPERSEDED to the frontend: %s", js)
	}
	if !strings.Contains(js, `"status":"CANCELLED"`) {
		t.Fatalf(`expected "status":"CANCELLED" in payload, got: %s`, js)
	}
}
