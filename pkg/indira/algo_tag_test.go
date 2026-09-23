package indira

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// The SEBI algo tag (NSE approval 162933) must land on the order BODY as
// "algoId" from MANTHAN_ALGO_ID, on every order type, without the caller
// setting it. Field NAME is compliance-critical — pin it.
func TestApplyAlgoTag_StampsBodyFromEnv(t *testing.T) {
	os.Setenv("MANTHAN_ALGO_ID", "162933")
	defer os.Unsetenv("MANTHAN_ALGO_ID")

	// Place order
	po := &PlaceOrderRequest{Symbol: "STK_IDEA_EQ_NSE_14366", OrdAction: "BUY"}
	applyAlgoTag(&po.AlgoID, &po.AlgoCategory)
	if po.AlgoID != "162933" {
		t.Fatalf("PlaceOrder AlgoID = %q, want \"162933\"", po.AlgoID)
	}
	b, _ := json.Marshal(po)
	// Exchange spec: the tag is a STRING on the wire — "algoId":"162933".
	if !strings.Contains(string(b), `"algoId":"162933"`) {
		t.Fatalf("body must serialize \"algoId\":\"162933\" (string), got %s", b)
	}
}

// A caller that explicitly set an id is not overridden; no env = omitted.
func TestApplyAlgoTag_RespectsExplicitAndEmpty(t *testing.T) {
	os.Unsetenv("MANTHAN_ALGO_ID")
	po := &PlaceOrderRequest{}
	applyAlgoTag(&po.AlgoID, &po.AlgoCategory)
	b, _ := json.Marshal(po)
	if strings.Contains(string(b), "algoId") {
		t.Fatalf("no env → algoId must be omitted, got %s", b)
	}
	explicit := &PlaceOrderRequest{AlgoID: "999"}
	os.Setenv("MANTHAN_ALGO_ID", "162933")
	defer os.Unsetenv("MANTHAN_ALGO_ID")
	applyAlgoTag(&explicit.AlgoID, &explicit.AlgoCategory)
	if explicit.AlgoID != "999" {
		t.Fatalf("explicit AlgoID overridden: %s", explicit.AlgoID)
	}
}
