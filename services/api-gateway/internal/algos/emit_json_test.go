package algos

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestKeyStatsJSONCarriesBothPFNames(t *testing.T) {
	d, err := NewStaticCatalog().ByID(context.Background(), "algo_manthan_v1")
	if err != nil { t.Fatal(err) }
	b, _ := json.Marshal(d)
	js := string(b)
	for _, want := range []string{`"profitFactor":2.42`, `"profitFactorPct":2.42`, `"winRatePct":48.78`, `"avgHoldingDays":96`, `"sortino":2.25`, `"totalTradesPct":205`, `"maxDrawdown":-17`} {
		if !strings.Contains(js, want) {
			t.Errorf("emitted JSON missing %s", want)
		}
	}
}
