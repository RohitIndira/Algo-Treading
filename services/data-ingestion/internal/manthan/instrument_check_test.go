package manthan

import (
	"context"
	"testing"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

func fakeFeed(rows map[string]string) func(ctx context.Context, key string) (string, error) {
	return func(_ context.Context, key string) (string, error) {
		if v, ok := rows[key]; ok {
			return v, nil
		}
		return "", goredis.Nil
	}
}

// A signal may exist only when the live feed knows the ISIN, agrees with the
// sheet on which company that ISIN is, and publishes a live price for it.
func TestInstrumentCheck(t *testing.T) {
	now := func() time.Time { return time.Date(2026, 10, 22, 11, 0, 0, 0, time.FixedZone("IST", 19800)) }
	feed := fakeFeed(map[string]string{
		"isin:INE338D01028": `{"isin":"INE338D01028","nsecode":"25021"}`,
		"symbol:BODALCHEM":  `{"symbol":"BODALCHEM","isin":"INE338D01028"}`,
		"market:nse:25021":  `{"ltp":153,"last_updated":"2026-10-22T10:59:00+05:30"}`,
		"isin:INE729D01010": `{"isin":"INE729D01010","nsecode":"19937"}`,
		"symbol:KILITCH":    `{"symbol":"KILITCH","isin":"INE729D01010"}`,
		"market:nse:19937":  `{"ltp":208,"timestamp":1792647996937}`,
		"isin:INE000A01010": `{"isin":"INE000A01010","nsecode":"25017"}`,
		"market:nse:25017":  `{"ltp":201.93,"last_updated":"2026-09-29T14:30:09+05:30"}`,
		"isin:INE111B01011": `{"isin":"INE111B01011","nsecode":""}`,
		"isin:INE222C01012": `{"isin":"INE222C01012","nsecode":"777"}`,
		"symbol:WRONGNAME":  `{"symbol":"WRONGNAME","isin":"INE999Z01019"}`,
	})
	c := newInstrumentCheckerWithGetter(feed, now, zap.NewNop())

	cases := []struct {
		name, symbol, isin string
		ok, transient      bool
		reasonHas          string
	}{
		{"live and consistent (RFC3339 stamp)", "BODALCHEM", "INE338D01028", true, false, ""},
		{"live and consistent (epoch ms stamp)", "KILITCH", "INE729D01010", true, false, ""},
		{"ISIN not in feed", "FOO", "INE123X01015", false, false, "not in the price feed"},
		{"malformed ISIN", "FOO", "12345", false, false, "malformed"},
		{"dead price row (23 days)", "OLDCO", "INE000A01010", false, false, "is dead"},
		{"no token on ISIN key", "BAR", "INE111B01011", false, false, "no NSE token"},
		{"no price row for token", "BAZ", "INE222C01012", false, false, "no price row"},
		{"symbol/ISIN mismatch", "WRONGNAME", "INE338D01028", false, false, "mismatch"},
	}
	for _, tc := range cases {
		v := c.Check(context.Background(), tc.symbol, tc.isin)
		if v.OK != tc.ok || v.Transient != tc.transient {
			t.Errorf("%s: got ok=%v transient=%v reason=%q", tc.name, v.OK, v.Transient, v.Reason)
			continue
		}
		if tc.reasonHas != "" && !containsFold(v.Reason, tc.reasonHas) {
			t.Errorf("%s: reason %q lacks %q", tc.name, v.Reason, tc.reasonHas)
		}
	}
}

// A feed outage is not a verdict on the stock: the publisher must let the row
// through (trade-execution re-checks freshness before buying).
func TestInstrumentCheck_TransientOnFeedError(t *testing.T) {
	boom := func(_ context.Context, _ string) (string, error) { return "", context.DeadlineExceeded }
	c := newInstrumentCheckerWithGetter(boom, time.Now, zap.NewNop())
	v := c.Check(context.Background(), "X", "INE338D01028")
	if v.OK || !v.Transient {
		t.Fatalf("feed error must be Transient, got %+v", v)
	}
}

func containsFold(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexFold(s, sub) >= 0)
}

func indexFold(s, sub string) int {
	ls, lsub := []rune(toLower(s)), []rune(toLower(sub))
	for i := 0; i+len(lsub) <= len(ls); i++ {
		match := true
		for j := range lsub {
			if ls[i+j] != lsub[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func toLower(s string) string {
	b := []rune(s)
	for i, r := range b {
		if r >= 'A' && r <= 'Z' {
			b[i] = r + 32
		}
	}
	return string(b)
}
