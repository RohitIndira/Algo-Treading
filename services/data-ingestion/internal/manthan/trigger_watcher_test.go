package manthan

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// fakeTriggerFeed mimics the external price Redis: isin:<ISIN> → {nsecode},
// market:nse:<token> → {ltp, timestamp}.
type fakeTriggerFeed struct {
	tokens map[string]string // ISIN → token
	prices map[string]string // token → raw JSON
	calls  int
}

func (f *fakeTriggerFeed) get(_ context.Context, key string) (string, error) {
	f.calls++
	const p = "isin:"
	if len(key) > len(p) && key[:len(p)] == p {
		if t, ok := f.tokens[key[len(p):]]; ok {
			return fmt.Sprintf(`{"nsecode":"%s"}`, t), nil
		}
	}
	return "", goredis.Nil
}

func (f *fakeTriggerFeed) mget(_ context.Context, keys []string) ([]any, error) {
	f.calls++
	out := make([]any, len(keys))
	for i, k := range keys {
		if v, ok := f.prices[k[len("market:nse:"):]]; ok {
			out[i] = v
		}
	}
	return out, nil
}

func price(ltp float64, at time.Time) string {
	b, _ := json.Marshal(map[string]any{"ltp": ltp, "timestamp": at.UnixMilli()})
	return string(b)
}

// Fixture TDBRA (ATH 98.5, passes every filter) crosses at 100.2 → one row;
// a second scan at a higher price writes nothing new; a stale price never
// triggers; TDBRC (no fundamentals) is not even watched.
func TestTriggerWatcher_WatchScanRecord(t *testing.T) {
	db := openSignalsDB(t)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Date(2099, 1, 2, 11, 0, 0, 0, ist) // Friday 11:00 IST
	today := now.Format("2006-01-02")
	seedDBReader(t, db, today)
	// The fixture pre-seeds TDBRA as already triggered today; remove that so
	// the watcher is the one to trigger it.
	if _, err := db.Exec(`DELETE FROM ref_buy_triggers WHERE symbol = 'TDBRA'`); err != nil {
		t.Fatal(err)
	}

	reader := NewDBReader(db, "", zap.NewNop())
	reader.now = func() time.Time { return now }
	feed := &fakeTriggerFeed{
		tokens: map[string]string{"INETDBRA": "1001", "INETDBRC": "1003"},
		prices: map[string]string{"1001": price(100.2, now.Add(-10*time.Second))},
	}
	w := newTriggerWatcherWithGetters(db, reader, feed.get, feed.mget, func() time.Time { return now }, zap.NewNop())

	cands, err := w.Watchlist(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var mine []TriggerCandidate
	for _, c := range cands {
		if len(c.Symbol) >= 4 && c.Symbol[:4] == "TDBR" {
			mine = append(mine, c)
		}
	}
	if len(mine) != 1 || mine[0].Symbol != "TDBRA" || mine[0].ATHClose != 98.5 || mine[0].ISIN != "INETDBRA" {
		t.Fatalf("watchlist must hold only the filter-passing TDBRA: %+v", mine)
	}

	xs, err := w.Scan(context.Background(), mine)
	if err != nil {
		t.Fatal(err)
	}
	if len(xs) != 1 || xs[0].Symbol != "TDBRA" || xs[0].LTP != 100.2 {
		t.Fatalf("crossing: %+v", xs)
	}
	fresh, err := w.Record(context.Background(), xs)
	if err != nil {
		t.Fatal(err)
	}
	if len(fresh) != 1 || fresh[0] != "TDBRA" {
		t.Fatalf("fresh: %v", fresh)
	}
	var ltp, ath float64
	var src string
	if err := db.QueryRow(`SELECT ltp, ath_close, source FROM ref_buy_triggers WHERE symbol='TDBRA' AND trade_date=$1`, today).Scan(&ltp, &ath, &src); err != nil {
		t.Fatal(err)
	}
	if ltp != 100.2 || ath != 98.5 || src != "live:ext-redis" {
		t.Fatalf("row: %v %v %s", ltp, ath, src)
	}

	// Same day, higher price: still one row, nothing "fresh".
	feed.prices["1001"] = price(104, now.Add(-5*time.Second))
	xs, _ = w.Scan(context.Background(), mine)
	fresh, err = w.Record(context.Background(), xs)
	if err != nil || len(fresh) != 0 {
		t.Fatalf("second cross must be a no-op: fresh=%v err=%v", fresh, err)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM ref_buy_triggers WHERE symbol='TDBRA'`).Scan(&n)
	if n != 1 {
		t.Fatalf("rows=%d want 1", n)
	}

	// After a refresh the watchlist no longer contains TDBRA (already triggered).
	cands, _ = w.Watchlist(context.Background())
	for _, c := range cands {
		if c.Symbol == "TDBRA" {
			t.Fatal("triggered symbol must leave the watchlist")
		}
	}

	// Stale price (20 min old) above ATH does not trigger.
	_, _ = db.Exec(`DELETE FROM ref_buy_triggers WHERE symbol = 'TDBRA'`)
	feed.prices["1001"] = price(120, now.Add(-20*time.Minute))
	xs, _ = w.Scan(context.Background(), mine)
	if len(xs) != 0 {
		t.Fatalf("stale price must not trigger: %+v", xs)
	}
	// Equal to ATH is not a breakout.
	feed.prices["1001"] = price(98.5, now)
	xs, _ = w.Scan(context.Background(), mine)
	if len(xs) != 0 {
		t.Fatalf("ltp == ath must not trigger: %+v", xs)
	}
}

func TestTriggerWatcher_Window(t *testing.T) {
	ist, _ := time.LoadLocation("Asia/Kolkata")
	w := newTriggerWatcherWithGetters(nil, nil, nil, nil, time.Now, zap.NewNop())
	cases := []struct {
		at   time.Time
		want bool
	}{
		{time.Date(2026, 10, 30, 9, 15, 0, 0, ist), false}, // before 09:15:30
		{time.Date(2026, 10, 30, 9, 15, 30, 0, ist), true},
		{time.Date(2026, 10, 30, 12, 0, 0, 0, ist), true},
		{time.Date(2026, 10, 30, 15, 15, 0, 0, ist), true},
		{time.Date(2026, 10, 30, 15, 15, 1, 0, ist), false}, // after 15:15
		{time.Date(2026, 10, 31, 12, 0, 0, 0, ist), false},  // Saturday
	}
	for _, c := range cases {
		if got := w.inWindow(c.at); got != c.want {
			t.Errorf("%s: got %v want %v", c.at.Format("Mon 15:04:05"), got, c.want)
		}
	}
}
