package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// One day across all four databases:
//
//	TDSIG1  ELIGIBLE, triggered, published
//	        TDS_A  decision DISPATCHED → inbox DONE → order FILLED   → FILLED
//	        TDS_B  decision REJECTED "portfolio full"                → SKIPPED
//	        TDS_C  decision DISPATCHED → inbox DLQ "too close…"     → FAILED
//	        TDS_D  order REJECTED AU004                              → REFUSED
//	TDSIG2  FILTER_REJECTED "PAT <= 0", no clients
func TestDailySignals(t *testing.T) {
	sig := openSignalsTestDB(t)
	trading, exec, pos := openFleetDBs(t)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	n := time.Now().In(ist)
	day := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, ist)
	date := day.Format("2006-01-02")
	at := func(h, m int) time.Time { return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute) }
	strat := "aaaaaaaa-7777-2222-3333-000000000d51"

	clean := func() {
		_, _ = sig.Exec(`DELETE FROM manthan_stocks WHERE symbol LIKE 'TDSIG%'`)
		_, _ = sig.Exec(`DELETE FROM manthan_signals WHERE symbol LIKE 'TDSIG%'`)
		_, _ = sig.Exec(`DELETE FROM ref_buy_triggers WHERE symbol LIKE 'TDSIG%'`)
		_, _ = trading.Exec(`DELETE FROM manthan_signal_decisions WHERE symbol LIKE 'TDSIG%'`)
		_, _ = exec.Exec(`DELETE FROM signal_inbox WHERE payload->>'symbol' LIKE 'TDSIG%'`)
		_, _ = exec.Exec(`DELETE FROM manthan_orders WHERE symbol LIKE 'TDSIG%'`)
	}
	clean()
	t.Cleanup(clean)

	mustExec(t, sig, `INSERT INTO manthan_stocks (run_date, symbol, company_name, status, reason, industry, mcap_bucket, index_name, market_cap, pe, pat, fscore, latest_price, ath_close, allocation, ath_entry)
		VALUES ($1::date, 'TDSIG1', 'Sig One Ltd', 'ELIGIBLE', NULL, 'Test', 'SMALL', 'NTYSLCP250', 4800, 25, 187, 68, 740.1, 734.95, 0.2, 'Buy'),
		       ($1::date, 'TDSIG2', 'Sig Two Ltd', 'FILTER_REJECTED', 'PAT <= 0', 'Test', 'SMALL', 'NTYSLCP250', 900, 20, 0, 70, 50, 60, 0.2, 'Buy')`, date)
	mustExec(t, sig, `INSERT INTO ref_buy_triggers (symbol, trade_date, triggered_at, ltp, ath_close, source) VALUES ('TDSIG1', $1::date, $2, 740.1, 734.95, 'live:ext-redis')`, date, at(10, 5))
	mustExec(t, sig, `INSERT INTO manthan_signals (run_date, symbol, published_to_kafka_at, first_seen_at) VALUES ($1::date, 'TDSIG1', $2, $2)`, date, at(10, 6))

	decN := 0
	dec := func(sigID, user, status, reason string) {
		decN++ // uq_msd_entry_per_attempt: (strategy, symbol, decided_at) must differ
		var r any
		if reason != "" {
			r = reason
		}
		mustExec(t, trading, `INSERT INTO manthan_signal_decisions (signal_id, user_id, strategy_id, symbol, ltp_at_decision, intended_qty,
			intended_invested, initial_sl_target, status, rejection_reason, decided_at)
			VALUES ($1::uuid, $2, $3::uuid, 'TDSIG1', 740.1, 27, 19983, 592, $4, $5, $6)`, sigID, user, strat, status, r, at(10, 6).Add(time.Duration(decN)*time.Second))
	}
	dec("aaaaaaaa-7777-2222-3333-0000000000a1", "TDS_A", "DISPATCHED", "")
	dec("aaaaaaaa-7777-2222-3333-0000000000b1", "TDS_B", "REJECTED", "portfolio full (10/10 slots)")
	dec("aaaaaaaa-7777-2222-3333-0000000000c1", "TDS_C", "DISPATCHED", "")
	inbox := func(sigID, user, status, class, lastErr string, off int) {
		mustExec(t, exec, `INSERT INTO signal_inbox (signal_id, user_id, order_type, payload, kafka_topic, kafka_partition, kafka_offset, status, attempts, last_error_class, last_error, created_at)
			VALUES ($1, $2, 'MANTHAN_ENTRY', jsonb_build_object('symbol','TDSIG1'), 't', 0, $3, $4, 1, NULLIF($5,''), NULLIF($6,''), $7)`,
			sigID, user, off, status, class, lastErr, at(10, 7))
	}
	inbox("aaaaaaaa-7777-2222-3333-0000000000a1", "TDS_A", "DONE", "", "", 9001)
	inbox("aaaaaaaa-7777-2222-3333-0000000000c1", "TDS_C", "DLQ", "POISON", "pre-check: too close to market close (after 15:20)", 9002)
	order := func(user, status, lastErr string, filled int, avg any) {
		mustExec(t, exec, `INSERT INTO manthan_orders (strategy_id, user_id, symbol, order_type, order_side, qty, filled_qty, avg_fill_price, status, last_error, created_at, updated_at)
			VALUES ($1::uuid, $2, 'TDSIG1', 'LIMIT_BUY', 'BUY', 27, $3, $4::numeric, $5, NULLIF($6,''), $7, $7)`, strat, user, filled, avg, status, lastErr, at(10, 8))
	}
	order("TDS_A", "FILLED", "", 27, 739.5)
	order("TDS_D", "REJECTED", "AU004: Unauthorized access, invalid token", 0, nil)

	e := NewExplorer(sig, NewFleetStore(trading, exec, pos))
	res, err := e.DailySignals(context.Background(), date)
	if err != nil {
		t.Fatal(err)
	}
	var s1, s2 *DailySignal
	for i := range res.Signals {
		switch res.Signals[i].Symbol {
		case "TDSIG1":
			s1 = &res.Signals[i]
		case "TDSIG2":
			s2 = &res.Signals[i]
		}
	}
	if s1 == nil || s2 == nil {
		t.Fatalf("signals missing: %+v", res.Signals)
	}
	if s1.Verdict != "ELIGIBLE" || s1.Trigger == nil || s1.Trigger.LTP != 740.1 || s1.PublishedAt == nil || s1.Allocation != 0.2 {
		t.Fatalf("TDSIG1: %+v", s1)
	}
	if s2.Verdict != "FILTER_REJECTED" || s2.Reason != "PAT <= 0" || len(s2.Clients) != 0 {
		t.Fatalf("TDSIG2: %+v", s2)
	}
	got := map[string]ClientOutcome{}
	for _, c := range s1.Clients {
		got[c.UserID] = c
	}
	want := map[string]string{"TDS_A": "FILLED", "TDS_B": "SKIPPED", "TDS_C": "FAILED", "TDS_D": "REFUSED"}
	for u, o := range want {
		if got[u].Outcome != o {
			t.Fatalf("%s outcome %q want %q (%+v)", u, got[u].Outcome, o, got[u])
		}
	}
	if got["TDS_B"].Note != "portfolio full (10/10 slots)" || got["TDS_D"].Note != "Broker session expired — client must log in again" {
		t.Fatalf("notes: B=%q D=%q", got["TDS_B"].Note, got["TDS_D"].Note)
	}
	if got["TDS_A"].AvgPrice == nil || *got["TDS_A"].AvgPrice != 739.5 || got["TDS_A"].FilledQty != 27 {
		t.Fatalf("TDS_A fill: %+v", got["TDS_A"])
	}

	if _, err := e.DailySignals(context.Background(), "30-10-2026"); err == nil {
		t.Fatal("bad date must error")
	}

	// HTTP.
	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_DSIG_ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetExplorer(e)
	root := newRouterFor(t, h)
	tok := elevateViaHTTP(t, root, "TADM_DSIG_ADMIN")
	for path, code := range map[string]int{"/api/v1/admin/signals/daily?date=" + date: 200, "/api/v1/admin/signals/daily?date=bad": 400} {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set(TokenHeader, tok)
		rec := httptest.NewRecorder()
		root.ServeHTTP(rec, req)
		if rec.Code != code {
			t.Fatalf("%s → %d %s", path, rec.Code, rec.Body.String())
		}
		if code == http.StatusOK {
			var env struct {
				Data DailySignalsResult `json:"data"`
			}
			_ = json.Unmarshal(rec.Body.Bytes(), &env)
			if env.Data.Counts.Triggers < 1 || env.Data.Counts.Filled < 1 {
				t.Fatalf("http counts: %+v", env.Data.Counts)
			}
		}
	}
}
