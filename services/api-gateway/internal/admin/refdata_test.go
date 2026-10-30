package admin

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func openSignalsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=signals_db sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Skipf("signals_db not reachable — skipping (%v)", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var ok bool
	if err := db.QueryRow(`SELECT to_regclass('ref_buy_triggers') IS NOT NULL`).Scan(&ok); err != nil || !ok {
		t.Skip("migration 013 not applied locally — skipping")
	}
	return db
}

func TestRefdata_StatusAndHTTP(t *testing.T) {
	sig := openSignalsTestDB(t)
	ist, _ := time.LoadLocation("Asia/Kolkata")
	now := time.Date(2099, 1, 2, 11, 0, 0, 0, ist)
	day := now.Format("2006-01-02")
	clean := func() {
		_, _ = sig.Exec(`DELETE FROM ref_buy_triggers WHERE symbol = 'TRFD1'`)
		_, _ = sig.Exec(`DELETE FROM ref_load_runs WHERE kind = 'trfd_test'`)
	}
	clean()
	t.Cleanup(clean)
	mustExec(t, sig, `INSERT INTO ref_buy_triggers (symbol, trade_date, ltp, ath_close, source) VALUES ('TRFD1', $1, 101, 100, 'live:ext-redis')`, day)
	mustExec(t, sig, `INSERT INTO ref_load_runs (kind, params, rows_written, ok, error, started_at, finished_at)
		VALUES ('trfd_test', 'p', 7, false, 'boom', $1, $1)`, now.Add(-2*time.Hour))

	s := NewRefdataStore(sig)
	s.now = func() time.Time { return now }
	res, err := s.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !res.Configured || res.Date != day || len(res.Tables) != 7 {
		t.Fatalf("status: configured=%v date=%s tables=%d", res.Configured, res.Date, len(res.Tables))
	}
	if len(res.TriggersToday) != 1 || res.TriggersToday[0].Symbol != "TRFD1" || res.TriggersToday[0].LTP != 101 {
		t.Fatalf("triggers: %+v", res.TriggersToday)
	}
	var run *LoadRun
	for i := range res.LoadRuns {
		if res.LoadRuns[i].Kind == "trfd_test" {
			run = &res.LoadRuns[i]
		}
	}
	if run == nil || run.OK || run.Error != "boom" || run.Rows != 7 || run.AgeHours != 2 {
		t.Fatalf("load run: %+v", run)
	}

	// Nil handle → configured=false, not an error.
	if r, err := NewRefdataStore(nil).Status(context.Background()); err != nil || r.Configured {
		t.Fatalf("nil store: %+v %v", r, err)
	}

	adminDB := openAdminTestDB(t)
	seedAdmin(t, adminDB, "TADM_RFD_ADMIN", true)
	h := NewHTTP(NewService(NewStore(adminDB)))
	h.SetRefdata(s)
	root := newRouterFor(t, h)
	tok := elevateViaHTTP(t, root, "TADM_RFD_ADMIN")
	req := httptest.NewRequest("GET", "/api/v1/admin/refdata", nil)
	req.Header.Set(TokenHeader, tok)
	rec := httptest.NewRecorder()
	root.ServeHTTP(rec, req)
	var env struct {
		InfoID string        `json:"infoID"`
		Data   RefdataResult `json:"data"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	if rec.Code != http.StatusOK || env.InfoID != "0" || len(env.Data.TriggersToday) != 1 {
		t.Fatalf("GET /refdata → %d %s", rec.Code, rec.Body.String())
	}
}
