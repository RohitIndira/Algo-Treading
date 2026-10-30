package admin

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Reference-data status (2026-10-30): the Manthan inputs moved from the
// Google Sheet to signals_db ref_* tables filled by manthan-refdata and the
// live ATH trigger watcher. This read-only view answers the operator's
// morning questions without a psql session: did the 08:35 load run, how
// fresh is each table, which stocks triggered today, what are the three
// index allocations the allocator will size with.
//
//	GET /admin/refdata

// RefdataStore reads signals_db.
type RefdataStore struct {
	db  *sql.DB
	ist *time.Location
	now func() time.Time
}

// NewRefdataStore wraps the gateway's signals_db handle (nil-safe: a nil
// handle makes every call return "not configured").
func NewRefdataStore(signalsDB *sql.DB) *RefdataStore {
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		ist = time.FixedZone("IST", 5*3600+1800)
	}
	return &RefdataStore{db: signalsDB, ist: ist, now: time.Now}
}

// LoadRun is the latest ref_load_runs row for one kind.
type LoadRun struct {
	Kind       string    `json:"kind"`
	Params     string    `json:"params,omitempty"`
	Rows       int       `json:"rows"`
	OK         bool      `json:"ok"`
	Error      string    `json:"error,omitempty"`
	FinishedAt time.Time `json:"finished_at"`
	AgeHours   float64   `json:"age_hours"`
}

// TableFreshness is one ref_* table's row count and newest date.
type TableFreshness struct {
	Table  string  `json:"table"`
	Rows   int64   `json:"rows"`
	Latest *string `json:"latest,omitempty"` // YYYY-MM-DD or YYYY-MM
}

// Trigger is one ref_buy_triggers row for the day.
type Trigger struct {
	Symbol      string    `json:"symbol"`
	TriggeredAt time.Time `json:"triggered_at"`
	LTP         float64   `json:"ltp"`
	ATHClose    float64   `json:"ath_close"`
	Source      string    `json:"source"`
}

// IndexAlloc is the latest ref_index_allocation row per index.
type IndexAlloc struct {
	Index      string  `json:"index"`
	TradeDate  string  `json:"trade_date"`
	Close      float64 `json:"close"`
	EMA21      float64 `json:"ema21"`
	EMA50      float64 `json:"ema50"`
	EMA100     float64 `json:"ema100"`
	Score      int     `json:"score"`
	Allocation float64 `json:"allocation"`
}

// RefdataResult is the whole status payload.
type RefdataResult struct {
	Configured     bool             `json:"configured"`
	Date           string           `json:"date"` // IST day the triggers are for
	LoadRuns       []LoadRun        `json:"load_runs"`
	Tables         []TableFreshness `json:"tables"`
	TriggersToday  []Trigger        `json:"triggers_today"`
	IndexAlloc     []IndexAlloc     `json:"index_allocation"`
	ATHSources     map[string]int   `json:"ath_sources"`         // source → symbols
	UnmappedIndust int              `json:"unmapped_industries"` // API industries on the latest EOD day with no map row
	GeneratedAt    time.Time        `json:"generated_at"`
}

// Status assembles the payload. Missing tables (migration 013 not applied)
// are reported, not fatal.
func (s *RefdataStore) Status(ctx context.Context) (*RefdataResult, error) {
	now := s.now()
	res := &RefdataResult{Date: now.In(s.ist).Format("2006-01-02"), GeneratedAt: now.UTC(),
		LoadRuns: []LoadRun{}, Tables: []TableFreshness{}, TriggersToday: []Trigger{}, IndexAlloc: []IndexAlloc{}, ATHSources: map[string]int{}}
	if s.db == nil {
		return res, nil
	}
	res.Configured = true
	var ok bool
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass('ref_load_runs') IS NOT NULL`).Scan(&ok); err != nil {
		return nil, fmt.Errorf("refdata: %w", err)
	}
	if !ok {
		res.Configured = false
		return res, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT ON (kind) kind, COALESCE(params,''), rows_written, ok, COALESCE(error,''), finished_at
		  FROM ref_load_runs ORDER BY kind, finished_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("refdata runs: %w", err)
	}
	for rows.Next() {
		var r LoadRun
		if err := rows.Scan(&r.Kind, &r.Params, &r.Rows, &r.OK, &r.Error, &r.FinishedAt); err != nil {
			rows.Close()
			return nil, err
		}
		r.FinishedAt = r.FinishedAt.UTC()
		r.AgeHours = round2f(now.Sub(r.FinishedAt).Hours())
		res.LoadRuns = append(res.LoadRuns, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, t := range []struct{ table, dateExpr string }{
		{"ref_equity_eod", "max(trade_date)::text"},
		{"ref_fundamentals", "max(period)"},
		{"ref_price_levels", "max(loaded_at)::date::text"},
		{"ref_ath_close", "max(updated_at)::date::text"},
		{"ref_index_close", "max(trade_date)::text"},
		{"ref_index_allocation", "max(trade_date)::text"},
		{"ref_buy_triggers", "max(trade_date)::text"},
	} {
		var f TableFreshness
		f.Table = t.table
		var latest sql.NullString
		if err := s.db.QueryRowContext(ctx, `SELECT count(*), `+t.dateExpr+` FROM `+t.table).Scan(&f.Rows, &latest); err != nil {
			return nil, fmt.Errorf("refdata %s: %w", t.table, err)
		}
		if latest.Valid {
			v := latest.String
			f.Latest = &v
		}
		res.Tables = append(res.Tables, f)
	}

	rows, err = s.db.QueryContext(ctx, `
		SELECT symbol, triggered_at, ltp, ath_close, source FROM ref_buy_triggers
		 WHERE trade_date = $1 ORDER BY triggered_at`, res.Date)
	if err != nil {
		return nil, fmt.Errorf("refdata triggers: %w", err)
	}
	for rows.Next() {
		var t Trigger
		if err := rows.Scan(&t.Symbol, &t.TriggeredAt, &t.LTP, &t.ATHClose, &t.Source); err != nil {
			rows.Close()
			return nil, err
		}
		t.TriggeredAt = t.TriggeredAt.UTC()
		res.TriggersToday = append(res.TriggersToday, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.db.QueryContext(ctx, `
		SELECT DISTINCT ON (index_name) index_name, trade_date::text, close, ema21, ema50, ema100, score, allocation
		  FROM ref_index_allocation ORDER BY index_name, trade_date DESC`)
	if err != nil {
		return nil, fmt.Errorf("refdata index: %w", err)
	}
	for rows.Next() {
		var a IndexAlloc
		if err := rows.Scan(&a.Index, &a.TradeDate, &a.Close, &a.EMA21, &a.EMA50, &a.EMA100, &a.Score, &a.Allocation); err != nil {
			rows.Close()
			return nil, err
		}
		res.IndexAlloc = append(res.IndexAlloc, a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rows, err = s.db.QueryContext(ctx, `SELECT source, count(*) FROM ref_ath_close GROUP BY source`)
	if err != nil {
		return nil, fmt.Errorf("refdata ath: %w", err)
	}
	for rows.Next() {
		var src string
		var n int
		if err := rows.Scan(&src, &n); err != nil {
			rows.Close()
			return nil, err
		}
		res.ATHSources[src] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if err := s.db.QueryRowContext(ctx, `
		SELECT count(DISTINCT e.industry)
		  FROM ref_equity_eod e
		  LEFT JOIN ref_industry_map m ON lower(m.api_industry) = lower(e.industry)
		 WHERE e.trade_date = (SELECT max(trade_date) FROM ref_equity_eod)
		   AND e.industry IS NOT NULL AND m.api_industry IS NULL`).Scan(&res.UnmappedIndust); err != nil {
		return nil, fmt.Errorf("refdata industry map: %w", err)
	}
	return res, nil
}
