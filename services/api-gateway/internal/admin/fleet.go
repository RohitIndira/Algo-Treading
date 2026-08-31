package admin

// M2 — Fleet overview: the console landing data.
//
//	2.1 GET /admin/fleet     — one row per (user, strategy): status, open
//	    positions, protection coverage (armed/deferred/NAKED), realized
//	    P&L, credential age.
//	2.2 GET /admin/attention — the ranked "needs a human" queue: dead
//	    sessions, naked positions, AMO give-ups, DLQ'd signals, reconciler
//	    activity. The queue that existed all August as an engineer grepping.
//
// Reads span three databases through handles the gateway already holds:
// trading_db (strategies), execution_db (orders, credentials, inbox),
// positions_db (position book). All queries are read-only; rows are
// assembled in Go — no cross-DB SQL exists, so the joins live here.

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// credStaleAfter: an Indira session survives roughly one trading day; a
// token older than this cannot be serving a live session (2026-08-27:
// "fresh" 00:39 token was dead by 07:00 — age is a heuristic, the live
// probe in M3 is the truth; the fleet dot is the early warning).
const credStaleAfter = 20 * time.Hour

// FleetStore reads the business tables. Deliberately separate from Store
// (admin_* tables, grant-hardened connection): fleet reads ride the shared
// handles until a read-only business role lands.
type FleetStore struct {
	tradingDB *sql.DB // strategies
	execDB    *sql.DB // manthan_orders, user_credentials, signal_inbox, manthan_order_events
	posDB     *sql.DB // positions
	ist       *time.Location
	now       func() time.Time
}

func NewFleetStore(tradingDB, execDB, posDB *sql.DB) *FleetStore {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil || loc == nil {
		loc = time.FixedZone("IST", 5*3600+1800)
	}
	return &FleetStore{tradingDB: tradingDB, execDB: execDB, posDB: posDB, ist: loc, now: time.Now}
}

// FleetRow is one user×strategy line of the grid.
type FleetRow struct {
	UserID       string     `json:"user_id"`
	StrategyID   string     `json:"strategy_id"`
	StrategyType string     `json:"strategy_type"`
	TradingMode  string     `json:"trading_mode"`
	Active       bool       `json:"active"`
	StoppedAt    *time.Time `json:"stopped_at,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`

	OpenPositions int      `json:"open_positions"` // DISTINCT active symbols
	Armed         int      `json:"armed"`          // standing SL/AMO
	Deferred      int      `json:"deferred"`       // SL_DEFERRED_BAND (by design)
	Naked         int      `json:"naked"`          // open − armed − deferred
	NakedSymbols  []string `json:"naked_symbols,omitempty"`

	RealizedPnLTotal float64 `json:"realized_pnl_total"`
	RealizedPnLToday float64 `json:"realized_pnl_today"`

	CredentialAgeHours float64 `json:"credential_age_hours"` // -1 = no credential stored
	CredentialStale    bool    `json:"credential_stale"`     // age > 20h heuristic; M3 probe is the truth
}

// Fleet assembles the grid.
func (f *FleetStore) Fleet(ctx context.Context) ([]FleetRow, error) {
	rows, err := f.tradingDB.QueryContext(ctx, `
		SELECT strategy_id, user_id, strategy_type, trading_mode, active,
		       stopped_at, created_at
		  FROM strategies
		 WHERE deleted_at IS NULL
		 ORDER BY user_id, created_at`)
	if err != nil {
		return nil, fmt.Errorf("fleet strategies: %w", err)
	}
	defer rows.Close()

	var out []FleetRow
	for rows.Next() {
		var r FleetRow
		if err := rows.Scan(&r.StrategyID, &r.UserID, &r.StrategyType, &r.TradingMode,
			&r.Active, &r.StoppedAt, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	credAges, err := f.credentialAges(ctx)
	if err != nil {
		return nil, err
	}
	for i := range out {
		if age, ok := credAges[out[i].UserID]; ok {
			out[i].CredentialAgeHours = age.Hours()
			out[i].CredentialStale = age > credStaleAfter
		} else {
			out[i].CredentialAgeHours = -1
			out[i].CredentialStale = true
		}
		if err := f.fillPositions(ctx, &out[i]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (f *FleetStore) credentialAges(ctx context.Context) (map[string]time.Duration, error) {
	// Age computed SERVER-side: updated_at is timestamp-without-tz written
	// by the DB's own now(), so only the DB clock compares it consistently
	// (Go-side subtraction shifts by the session-timezone offset).
	// Blank token = force-expired (or never completed) — treated as absent
	// everywhere, regardless of updated_at (a DB trigger auto-bumps it).
	rows, err := f.execDB.QueryContext(ctx,
		`SELECT user_id, EXTRACT(EPOCH FROM (now() - updated_at))
		   FROM user_credentials WHERE indira_bearer_token <> ''`)
	if err != nil {
		return nil, fmt.Errorf("fleet credentials: %w", err)
	}
	defer rows.Close()
	ages := map[string]time.Duration{}
	for rows.Next() {
		var uid string
		var secs float64
		if err := rows.Scan(&uid, &secs); err != nil {
			return nil, err
		}
		ages[uid] = time.Duration(secs * float64(time.Second))
	}
	return ages, rows.Err()
}

// fillPositions computes open/armed/deferred/naked plus realized P&L for one
// strategy. Position rows are deduped by symbol — the book has a known
// duplicate-row defect (BALUFORGE ×17) and the grid must not inherit it.
func (f *FleetStore) fillPositions(ctx context.Context, r *FleetRow) error {
	posRows, err := f.posDB.QueryContext(ctx, `
		SELECT DISTINCT symbol FROM positions
		 WHERE strategy_id = $1 AND status = 'ACTIVE'`, r.StrategyID)
	if err != nil {
		return fmt.Errorf("fleet positions: %w", err)
	}
	open := map[string]bool{}
	for posRows.Next() {
		var s string
		if err := posRows.Scan(&s); err != nil {
			posRows.Close()
			return err
		}
		open[strings.ToUpper(s)] = true
	}
	posRows.Close()
	if err := posRows.Err(); err != nil {
		return err
	}
	r.OpenPositions = len(open)

	// Protection per symbol from the order ledger: a standing stop
	// (SL_PLACED) arms a symbol; SL_DEFERRED_BAND marks the deliberate
	// band-deferral state (software-supervised, not naked).
	protRows, err := f.execDB.QueryContext(ctx, `
		SELECT DISTINCT symbol, status FROM manthan_orders
		 WHERE strategy_id = $1
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status IN ('SL_PLACED','SL_DEFERRED_BAND')`, r.StrategyID)
	if err != nil {
		return fmt.Errorf("fleet protection: %w", err)
	}
	armed, deferred := map[string]bool{}, map[string]bool{}
	for protRows.Next() {
		var sym, st string
		if err := protRows.Scan(&sym, &st); err != nil {
			protRows.Close()
			return err
		}
		sym = strings.ToUpper(sym)
		if st == "SL_PLACED" {
			armed[sym] = true
		} else {
			deferred[sym] = true
		}
	}
	protRows.Close()
	if err := protRows.Err(); err != nil {
		return err
	}

	for sym := range open {
		switch {
		case armed[sym]:
			r.Armed++
		case deferred[sym]:
			r.Deferred++
		default:
			r.Naked++
			r.NakedSymbols = append(r.NakedSymbols, sym)
		}
	}
	sort.Strings(r.NakedSymbols)

	// Realized P&L (exact, from broker-confirmed exits). Day boundary in IST.
	now := f.now().In(f.ist)
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, f.ist)
	err = f.posDB.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(realized_pnl), 0),
		       COALESCE(SUM(realized_pnl) FILTER (WHERE exit_time >= $2), 0)
		  FROM positions
		 WHERE strategy_id = $1 AND realized_pnl IS NOT NULL`,
		r.StrategyID, todayStart).Scan(&r.RealizedPnLTotal, &r.RealizedPnLToday)
	if err != nil {
		return fmt.Errorf("fleet pnl: %w", err)
	}
	return nil
}

// ── M3 helpers ──────────────────────────────────────────────────────────

// CredentialFacts is the stored-side view (the probe adds the live side).
type CredentialFacts struct {
	UserID   string  `json:"user_id"`
	Stored   bool    `json:"stored"`
	AgeHours float64 `json:"age_hours"`
	Source   string  `json:"source,omitempty"`
}

// CredentialFacts reads what the credential store holds for one user.
func (f *FleetStore) CredentialFacts(ctx context.Context, userID string) (CredentialFacts, error) {
	c := CredentialFacts{UserID: userID, AgeHours: -1}
	var secs float64
	var token string
	err := f.execDB.QueryRowContext(ctx, `
		SELECT EXTRACT(EPOCH FROM (now() - updated_at)), COALESCE(indira_source,''), indira_bearer_token
		  FROM user_credentials WHERE user_id = $1`, userID).Scan(&secs, &c.Source, &token)
	if err == sql.ErrNoRows {
		return c, nil
	}
	if err != nil {
		return c, fmt.Errorf("credential facts: %w", err)
	}
	if token == "" { // force-expired: row exists, credential does not
		return c, nil
	}
	c.Stored, c.AgeHours = true, secs/3600
	return c, nil
}

// ExpireCredential force-expires a stored credential: the token is blanked
// and the timestamp pushed to the epoch, so every consumer (services via
// user-config gRPC, the fleet staleness dot, the probe) treats it as absent
// until the user's next platform login overwrites it. Refuses when nothing
// is stored — expiring a ghost is a UI bug worth surfacing.
func (f *FleetStore) ExpireCredential(ctx context.Context, userID string) error {
	// Blanking the token is the whole mechanism: every consumer (services
	// via user-config gRPC, fleet staleness, probe) treats a blank token as
	// no-credential. updated_at is left to its trigger — it is ignored for
	// blank rows. Refuses when nothing effective is stored.
	res, err := f.execDB.ExecContext(ctx, `
		UPDATE user_credentials SET indira_bearer_token = ''
		 WHERE user_id = $1 AND indira_bearer_token <> ''`, userID)
	if err != nil {
		return fmt.Errorf("expire credential: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("no live credential stored for %s", userID)
	}
	return nil
}

// ActiveUserIDs returns the distinct users holding non-deleted active
// strategies — the pre-market sweep population.
func (f *FleetStore) ActiveUserIDs(ctx context.Context) ([]string, error) {
	rows, err := f.tradingDB.QueryContext(ctx, `
		SELECT DISTINCT user_id FROM strategies
		 WHERE deleted_at IS NULL AND active = true ORDER BY user_id`)
	if err != nil {
		return nil, fmt.Errorf("active users: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

// ── 2.2 Attention queue ─────────────────────────────────────────────────

// AttentionItem is one "needs a human" row. Severity ranks the queue:
// CRITICAL (money exposed now) > HIGH (lost/failing work) > INFO (counters).
type AttentionItem struct {
	Severity string `json:"severity"` // CRITICAL | HIGH | INFO
	Kind     string `json:"kind"`     // DEAD_SESSION | NAKED_POSITION | AMO_GIVEUP | DLQ_SIGNAL | RECONCILER_ACTIVITY
	UserID   string `json:"user_id,omitempty"`
	Strategy string `json:"strategy_id,omitempty"`
	Symbol   string `json:"symbol,omitempty"`
	Detail   string `json:"detail"`
	Module   string `json:"module"` // deep-link hint for the UI (M3, M5, M6, M8)
}

// Attention builds the ranked queue from the fleet plus the failure tables.
// notWired names the spec'd classes this version deliberately does not
// cover yet — the response carries them so absence is never mistaken for
// health.
func (f *FleetStore) Attention(ctx context.Context) (items []AttentionItem, notWired []string, err error) {
	fleet, err := f.Fleet(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Dead sessions and naked positions fall out of the fleet rows.
	staleSeen := map[string]bool{}
	for _, r := range fleet {
		if r.CredentialStale && r.Active && !staleSeen[r.UserID] {
			staleSeen[r.UserID] = true
			age := "none stored"
			if r.CredentialAgeHours >= 0 {
				age = fmt.Sprintf("%.0fh old", r.CredentialAgeHours)
			}
			items = append(items, AttentionItem{
				Severity: "CRITICAL", Kind: "DEAD_SESSION", UserID: r.UserID,
				Detail: fmt.Sprintf("broker credential %s — trail modifies, entries and re-arms all fail until platform re-login", age),
				Module: "M3",
			})
		}
		for _, sym := range r.NakedSymbols {
			items = append(items, AttentionItem{
				Severity: "CRITICAL", Kind: "NAKED_POSITION",
				UserID: r.UserID, Strategy: r.StrategyID, Symbol: sym,
				Detail: "open position with no standing stop and no deferral record",
				Module: "M5",
			})
		}
	}

	// AMO give-ups: positions whose overnight arming burned the attempt cap
	// in the current window without ever standing (the bounded churn).
	giveups, err := f.execDB.QueryContext(ctx, `
		SELECT user_id, strategy_id, symbol,
		       COUNT(*) FILTER (WHERE status IN ('CANCELLED','REJECTED')) AS failed
		  FROM manthan_orders
		 WHERE order_type = 'SL_SELL_AMO'
		   AND created_at >= now() - interval '24 hours'
		 GROUP BY user_id, strategy_id, symbol
		HAVING COUNT(*) FILTER (WHERE status IN ('CANCELLED','REJECTED')) >= 5
		   AND NOT bool_or(status = 'SL_PLACED')`)
	if err != nil {
		return nil, nil, fmt.Errorf("attention giveups: %w", err)
	}
	for giveups.Next() {
		var it AttentionItem
		var n int
		if err := giveups.Scan(&it.UserID, &it.Strategy, &it.Symbol, &n); err != nil {
			giveups.Close()
			return nil, nil, err
		}
		it.Severity, it.Kind, it.Module = "HIGH", "AMO_GIVEUP", "M8"
		it.Detail = fmt.Sprintf("%d failed overnight-stop attempts in 24h — broker rejecting deterministically, needs review", n)
		items = append(items, it)
	}
	giveups.Close()
	if err := giveups.Err(); err != nil {
		return nil, nil, err
	}

	// DLQ'd entry signals from today: work the system gave up on.
	dlq, err := f.execDB.QueryContext(ctx, `
		SELECT signal_id, user_id, COALESCE(last_error,'')
		  FROM signal_inbox
		 WHERE status = 'DLQ' AND created_at >= now() - interval '24 hours'
		 ORDER BY id DESC LIMIT 50`)
	if err != nil {
		return nil, nil, fmt.Errorf("attention dlq: %w", err)
	}
	for dlq.Next() {
		var it AttentionItem
		var sigID, lastErr string
		if err := dlq.Scan(&sigID, &it.UserID, &lastErr); err != nil {
			dlq.Close()
			return nil, nil, err
		}
		it.Severity, it.Kind, it.Module = "HIGH", "DLQ_SIGNAL", "M6"
		if len(lastErr) > 120 {
			lastErr = lastErr[:120]
		}
		it.Detail = fmt.Sprintf("signal %s dead-lettered: %s", sigID, lastErr)
		items = append(items, it)
	}
	dlq.Close()
	if err := dlq.Err(); err != nil {
		return nil, nil, err
	}

	// Reconciler activity in 24h — drift being FIXED is normal in small
	// numbers and a symptom in large ones; surfaced as an INFO counter.
	var reconFixed int
	if err := f.execDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM manthan_order_events
		 WHERE event_type = 'RECONCILER_FIXED'
		   AND created_at >= now() - interval '24 hours'`).Scan(&reconFixed); err != nil {
		return nil, nil, fmt.Errorf("attention reconciler: %w", err)
	}
	if reconFixed > 0 {
		items = append(items, AttentionItem{
			Severity: "INFO", Kind: "RECONCILER_ACTIVITY",
			Detail: fmt.Sprintf("%d order-state drifts auto-synced from broker truth in 24h", reconFixed),
			Module: "M5",
		})
	}

	// Rank: CRITICAL, HIGH, INFO — stable within severity.
	rank := map[string]int{"CRITICAL": 0, "HIGH": 1, "INFO": 2}
	sort.SliceStable(items, func(i, j int) bool { return rank[items[i].Severity] < rank[items[j].Severity] })

	return items, []string{"HOLDINGS_DRIFT (kafka consumer pending)", "CORP_ACTION_FLAG (detector pending)"}, nil
}
