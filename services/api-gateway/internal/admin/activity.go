package admin

// Daily activity + client login history (2026-10-29).
//
// Two questions an admin asks every morning and every close, answered from
// the system's own records rather than log archaeology:
//
//   GET /admin/logins    — who logged in, when, from which app, and does
//                          the stored broker token still cover the session.
//                          Backed by execution_db.user_login_events (one row
//                          per distinct token seen; migration 018). When the
//                          table is not there yet the store degrades to the
//                          single latest timestamp in user_credentials and
//                          says so in `source`.
//   GET /admin/activity  — one IST calendar day: every buy that filled,
//                          every buy attempt the broker refused (FIV99's
//                          dead-session mornings), every position the book
//                          closed, and the broker SELL fill each exit rests
//                          on. A SELL fill with no closed book row is listed
//                          on its own — a safety-monitor exit the book has
//                          not caught up with yet. SELL orders still open
//                          (placed, chasing, not filled) are "exits in
//                          flight" — the rules-engine's EXIT_PENDING state
//                          is in-memory only and never reaches the table.
//
// Read-only. Timestamps are returned as RFC3339 in UTC; the UI formats IST.

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

// ActivityStore reads through the fleet handles (trading_db + execution_db).
type ActivityStore struct {
	fleet *FleetStore
	ist   *time.Location
	now   func() time.Time
}

func NewActivityStore(fleet *FleetStore) *ActivityStore {
	return &ActivityStore{fleet: fleet, ist: fleet.ist, now: time.Now}
}

// ── Logins ───────────────────────────────────────────────────────────────

// LoginEvent is one credential refresh seen by the platform.
type LoginEvent struct {
	UserID         string     `json:"user_id"`
	RecordedAt     time.Time  `json:"recorded_at"`                // when the platform stored the token
	TokenIssuedAt  *time.Time `json:"token_issued_at,omitempty"`  // JWT iat — the broker login moment
	TokenExpiresAt *time.Time `json:"token_expires_at,omitempty"` // JWT exp
	Source         string     `json:"source,omitempty"`           // SSO | WEB | IOS | AND (as declared)
	LoginSource    string     `json:"login_source,omitempty"`     // JWT loginSource claim
	AppID          string     `json:"app_id,omitempty"`
	Kind           string     `json:"kind"` // LOGIN | BACKFILL
}

// LoginClient is the per-client roll-up.
type LoginClient struct {
	UserID             string     `json:"user_id"`
	ManthanClient      bool       `json:"manthan_client"`  // has (had) a Manthan strategy or book rows
	ActiveStrategy     bool       `json:"active_strategy"` // a live strategy right now
	OpenPositions      int        `json:"open_positions"`
	CredentialPresent  bool       `json:"credential_present"` // stored token non-blank
	LastLoginAt        *time.Time `json:"last_login_at,omitempty"`
	LastTokenIssuedAt  *time.Time `json:"last_token_issued_at,omitempty"`
	LastTokenExpiresAt *time.Time `json:"last_token_expires_at,omitempty"`
	LastSource         string     `json:"last_source,omitempty"`
	TokenState         string     `json:"token_state"` // VALID | EXPIRED | UNKNOWN | NO_CREDENTIAL
	LoginsToday        int        `json:"logins_today"`
	LoginsWindow       int        `json:"logins_window"`
}

// LoginsResult is the /admin/logins payload.
type LoginsResult struct {
	Today       string        `json:"today"` // IST calendar day
	From        time.Time     `json:"from"`
	Days        int           `json:"days"`
	Scope       string        `json:"scope"`  // manthan | all
	Source      string        `json:"source"` // login_events | credentials_only
	Note        string        `json:"note,omitempty"`
	Clients     []LoginClient `json:"clients"`
	Events      []LoginEvent  `json:"events"`
	EventCount  int           `json:"event_count"`
	GeneratedAt time.Time     `json:"generated_at"`
}

// LoginsQuery narrows /admin/logins.
type LoginsQuery struct {
	UserID string // one client; empty = all in scope
	Days   int    // history window (default 7, max 90)
	Scope  string // manthan (default) | all
	Limit  int    // max events returned (default 300, max 2000)
}

func (q *LoginsQuery) normalize() {
	if q.Days <= 0 {
		q.Days = 7
	}
	if q.Days > 90 {
		q.Days = 90
	}
	if q.Limit <= 0 {
		q.Limit = 300
	}
	if q.Limit > 2000 {
		q.Limit = 2000
	}
	if q.Scope != "all" {
		q.Scope = "manthan"
	}
	q.UserID = strings.TrimSpace(q.UserID)
}

// manthanClient is what trading_db knows about one user.
type manthanClient struct {
	active bool
	open   int
}

// manthanClients lists the clients ENROLLED in Manthan right now: a live
// strategy (active, not stopped, not deleted) or an open book row. Clients
// whose strategy was stopped or deleted (N3, SV39, ND03920 on prod) and
// whose book is fully closed are not Manthan clients any more and must not
// appear on the Manthan-scoped screens.
func (a *ActivityStore) manthanClients(ctx context.Context) (map[string]*manthanClient, error) {
	out := map[string]*manthanClient{}
	rows, err := a.fleet.tradingDB.QueryContext(ctx, `
		SELECT user_id, true
		  FROM strategies
		 WHERE deleted_at IS NULL AND active AND stopped_at IS NULL
		 GROUP BY user_id`)
	if err != nil {
		return nil, fmt.Errorf("logins strategies: %w", err)
	}
	for rows.Next() {
		var uid string
		var active sql.NullBool
		if err := rows.Scan(&uid, &active); err != nil {
			rows.Close()
			return nil, err
		}
		out[uid] = &manthanClient{active: active.Valid && active.Bool}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = a.fleet.tradingDB.QueryContext(ctx, `
		SELECT user_id, count(*)
		  FROM manthan_positions
		 WHERE status IN ('ACTIVE','PENDING_ENTRY','EXIT_PENDING')
		 GROUP BY user_id`)
	if err != nil {
		return nil, fmt.Errorf("logins book: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var uid string
		var open int
		if err := rows.Scan(&uid, &open); err != nil {
			return nil, err
		}
		c := out[uid]
		if c == nil {
			c = &manthanClient{}
			out[uid] = c
		}
		c.open = open
	}
	return out, rows.Err()
}

// enrolledIDs is manthanClients as a sorted list, for `user_id = ANY($n)`.
func (a *ActivityStore) enrolledIDs(ctx context.Context) ([]string, error) {
	m, err := a.manthanClients(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(m))
	for uid := range m {
		ids = append(ids, uid)
	}
	sort.Strings(ids)
	return ids, nil
}

// credentialRows: user → (present, updated_at as UTC). updated_at is a
// timestamp WITHOUT tz written by the DB's now() on a UTC server.
func (a *ActivityStore) credentialRows(ctx context.Context) (map[string]bool, map[string]time.Time, error) {
	rows, err := a.fleet.execDB.QueryContext(ctx,
		`SELECT user_id, indira_bearer_token <> '', (updated_at AT TIME ZONE 'UTC') FROM user_credentials`)
	if err != nil {
		return nil, nil, fmt.Errorf("logins credentials: %w", err)
	}
	defer rows.Close()
	present, updated := map[string]bool{}, map[string]time.Time{}
	for rows.Next() {
		var uid string
		var ok bool
		var up sql.NullTime
		if err := rows.Scan(&uid, &ok, &up); err != nil {
			return nil, nil, err
		}
		present[uid] = ok
		if up.Valid {
			updated[uid] = up.Time.UTC()
		}
	}
	return present, updated, rows.Err()
}

func (a *ActivityStore) loginTableExists(ctx context.Context) bool {
	var ok sql.NullString
	if err := a.fleet.execDB.QueryRowContext(ctx, `SELECT to_regclass('user_login_events')::text`).Scan(&ok); err != nil {
		return false
	}
	return ok.Valid && ok.String != ""
}

// inList renders ($k, $k+1, …) for ids and appends them to args.
func inList(args *[]any, ids []string) string {
	ph := make([]string, len(ids))
	for i, id := range ids {
		*args = append(*args, id)
		ph[i] = fmt.Sprintf("$%d", len(*args))
	}
	return "(" + strings.Join(ph, ",") + ")"
}

// Logins assembles the roll-up and the event list.
func (a *ActivityStore) Logins(ctx context.Context, q LoginsQuery) (*LoginsResult, error) {
	q.normalize()
	now := a.now()
	nowIST := now.In(a.ist)
	dayStart := time.Date(nowIST.Year(), nowIST.Month(), nowIST.Day(), 0, 0, 0, 0, a.ist)
	from := dayStart.AddDate(0, 0, -(q.Days - 1)) // window INCLUDES today

	res := &LoginsResult{
		Today: dayStart.Format("2006-01-02"), From: from.UTC(), Days: q.Days, Scope: q.Scope,
		Clients: []LoginClient{}, Events: []LoginEvent{}, GeneratedAt: now.UTC(),
	}

	manthan, err := a.manthanClients(ctx)
	if err != nil {
		return nil, err
	}
	present, updated, err := a.credentialRows(ctx)
	if err != nil {
		return nil, err
	}

	// Which users are in scope.
	inScope := func(uid string) bool {
		if q.UserID != "" {
			return uid == q.UserID
		}
		if q.Scope == "all" {
			return true
		}
		_, ok := manthan[uid]
		return ok
	}
	var ids []string
	seen := map[string]bool{}
	for uid := range manthan {
		if inScope(uid) && !seen[uid] {
			seen[uid] = true
			ids = append(ids, uid)
		}
	}
	for uid := range present {
		if inScope(uid) && !seen[uid] {
			seen[uid] = true
			ids = append(ids, uid)
		}
	}
	if q.UserID != "" && !seen[q.UserID] {
		ids = append(ids, q.UserID) // unknown client: still answer with an empty row
	}
	sort.Strings(ids)

	clients := map[string]*LoginClient{}
	for _, uid := range ids {
		c := &LoginClient{UserID: uid, CredentialPresent: present[uid], TokenState: "UNKNOWN"}
		if m := manthan[uid]; m != nil {
			c.ManthanClient, c.ActiveStrategy, c.OpenPositions = true, m.active, m.open
		}
		if !c.CredentialPresent {
			c.TokenState = "NO_CREDENTIAL"
		}
		clients[uid] = c
	}

	if !a.loginTableExists(ctx) {
		// Degraded: the latest timestamp per client is all we have.
		res.Source = "credentials_only"
		res.Note = "user_login_events is not present in execution_db (migration trade-execution/018 not applied) — showing only each client's latest credential refresh"
		for _, uid := range ids {
			c := clients[uid]
			if up, ok := updated[uid]; ok && present[uid] {
				t := up
				c.LastLoginAt = &t
				if !t.Before(from) {
					c.LoginsWindow = 1
					if !t.Before(dayStart) {
						c.LoginsToday = 1
					}
				}
				res.Events = append(res.Events, LoginEvent{UserID: uid, RecordedAt: t, Kind: "LATEST"})
			}
			res.Clients = append(res.Clients, *c)
		}
		sortLogins(res)
		res.EventCount = len(res.Events)
		return res, nil
	}
	res.Source = "login_events"

	// Scope filter for SQL. For scope=all with no user filter there is no
	// WHERE on user_id at all (1,200 clients is fine; events are bounded by
	// the window and the limit).
	var scopeSQL string
	scopeArgs := func(args *[]any) string {
		if q.UserID != "" {
			*args = append(*args, q.UserID)
			return fmt.Sprintf(" AND user_id = $%d", len(*args))
		}
		if q.Scope == "manthan" {
			if len(ids) == 0 {
				return " AND false"
			}
			return " AND user_id IN " + inList(args, ids)
		}
		return ""
	}

	// Latest LOGIN-or-BACKFILL row per client (no window: "last login" may be weeks ago).
	{
		args := []any{}
		scopeSQL = scopeArgs(&args)
		rows, err := a.fleet.execDB.QueryContext(ctx, `
			SELECT DISTINCT ON (user_id) user_id, recorded_at, token_issued_at, token_expires_at, COALESCE(source,'')
			  FROM user_login_events WHERE true`+scopeSQL+`
			 ORDER BY user_id, recorded_at DESC`, args...)
		if err != nil {
			return nil, fmt.Errorf("logins latest: %w", err)
		}
		for rows.Next() {
			var uid, src string
			var rec time.Time
			var iat, exp sql.NullTime
			if err := rows.Scan(&uid, &rec, &iat, &exp, &src); err != nil {
				rows.Close()
				return nil, err
			}
			c := clients[uid]
			if c == nil {
				continue
			}
			rec = rec.UTC()
			c.LastLoginAt, c.LastSource = &rec, src
			if iat.Valid {
				t := iat.Time.UTC()
				c.LastTokenIssuedAt = &t
			}
			if exp.Valid {
				t := exp.Time.UTC()
				c.LastTokenExpiresAt = &t
			}
			if c.CredentialPresent {
				switch {
				case !exp.Valid:
					c.TokenState = "UNKNOWN"
				case now.Before(exp.Time):
					c.TokenState = "VALID"
				default:
					c.TokenState = "EXPIRED"
				}
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	// Counts in the window (LOGIN rows only — a backfill row is not a login).
	{
		args := []any{from.UTC(), dayStart.UTC()}
		scopeSQL = scopeArgs(&args)
		rows, err := a.fleet.execDB.QueryContext(ctx, `
			SELECT user_id, count(*), count(*) FILTER (WHERE recorded_at >= $2)
			  FROM user_login_events WHERE kind = 'LOGIN' AND recorded_at >= $1`+scopeSQL+`
			 GROUP BY user_id`, args...)
		if err != nil {
			return nil, fmt.Errorf("logins counts: %w", err)
		}
		for rows.Next() {
			var uid string
			var win, today int
			if err := rows.Scan(&uid, &win, &today); err != nil {
				rows.Close()
				return nil, err
			}
			if c := clients[uid]; c != nil {
				c.LoginsWindow, c.LoginsToday = win, today
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	// Event list, newest first, bounded.
	{
		args := []any{from.UTC()}
		scopeSQL = scopeArgs(&args)
		args = append(args, q.Limit)
		rows, err := a.fleet.execDB.QueryContext(ctx, `
			SELECT user_id, recorded_at, token_issued_at, token_expires_at,
			       COALESCE(source,''), COALESCE(login_source,''), COALESCE(app_id,''), kind
			  FROM user_login_events WHERE recorded_at >= $1`+scopeSQL+`
			 ORDER BY recorded_at DESC LIMIT $`+fmt.Sprint(len(args)), args...)
		if err != nil {
			return nil, fmt.Errorf("logins events: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var e LoginEvent
			var iat, exp sql.NullTime
			if err := rows.Scan(&e.UserID, &e.RecordedAt, &iat, &exp, &e.Source, &e.LoginSource, &e.AppID, &e.Kind); err != nil {
				return nil, err
			}
			e.RecordedAt = e.RecordedAt.UTC()
			if iat.Valid {
				t := iat.Time.UTC()
				e.TokenIssuedAt = &t
			}
			if exp.Valid {
				t := exp.Time.UTC()
				e.TokenExpiresAt = &t
			}
			res.Events = append(res.Events, e)
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	for _, uid := range ids {
		res.Clients = append(res.Clients, *clients[uid])
	}
	sortLogins(res)
	res.EventCount = len(res.Events)
	return res, nil
}

// sortLogins: Manthan clients first, then active strategy, then most
// recent login; the long tail of platform-only users sinks to the bottom.
func sortLogins(res *LoginsResult) {
	sort.SliceStable(res.Clients, func(i, j int) bool {
		a, b := res.Clients[i], res.Clients[j]
		if a.ManthanClient != b.ManthanClient {
			return a.ManthanClient
		}
		if a.ActiveStrategy != b.ActiveStrategy {
			return a.ActiveStrategy
		}
		at, bt := time.Time{}, time.Time{}
		if a.LastLoginAt != nil {
			at = *a.LastLoginAt
		}
		if b.LastLoginAt != nil {
			bt = *b.LastLoginAt
		}
		if !at.Equal(bt) {
			return at.After(bt)
		}
		return a.UserID < b.UserID
	})
}

// ── Daily activity ───────────────────────────────────────────────────────

// BuyRow is one entry order.
type BuyRow struct {
	OrderID       int64      `json:"order_id"`
	UserID        string     `json:"user_id"`
	StrategyID    string     `json:"strategy_id,omitempty"`
	Symbol        string     `json:"symbol"`
	OrderType     string     `json:"order_type"`
	Status        string     `json:"status"`
	Qty           int        `json:"qty"`
	FilledQty     int        `json:"filled_qty"`
	AvgFillPrice  *float64   `json:"avg_fill_price,omitempty"`
	LimitPrice    *float64   `json:"limit_price,omitempty"`
	Value         float64    `json:"value"` // filled_qty × avg fill
	BrokerOrderID string     `json:"broker_order_id,omitempty"`
	SignalID      string     `json:"signal_id,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	FilledAt      *time.Time `json:"filled_at,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
}

// RejectedBuyGroup folds the retry ladder (5 attempts in one second) into
// one line per client+symbol.
type RejectedBuyGroup struct {
	UserID     string    `json:"user_id"`
	Symbol     string    `json:"symbol"`
	Attempts   int       `json:"attempts"`
	Qty        int       `json:"qty"` // largest attempted lot
	FirstAt    time.Time `json:"first_at"`
	LastAt     time.Time `json:"last_at"`
	LastStatus string    `json:"last_status"`
	LastError  string    `json:"last_error,omitempty"`
	Reason     string    `json:"reason"` // plain-language classification
}

// SellFill is the broker SELL an exit rests on.
type SellFill struct {
	OrderID       int64     `json:"order_id"`
	UserID        string    `json:"user_id"`
	Symbol        string    `json:"symbol"`
	OrderType     string    `json:"order_type"` // SL_SELL | MARKET_SELL | AMO_SELL | …
	Status        string    `json:"status"`
	Qty           int       `json:"qty"`
	FilledQty     int       `json:"filled_qty"`
	AvgFillPrice  *float64  `json:"avg_fill_price,omitempty"`
	BrokerOrderID string    `json:"broker_order_id,omitempty"`
	FilledAt      time.Time `json:"filled_at"`
}

// ExitRow is one closed (or closing) book row.
type ExitRow struct {
	PositionID     string     `json:"position_id"`
	UserID         string     `json:"user_id"`
	StrategyID     string     `json:"strategy_id,omitempty"`
	Symbol         string     `json:"symbol"`
	Status         string     `json:"status"` // EXITED
	Qty            int        `json:"qty"`
	EntryPrice     float64    `json:"entry_price"`
	EntryTime      *time.Time `json:"entry_time,omitempty"`
	ExitPrice      *float64   `json:"exit_price,omitempty"`
	ExitPriceKnown bool       `json:"exit_price_known"`
	ExitTime       *time.Time `json:"exit_time,omitempty"`
	RealizedPnL    *float64   `json:"realized_pnl,omitempty"`
	PnLPct         *float64   `json:"pnl_pct,omitempty"`
	HoldingDays    int        `json:"holding_period_days"`
	ExitReason     string     `json:"exit_reason,omitempty"`
	Note           string     `json:"note,omitempty"`
	SellFill       *SellFill  `json:"sell_fill,omitempty"` // broker leg, when found that day
}

// ActivityCounts is the stat strip.
type ActivityCounts struct {
	Buys             int     `json:"buys"`
	BuyQty           int     `json:"buy_qty"`
	BuyValue         float64 `json:"buy_value"`
	OpenBuys         int     `json:"open_buys"`
	RejectedAttempts int     `json:"rejected_attempts"`
	RejectedGroups   int     `json:"rejected_groups"`
	Exits            int     `json:"exits"`
	OpenSells        int     `json:"open_sells"`   // SELL orders placed, not yet filled
	RealizedPnL      float64 `json:"realized_pnl"` // exits with a known price only
	UnmatchedSells   int     `json:"unmatched_sells"`
}

// ActivityResult is the /admin/activity payload for one IST day.
type ActivityResult struct {
	Date           string             `json:"date"`
	IsToday        bool               `json:"is_today"`
	Clients        []string           `json:"clients"` // enrolled Manthan clients the day was built for
	Counts         ActivityCounts     `json:"counts"`
	Buys           []BuyRow           `json:"buys"`
	OpenBuys       []BuyRow           `json:"open_buys"` // placed, not yet filled/terminal
	RejectedBuys   []RejectedBuyGroup `json:"rejected_buys"`
	Exits          []ExitRow          `json:"exits"`
	OpenSells      []SellFill         `json:"open_sells"` // exits in flight: SELL placed that day, not filled
	UnmatchedSells []SellFill         `json:"unmatched_sells"`
	GeneratedAt    time.Time          `json:"generated_at"`
}

// dayBounds returns [start, end) of an IST calendar day.
func (a *ActivityStore) dayBounds(date string) (start, end time.Time, err error) {
	if date == "" {
		n := a.now().In(a.ist)
		start = time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, a.ist)
	} else {
		d, perr := time.ParseInLocation("2006-01-02", date, a.ist)
		if perr != nil {
			return start, end, fmt.Errorf("date must be YYYY-MM-DD")
		}
		start = d
	}
	return start, start.AddDate(0, 0, 1), nil
}

func nullF(v sql.NullFloat64) *float64 {
	if !v.Valid {
		return nil
	}
	f := round2f(v.Float64)
	return &f
}

func nullT(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time.UTC()
	return &t
}

// brokerID strips the trailing ':' the order path stores ("NYMZX0000DM:").
func brokerID(s string) string { return strings.TrimSuffix(strings.TrimSpace(s), ":") }

// rejectReason turns the broker/system text into one plain phrase.
func rejectReason(status, lastErr string) string {
	e := strings.ToLower(lastErr)
	switch {
	case strings.Contains(e, "au004") || strings.Contains(e, "unauthor") || strings.Contains(e, "session") && strings.Contains(e, "expir") || strings.Contains(e, "invalid token"):
		return "Broker session expired — client must log in again"
	case strings.Contains(e, "margin") || strings.Contains(e, "insufficient") || strings.Contains(e, "fund"):
		return "Insufficient funds / margin at the broker"
	case strings.Contains(e, "circuit") || strings.Contains(e, "dpr") || strings.Contains(e, "price band") || strings.Contains(e, "outside"):
		return "Price outside the day's band"
	case strings.Contains(e, "duplicate"):
		return "Duplicate entry blocked"
	case status == "CANCELLED":
		return "Cancelled before it filled"
	case lastErr == "":
		return "Rejected (no reason recorded)"
	}
	return "Rejected: " + lastErr
}

// exitNote: plain-language line per exit reason (rules-engine book codes
// plus the shared closeNote for reconciliation/manual closes).
func exitNote(reason string, priceKnown bool) string {
	switch strings.ToUpper(reason) {
	case "TSL_HIT":
		return "Trailing stop hit — the protective stop filled at the broker."
	case "SL_TRIGGER", "SL":
		return "Stop-loss triggered at the broker."
	case "STRATEGY_EXIT":
		return "Exited by the strategy."
	case "LIQUIDATION":
		return "Liquidated."
	}
	return closeNote(reason, priceKnown)
}

// Activity assembles one day.
func (a *ActivityStore) Activity(ctx context.Context, date string) (*ActivityResult, error) {
	start, end, err := a.dayBounds(date)
	if err != nil {
		return nil, err
	}
	n := a.now().In(a.ist)
	today := time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, a.ist)
	res := &ActivityResult{
		Date: start.Format("2006-01-02"), IsToday: start.Equal(today),
		Buys: []BuyRow{}, OpenBuys: []BuyRow{}, RejectedBuys: []RejectedBuyGroup{},
		Exits: []ExitRow{}, OpenSells: []SellFill{}, UnmatchedSells: []SellFill{},
		GeneratedAt: a.now().UTC(),
	}
	su, eu := start.UTC(), end.UTC()

	// Only clients enrolled in Manthan today. The manthan_* tables keep
	// rows for stopped/deleted strategies and test accounts forever; the
	// admin wants the live book, not the archive.
	enrolled, err := a.enrolledIDs(ctx)
	if err != nil {
		return nil, err
	}
	res.Clients = enrolled
	who := pq.Array(enrolled)

	// Buys that filled in the day (fill time; falls back to last update).
	rows, err := a.fleet.execDB.QueryContext(ctx, `
		SELECT id, user_id, COALESCE(strategy_id::text,''), symbol, order_type, status, qty,
		       COALESCE(filled_qty,0), avg_fill_price, limit_price, COALESCE(broker_order_id,''),
		       COALESCE(signal_id,''), created_at, filled_at, COALESCE(last_error,'')
		  FROM manthan_orders
		 WHERE order_side = 'BUY' AND user_id = ANY($3)
		   AND ( (status IN ('FILLED','PARTIAL') AND COALESCE(filled_at, updated_at) >= $1 AND COALESCE(filled_at, updated_at) < $2)
		      OR (status IN ('PENDING','PLACED') AND created_at >= $1 AND created_at < $2) )
		 ORDER BY COALESCE(filled_at, created_at)`, su, eu, who)
	if err != nil {
		return nil, fmt.Errorf("activity buys: %w", err)
	}
	for rows.Next() {
		var r BuyRow
		var avg, lim sql.NullFloat64
		var filled sql.NullTime
		if err := rows.Scan(&r.OrderID, &r.UserID, &r.StrategyID, &r.Symbol, &r.OrderType, &r.Status, &r.Qty,
			&r.FilledQty, &avg, &lim, &r.BrokerOrderID, &r.SignalID, &r.CreatedAt, &filled, &r.LastError); err != nil {
			rows.Close()
			return nil, err
		}
		r.CreatedAt = r.CreatedAt.UTC()
		r.AvgFillPrice, r.LimitPrice, r.FilledAt = nullF(avg), nullF(lim), nullT(filled)
		r.BrokerOrderID = brokerID(r.BrokerOrderID)
		if avg.Valid {
			r.Value = round2f(avg.Float64 * float64(r.FilledQty))
		}
		if r.Status == "FILLED" || r.Status == "PARTIAL" {
			res.Buys = append(res.Buys, r)
			res.Counts.Buys++
			res.Counts.BuyQty += r.FilledQty
			res.Counts.BuyValue = round2f(res.Counts.BuyValue + r.Value)
		} else {
			res.OpenBuys = append(res.OpenBuys, r)
			res.Counts.OpenBuys++
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Buy attempts the broker (or the system) refused, folded per client+symbol.
	rows, err = a.fleet.execDB.QueryContext(ctx, `
		SELECT user_id, symbol, count(*), max(qty), min(created_at), max(created_at),
		       (array_agg(status ORDER BY created_at DESC, id DESC))[1],
		       (array_agg(COALESCE(last_error,'') ORDER BY created_at DESC, id DESC))[1]
		  FROM manthan_orders
		 WHERE order_side = 'BUY' AND status IN ('REJECTED','CANCELLED') AND user_id = ANY($3)
		   AND created_at >= $1 AND created_at < $2
		 GROUP BY user_id, symbol
		 ORDER BY min(created_at)`, su, eu, who)
	if err != nil {
		return nil, fmt.Errorf("activity rejected buys: %w", err)
	}
	for rows.Next() {
		var g RejectedBuyGroup
		if err := rows.Scan(&g.UserID, &g.Symbol, &g.Attempts, &g.Qty, &g.FirstAt, &g.LastAt, &g.LastStatus, &g.LastError); err != nil {
			rows.Close()
			return nil, err
		}
		g.FirstAt, g.LastAt = g.FirstAt.UTC(), g.LastAt.UTC()
		g.Reason = rejectReason(g.LastStatus, g.LastError)
		res.RejectedBuys = append(res.RejectedBuys, g)
		res.Counts.RejectedAttempts += g.Attempts
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	res.Counts.RejectedGroups = len(res.RejectedBuys)

	// SELL fills of the day — the broker leg of every exit — plus exit
	// orders still open (placed/chasing). Standing stop-loss orders are not
	// exits in flight: only non-SL sell types count as "open sells".
	sells := map[string][]SellFill{} // user|symbol → fills
	rows, err = a.fleet.execDB.QueryContext(ctx, `
		SELECT id, user_id, symbol, order_type, status, qty, COALESCE(filled_qty,0), avg_fill_price,
		       COALESCE(broker_order_id,''), COALESCE(filled_at, updated_at)
		  FROM manthan_orders
		 WHERE order_side = 'SELL' AND user_id = ANY($3)
		   AND ( (status IN ('FILLED','SL_FILLED','PARTIAL')
		          AND COALESCE(filled_at, updated_at) >= $1 AND COALESCE(filled_at, updated_at) < $2)
		      OR (status IN ('PENDING','PLACED') AND order_type NOT IN ('SL_SELL','SL_SELL_AMO')
		          AND created_at >= $1 AND created_at < $2) )
		 ORDER BY COALESCE(filled_at, updated_at)`, su, eu, who)
	if err != nil {
		return nil, fmt.Errorf("activity sells: %w", err)
	}
	for rows.Next() {
		var s SellFill
		var avg sql.NullFloat64
		if err := rows.Scan(&s.OrderID, &s.UserID, &s.Symbol, &s.OrderType, &s.Status, &s.Qty, &s.FilledQty, &avg, &s.BrokerOrderID, &s.FilledAt); err != nil {
			rows.Close()
			return nil, err
		}
		s.FilledAt = s.FilledAt.UTC()
		s.AvgFillPrice = nullF(avg)
		s.BrokerOrderID = brokerID(s.BrokerOrderID)
		if s.Status == "PENDING" || s.Status == "PLACED" {
			res.OpenSells = append(res.OpenSells, s)
			res.Counts.OpenSells++
			continue
		}
		k := s.UserID + "|" + strings.ToUpper(s.Symbol)
		sells[k] = append(sells[k], s)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Book rows closed in the day.
	rows, err = a.fleet.tradingDB.QueryContext(ctx, `
		SELECT id, user_id, COALESCE(strategy_id::text,''), symbol, status, quantity, entry_price, entry_time,
		       exit_price, exit_time, realized_pnl, COALESCE(exit_reason,''), COALESCE(invested_amt,0), updated_at
		  FROM manthan_positions
		 WHERE status = 'EXITED' AND user_id = ANY($3)
		   AND COALESCE(exit_time, updated_at) >= $1 AND COALESCE(exit_time, updated_at) < $2
		 ORDER BY COALESCE(exit_time, updated_at)`, su, eu, who)
	if err != nil {
		return nil, fmt.Errorf("activity exits: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r ExitRow
		var id int64
		var entryT, exitT sql.NullTime
		var exitPx, realized sql.NullFloat64
		var invested float64
		var updated time.Time
		if err := rows.Scan(&id, &r.UserID, &r.StrategyID, &r.Symbol, &r.Status, &r.Qty, &r.EntryPrice, &entryT,
			&exitPx, &exitT, &realized, &r.ExitReason, &invested, &updated); err != nil {
			return nil, err
		}
		r.PositionID = fmt.Sprintf("MP_%d", id)
		r.EntryPrice = round2f(r.EntryPrice)
		r.EntryTime, r.ExitTime = nullT(entryT), nullT(exitT)
		r.ExitPriceKnown = exitPx.Valid && exitPx.Float64 > 0
		if r.ExitPriceKnown {
			r.ExitPrice = nullF(exitPx)
			if realized.Valid {
				r.RealizedPnL = nullF(realized)
				if invested > 0 {
					p := pct(realized.Float64, invested)
					r.PnLPct = &p
				}
			}
		}
		endT := updated
		if exitT.Valid {
			endT = exitT.Time
		}
		if entryT.Valid {
			r.HoldingDays = holdingDays(entryT.Time, endT, a.ist)
		}
		k := r.UserID + "|" + strings.ToUpper(r.Symbol)
		if fills := sells[k]; len(fills) > 0 {
			f := fills[0]
			r.SellFill = &f
			sells[k] = fills[1:]
		}
		r.Note = exitNote(r.ExitReason, r.ExitPriceKnown)
		res.Exits = append(res.Exits, r)
		res.Counts.Exits++
		if r.RealizedPnL != nil {
			res.Counts.RealizedPnL = round2f(res.Counts.RealizedPnL + *r.RealizedPnL)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Broker SELL fills with no closed book row that day.
	keys := make([]string, 0, len(sells))
	for k := range sells {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		res.UnmatchedSells = append(res.UnmatchedSells, sells[k]...)
	}
	res.Counts.UnmatchedSells = len(res.UnmatchedSells)
	return res, nil
}
