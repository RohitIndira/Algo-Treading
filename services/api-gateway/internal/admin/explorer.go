package admin

// M6 — signal pipeline explorer: the SHREEPUSHK debugging session as a
// screen anyone can drive.
//
// 6.1 Trace: one symbol (optionally one user) rendered as a timeline —
//     sheet appearance → eligibility verdict → publication → per-user
//     allocation decision → inbox row → order attempts → fills →
//     position → protection/cooldown. The cross-DB JOIN that used to
//     live in an engineer's head.
// 6.2 Candidates: the whole per-day universe with each drop reason —
//     manthan_stocks already persists status (ELIGIBLE / FILTER_REJECTED
//     / DATA_DROPPED) + reason per security per run_date.
// 6.3 Inbox browser + rejection analytics: signal_inbox by class
//     (AUTH_EXPIRED / UPPER_CIRCUIT / PRE_OPEN / TRANSIENT / POISON)
//     with attempts and errors; rejected/cancelled orders grouped by
//     exchange-error taxonomy, user and day.
//
// Pure DB reads, READ tier, no broker calls. Sources:
//   signals_db   manthan_stocks, manthan_signals      (universe, publication)
//   trading_db   manthan_signal_decisions, manthan_positions, manthan_cooldown
//   execution_db signal_inbox, manthan_orders
//   positions_db positions

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Explorer bundles M6's read paths. signalsDB may be nil — the universe/
// publication stages and 6.2 then degrade loudly instead of half-lying.
type Explorer struct {
	signalsDB *sql.DB
	fleet     *FleetStore
}

func NewExplorer(signalsDB *sql.DB, fleet *FleetStore) *Explorer {
	return &Explorer{signalsDB: signalsDB, fleet: fleet}
}

// TraceEvent is one timeline entry.
type TraceEvent struct {
	TS      time.Time      `json:"ts"`
	Stage   string         `json:"stage"` // UNIVERSE | SIGNAL | DECISION | INBOX | ORDER | FILL | POSITION | EXIT | PROTECTION | COOLDOWN
	Summary string         `json:"summary"`
	Detail  map[string]any `json:"detail,omitempty"`
}

// TraceResult is the assembled chain.
type TraceResult struct {
	Symbol      string       `json:"symbol"`
	UserID      string       `json:"user_id,omitempty"`
	Days        int          `json:"days"`
	Events      []TraceEvent `json:"events"`
	Notes       []string     `json:"notes,omitempty"` // degradations, caps
	GeneratedAt time.Time    `json:"generated_at"`
}

const traceCap = 300 // per-stage row cap; the note says when it bites

// Trace assembles the end-to-end chain for one symbol.
func (e *Explorer) Trace(ctx context.Context, symbol, userID string, days int) (*TraceResult, error) {
	if days <= 0 || days > 365 {
		days = 60
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	res := &TraceResult{Symbol: symbol, UserID: userID, Days: days, GeneratedAt: time.Now()}
	cutoff := time.Now().AddDate(0, 0, -days)

	if e.signalsDB != nil {
		if err := e.traceSignalsDB(ctx, res, symbol, cutoff); err != nil {
			return nil, err
		}
	} else {
		res.Notes = append(res.Notes, "signals_db unwired — UNIVERSE and SIGNAL stages absent")
	}
	if err := e.traceDecisions(ctx, res, symbol, userID, cutoff); err != nil {
		return nil, err
	}
	if err := e.traceInbox(ctx, res, symbol, userID, cutoff); err != nil {
		return nil, err
	}
	if err := e.traceOrders(ctx, res, symbol, userID, cutoff); err != nil {
		return nil, err
	}
	if err := e.tracePositions(ctx, res, symbol, userID, cutoff); err != nil {
		return nil, err
	}
	if err := e.traceProtection(ctx, res, symbol, userID); err != nil {
		return nil, err
	}

	sort.SliceStable(res.Events, func(i, j int) bool { return res.Events[i].TS.Before(res.Events[j].TS) })
	if res.Events == nil {
		res.Events = []TraceEvent{}
	}
	return res, nil
}

func (e *Explorer) traceSignalsDB(ctx context.Context, res *TraceResult, symbol string, cutoff time.Time) error {
	rows, err := e.signalsDB.QueryContext(ctx, `
		SELECT created_at, run_date::text, status, COALESCE(reason,''),
		       COALESCE(pe,0), COALESCE(fscore,0), COALESCE(mcap_bucket,''), COALESCE(index_name,'')
		  FROM manthan_stocks
		 WHERE UPPER(symbol) = $1 AND created_at >= $2
		 ORDER BY created_at LIMIT `+fmt.Sprint(traceCap), symbol, cutoff)
	if err != nil {
		return fmt.Errorf("trace universe: %w", err)
	}
	for rows.Next() {
		var ts time.Time
		var runDate, status, reason, bucket, index string
		var pe, fscore float64
		if err := rows.Scan(&ts, &runDate, &status, &reason, &pe, &fscore, &bucket, &index); err != nil {
			rows.Close()
			return err
		}
		sum := fmt.Sprintf("sheet run %s: %s", runDate, status)
		if reason != "" {
			sum += " — " + reason
		}
		res.Events = append(res.Events, TraceEvent{TS: ts, Stage: "UNIVERSE", Summary: sum,
			Detail: map[string]any{"run_date": runDate, "status": status, "reason": reason,
				"pe": pe, "fscore": fscore, "mcap_bucket": bucket, "index": index}})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	sig, err := e.signalsDB.QueryContext(ctx, `
		SELECT COALESCE(published_to_kafka_at, first_seen_at, created_at), run_date::text,
		       first_seen_at, published_to_kafka_at, COALESCE(latest_price,0)
		  FROM manthan_signals
		 WHERE UPPER(symbol) = $1 AND created_at >= $2
		 ORDER BY created_at LIMIT `+fmt.Sprint(traceCap), symbol, cutoff)
	if err != nil {
		return fmt.Errorf("trace signal: %w", err)
	}
	defer sig.Close()
	for sig.Next() {
		var ts time.Time
		var runDate string
		var firstSeen, published sql.NullTime
		var price float64
		if err := sig.Scan(&ts, &runDate, &firstSeen, &published, &price); err != nil {
			return err
		}
		sum := fmt.Sprintf("signal for run %s", runDate)
		if published.Valid {
			sum += " published to kafka"
		} else {
			sum += " recorded (not published)"
		}
		d := map[string]any{"run_date": runDate, "latest_price": price}
		if firstSeen.Valid {
			d["first_seen_at"] = firstSeen.Time
		}
		if published.Valid {
			d["published_at"] = published.Time
		}
		res.Events = append(res.Events, TraceEvent{TS: ts, Stage: "SIGNAL", Summary: sum, Detail: d})
	}
	return sig.Err()
}

func (e *Explorer) traceDecisions(ctx context.Context, res *TraceResult, symbol, userID string, cutoff time.Time) error {
	q := `
		SELECT decided_at, signal_id::text, user_id, status,
		       intended_qty, ltp_at_decision, initial_sl_target,
		       COALESCE(rejection_reason,''), dispatched_at
		  FROM manthan_signal_decisions
		 WHERE UPPER(symbol) = $1 AND decided_at >= $2`
	args := []any{symbol, cutoff}
	if userID != "" {
		q += ` AND user_id = $3`
		args = append(args, userID)
	}
	q += ` ORDER BY decided_at LIMIT ` + fmt.Sprint(traceCap)
	rows, err := e.fleet.tradingDB.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("trace decisions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ts time.Time
		var sigID, uid, status, reject string
		var qty int
		var ltp, slTarget float64
		var dispatched sql.NullTime
		if err := rows.Scan(&ts, &sigID, &uid, &status, &qty, &ltp, &slTarget, &reject, &dispatched); err != nil {
			return err
		}
		sum := fmt.Sprintf("allocator → %s: %s qty %d @ %.2f", uid, status, qty, ltp)
		if reject != "" {
			sum += " — " + reject
		}
		d := map[string]any{"signal_id": sigID, "user_id": uid, "status": status,
			"intended_qty": qty, "ltp_at_decision": ltp, "initial_sl_target": slTarget}
		if dispatched.Valid {
			d["dispatched_at"] = dispatched.Time
		}
		if reject != "" {
			d["rejection_reason"] = reject
		}
		res.Events = append(res.Events, TraceEvent{TS: ts, Stage: "DECISION", Summary: sum, Detail: d})
	}
	return rows.Err()
}

func (e *Explorer) traceInbox(ctx context.Context, res *TraceResult, symbol, userID string, cutoff time.Time) error {
	q := `
		SELECT created_at, signal_id, user_id, order_type, status, attempts,
		       COALESCE(last_error_class,''), COALESCE(last_error,''), completed_at
		  FROM signal_inbox
		 WHERE payload->>'symbol' = $1 AND created_at >= $2`
	args := []any{symbol, cutoff}
	if userID != "" {
		q += ` AND user_id = $3`
		args = append(args, userID)
	}
	q += ` ORDER BY created_at LIMIT ` + fmt.Sprint(traceCap)
	rows, err := e.fleet.execDB.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("trace inbox: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ts time.Time
		var sigID, uid, otype, status, class, lastErr string
		var attempts int
		var completed sql.NullTime
		if err := rows.Scan(&ts, &sigID, &uid, &otype, &status, &attempts, &class, &lastErr, &completed); err != nil {
			return err
		}
		sum := fmt.Sprintf("inbox %s for %s: %s", otype, uid, status)
		if class != "" {
			sum += " [" + class + "]"
		}
		if attempts > 1 {
			sum += fmt.Sprintf(" after %d attempts", attempts)
		}
		d := map[string]any{"signal_id": sigID, "user_id": uid, "order_type": otype,
			"status": status, "attempts": attempts}
		if class != "" {
			d["error_class"] = class
		}
		if lastErr != "" {
			d["last_error"] = truncate(lastErr, 200)
		}
		if completed.Valid {
			d["completed_at"] = completed.Time
		}
		res.Events = append(res.Events, TraceEvent{TS: ts, Stage: "INBOX", Summary: sum, Detail: d})
	}
	return rows.Err()
}

func (e *Explorer) traceOrders(ctx context.Context, res *TraceResult, symbol, userID string, cutoff time.Time) error {
	q := `
		SELECT created_at, signal_id, user_id, order_type, order_side, status,
		       qty, filled_qty, COALESCE(avg_fill_price,0),
		       COALESCE(broker_order_id,''), COALESCE(broker_status,''),
		       retry_count, COALESCE(last_error,''),
		       COALESCE(trigger_price,0), COALESCE(broker_trigger_price,0),
		       COALESCE(parent_order_id::text,''), filled_at
		  FROM manthan_orders
		 WHERE UPPER(symbol) = $1 AND created_at >= $2`
	args := []any{symbol, cutoff}
	if userID != "" {
		q += ` AND user_id = $3`
		args = append(args, userID)
	}
	q += ` ORDER BY created_at LIMIT ` + fmt.Sprint(traceCap)
	rows, err := e.fleet.execDB.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("trace orders: %w", err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var ts time.Time
		var sigID, uid, otype, side, status, brokerID, brokerStatus, lastErr, parent string
		var qty, filledQty, retries int
		var fillPrice, trig, brokerTrig float64
		var filledAt sql.NullTime
		if err := rows.Scan(&ts, &sigID, &uid, &otype, &side, &status, &qty, &filledQty, &fillPrice,
			&brokerID, &brokerStatus, &retries, &lastErr, &trig, &brokerTrig, &parent, &filledAt); err != nil {
			return err
		}
		n++
		sum := fmt.Sprintf("%s %s ×%d for %s → %s", otype, side, qty, uid, status)
		if brokerID != "" {
			sum += " (broker " + brokerID + ")"
		}
		d := map[string]any{"signal_id": sigID, "user_id": uid, "order_type": otype, "side": side,
			"status": status, "qty": qty, "retry_count": retries}
		if brokerID != "" {
			d["broker_order_id"] = brokerID
		}
		if brokerStatus != "" {
			d["broker_status"] = brokerStatus
		}
		if trig > 0 {
			d["trigger_price"] = trig
		}
		if brokerTrig > 0 {
			d["broker_trigger_price"] = brokerTrig
		}
		if parent != "" {
			d["parent_order_id"] = parent // SL lineage
		}
		if lastErr != "" {
			d["last_error"] = truncate(lastErr, 200)
		}
		res.Events = append(res.Events, TraceEvent{TS: ts, Stage: "ORDER", Summary: sum, Detail: d})

		if filledAt.Valid && filledQty > 0 {
			res.Events = append(res.Events, TraceEvent{TS: filledAt.Time, Stage: "FILL",
				Summary: fmt.Sprintf("%s filled ×%d @ %.2f for %s", otype, filledQty, fillPrice, uid),
				Detail:  map[string]any{"broker_order_id": brokerID, "filled_qty": filledQty, "avg_fill_price": fillPrice}})
		}
	}
	if n == traceCap {
		res.Notes = append(res.Notes, fmt.Sprintf("ORDER stage capped at %d rows — narrow the window", traceCap))
	}
	return rows.Err()
}

func (e *Explorer) tracePositions(ctx context.Context, res *TraceResult, symbol, userID string, cutoff time.Time) error {
	q := `
		SELECT entry_time, position_id::text, user_id, origin, status, quantity, entry_price,
		       exit_time, COALESCE(exit_price,0), COALESCE(exit_reason,''), realized_pnl
		  FROM positions
		 WHERE UPPER(symbol) = $1 AND entry_time >= $2`
	args := []any{symbol, cutoff}
	if userID != "" {
		q += ` AND user_id = $3`
		args = append(args, userID)
	}
	q += ` ORDER BY entry_time LIMIT ` + fmt.Sprint(traceCap)
	rows, err := e.fleet.posDB.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("trace positions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ts time.Time
		var posID, uid, origin, status, exitReason string
		var qty int
		var entryPrice, exitPrice float64
		var exitTime sql.NullTime
		var pnl sql.NullFloat64
		if err := rows.Scan(&ts, &posID, &uid, &origin, &status, &qty, &entryPrice,
			&exitTime, &exitPrice, &exitReason, &pnl); err != nil {
			return err
		}
		res.Events = append(res.Events, TraceEvent{TS: ts, Stage: "POSITION",
			Summary: fmt.Sprintf("position opened ×%d @ %.2f for %s (%s, now %s)", qty, entryPrice, uid, origin, status),
			Detail:  map[string]any{"position_id": posID, "user_id": uid, "origin": origin, "status": status}})
		if exitTime.Valid {
			sum := fmt.Sprintf("position exited @ %.2f for %s", exitPrice, uid)
			if exitReason != "" {
				sum += " — " + exitReason
			}
			d := map[string]any{"position_id": posID, "exit_reason": exitReason}
			if pnl.Valid {
				d["realized_pnl"] = pnl.Float64
				sum += fmt.Sprintf(" (P&L %.2f)", pnl.Float64)
			}
			res.Events = append(res.Events, TraceEvent{TS: exitTime.Time, Stage: "EXIT", Summary: sum, Detail: d})
		}
	}
	return rows.Err()
}

func (e *Explorer) traceProtection(ctx context.Context, res *TraceResult, symbol, userID string) error {
	q := `
		SELECT updated_at, user_id, status, COALESCE(current_sl,0),
		       COALESCE(high_since_entry,0), COALESCE(last_trail_level,0)
		  FROM manthan_positions
		 WHERE UPPER(symbol) = $1`
	args := []any{symbol}
	if userID != "" {
		q += ` AND user_id = $2`
		args = append(args, userID)
	}
	q += ` ORDER BY updated_at DESC LIMIT 5`
	rows, err := e.fleet.tradingDB.QueryContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("trace protection: %w", err)
	}
	for rows.Next() {
		var ts time.Time
		var uid, status string
		var sl, high, trail float64
		if err := rows.Scan(&ts, &uid, &status, &sl, &high, &trail); err != nil {
			rows.Close()
			return err
		}
		res.Events = append(res.Events, TraceEvent{TS: ts, Stage: "PROTECTION",
			Summary: fmt.Sprintf("trail state for %s: %s, SL %.2f (high %.2f)", uid, status, sl, high),
			Detail:  map[string]any{"user_id": uid, "status": status, "current_sl": sl, "high_since_entry": high, "last_trail_level": trail}})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	cd, err := e.fleet.tradingDB.QueryContext(ctx, `
		SELECT exit_time, strategy_id::text, exit_price, reentry_below, cleared, cleared_at
		  FROM manthan_cooldown WHERE UPPER(symbol) = $1 ORDER BY exit_time DESC LIMIT 5`, symbol)
	if err != nil {
		return fmt.Errorf("trace cooldown: %w", err)
	}
	defer cd.Close()
	for cd.Next() {
		var ts time.Time
		var sid string
		var exitPrice, reentry float64
		var cleared bool
		var clearedAt sql.NullTime
		if err := cd.Scan(&ts, &sid, &exitPrice, &reentry, &cleared, &clearedAt); err != nil {
			return err
		}
		sum := fmt.Sprintf("cooldown set: re-entry only below %.2f", reentry)
		if cleared {
			sum += " (since cleared)"
		}
		d := map[string]any{"strategy_id": sid, "exit_price": exitPrice, "reentry_below": reentry, "cleared": cleared}
		if clearedAt.Valid {
			d["cleared_at"] = clearedAt.Time
		}
		res.Events = append(res.Events, TraceEvent{TS: ts, Stage: "COOLDOWN", Summary: sum, Detail: d})
	}
	return cd.Err()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// ── 6.2 candidates ──────────────────────────────────────────────────────

// CandidateRow is one security's verdict for one run day.
type CandidateRow struct {
	Symbol     string  `json:"symbol"`
	Company    string  `json:"company,omitempty"`
	Status     string  `json:"status"` // ELIGIBLE | FILTER_REJECTED | DATA_DROPPED
	Reason     string  `json:"reason,omitempty"`
	PE         float64 `json:"pe,omitempty"`
	FScore     float64 `json:"fscore,omitempty"`
	MarketCap  float64 `json:"market_cap,omitempty"`
	McapBucket string  `json:"mcap_bucket,omitempty"`
	IndexName  string  `json:"index_name,omitempty"`
}

// CandidatesResult is the whole universe for one day.
type CandidatesResult struct {
	RunDate        string         `json:"run_date"`
	Rows           []CandidateRow `json:"rows"`
	CountsByStatus map[string]int `json:"counts_by_status"`
	CountsByReason map[string]int `json:"counts_by_reason"`
	RecentDates    []string       `json:"recent_dates"` // for the date picker
}

// Candidates answers "sheet had X — why no trade?" for one run day.
func (e *Explorer) Candidates(ctx context.Context, date string) (*CandidatesResult, error) {
	if e.signalsDB == nil {
		return nil, fmt.Errorf("signals_db unwired — candidate universe unavailable")
	}
	res := &CandidatesResult{CountsByStatus: map[string]int{}, CountsByReason: map[string]int{}}

	if date == "" {
		if err := e.signalsDB.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(run_date)::text,'') FROM manthan_stocks`).Scan(&date); err != nil {
			return nil, fmt.Errorf("candidates latest date: %w", err)
		}
		if date == "" {
			return nil, fmt.Errorf("no candidate runs recorded yet")
		}
	}
	res.RunDate = date

	rows, err := e.signalsDB.QueryContext(ctx, `
		SELECT symbol, COALESCE(company_name,''), status, COALESCE(reason,''),
		       COALESCE(pe,0), COALESCE(fscore,0), COALESCE(market_cap,0),
		       COALESCE(mcap_bucket,''), COALESCE(index_name,'')
		  FROM manthan_stocks
		 WHERE run_date = $1::date
		 ORDER BY status, symbol`, date)
	if err != nil {
		return nil, fmt.Errorf("candidates: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r CandidateRow
		if err := rows.Scan(&r.Symbol, &r.Company, &r.Status, &r.Reason,
			&r.PE, &r.FScore, &r.MarketCap, &r.McapBucket, &r.IndexName); err != nil {
			return nil, err
		}
		res.Rows = append(res.Rows, r)
		res.CountsByStatus[r.Status]++
		if r.Reason != "" {
			res.CountsByReason[r.Reason]++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if res.Rows == nil {
		res.Rows = []CandidateRow{}
	}

	dates, err := e.signalsDB.QueryContext(ctx,
		`SELECT DISTINCT run_date::text FROM manthan_stocks ORDER BY 1 DESC LIMIT 15`)
	if err != nil {
		return nil, err
	}
	defer dates.Close()
	for dates.Next() {
		var d string
		if err := dates.Scan(&d); err != nil {
			return nil, err
		}
		res.RecentDates = append(res.RecentDates, d)
	}
	return res, dates.Err()
}

// ── 6.3 inbox browser + rejection analytics ─────────────────────────────

// InboxRow is one signal_inbox row for the browser.
type InboxRow struct {
	SignalID   string     `json:"signal_id"`
	UserID     string     `json:"user_id"`
	OrderType  string     `json:"order_type"`
	Symbol     string     `json:"symbol,omitempty"`
	Status     string     `json:"status"`
	ErrorClass string     `json:"error_class,omitempty"`
	Attempts   int        `json:"attempts"`
	LastError  string     `json:"last_error,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	Completed  *time.Time `json:"completed_at,omitempty"`
}

// InboxResult is the filtered browse + aggregates.
type InboxResult struct {
	Rows          []InboxRow     `json:"rows"`
	CountByStatus map[string]int `json:"count_by_status"`
	CountByClass  map[string]int `json:"count_by_class"`
	CountByUser   map[string]int `json:"count_by_user"`
	Days          int            `json:"days"`
}

// Inbox browses signal_inbox with optional status/class/user filters.
func (e *Explorer) Inbox(ctx context.Context, status, class, userID string, days int) (*InboxResult, error) {
	if days <= 0 || days > 90 {
		days = 7
	}
	res := &InboxResult{Days: days,
		CountByStatus: map[string]int{}, CountByClass: map[string]int{}, CountByUser: map[string]int{}}
	cutoff := time.Now().AddDate(0, 0, -days)

	q := `
		SELECT signal_id, user_id, order_type, COALESCE(payload->>'symbol',''),
		       status, COALESCE(last_error_class,''), attempts,
		       COALESCE(last_error,''), created_at, completed_at
		  FROM signal_inbox WHERE created_at >= $1`
	args := []any{cutoff}
	add := func(cond, val string) {
		args = append(args, val)
		q += fmt.Sprintf(" AND %s = $%d", cond, len(args))
	}
	if status != "" {
		add("status", status)
	}
	if class != "" {
		add("last_error_class", class)
	}
	if userID != "" {
		add("user_id", userID)
	}
	q += ` ORDER BY created_at DESC LIMIT 200`

	rows, err := e.fleet.execDB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("inbox browse: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r InboxRow
		var completed sql.NullTime
		if err := rows.Scan(&r.SignalID, &r.UserID, &r.OrderType, &r.Symbol,
			&r.Status, &r.ErrorClass, &r.Attempts, &r.LastError, &r.CreatedAt, &completed); err != nil {
			return nil, err
		}
		r.LastError = truncate(r.LastError, 200)
		if completed.Valid {
			t := completed.Time
			r.Completed = &t
		}
		res.Rows = append(res.Rows, r)
		res.CountByStatus[r.Status]++
		if r.ErrorClass != "" {
			res.CountByClass[r.ErrorClass]++
		}
		res.CountByUser[r.UserID]++
	}
	if res.Rows == nil {
		res.Rows = []InboxRow{}
	}
	return res, rows.Err()
}

// RejectionBucket is one taxonomy group of failed orders.
type RejectionBucket struct {
	ExchangeErrorTag string `json:"exchange_error_tag,omitempty"`
	RejectCategory   string `json:"reject_category,omitempty"`
	UserID           string `json:"user_id"`
	Day              string `json:"day"`
	Count            int    `json:"count"`
	SampleError      string `json:"sample_error,omitempty"`
}

// Rejections groups REJECTED/CANCELLED orders by the exchange-error
// taxonomy, user and day. sample_error carries one representative
// last_error so an untagged bucket is still readable.
func (e *Explorer) Rejections(ctx context.Context, days int) ([]RejectionBucket, error) {
	if days <= 0 || days > 90 {
		days = 14
	}
	rows, err := e.fleet.execDB.QueryContext(ctx, `
		SELECT COALESCE(exchange_error_tag,''), COALESCE(reject_category,''),
		       user_id, created_at::date::text, COUNT(*),
		       COALESCE(MAX(LEFT(last_error, 160)),'')
		  FROM manthan_orders
		 WHERE status IN ('REJECTED','CANCELLED','AMO_REJECTED')
		   AND created_at >= $1
		 GROUP BY 1, 2, user_id, created_at::date
		 ORDER BY created_at::date DESC, COUNT(*) DESC
		 LIMIT 200`, time.Now().AddDate(0, 0, -days))
	if err != nil {
		return nil, fmt.Errorf("rejections: %w", err)
	}
	defer rows.Close()
	out := []RejectionBucket{}
	for rows.Next() {
		var b RejectionBucket
		if err := rows.Scan(&b.ExchangeErrorTag, &b.RejectCategory, &b.UserID, &b.Day, &b.Count, &b.SampleError); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
