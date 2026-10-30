package admin

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Daily signals (2026-10-30): one IST trading day, one row per stock on the
// day's Buy list, followed end to end —
//
//	VERDICT    data-ingestion pipeline (signals_db.manthan_stocks): ELIGIBLE /
//	           FILTER_REJECTED / DATA_DROPPED with the reason and the numbers
//	TRIGGER    mechanical ATH breakout (signals_db.ref_buy_triggers, db source):
//	           when, at what price, against which ATH close
//	SIGNAL     published to rules-engine (signals_db.manthan_signals)
//	CLIENTS    per client: rules-engine decision (trading_db.
//	           manthan_signal_decisions) → trade-execution inbox (execution_db.
//	           signal_inbox) → broker order (execution_db.manthan_orders)
//
// It answers the operator's daily question — "SGFIN was a Buy, why does
// FIV99 not own it?" — on one screen instead of four tables.
//
//	GET /admin/signals/daily?date=YYYY-MM-DD   (default: today IST)

// SignalTrigger is the breakout row for a symbol.
type SignalTrigger struct {
	At       time.Time `json:"at"`
	LTP      float64   `json:"ltp"`
	ATHClose float64   `json:"ath_close"`
	Source   string    `json:"source"`
}

// ClientOutcome is one client's path for one signal.
type ClientOutcome struct {
	UserID         string     `json:"user_id"`
	Outcome        string     `json:"outcome"` // FILLED | PARTIAL | PLACED | REFUSED | FAILED | SKIPPED | PENDING
	Note           string     `json:"note,omitempty"`
	DecisionStatus string     `json:"decision_status,omitempty"`
	DecisionReason string     `json:"decision_reason,omitempty"`
	IntendedQty    int        `json:"intended_qty,omitempty"`
	InboxStatus    string     `json:"inbox_status,omitempty"`
	InboxClass     string     `json:"inbox_class,omitempty"`
	InboxError     string     `json:"inbox_error,omitempty"`
	OrderStatus    string     `json:"order_status,omitempty"`
	FilledQty      int        `json:"filled_qty,omitempty"`
	AvgPrice       *float64   `json:"avg_price,omitempty"`
	OrderError     string     `json:"order_error,omitempty"`
	DecidedAt      *time.Time `json:"decided_at,omitempty"`
}

// DailySignal is one stock for the day.
type DailySignal struct {
	Symbol      string          `json:"symbol"`
	Company     string          `json:"company,omitempty"`
	Verdict     string          `json:"verdict"` // ELIGIBLE | FILTER_REJECTED | DATA_DROPPED | NOT_EVALUATED
	Reason      string          `json:"reason,omitempty"`
	Industry    string          `json:"industry,omitempty"`
	MCapBucket  string          `json:"mcap_bucket,omitempty"`
	IndexName   string          `json:"index_name,omitempty"`
	MarketCap   float64         `json:"market_cap,omitempty"`
	PE          float64         `json:"pe,omitempty"`
	PAT         float64         `json:"pat,omitempty"`
	FScore      float64         `json:"fscore,omitempty"`
	Price       float64         `json:"price,omitempty"`
	ATHClose    float64         `json:"ath_close,omitempty"`
	Allocation  float64         `json:"allocation,omitempty"`
	Trigger     *SignalTrigger  `json:"trigger,omitempty"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
	FirstSeenAt *time.Time      `json:"first_seen_at,omitempty"`
	Clients     []ClientOutcome `json:"clients"`
}

// DailySignalCounts is the stat strip.
type DailySignalCounts struct {
	BuyList   int `json:"buy_list"`
	Eligible  int `json:"eligible"`
	Rejected  int `json:"rejected"` // FILTER_REJECTED + DATA_DROPPED
	Triggers  int `json:"triggers"`
	Published int `json:"published"`
	Filled    int `json:"filled"`  // client entries filled (incl. partial)
	Placed    int `json:"placed"`  // at the broker, not yet filled
	Refused   int `json:"refused"` // broker rejected / dead-lettered
	Skipped   int `json:"skipped"` // rules-engine decided not to enter
}

// DailySignalsResult is the payload.
type DailySignalsResult struct {
	Date        string            `json:"date"`
	IsToday     bool              `json:"is_today"`
	Counts      DailySignalCounts `json:"counts"`
	Signals     []DailySignal     `json:"signals"`
	Notes       []string          `json:"notes,omitempty"`
	GeneratedAt time.Time         `json:"generated_at"`
}

// DailySignals assembles one IST day.
func (e *Explorer) DailySignals(ctx context.Context, date string) (*DailySignalsResult, error) {
	if e.signalsDB == nil {
		return nil, fmt.Errorf("signals_db unwired — daily signals unavailable")
	}
	ist, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		ist = time.FixedZone("IST", 5*3600+1800)
	}
	now := time.Now().In(ist)
	if date == "" {
		date = now.Format("2006-01-02")
	}
	day, err := time.ParseInLocation("2006-01-02", date, ist)
	if err != nil {
		return nil, fmt.Errorf("date must be YYYY-MM-DD")
	}
	start, end := day.UTC(), day.AddDate(0, 0, 1).UTC()
	res := &DailySignalsResult{Date: date, IsToday: date == now.Format("2006-01-02"), Signals: []DailySignal{}, GeneratedAt: time.Now().UTC()}

	bySym := map[string]*DailySignal{}
	get := func(sym string) *DailySignal {
		sym = strings.ToUpper(strings.TrimSpace(sym))
		if s, ok := bySym[sym]; ok {
			return s
		}
		s := &DailySignal{Symbol: sym, Verdict: "NOT_EVALUATED", Clients: []ClientOutcome{}}
		bySym[sym] = s
		return s
	}

	// 1. Verdicts.
	rows, err := e.signalsDB.QueryContext(ctx, `
		SELECT symbol, COALESCE(company_name,''), status, COALESCE(reason,''), COALESCE(industry,''),
		       COALESCE(mcap_bucket,''), COALESCE(index_name,''), COALESCE(market_cap,0), COALESCE(pe,0),
		       COALESCE(pat,0), COALESCE(fscore,0), COALESCE(latest_price,0), COALESCE(ath_close,0), COALESCE(allocation,0)
		  FROM manthan_stocks WHERE run_date = $1::date`, date)
	if err != nil {
		return nil, fmt.Errorf("daily signals verdicts: %w", err)
	}
	for rows.Next() {
		var sym string
		var d DailySignal
		if err := rows.Scan(&sym, &d.Company, &d.Verdict, &d.Reason, &d.Industry, &d.MCapBucket, &d.IndexName,
			&d.MarketCap, &d.PE, &d.PAT, &d.FScore, &d.Price, &d.ATHClose, &d.Allocation); err != nil {
			rows.Close()
			return nil, err
		}
		s := get(sym)
		d.Symbol, d.Clients, d.Trigger = s.Symbol, s.Clients, s.Trigger
		*s = d
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// 2. Mechanical triggers (table exists once migration 013 is applied).
	var haveTriggers bool
	if err := e.signalsDB.QueryRowContext(ctx, `SELECT to_regclass('ref_buy_triggers') IS NOT NULL`).Scan(&haveTriggers); err != nil {
		return nil, err
	}
	if haveTriggers {
		rows, err = e.signalsDB.QueryContext(ctx, `
			SELECT symbol, triggered_at, ltp, ath_close, source FROM ref_buy_triggers WHERE trade_date = $1::date`, date)
		if err != nil {
			return nil, fmt.Errorf("daily signals triggers: %w", err)
		}
		for rows.Next() {
			var sym string
			var t SignalTrigger
			if err := rows.Scan(&sym, &t.At, &t.LTP, &t.ATHClose, &t.Source); err != nil {
				rows.Close()
				return nil, err
			}
			t.At = t.At.UTC()
			get(sym).Trigger = &t
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	// 3. Published signals.
	rows, err = e.signalsDB.QueryContext(ctx, `
		SELECT symbol, published_to_kafka_at, first_seen_at FROM manthan_signals WHERE run_date = $1::date`, date)
	if err != nil {
		return nil, fmt.Errorf("daily signals published: %w", err)
	}
	for rows.Next() {
		var sym string
		var pub, first sql.NullTime
		if err := rows.Scan(&sym, &pub, &first); err != nil {
			rows.Close()
			return nil, err
		}
		s := get(sym)
		if pub.Valid {
			t := pub.Time.UTC()
			s.PublishedAt = &t
		}
		if first.Valid {
			t := first.Time.UTC()
			s.FirstSeenAt = &t
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	syms := make([]string, 0, len(bySym))
	for sym := range bySym {
		syms = append(syms, sym)
	}

	// 4. Per-client fan-out. Each source is optional: a nil fleet handle
	//    degrades to a note, never a 500.
	type key struct{ sym, user string }
	outcomes := map[key]*ClientOutcome{}
	co := func(sym, user string) *ClientOutcome {
		k := key{strings.ToUpper(sym), user}
		if o, ok := outcomes[k]; ok {
			return o
		}
		o := &ClientOutcome{UserID: user}
		outcomes[k] = o
		return o
	}
	if e.fleet != nil && e.fleet.tradingDB != nil && len(syms) > 0 {
		rows, err = e.fleet.tradingDB.QueryContext(ctx, `
			SELECT DISTINCT ON (UPPER(symbol), user_id) UPPER(symbol), user_id, status, COALESCE(rejection_reason,''),
			       COALESCE(intended_qty,0), decided_at
			  FROM manthan_signal_decisions
			 WHERE decided_at >= $1 AND decided_at < $2 AND UPPER(symbol) = ANY($3)
			   AND COALESCE(signal_type,'ENTRY_BUY') IN ('ENTRY_BUY','MANTHAN_ENTRY','ENTRY')
			 ORDER BY UPPER(symbol), user_id, decided_at DESC`, start, end, pq.Array(syms))
		if err != nil {
			res.Notes = append(res.Notes, "rules-engine decisions unavailable: "+err.Error())
		} else {
			for rows.Next() {
				var sym, uid, st, reason string
				var qty int
				var at time.Time
				if err := rows.Scan(&sym, &uid, &st, &reason, &qty, &at); err != nil {
					rows.Close()
					return nil, err
				}
				o := co(sym, uid)
				at = at.UTC()
				o.DecisionStatus, o.DecisionReason, o.IntendedQty, o.DecidedAt = st, reason, qty, &at
			}
			rows.Close()
		}
	} else if e.fleet == nil || e.fleet.tradingDB == nil {
		res.Notes = append(res.Notes, "trading_db unwired — rules-engine decisions not shown")
	}
	if e.fleet != nil && e.fleet.execDB != nil && len(syms) > 0 {
		rows, err = e.fleet.execDB.QueryContext(ctx, `
			SELECT DISTINCT ON (UPPER(payload->>'symbol'), user_id) UPPER(payload->>'symbol'), user_id, status,
			       COALESCE(last_error_class,''), COALESCE(last_error,'')
			  FROM signal_inbox
			 WHERE created_at >= $1 AND created_at < $2 AND order_type = 'MANTHAN_ENTRY'
			   AND UPPER(payload->>'symbol') = ANY($3)
			 ORDER BY UPPER(payload->>'symbol'), user_id, id DESC`, start, end, pq.Array(syms))
		if err != nil {
			res.Notes = append(res.Notes, "trade-execution inbox unavailable: "+err.Error())
		} else {
			for rows.Next() {
				var sym, uid, st, class, lastErr string
				if err := rows.Scan(&sym, &uid, &st, &class, &lastErr); err != nil {
					rows.Close()
					return nil, err
				}
				o := co(sym, uid)
				o.InboxStatus, o.InboxClass, o.InboxError = st, class, lastErr
			}
			rows.Close()
		}
		rows, err = e.fleet.execDB.QueryContext(ctx, `
			SELECT DISTINCT ON (UPPER(symbol), user_id) UPPER(symbol), user_id, status, COALESCE(filled_qty,0),
			       avg_fill_price, COALESCE(last_error,'')
			  FROM manthan_orders
			 WHERE order_side = 'BUY' AND created_at >= $1 AND created_at < $2 AND UPPER(symbol) = ANY($3)
			 ORDER BY UPPER(symbol), user_id,
			          CASE status WHEN 'FILLED' THEN 0 WHEN 'PARTIAL' THEN 1 WHEN 'PLACED' THEN 2 WHEN 'PENDING' THEN 3 ELSE 4 END,
			          id DESC`, start, end, pq.Array(syms))
		if err != nil {
			res.Notes = append(res.Notes, "orders unavailable: "+err.Error())
		} else {
			for rows.Next() {
				var sym, uid, st, lastErr string
				var filled int
				var avg sql.NullFloat64
				if err := rows.Scan(&sym, &uid, &st, &filled, &avg, &lastErr); err != nil {
					rows.Close()
					return nil, err
				}
				o := co(sym, uid)
				o.OrderStatus, o.FilledQty, o.OrderError = st, filled, lastErr
				if avg.Valid && avg.Float64 > 0 {
					v := round2f(avg.Float64)
					o.AvgPrice = &v
				}
			}
			rows.Close()
		}
	} else if e.fleet == nil || e.fleet.execDB == nil {
		res.Notes = append(res.Notes, "execution_db unwired — inbox and orders not shown")
	}

	for k, o := range outcomes {
		o.Outcome, o.Note = classifyOutcome(o)
		s := get(k.sym)
		s.Clients = append(s.Clients, *o)
	}

	// 5. Assemble, count, sort (eligible first, then by trigger/publish time).
	for _, s := range bySym {
		sort.Slice(s.Clients, func(i, j int) bool { return s.Clients[i].UserID < s.Clients[j].UserID })
		res.Counts.BuyList++
		switch s.Verdict {
		case "ELIGIBLE":
			res.Counts.Eligible++
		case "FILTER_REJECTED", "DATA_DROPPED":
			res.Counts.Rejected++
		}
		if s.Trigger != nil {
			res.Counts.Triggers++
		}
		if s.PublishedAt != nil || s.FirstSeenAt != nil {
			res.Counts.Published++
		}
		for _, c := range s.Clients {
			switch c.Outcome {
			case "FILLED", "PARTIAL":
				res.Counts.Filled++
			case "PLACED", "PENDING":
				res.Counts.Placed++
			case "REFUSED", "FAILED":
				res.Counts.Refused++
			case "SKIPPED":
				res.Counts.Skipped++
			}
		}
		res.Signals = append(res.Signals, *s)
	}
	rank := map[string]int{"ELIGIBLE": 0, "FILTER_REJECTED": 1, "DATA_DROPPED": 2, "NOT_EVALUATED": 3}
	sort.Slice(res.Signals, func(i, j int) bool {
		a, b := res.Signals[i], res.Signals[j]
		if rank[a.Verdict] != rank[b.Verdict] {
			return rank[a.Verdict] < rank[b.Verdict]
		}
		return a.Symbol < b.Symbol
	})
	return res, nil
}

// classifyOutcome folds decision → inbox → order into one word and a
// plain-language note. The broker order is the strongest evidence, then the
// inbox, then the rules-engine decision.
func classifyOutcome(o *ClientOutcome) (string, string) {
	switch o.OrderStatus {
	case "FILLED":
		return "FILLED", ""
	case "PARTIAL":
		return "PARTIAL", fmt.Sprintf("%d filled so far", o.FilledQty)
	case "PLACED", "PENDING":
		return "PLACED", "at the broker, not filled yet"
	case "REJECTED", "CANCELLED":
		return "REFUSED", rejectReason(o.OrderStatus, o.OrderError)
	}
	switch o.InboxStatus {
	case "DLQ":
		return "FAILED", "trade-execution gave up: " + firstNonEmpty(o.InboxError, o.InboxClass)
	case "PENDING", "RUNNING", "RETRY":
		return "PENDING", "trade-execution still processing" + suffix(o.InboxError)
	case "DONE":
		if o.InboxError != "" {
			return "SKIPPED", "trade-execution: " + o.InboxError
		}
		return "PENDING", "processed, no order row yet"
	}
	st := strings.ToUpper(o.DecisionStatus)
	switch {
	case strings.Contains(st, "REJECT") || strings.Contains(st, "SKIP"):
		return "SKIPPED", firstNonEmpty(o.DecisionReason, "rules-engine did not enter")
	case st != "":
		return "PENDING", "rules-engine " + strings.ToLower(st) + ", not yet at trade-execution"
	}
	return "PENDING", ""
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func suffix(s string) string {
	if s == "" {
		return ""
	}
	return ": " + s
}
