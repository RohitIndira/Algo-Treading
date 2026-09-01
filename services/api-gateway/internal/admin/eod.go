package admin

// M8.1 — overnight arming board: the Aug 26 evening (dozens of doomed AMO
// placements, two users half-armed, visible only via log archaeology),
// self-reporting. Per open position: tonight's protection state with
// attempt counts, this morning's 08:50 conversion outcomes, the AMO
// window clock, and per-row action hints into the M7 endpoints.

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

// EODRow is one open position's overnight story.
type EODRow struct {
	UserID     string `json:"user_id"`
	StrategyID string `json:"strategy_id,omitempty"`
	Symbol     string `json:"symbol"`
	Qty        int    `json:"qty"`

	// Tonight (rows targeting the upcoming session).
	State         string `json:"state"` // ARMED | AMO_PENDING | REJECTED | CAPPED | DEFERRED | USER_BLOCKED | NAKED
	BrokerOrderID string `json:"broker_order_id,omitempty"`
	Attempts      int    `json:"attempts,omitempty"` // failed overnight attempts, n/5
	LastError     string `json:"last_error,omitempty"`

	// This morning's 08:50 conversion outcome (rows whose trade_date was today).
	Conversion string `json:"conversion,omitempty"` // PROMOTED | REJECTED_AT_CONVERSION | ""

	Actions []string `json:"actions,omitempty"` // per-row admin hints
}

// EODBoard is the whole overnight picture.
type EODBoard struct {
	Rows        []EODRow       `json:"rows"`
	Counts      map[string]int `json:"counts"`
	AMOWindow   string         `json:"amo_window"` // OPEN | CLOSED
	WindowNote  string         `json:"window_note"`
	GeneratedAt time.Time      `json:"generated_at"`
}

// EODStore assembles the board. Prober is optional (dead-session facts).
type EODStore struct {
	fleet  *FleetStore
	prober *Prober
	ist    *time.Location
	now    func() time.Time
}

func NewEODStore(fleet *FleetStore, prober *Prober, ist *time.Location) *EODStore {
	return &EODStore{fleet: fleet, prober: prober, ist: ist, now: time.Now}
}

// amoWindowState mirrors trade-execution's placement gate: overnight AMO
// placement runs 16:00 → 08:45 IST; conversion fires ~08:50.
func (e *EODStore) amoWindowState() (string, string) {
	now := e.now().In(e.ist)
	mins := now.Hour()*60 + now.Minute()
	switch {
	case mins >= 16*60:
		return "OPEN", "overnight placement window open (16:00 → 08:45 IST); conversion at ~08:50"
	case mins < 8*60+45:
		return "OPEN", fmt.Sprintf("overnight window open until 08:45 IST (now %s); conversion at ~08:50", now.Format("15:04"))
	case mins < 8*60+55:
		return "CLOSED", "conversion in progress (~08:50 IST) — outcomes appear below as they land"
	default:
		return "CLOSED", fmt.Sprintf("intraday (%s IST) — hot SLs govern; overnight placement resumes 16:00", now.Format("15:04"))
	}
}

// Board builds the overnight view for every open position.
func (e *EODStore) Board(ctx context.Context) (*EODBoard, error) {
	b := &EODBoard{Counts: map[string]int{}, GeneratedAt: e.now()}
	b.AMOWindow, b.WindowNote = e.amoWindowState()

	// Open book (whole fleet, strategy-less legacy rows included).
	rows, err := e.fleet.posDB.QueryContext(ctx, `
		SELECT user_id, COALESCE(strategy_id::text,''), UPPER(symbol), COALESCE(SUM(quantity),0)
		  FROM positions WHERE status = 'ACTIVE'
		 GROUP BY user_id, strategy_id, symbol`)
	if err != nil {
		return nil, fmt.Errorf("eod book: %w", err)
	}
	for rows.Next() {
		var r EODRow
		if err := rows.Scan(&r.UserID, &r.StrategyID, &r.Symbol, &r.Qty); err != nil {
			rows.Close()
			return nil, err
		}
		b.Rows = append(b.Rows, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Tonight's ledger, per (strategy, symbol): standing rows, failed
	// attempts targeting the UPCOMING session, and today's conversion
	// outcomes — one pass over recent protection rows.
	type ledgerFacts struct {
		standingState string // ARMED | AMO_PENDING | DEFERRED
		brokerID      string
		attempts      int
		lastError     string
		conversion    string
	}
	facts := map[string]*ledgerFacts{}
	// Standing rows regardless of age (a DEFERRED_BAND row can stand for
	// weeks — the 36h window on the first deploy misread MANORAMA as
	// NAKED); failures only from the last cycle-and-a-half.
	lr, err := e.fleet.execDB.QueryContext(ctx, `
		SELECT COALESCE(strategy_id::text,''), UPPER(symbol), order_type, status,
		       COALESCE(broker_order_id,''), COALESCE(last_error,''),
		       trade_date, created_at
		  FROM manthan_orders
		 WHERE order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND (status IN ('SL_PLACED','SL_MODIFY_PENDING','AMO_PENDING','SL_DEFERRED_BAND')
		        OR created_at >= now() - interval '36 hours')
		 ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("eod ledger: %w", err)
	}
	todayIST := e.now().In(e.ist).Format("2006-01-02")
	for lr.Next() {
		var sid, sym, otype, status, brokerID, lastErr string
		var tradeDate sql.NullTime
		var createdAt time.Time
		if err := lr.Scan(&sid, &sym, &otype, &status, &brokerID, &lastErr, &tradeDate, &createdAt); err != nil {
			lr.Close()
			return nil, err
		}
		key := sid + "|" + sym
		f := facts[key]
		if f == nil {
			f = &ledgerFacts{}
			facts[key] = f
		}
		tradeDay := ""
		if tradeDate.Valid {
			tradeDay = tradeDate.Time.Format("2006-01-02")
		}
		switch status {
		case "SL_PLACED", "SL_MODIFY_PENDING":
			f.standingState, f.brokerID = "ARMED", brokerID
		case "AMO_PENDING":
			if f.standingState != "ARMED" {
				f.standingState, f.brokerID = "AMO_PENDING", brokerID
			}
		case "SL_DEFERRED_BAND":
			if f.standingState == "" {
				f.standingState = "DEFERRED"
			}
		case "CANCELLED", "REJECTED", "AMO_REJECTED":
			// Conversion outcome: a failed row whose TARGET was today and
			// which died at/after conversion time.
			if tradeDay == todayIST && strings.Contains(lastErr, "conversion") {
				f.conversion = "REJECTED_AT_CONVERSION"
			}
			// Attempt counting: the current overnight cycle — intraday that
			// is LAST night's attempts (targeting today) which never
			// promoted; after 16:00 it becomes tonight's (targeting
			// tomorrow). >= keeps SHANTIGOLD's story visible all day.
			if otype == "SL_SELL_AMO" && tradeDay >= todayIST {
				f.attempts++
				if lastErr != "" {
					f.lastError = truncate(lastErr, 160)
				}
			}
		}
		// A standing SL_SELL created today whose lineage came through
		// conversion shows as promoted.
		if status == "SL_PLACED" && otype == "SL_SELL_AMO" && tradeDay == todayIST {
			f.conversion = "PROMOTED"
		}
		_ = createdAt
	}
	lr.Close()
	if err := lr.Err(); err != nil {
		return nil, err
	}

	// Dead sessions: stale credentials block every placement for the user.
	deadUsers := map[string]bool{}
	ages, err := e.fleet.credentialAges(ctx)
	if err == nil {
		for uid, age := range ages {
			if age < 0 || age > 20*time.Hour {
				deadUsers[uid] = true
			}
		}
	}
	if e.prober != nil {
		if sweep, at := e.prober.LastSweep(); !at.IsZero() {
			for uid, v := range sweep {
				if v.Verdict == "AUTH_EXPIRED" || v.Verdict == "NO_CREDENTIAL" {
					deadUsers[uid] = true
				} else if v.Verdict == "WORKS" {
					delete(deadUsers, uid) // live probe outranks the age heuristic
				}
			}
		}
	}

	for i := range b.Rows {
		r := &b.Rows[i]
		f := facts[r.StrategyID+"|"+r.Symbol]
		switch {
		case f != nil && f.standingState != "":
			r.State, r.BrokerOrderID = f.standingState, f.brokerID
		case f != nil && f.attempts >= 5:
			r.State, r.Attempts, r.LastError = "CAPPED", f.attempts, f.lastError
			r.Actions = append(r.Actions, "POST /admin/users/"+r.UserID+"/amo-cap/reset")
		case f != nil && f.attempts > 0:
			r.State, r.Attempts, r.LastError = "REJECTED", f.attempts, f.lastError
		case deadUsers[r.UserID]:
			r.State = "USER_BLOCKED"
			r.Actions = append(r.Actions, "GET /admin/users/"+r.UserID+"/credential")
		default:
			r.State = "NAKED"
		}
		if f != nil {
			r.Conversion = f.conversion
			if r.Attempts == 0 && f.attempts > 0 {
				r.Attempts = f.attempts
			}
		}
		if deadUsers[r.UserID] && (r.State == "NAKED" || r.State == "REJECTED") {
			// A dead session dooms retries regardless of the ledger state —
			// the credential is the thing to fix first.
			r.State = "USER_BLOCKED"
			r.Actions = append(r.Actions, "GET /admin/users/"+r.UserID+"/credential")
		}
		if r.State != "ARMED" && r.State != "AMO_PENDING" && r.State != "USER_BLOCKED" {
			r.Actions = append(r.Actions, "POST /admin/users/"+r.UserID+"/rearm-protection")
		}
		b.Counts[r.State]++
	}

	sort.Slice(b.Rows, func(i, j int) bool {
		a, c := b.Rows[i], b.Rows[j]
		if ra, rc := eodRank(a.State), eodRank(c.State); ra != rc {
			return ra < rc
		}
		if a.UserID != c.UserID {
			return a.UserID < c.UserID
		}
		return a.Symbol < c.Symbol
	})
	if b.Rows == nil {
		b.Rows = []EODRow{}
	}
	return b, nil
}

func eodRank(s string) int {
	switch s {
	case "NAKED":
		return 0
	case "USER_BLOCKED":
		return 1
	case "CAPPED":
		return 2
	case "REJECTED":
		return 3
	case "DEFERRED":
		return 4
	case "AMO_PENDING":
		return 5
	default: // ARMED
		return 6
	}
}
