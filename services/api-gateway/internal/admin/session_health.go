package admin

// Session health — the AUTH_EXPIRED / STALE_CREDENTIAL attention items,
// decided from the NEWEST evidence per client instead of a once-a-day
// verdict.
//
// Before 2026-10-13 the queue carried the 08:30 sweep's result all day: a
// client who logged in at 12:38 still showed "live probe AUTH_EXPIRED" at
// 16:00, and the age heuristic added a second CRITICAL for the same user.
// Now, per client:
//
//	evidence = newest of { last probe verdict, last AU004-rejected broker
//	           call, last broker-ACCEPTED call (order id issued) }
//	login after that evidence → re-probe live (rate-limited) and decide on
//	           the fresh verdict; the login hooks (POST /auth/credentials,
//	           token auto-capture) also trigger the re-probe, so the item
//	           clears within seconds of a re-login.
//	no evidence at all → STALE_CREDENTIAL (WARN) once the token is older
//	           than credStaleAfter; otherwise nothing.
//
// Severity: CRITICAL while the client has an active strategy or open
// positions (entries, trail modifies and re-arms all fail); INFO otherwise.
// Times in details are IST.

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// probeMinGap bounds on-demand probes to one broker call per client per
// gap while the queue is polled every 30 s.
const probeMinGap = 60 * time.Second

type sessionEvidence struct {
	UserID        string
	Active        bool
	OpenPositions int
	HasCred       bool          // a non-blank token is stored
	CredAge       time.Duration // since user_credentials.updated_at (valid when HasCred)
	ProbeVerdict  string        // "" = never probed
	ProbeAge      time.Duration
	ProbeDetail   string
	AU004Age      time.Duration // since the last AU004-rejected broker call; < 0 = none
	AU004Symbol   string
	BrokerOKAge   time.Duration // since the last call the broker accepted; < 0 = none
}

type sessionState struct {
	State      string // OK | EXPIRED | STALE | UNKNOWN
	Reason     string // the evidence that decided it
	NeedsProbe bool   // credential refreshed after every piece of evidence — verify live
}

// evalSession is the pure decision. Exposed for tests.
func evalSession(e sessionEvidence) sessionState {
	if !e.HasCred {
		return sessionState{State: "EXPIRED", Reason: "no broker credential stored"}
	}
	if e.CredAge < 0 {
		e.CredAge = 0 // DB-side ages cannot be negative; guard against a skewed caller
	}
	type ev struct {
		age    time.Duration
		kind   string
		detail string
	}
	var newest *ev
	consider := func(age time.Duration, kind, detail string) {
		if age < 0 {
			return
		}
		if newest == nil || age < newest.age {
			newest = &ev{age, kind, detail}
		}
	}
	if e.ProbeVerdict != "" {
		consider(e.ProbeAge, "probe", e.ProbeDetail)
	}
	consider(e.AU004Age, "au004", e.AU004Symbol)
	consider(e.BrokerOKAge, "broker_ok", "")

	if newest == nil {
		if e.CredAge > credStaleAfter {
			return sessionState{State: "STALE", Reason: fmt.Sprintf("token %s old and no broker activity to confirm it", fmtAge(e.CredAge))}
		}
		return sessionState{State: "UNKNOWN", Reason: "fresh credential, not yet verified", NeedsProbe: true}
	}
	needsProbe := e.CredAge < newest.age // re-login after the newest evidence
	switch newest.kind {
	case "probe":
		switch e.ProbeVerdict {
		case "WORKS":
			return sessionState{State: "OK", Reason: fmt.Sprintf("probe WORKS %s ago", fmtAge(newest.age)), NeedsProbe: needsProbe}
		case "ERROR":
			if e.CredAge > credStaleAfter {
				return sessionState{State: "STALE", Reason: "probe could not reach the broker; token " + fmtAge(e.CredAge) + " old", NeedsProbe: needsProbe}
			}
			return sessionState{State: "UNKNOWN", Reason: "probe could not reach the broker", NeedsProbe: needsProbe}
		default: // AUTH_EXPIRED, NO_CREDENTIAL
			return sessionState{State: "EXPIRED", Reason: fmt.Sprintf("live probe %s %s ago", e.ProbeVerdict, fmtAge(newest.age)), NeedsProbe: needsProbe}
		}
	case "au004":
		r := fmt.Sprintf("broker rejected a call with AU004 %s ago", fmtAge(newest.age))
		if e.AU004Symbol != "" {
			r = fmt.Sprintf("broker rejected %s with AU004 %s ago", e.AU004Symbol, fmtAge(newest.age))
		}
		return sessionState{State: "EXPIRED", Reason: r, NeedsProbe: needsProbe}
	default: // broker_ok
		return sessionState{State: "OK", Reason: fmt.Sprintf("broker accepted an order %s ago", fmtAge(newest.age)), NeedsProbe: needsProbe}
	}
}

func fmtAge(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	default:
		return fmt.Sprintf("%.0fd", d.Hours()/24)
	}
}

// SessionHealth joins fleet rows, broker evidence and the prober.
type SessionHealth struct {
	fleet  *FleetStore
	prober *Prober
	minGap time.Duration
	now    func() time.Time
	ist    *time.Location
}

func NewSessionHealth(f *FleetStore, p *Prober) *SessionHealth {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil || loc == nil {
		loc = time.FixedZone("IST", 5*3600+30*60)
	}
	return &SessionHealth{fleet: f, prober: p, minGap: probeMinGap, now: time.Now, ist: loc}
}

type brokerEvidence struct {
	au004Age    time.Duration
	au004Symbol string
	okAge       time.Duration
}

// brokerEvidence reads, per user, the last AU004 rejection and the last
// broker-accepted call (an order id was issued) in the last 7 days. Ages
// are computed DB-side so clock skew cannot flip a verdict.
func (f *FleetStore) brokerEvidence(ctx context.Context) (map[string]brokerEvidence, error) {
	rows, err := f.execDB.QueryContext(ctx, `
		SELECT user_id,
		       COALESCE(EXTRACT(EPOCH FROM (now() - MAX(created_at) FILTER (WHERE last_error LIKE '%AU004%'))), -1),
		       COALESCE((array_agg(symbol ORDER BY created_at DESC) FILTER (WHERE last_error LIKE '%AU004%'))[1], ''),
		       COALESCE(EXTRACT(EPOCH FROM (now() - MAX(COALESCE(placed_at, created_at))
		                 FILTER (WHERE COALESCE(broker_order_id,'') <> ''))), -1)
		  FROM manthan_orders
		 WHERE created_at >= now() - interval '7 days'
		 GROUP BY user_id`)
	if err != nil {
		return nil, fmt.Errorf("session evidence: %w", err)
	}
	defer rows.Close()
	out := map[string]brokerEvidence{}
	for rows.Next() {
		var uid, sym string
		var au, ok float64
		if err := rows.Scan(&uid, &au, &sym, &ok); err != nil {
			return nil, err
		}
		be := brokerEvidence{au004Age: -1, okAge: -1, au004Symbol: sym}
		if au >= 0 {
			be.au004Age = time.Duration(au * float64(time.Second))
		}
		if ok >= 0 {
			be.okAge = time.Duration(ok * float64(time.Second))
		}
		out[uid] = be
	}
	return out, rows.Err()
}

// Items builds one session item per client that needs a human: AUTH_EXPIRED
// (CRITICAL/INFO) or STALE_CREDENTIAL (WARN). Healthy clients produce nothing.
func (s *SessionHealth) Items(ctx context.Context) ([]AttentionItem, error) {
	fleet, err := s.fleet.Fleet(ctx)
	if err != nil {
		return nil, err
	}
	type agg struct {
		active  bool
		open    int
		hasCred bool
		credAge time.Duration
	}
	users := map[string]*agg{}
	for _, r := range fleet {
		a := users[r.UserID]
		if a == nil {
			a = &agg{}
			users[r.UserID] = a
		}
		a.active = a.active || r.Active
		a.open += r.OpenPositions
		if r.CredentialAgeHours >= 0 {
			a.hasCred, a.credAge = true, time.Duration(r.CredentialAgeHours*float64(time.Hour))
		}
	}
	evidence, err := s.fleet.brokerEvidence(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(users))
	for uid := range users {
		ids = append(ids, uid)
	}
	sort.Strings(ids)

	now := s.now()
	var items []AttentionItem
	for _, uid := range ids {
		a := users[uid]
		e := sessionEvidence{UserID: uid, Active: a.active, OpenPositions: a.open, HasCred: a.hasCred, CredAge: a.credAge, AU004Age: -1, BrokerOKAge: -1}
		if be, ok := evidence[uid]; ok {
			e.AU004Age, e.AU004Symbol, e.BrokerOKAge = be.au004Age, be.au004Symbol, be.okAge
		}
		applyProbe := func(v ProbeVerdict, ok bool) {
			if ok && v.Verdict != "" {
				age := now.Sub(v.ProbedAt)
				if age < 0 {
					age = 0 // an on-demand probe taken after this call's clock snapshot
				}
				e.ProbeVerdict, e.ProbeDetail, e.ProbeAge = v.Verdict, v.Detail, age
			}
		}
		if s.prober != nil {
			applyProbe(s.prober.LastVerdict(uid))
		}
		st := evalSession(e)
		if st.NeedsProbe && s.prober != nil && s.prober.canProbe(uid, s.minGap, now) {
			v := s.prober.ProbeNow(ctx, uid)
			applyProbe(v, true)
			st = evalSession(e)
		}
		refreshed := "never"
		if e.HasCred {
			refreshed = fmt.Sprintf("%s (%s ago)", now.Add(-e.CredAge).In(s.ist).Format("02 Jan 15:04 IST"), fmtAge(e.CredAge))
		}
		switch st.State {
		case "EXPIRED":
			sev := "INFO"
			if a.active || a.open > 0 {
				sev = "CRITICAL"
			}
			detail := fmt.Sprintf("broker session dead — %s; token last refreshed %s; platform re-login required", st.Reason, refreshed)
			if st.NeedsProbe {
				detail += " (re-login seen, awaiting live verification)"
			}
			items = append(items, AttentionItem{Severity: sev, Kind: "AUTH_EXPIRED", UserID: uid, Detail: detail, Module: "session"})
		case "STALE":
			items = append(items, AttentionItem{Severity: "WARN", Kind: "STALE_CREDENTIAL", UserID: uid,
				Detail: fmt.Sprintf("broker session unverified — %s; last refreshed %s; a re-login before 09:14 IST keeps stops armed", st.Reason, refreshed),
				Module: "session"})
		}
	}
	return items, nil
}
