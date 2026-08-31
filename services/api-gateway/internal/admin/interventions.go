package admin

// M7 Phase A — interventions: act on anything, with each procedure's
// validated guardrail encoded server-side. The dashboard's job is to make
// the safe way the only way.
//
// 7.1 Resurrect a dead entry signal — the EXACT UPDATE validated live
//     three times on 2026-08-27 (SHREEPUSHK). Guardrail: same TRADING DAY
//     only, enforced by refusal (yesterday's signal never places today) —
//     and MANTHAN_ENTRY only (a stale SL_MODIFY must never be replayed;
//     re-arming rebuilds stops from current truth instead — 7.2).
//
// 7.2 Re-arm protection for a user — authenticated, audited proxy to
//     trade-execution's existing operator override
//     (POST {metrics}/manthan/replay/runOnceForUser), which runs the same
//     buildPlans → fireAll → reconcile flow as the 09:14 cron for one user.
//
// 7.6 Cap & hold overrides —
//     release: a held inbox row (upper-circuit / auth / pre-open) gets
//       next_attempt_at=now(); the worker's own gates still apply, so an
//       auth-dead user's row simply re-queues, it cannot skip the gate.
//     cap reset: tonight's failed SL_SELL_AMO attempts get trade_date=NULL
//       — CountAMOAttemptsToday filters on trade_date, so the counter
//       restarts while every attempt row survives for audit. Reason text
//       is REQUIRED on both and lands in the audit params.
//
// Phase A2 (next): 7.4 order view/cancel (SL-family cancel REFUSED — the
// June liquidation trigger), 7.5 tradebook-verified ghost cleanup.
// Phase B (needs trade-execution changes): 7.3 supervised square-off,
// 7.7 rebalance trigger.

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Interventions bundles Phase A's dependencies.
type Interventions struct {
	fleet    *FleetStore
	ist      *time.Location
	rearmURL string // trade-execution metrics base, e.g. http://localhost:9090
	httpc    *http.Client
}

func NewInterventions(fleet *FleetStore, ist *time.Location, rearmURL string) *Interventions {
	return &Interventions{
		fleet: fleet, ist: ist,
		rearmURL: strings.TrimRight(rearmURL, "/"),
		// RunOnceForUser walks every position for the user against the live
		// broker — trade-execution caps it at 2 minutes; we allow slack.
		httpc: &http.Client{Timeout: 150 * time.Second},
	}
}

// ── 7.1 resurrect ───────────────────────────────────────────────────────

// ResurrectInfo echoes what was resurrected so the audit row carries it.
type ResurrectInfo struct {
	InboxID   int64  `json:"inbox_id"`
	UserID    string `json:"user_id"`
	Symbol    string `json:"symbol,omitempty"`
	OldStatus string `json:"old_status"`
}

// Resurrect resets one dead MANTHAN_ENTRY inbox row for retry — the
// validated procedure, verbatim: status='FAILED', attempts=0,
// next_attempt_at=now(), completed_at=NULL.
func (iv *Interventions) Resurrect(ctx context.Context, inboxID int64) (*ResurrectInfo, error) {
	var info ResurrectInfo
	var orderType string
	var createdAt time.Time
	err := iv.fleet.execDB.QueryRowContext(ctx, `
		SELECT id, user_id, order_type, status, created_at, COALESCE(payload->>'symbol','')
		  FROM signal_inbox WHERE id = $1`, inboxID).
		Scan(&info.InboxID, &info.UserID, &orderType, &info.OldStatus, &createdAt, &info.Symbol)
	if err == sql.ErrNoRows {
		return nil, &refusal{code: 404, msg: fmt.Sprintf("inbox row %d not found", inboxID)}
	}
	if err != nil {
		return nil, fmt.Errorf("resurrect lookup: %w", err)
	}

	if orderType != "MANTHAN_ENTRY" {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"row is %s — only MANTHAN_ENTRY signals are resurrectable; for protection, re-arm the user instead (stops rebuild from current truth)", orderType)}
	}
	// Same TRADING DAY only, IST — refused, not warned.
	now := time.Now().In(iv.ist)
	created := createdAt.In(iv.ist)
	if created.Year() != now.Year() || created.YearDay() != now.YearDay() {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"signal is from %s — same-trading-day only: yesterday's signal never places today (entry price basis is stale)", created.Format("2006-01-02"))}
	}
	if info.OldStatus == "RUNNING" || info.OldStatus == "PENDING" {
		return nil, &refusal{code: 422, msg: fmt.Sprintf("row is %s — already queued/live, nothing to resurrect", info.OldStatus)}
	}

	res, err := iv.fleet.execDB.ExecContext(ctx, `
		UPDATE signal_inbox
		   SET status = 'FAILED', attempts = 0, next_attempt_at = now(), completed_at = NULL
		 WHERE id = $1`, inboxID)
	if err != nil {
		return nil, fmt.Errorf("resurrect update: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, &refusal{code: 422, msg: "row vanished mid-flight — re-check and retry"}
	}
	return &info, nil
}

// ── 7.2 re-arm protection ───────────────────────────────────────────────

// Rearm proxies trade-execution's operator override for one user.
func (iv *Interventions) Rearm(ctx context.Context, userID string) (string, error) {
	u := iv.rearmURL + "/manthan/replay/runOnceForUser?user_id=" + url.QueryEscape(userID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return "", err
	}
	resp, err := iv.httpc.Do(req)
	if err != nil {
		return "", fmt.Errorf("trade-execution unreachable: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return "", &refusal{code: 502, msg: fmt.Sprintf(
			"trade-execution refused re-arm (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))}
	}
	return string(body), nil
}

// ── 7.6 cap & hold overrides ────────────────────────────────────────────

// ReleaseHold re-queues one held inbox row now. The worker's own gates
// (auth, upper-circuit re-check, market hours) still apply on pickup.
func (iv *Interventions) ReleaseHold(ctx context.Context, inboxID int64) (*ResurrectInfo, error) {
	var info ResurrectInfo
	var nextAt sql.NullTime
	err := iv.fleet.execDB.QueryRowContext(ctx, `
		SELECT id, user_id, status, next_attempt_at, COALESCE(payload->>'symbol','')
		  FROM signal_inbox WHERE id = $1`, inboxID).
		Scan(&info.InboxID, &info.UserID, &info.OldStatus, &nextAt, &info.Symbol)
	if err == sql.ErrNoRows {
		return nil, &refusal{code: 404, msg: fmt.Sprintf("inbox row %d not found", inboxID)}
	}
	if err != nil {
		return nil, fmt.Errorf("release lookup: %w", err)
	}
	if info.OldStatus != "PENDING" && info.OldStatus != "FAILED" {
		return nil, &refusal{code: 422, msg: fmt.Sprintf(
			"row is %s — only PENDING/FAILED holds can be released (a DLQ'd entry wants resurrect; a DLQ'd SL wants re-arm)", info.OldStatus)}
	}
	if _, err := iv.fleet.execDB.ExecContext(ctx,
		`UPDATE signal_inbox SET next_attempt_at = now() WHERE id = $1`, inboxID); err != nil {
		return nil, fmt.Errorf("release update: %w", err)
	}
	return &info, nil
}

// ResetAMOCap clears tonight's give-up counter for one (user, symbol):
// failed SL_SELL_AMO attempts targeting the upcoming session get
// trade_date=NULL — out of CountAMOAttemptsToday, preserved for audit.
func (iv *Interventions) ResetAMOCap(ctx context.Context, userID, symbol string) (int64, error) {
	res, err := iv.fleet.execDB.ExecContext(ctx, `
		UPDATE manthan_orders
		   SET trade_date = NULL
		 WHERE user_id = $1
		   AND UPPER(symbol) = $2
		   AND order_type = 'SL_SELL_AMO'
		   AND status IN ('CANCELLED','REJECTED')
		   AND trade_date >= CURRENT_DATE
		   AND created_at >= now() - interval '24 hours'`,
		userID, strings.ToUpper(symbol))
	if err != nil {
		return 0, fmt.Errorf("cap reset: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, &refusal{code: 422, msg: fmt.Sprintf(
			"no counted give-up attempts for %s/%s tonight — nothing to reset", userID, strings.ToUpper(symbol))}
	}
	return n, nil
}

// refusal is a guardrail rejection with an HTTP status — distinct from
// infrastructure errors so handlers map it without string matching.
type refusal struct {
	code int
	msg  string
}

func (r *refusal) Error() string { return r.msg }

// asRefusal unwraps a guardrail refusal (nil if infra error).
func asRefusal(err error) *refusal {
	if r, ok := err.(*refusal); ok {
		return r
	}
	return nil
}
