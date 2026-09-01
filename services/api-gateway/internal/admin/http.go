package admin

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/auth"
)

// TokenHeader carries the elevation token on every /admin/* request.
// Deliberately NOT the Authorization header: the broker JWT and the admin
// token are different credentials and must never be confused for each other.
const TokenHeader = "X-Admin-Token"

type sessionKeyType int

const sessionKey sessionKeyType = 0

// SessionFromContext returns the validated admin session attached by
// Required. (nil, false) means the middleware did not run — a wiring bug.
func SessionFromContext(ctx context.Context) (*Session, bool) {
	s, ok := ctx.Value(sessionKey).(*Session)
	return s, ok
}

// HTTP bundles the service for route registration.
type HTTP struct {
	svc        *Service
	fleet      *FleetStore      // nil-safe: M2 routes absent when business DBs are unavailable
	prober     *Prober          // nil-safe: M3 probe routes absent without it
	strategies *StrategyControl // nil-safe: M4 strategy routes absent without it
	protection *ProtectionStore // nil-safe: M5.1 board route absent without it
	recon      *ReconStore      // nil-safe: M5.2/5.3 broker routes absent without it
	scale      *ScaleChecker    // nil-safe: M5.4 check route absent without it
	explorer   *Explorer        // nil-safe: M6 pipeline explorer routes absent without it
	interv     *Interventions   // nil-safe: M7 intervention routes absent without it
	actions    *Actions         // nil-safe: M7 A2/B routes absent without it
	eod        *EODStore        // nil-safe: M8 board route absent without it
	risk       *RiskStore       // nil-safe: M9 routes absent without it
	ops        *OpsStore        // nil-safe: M10 route absent without it
	exports    *ExportStore     // nil-safe: M11 routes absent without it
}

// SetProber enables the M3 credential endpoints. Call before Register.
func (h *HTTP) SetProber(p *Prober) { h.prober = p }

// SetStrategyControl enables the M4 strategy endpoints. Call before Register.
func (h *HTTP) SetStrategyControl(sc *StrategyControl) { h.strategies = sc }

// SetProtection enables the M5.1 protection board. Call before Register.
func (h *HTTP) SetProtection(p *ProtectionStore) { h.protection = p }

// SetRecon enables the M5.2 reconciliation + M5.3 broker mirror. Call
// before Register.
func (h *HTTP) SetRecon(r *ReconStore) { h.recon = r }

// SetScaleChecker enables the M5.4 on-demand check. Call before Register.
func (h *HTTP) SetScaleChecker(s *ScaleChecker) { h.scale = s }

// SetExplorer enables the M6 signal pipeline explorer. Call before Register.
func (h *HTTP) SetExplorer(e *Explorer) { h.explorer = e }

// SetInterventions enables the M7 Phase A intervention endpoints. Call
// before Register.
func (h *HTTP) SetInterventions(iv *Interventions) { h.interv = iv }

// SetActions enables the M7 A2/B endpoints (order cancel, ghost heal,
// square-off, rebalance). Call before Register.
func (h *HTTP) SetActions(a *Actions) { h.actions = a }

// SetEOD / SetRisk / SetOps / SetExports enable M8–M11. Call before Register.
func (h *HTTP) SetEOD(e *EODStore)         { h.eod = e }
func (h *HTTP) SetRisk(r *RiskStore)       { h.risk = r }
func (h *HTTP) SetOps(o *OpsStore)         { h.ops = o }
func (h *HTTP) SetExports(x *ExportStore)  { h.exports = x }

func NewHTTP(svc *Service) *HTTP { return &HTTP{svc: svc} }

// SetFleetStore enables the M2 fleet/attention endpoints. Call before
// Register; a nil store leaves those routes unregistered (a loud 404, not a
// half-working screen).
func (h *HTTP) SetFleetStore(f *FleetStore) { h.fleet = f }

// ── Route registration ──────────────────────────────────────────────────

// Register mounts the whole admin surface on adminRoot (an
// api.PathPrefix("/admin") subrouter — must be registered BEFORE any
// catch-all subrouter, or gorilla shadows it).
//
// Two nested zones with different credentials:
//
//	/admin/elevate  — behind platformAuth (the introspection-verified broker
//	                  JWT): you must be a live platform user to attempt it.
//	/admin/*        — behind Required (the opaque admin session token). The
//	                  broker JWT is deliberately NOT also required here: a
//	                  broker session dying mid-day (the FIV99/S4450 pattern)
//	                  must not lock the admin out of the console whose whole
//	                  purpose is fixing dead broker sessions.
//
// Every token-zone route goes through Route(), so tier enforcement and
// audit wiring are structural, not per-handler discipline.
func (h *HTTP) Register(adminRoot *mux.Router, platformAuth func(http.Handler) http.Handler) {
	elevate := adminRoot.PathPrefix("/elevate").Subrouter()
	elevate.Use(platformAuth)
	elevate.HandleFunc("", h.handleElevate).Methods("POST")

	token := adminRoot.PathPrefix("").Subrouter()
	token.Use(h.Required)
	h.Route(token, "GET", "/whoami", "ADMIN_WHOAMI", TierRead, h.handleWhoami)
	h.Route(token, "POST", "/logout", "ADMIN_LOGOUT", TierRead, h.handleLogout)
	h.Route(token, "GET", "/audit", "AUDIT_VIEW", TierRead, h.handleAuditList)
	if h.fleet != nil {
		h.Route(token, "GET", "/fleet", "FLEET_VIEW", TierRead, h.handleFleet)
		h.Route(token, "GET", "/attention", "ATTENTION_VIEW", TierRead, h.handleAttention)
	}
	if h.fleet != nil && h.prober != nil {
		h.Route(token, "GET", "/users/{user_id}/credential", "CREDENTIAL_PROBE", TierRead, h.handleCredential)
		h.Route(token, "POST", "/users/{user_id}/credential/expire", "CREDENTIAL_EXPIRE", TierConfirm, h.handleCredentialExpire)
		h.Route(token, "POST", "/credential-sweep", "CREDENTIAL_SWEEP", TierRead, h.handleCredentialSweep)
	}
	if h.strategies != nil {
		h.Route(token, "GET", "/strategies/{strategy_id}/timeline", "STRATEGY_TIMELINE", TierRead, h.handleTimeline)
		h.Route(token, "GET", "/strategies/{strategy_id}/blocks", "STRATEGY_BLOCKS", TierRead, h.handleBlocks)
		h.Route(token, "POST", "/strategies/{strategy_id}/blocks/clear", "STRATEGY_BLOCK_CLEAR", TierConfirm, h.handleBlockClear)
		h.Route(token, "POST", "/strategies/{strategy_id}/pause", "STRATEGY_PAUSE", TierConfirm, h.handlePause)
		h.Route(token, "POST", "/strategies/{strategy_id}/resume", "STRATEGY_RESUME", TierConfirm, h.handleResume)
		h.Route(token, "DELETE", "/strategies/{strategy_id}", "STRATEGY_DELETE", TierTyped, h.handleDelete)
	}
	if h.protection != nil {
		h.Route(token, "GET", "/protection", "PROTECTION_VIEW", TierRead, h.handleProtection)
	}
	if h.recon != nil {
		h.Route(token, "GET", "/users/{user_id}/reconcile", "RECONCILE_VIEW", TierRead, h.handleReconcile)
		h.Route(token, "GET", "/users/{user_id}/broker/holdings", "BROKER_MIRROR_HOLDINGS", TierRead, h.mirrorHandler("holdings"))
		h.Route(token, "GET", "/users/{user_id}/broker/orderbook", "BROKER_MIRROR_ORDERBOOK", TierRead, h.mirrorHandler("orderbook"))
		h.Route(token, "GET", "/users/{user_id}/broker/trades", "BROKER_MIRROR_TRADES", TierRead, h.mirrorHandler("trades"))
	}
	if h.scale != nil {
		h.Route(token, "POST", "/scale-check", "SCALE_CHECK", TierRead, h.handleScaleCheck)
	}
	if h.explorer != nil {
		h.Route(token, "GET", "/trace/{symbol}", "SIGNAL_TRACE", TierRead, h.handleTrace)
		h.Route(token, "GET", "/signals/candidates", "CANDIDATES_VIEW", TierRead, h.handleCandidates)
		h.Route(token, "GET", "/inbox", "INBOX_VIEW", TierRead, h.handleInbox)
		h.Route(token, "GET", "/rejections", "REJECTIONS_VIEW", TierRead, h.handleRejections)
	}
	if h.interv != nil {
		h.Route(token, "POST", "/inbox/{id}/resurrect", "SIGNAL_RESURRECT", TierConfirm, h.handleResurrect)
		h.Route(token, "POST", "/inbox/{id}/release", "HOLD_RELEASE", TierConfirm, h.handleReleaseHold)
		h.Route(token, "POST", "/users/{user_id}/rearm-protection", "PROTECTION_REARM", TierConfirm, h.handleRearm)
		h.Route(token, "POST", "/users/{user_id}/amo-cap/reset", "AMO_CAP_RESET", TierConfirm, h.handleCapReset)
	}
	if h.actions != nil {
		h.Route(token, "GET", "/users/{user_id}/orders/{broker_order_id}", "ORDER_VIEW", TierRead, h.handleOrderView)
		h.Route(token, "POST", "/users/{user_id}/orders/{broker_order_id}/cancel", "ORDER_CANCEL", TierConfirm, h.handleOrderCancel)
		h.Route(token, "GET", "/users/{user_id}/ghosts/{symbol}", "GHOST_PREVIEW", TierRead, h.handleGhostPreview)
		h.Route(token, "POST", "/users/{user_id}/ghosts/{symbol}/heal", "GHOST_HEAL", TierTyped, h.handleGhostHeal)
		h.Route(token, "POST", "/strategies/{strategy_id}/positions/{symbol}/squareoff", "POSITION_SQUAREOFF", TierTyped, h.handleSquareOff)
		h.Route(token, "POST", "/rebalance/preview", "REBALANCE_PREVIEW", TierRead, h.handleRebalancePreview)
		h.Route(token, "POST", "/rebalance/trigger", "REBALANCE_TRIGGER", TierTyped, h.handleRebalanceTrigger)
	}
	if h.eod != nil {
		h.Route(token, "GET", "/eod", "EOD_VIEW", TierRead, h.handleEOD)
	}
	if h.risk != nil {
		h.Route(token, "GET", "/risk/caps", "RISK_CAPS", TierRead, h.handleRiskCaps)
		h.Route(token, "GET", "/risk/drivers", "RISK_DRIVERS", TierRead, h.handleRiskDrivers)
	}
	if h.ops != nil {
		h.Route(token, "GET", "/infra", "INFRA_VIEW", TierRead, h.handleInfra)
	}
	if h.exports != nil {
		h.Route(token, "GET", "/exports/orders", "EXPORT_ORDERS", TierRead, h.handleExportOrders)
		h.Route(token, "GET", "/exports/events", "EXPORT_EVENTS", TierRead, h.handleExportEvents)
		h.Route(token, "GET", "/exports/admin-actions", "EXPORT_ADMIN_ACTIONS", TierRead, h.handleExportAdmin)
	}
}

// ── M8–M11 handlers ─────────────────────────────────────────────────────

func (h *HTTP) handleEOD(w http.ResponseWriter, ar *AdminRequest) {
	board, err := h.eod.Board(ar.Request.Context())
	if err != nil {
		log.Printf("admin: eod board failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "eod board failed")
		return
	}
	writeOK(w, board)
}

func (h *HTTP) handleRiskCaps(w http.ResponseWriter, ar *AdminRequest) {
	caps, err := h.risk.Caps(ar.Request.Context())
	if err != nil {
		log.Printf("admin: risk caps failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "caps computation failed")
		return
	}
	if aerr := ar.Audit("", "", map[string]int{"strategies": len(caps)}, "OK", "caps + live margin read"); aerr != nil {
		log.Printf("admin: caps audit failed: %v", aerr)
	}
	writeOK(w, map[string]any{"strategies": caps, "rules": "sector ≤ ceil(25% × max_positions); mcap bucket ≤ ceil(50%); in-flight reservations occupy caps"})
}

func (h *HTTP) handleRiskDrivers(w http.ResponseWriter, ar *AdminRequest) {
	d, err := h.risk.Drivers(ar.Request.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "drivers read failed")
		return
	}
	writeOK(w, d)
}

func (h *HTTP) handleInfra(w http.ResponseWriter, ar *AdminRequest) {
	writeOK(w, h.ops.Board(ar.Request.Context()))
}

func (h *HTTP) exportRange(w http.ResponseWriter, ar *AdminRequest) (from, to time.Time, ok bool) {
	qv := ar.Request.URL.Query()
	from, to, err := parseRange(qv.Get("from"), qv.Get("to"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "E_ADMIN_BAD_INPUT", err.Error())
		return from, to, false
	}
	return from, to, true
}

func (h *HTTP) auditExport(ar *AdminRequest, kind string, from, to time.Time) {
	if aerr := ar.Audit("", kind, map[string]string{"from": from.Format("2006-01-02"), "to": to.Format("2006-01-02")}, "OK", "compliance export"); aerr != nil {
		log.Printf("admin: export audit failed: %v", aerr)
	}
}

func (h *HTTP) handleExportOrders(w http.ResponseWriter, ar *AdminRequest) {
	from, to, ok := h.exportRange(w, ar)
	if !ok {
		return
	}
	h.auditExport(ar, "orders", from, to)
	if err := h.exports.OrdersCSV(ar.Request.Context(), w, from, to); err != nil {
		log.Printf("admin: orders export failed: %v", err)
	}
}

func (h *HTTP) handleExportEvents(w http.ResponseWriter, ar *AdminRequest) {
	from, to, ok := h.exportRange(w, ar)
	if !ok {
		return
	}
	h.auditExport(ar, "events", from, to)
	if err := h.exports.EventsCSV(ar.Request.Context(), w, from, to, ar.Request.URL.Query().Get("order_id")); err != nil {
		log.Printf("admin: events export failed: %v", err)
	}
}

func (h *HTTP) handleExportAdmin(w http.ResponseWriter, ar *AdminRequest) {
	from, to, ok := h.exportRange(w, ar)
	if !ok {
		return
	}
	h.auditExport(ar, "admin-actions", from, to)
	if err := h.exports.AdminActionsCSV(ar.Request.Context(), w, from, to); err != nil {
		log.Printf("admin: admin-actions export failed: %v", err)
	}
}

// ── M7 A2/B handlers ────────────────────────────────────────────────────

func (h *HTTP) handleOrderView(w http.ResponseWriter, ar *AdminRequest) {
	vars := mux.Vars(ar.Request)
	userID, brokerID := vars["user_id"], vars["broker_order_id"]
	view, err := h.actions.ViewOrder(ar.Request.Context(), userID, brokerID)
	if err != nil {
		h.intervErr(w, ar, userID, brokerID, err)
		return
	}
	if aerr := ar.Audit(userID, brokerID, map[string]string{"verdict": view.Verdict}, "OK", ""); aerr != nil {
		log.Printf("admin: order view audit failed: %v", aerr)
	}
	writeOK(w, view)
}

func (h *HTTP) handleOrderCancel(w http.ResponseWriter, ar *AdminRequest) {
	reason, ok := requireReason(w, ar)
	if !ok {
		return
	}
	vars := mux.Vars(ar.Request)
	userID, brokerID := vars["user_id"], vars["broker_order_id"]
	view, err := h.actions.CancelOrder(ar.Request.Context(), userID, brokerID)
	if err != nil {
		h.intervErr(w, ar, userID, brokerID, err)
		return
	}
	if aerr := ar.Audit(userID, brokerID, map[string]string{"reason": reason}, "OK", "broker order cancelled"); aerr != nil {
		log.Printf("admin: order cancel audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_AUDIT", "audit write failed")
		return
	}
	writeOK(w, view)
}

func (h *HTTP) handleGhostPreview(w http.ResponseWriter, ar *AdminRequest) {
	vars := mux.Vars(ar.Request)
	plan, err := h.actions.GhostPreview(ar.Request.Context(), vars["user_id"], vars["symbol"])
	if err != nil {
		h.intervErr(w, ar, vars["user_id"], vars["symbol"], err)
		return
	}
	if aerr := ar.Audit(vars["user_id"], vars["symbol"], plan.Evidence, "OK", "ghost heal previewed"); aerr != nil {
		log.Printf("admin: ghost preview audit failed: %v", aerr)
	}
	writeOK(w, plan)
}

func (h *HTTP) handleGhostHeal(w http.ResponseWriter, ar *AdminRequest) {
	vars := mux.Vars(ar.Request)
	userID, symbol := vars["user_id"], vars["symbol"]
	// Evidence is ALWAYS refetched live — a stale preview cannot authorize.
	plan, err := h.actions.GhostPreview(ar.Request.Context(), userID, symbol)
	if err != nil {
		h.intervErr(w, ar, userID, symbol, err)
		return
	}
	if ar.IsPreview() {
		writeOK(w, plan)
		return
	}
	if terr := ar.RequireTyped(plan.ConfirmationText); terr != nil {
		if aerr := ar.Audit(userID, symbol, nil, "DENIED", terr.Error()); aerr != nil {
			log.Printf("admin: ghost heal denial audit failed: %v", aerr)
		}
		writeErr(w, http.StatusPreconditionFailed, "E_ADMIN_CONFIRMATION", terr.Error())
		return
	}
	result, err := h.actions.GhostHeal(ar.Request.Context(), plan)
	if err != nil {
		h.intervErr(w, ar, userID, symbol, err)
		return
	}
	if aerr := ar.Audit(userID, symbol, map[string]any{"evidence": plan.Evidence, "result": result}, "OK", "ghost healed from broker-verified evidence"); aerr != nil {
		log.Printf("admin: ghost heal audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_AUDIT", "audit write failed")
		return
	}
	writeOK(w, map[string]any{"healed": result, "plan": plan})
}

func (h *HTTP) handleSquareOff(w http.ResponseWriter, ar *AdminRequest) {
	// Resolve through the Actions bundle's own strategy control — the M4
	// field may legitimately be unset while actions are wired (tests, or a
	// partial deployment).
	ref, err := h.actions.strategies.Resolve(ar.Request.Context(), mux.Vars(ar.Request)["strategy_id"])
	if err != nil {
		writeErr(w, http.StatusNotFound, "E_ADMIN_STRATEGY_NOT_FOUND", err.Error())
		return
	}
	symbol := mux.Vars(ar.Request)["symbol"]
	sc, err := h.actions.SquareOffPreview(ar.Request.Context(), ref, symbol)
	if err != nil {
		h.intervErr(w, ar, ref.UserID, symbol, err)
		return
	}
	if ar.IsPreview() {
		writeOK(w, sc)
		return
	}
	if terr := ar.RequireTyped(sc.ConfirmationText); terr != nil {
		if aerr := ar.Audit(ref.UserID, symbol, nil, "DENIED", terr.Error()); aerr != nil {
			log.Printf("admin: squareoff denial audit failed: %v", aerr)
		}
		writeErr(w, http.StatusPreconditionFailed, "E_ADMIN_CONFIRMATION", terr.Error())
		return
	}
	report, err := h.actions.SquareOffExecute(ar.Request.Context(), ref, symbol)
	if err != nil {
		h.intervErr(w, ar, ref.UserID, symbol, err)
		return
	}
	if aerr := ar.Audit(ref.UserID, symbol, map[string]any{"qty": sc.Qty, "approx_value": sc.ApproxValue}, "OK", "supervised square-off executed"); aerr != nil {
		log.Printf("admin: squareoff audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_AUDIT", "audit write failed")
		return
	}
	writeOK(w, map[string]any{"trade_execution": json.RawMessage(report), "context": sc})
}

func (h *HTTP) handleRebalancePreview(w http.ResponseWriter, ar *AdminRequest) {
	userID := ar.BodyString("user_id")
	out, err := h.actions.Rebalance(ar.Request.Context(), userID, true)
	if err != nil {
		h.intervErr(w, ar, userID, "", err)
		return
	}
	if aerr := ar.Audit(userID, "", nil, "OK", "rebalance dry-run"); aerr != nil {
		log.Printf("admin: rebalance preview audit failed: %v", aerr)
	}
	writeOK(w, map[string]any{"dry_run": true, "output": out})
}

func (h *HTTP) handleRebalanceTrigger(w http.ResponseWriter, ar *AdminRequest) {
	userID := strings.TrimSpace(ar.BodyString("user_id"))
	if userID == "" && !ar.IsPreview() {
		writeErr(w, http.StatusUnprocessableEntity, "E_ADMIN_BAD_INPUT", `"user_id" is required — a live rebalance runs one user at a time`)
		return
	}
	expected := fmt.Sprintf("REBALANCE %s — PUBLISH REAL ENTRY ORDERS", userID)
	if ar.IsPreview() {
		writeOK(w, map[string]any{"confirmation_text": expected,
			"note": "run /rebalance/preview first to see the plan; then resend with confirmation_text"})
		return
	}
	if terr := ar.RequireTyped(expected); terr != nil {
		if aerr := ar.Audit(userID, "", nil, "DENIED", terr.Error()); aerr != nil {
			log.Printf("admin: rebalance denial audit failed: %v", aerr)
		}
		writeErr(w, http.StatusPreconditionFailed, "E_ADMIN_CONFIRMATION", terr.Error())
		return
	}
	out, err := h.actions.Rebalance(ar.Request.Context(), userID, false)
	if err != nil {
		h.intervErr(w, ar, userID, "", err)
		return
	}
	if aerr := ar.Audit(userID, "", nil, "OK", "rebalance pass executed — entries published"); aerr != nil {
		log.Printf("admin: rebalance audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_AUDIT", "audit write failed")
		return
	}
	writeOK(w, map[string]any{"dry_run": false, "output": out})
}

// ── M7 Phase A handlers ─────────────────────────────────────────────────

// requireReason enforces the 7.6 rule: overrides carry WHY, in the audit.
func requireReason(w http.ResponseWriter, ar *AdminRequest) (string, bool) {
	reason := strings.TrimSpace(ar.BodyString("reason"))
	if reason == "" {
		writeErr(w, http.StatusUnprocessableEntity, "E_ADMIN_REASON_REQUIRED",
			`overrides require a "reason" in the body — it goes in the audit trail`)
		return "", false
	}
	return reason, true
}

// intervErr maps a guardrail refusal vs infra error, auditing the refusal.
func (h *HTTP) intervErr(w http.ResponseWriter, ar *AdminRequest, targetUser, targetRef string, err error) {
	if r := asRefusal(err); r != nil {
		if aerr := ar.Audit(targetUser, targetRef, nil, "REFUSED", r.msg); aerr != nil {
			log.Printf("admin: intervention refusal audit failed: %v", aerr)
		}
		writeErr(w, r.code, "E_ADMIN_GUARDRAIL", r.msg)
		return
	}
	log.Printf("admin: intervention failed: %v", err)
	writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "intervention failed")
}

func (h *HTTP) handleResurrect(w http.ResponseWriter, ar *AdminRequest) {
	id, err := strconv.ParseInt(mux.Vars(ar.Request)["id"], 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "E_ADMIN_BAD_ID", "inbox id must be numeric")
		return
	}
	info, rerr := h.interv.Resurrect(ar.Request.Context(), id)
	if rerr != nil {
		h.intervErr(w, ar, "", fmt.Sprint(id), rerr)
		return
	}
	if aerr := ar.Audit(info.UserID, fmt.Sprint(id), info, "OK", "entry signal reset for same-day retry"); aerr != nil {
		log.Printf("admin: resurrect audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_AUDIT", "audit write failed")
		return
	}
	writeOK(w, map[string]any{"resurrected": info,
		"note": "row re-queued — the inbox worker picks it up within its normal cadence; the auth gate still applies until the user's platform login"})
}

func (h *HTTP) handleReleaseHold(w http.ResponseWriter, ar *AdminRequest) {
	reason, ok := requireReason(w, ar)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(mux.Vars(ar.Request)["id"], 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "E_ADMIN_BAD_ID", "inbox id must be numeric")
		return
	}
	info, rerr := h.interv.ReleaseHold(ar.Request.Context(), id)
	if rerr != nil {
		h.intervErr(w, ar, "", fmt.Sprint(id), rerr)
		return
	}
	if aerr := ar.Audit(info.UserID, fmt.Sprint(id), map[string]any{"reason": reason, "row": info}, "OK", "hold released early"); aerr != nil {
		log.Printf("admin: release audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_AUDIT", "audit write failed")
		return
	}
	writeOK(w, map[string]any{"released": info, "note": "next_attempt_at=now — the worker's own gates (auth, circuit re-check, market hours) still apply on pickup"})
}

func (h *HTTP) handleRearm(w http.ResponseWriter, ar *AdminRequest) {
	userID := mux.Vars(ar.Request)["user_id"]
	body, rerr := h.interv.Rearm(ar.Request.Context(), userID)
	if rerr != nil {
		h.intervErr(w, ar, userID, "", rerr)
		return
	}
	if aerr := ar.Audit(userID, "", nil, "OK", "protective replay RunOnceForUser fired"); aerr != nil {
		log.Printf("admin: rearm audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_AUDIT", "audit write failed")
		return
	}
	writeOK(w, map[string]any{"trade_execution": json.RawMessage(body),
		"note": "same buildPlans→fireAll→reconcile flow as the 09:14 cron, scoped to this user"})
}

func (h *HTTP) handleCapReset(w http.ResponseWriter, ar *AdminRequest) {
	reason, ok := requireReason(w, ar)
	if !ok {
		return
	}
	userID := mux.Vars(ar.Request)["user_id"]
	symbol := strings.TrimSpace(ar.BodyString("symbol"))
	if symbol == "" {
		writeErr(w, http.StatusUnprocessableEntity, "E_ADMIN_BAD_INPUT", `"symbol" is required`)
		return
	}
	n, rerr := h.interv.ResetAMOCap(ar.Request.Context(), userID, symbol)
	if rerr != nil {
		h.intervErr(w, ar, userID, symbol, rerr)
		return
	}
	if aerr := ar.Audit(userID, symbol, map[string]any{"reason": reason, "attempts_cleared": n}, "OK", "AMO give-up cap reset for tonight"); aerr != nil {
		log.Printf("admin: cap reset audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_AUDIT", "audit write failed")
		return
	}
	writeOK(w, map[string]any{"attempts_cleared": n,
		"note": "tonight's 16:35 EOD cycle (or a manual re-arm) will attempt this stop again from zero"})
}

// ── M6 handlers ─────────────────────────────────────────────────────────

func (h *HTTP) handleTrace(w http.ResponseWriter, ar *AdminRequest) {
	qv := ar.Request.URL.Query()
	days, _ := strconv.Atoi(qv.Get("days"))
	res, err := h.explorer.Trace(ar.Request.Context(), mux.Vars(ar.Request)["symbol"], qv.Get("user_id"), days)
	if err != nil {
		log.Printf("admin: trace failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "trace failed")
		return
	}
	writeOK(w, res)
}

func (h *HTTP) handleCandidates(w http.ResponseWriter, ar *AdminRequest) {
	res, err := h.explorer.Candidates(ar.Request.Context(), ar.Request.URL.Query().Get("date"))
	if err != nil {
		log.Printf("admin: candidates failed: %v", err)
		writeErr(w, http.StatusServiceUnavailable, "E_ADMIN_CANDIDATES", err.Error())
		return
	}
	writeOK(w, res)
}

func (h *HTTP) handleInbox(w http.ResponseWriter, ar *AdminRequest) {
	qv := ar.Request.URL.Query()
	days, _ := strconv.Atoi(qv.Get("days"))
	res, err := h.explorer.Inbox(ar.Request.Context(), qv.Get("status"), qv.Get("class"), qv.Get("user_id"), days)
	if err != nil {
		log.Printf("admin: inbox browse failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "inbox browse failed")
		return
	}
	writeOK(w, res)
}

func (h *HTTP) handleRejections(w http.ResponseWriter, ar *AdminRequest) {
	days, _ := strconv.Atoi(ar.Request.URL.Query().Get("days"))
	buckets, err := h.explorer.Rejections(ar.Request.Context(), days)
	if err != nil {
		log.Printf("admin: rejections failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "rejection analytics failed")
		return
	}
	writeOK(w, map[string]any{"buckets": buckets, "count": len(buckets)})
}

// ── M5 handlers ─────────────────────────────────────────────────────────

func (h *HTTP) handleProtection(w http.ResponseWriter, ar *AdminRequest) {
	board, err := h.protection.Board(ar.Request.Context())
	if err != nil {
		log.Printf("admin: protection board failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "protection board failed")
		return
	}
	writeOK(w, board)
}

func (h *HTTP) handleReconcile(w http.ResponseWriter, ar *AdminRequest) {
	userID := mux.Vars(ar.Request)["user_id"]
	res, err := h.recon.Reconcile(ar.Request.Context(), userID)
	if err != nil {
		log.Printf("admin: reconcile %s failed: %v", userID, err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "reconciliation failed")
		return
	}
	if aerr := ar.Audit(userID, "", map[string]any{"broker_leg": res.BrokerLeg, "mismatches": len(res.Mismatches)}, "OK", ""); aerr != nil {
		log.Printf("admin: reconcile audit failed: %v", aerr)
	}
	writeOK(w, res)
}

// mirrorHandler builds the per-view broker mirror handler (M5.3): a live
// broker read on behalf of the target user, always audited.
func (h *HTTP) mirrorHandler(view string) func(http.ResponseWriter, *AdminRequest) {
	return func(w http.ResponseWriter, ar *AdminRequest) {
		userID := mux.Vars(ar.Request)["user_id"]
		res, err := h.recon.Mirror(ar.Request.Context(), userID, view)
		if err != nil {
			log.Printf("admin: mirror %s/%s failed: %v", userID, view, err)
			writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "broker mirror failed")
			return
		}
		if aerr := ar.Audit(userID, "", map[string]string{"view": view, "broker_leg": res.BrokerLeg}, "OK", ""); aerr != nil {
			log.Printf("admin: mirror audit failed: %v", aerr)
		}
		writeOK(w, res)
	}
}

func (h *HTTP) handleScaleCheck(w http.ResponseWriter, ar *AdminRequest) {
	findings, err := h.scale.Run(ar.Request.Context())
	if err != nil {
		log.Printf("admin: scale check failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "scale check failed")
		return
	}
	if aerr := ar.Audit("", "", map[string]int{"findings": len(findings)}, "OK", ""); aerr != nil {
		log.Printf("admin: scale check audit failed: %v", aerr)
	}
	if findings == nil {
		findings = []ScaleFinding{}
	}
	writeOK(w, map[string]any{"findings": findings, "count": len(findings)})
}

// ── M4 handlers ─────────────────────────────────────────────────────────

// resolveStrategy is the shared entry: 404s unknown strategies before any
// tier logic runs a real action.
func (h *HTTP) resolveStrategy(w http.ResponseWriter, ar *AdminRequest) *StrategyRef {
	ref, err := h.strategies.Resolve(ar.Request.Context(), mux.Vars(ar.Request)["strategy_id"])
	if err != nil {
		writeErr(w, http.StatusNotFound, "E_ADMIN_STRATEGY_NOT_FOUND", err.Error())
		return nil
	}
	return ref
}

func (h *HTTP) handleTimeline(w http.ResponseWriter, ar *AdminRequest) {
	ref := h.resolveStrategy(w, ar)
	if ref == nil {
		return
	}
	events, err := h.strategies.Timeline(ar.Request.Context(), ref.StrategyID)
	if err != nil {
		log.Printf("admin: timeline failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "timeline query failed")
		return
	}
	writeOK(w, map[string]any{"strategy": ref, "events": events, "count": len(events)})
}

func (h *HTTP) handleBlocks(w http.ResponseWriter, ar *AdminRequest) {
	ref := h.resolveStrategy(w, ar)
	if ref == nil {
		return
	}
	blocks, err := h.strategies.Blocks(ar.Request.Context(), ref.StrategyID)
	if err != nil {
		log.Printf("admin: blocks failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "blocks query failed")
		return
	}
	writeOK(w, map[string]any{"strategy": ref, "blocks": blocks, "count": len(blocks)})
}

func (h *HTTP) handleBlockClear(w http.ResponseWriter, ar *AdminRequest) {
	ref := h.resolveStrategy(w, ar)
	if ref == nil {
		return
	}
	symbol := strings.ToUpper(strings.TrimSpace(ar.BodyString("symbol")))
	kind := strings.ToUpper(strings.TrimSpace(ar.BodyString("kind")))
	if symbol == "" || kind == "" {
		writeErr(w, http.StatusBadRequest, "E_ADMIN_BAD_REQUEST", `body needs "symbol" and "kind" (COOLDOWN|OVERRIDE)`)
		return
	}
	if err := h.strategies.ClearBlock(ar.Request.Context(), ref.StrategyID, symbol, kind); err != nil {
		_ = ar.Audit(ref.UserID, ref.StrategyID, map[string]string{"symbol": symbol, "kind": kind}, "FAILED", err.Error())
		writeErr(w, http.StatusUnprocessableEntity, "E_ADMIN_CLEAR_FAILED", err.Error())
		return
	}
	if aerr := ar.Audit(ref.UserID, ref.StrategyID, map[string]string{"symbol": symbol, "kind": kind}, "OK",
		"re-entry block lifted early by admin"); aerr != nil {
		log.Printf("admin: CRITICAL — block cleared but audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "cleared, but audit write failed — check logs")
		return
	}
	writeOK(w, map[string]string{"status": "cleared", "symbol": symbol, "kind": kind})
}

// strategyAction runs pause/resume through the shared audit discipline.
func (h *HTTP) strategyAction(w http.ResponseWriter, ar *AdminRequest, verb string,
	act func(context.Context, *StrategyRef) error) {
	ref := h.resolveStrategy(w, ar)
	if ref == nil {
		return
	}
	if err := act(ar.Request.Context(), ref); err != nil {
		_ = ar.Audit(ref.UserID, ref.StrategyID, nil, "FAILED", err.Error())
		writeErr(w, http.StatusUnprocessableEntity, "E_ADMIN_STRATEGY_ACTION", err.Error())
		return
	}
	if aerr := ar.Audit(ref.UserID, ref.StrategyID, nil, "OK", verb+" on behalf of user"); aerr != nil {
		log.Printf("admin: CRITICAL — %s executed but audit failed: %v", verb, aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", verb+" done, but audit write failed — check logs")
		return
	}
	writeOK(w, map[string]any{"status": verb, "strategy_id": ref.StrategyID, "user_id": ref.UserID})
}

func (h *HTTP) handlePause(w http.ResponseWriter, ar *AdminRequest) {
	h.strategyAction(w, ar, "paused", h.strategies.Pause)
}

func (h *HTTP) handleResume(w http.ResponseWriter, ar *AdminRequest) {
	h.strategyAction(w, ar, "resumed", h.strategies.Resume)
}

func (h *HTTP) handleDelete(w http.ResponseWriter, ar *AdminRequest) {
	ref := h.resolveStrategy(w, ar)
	if ref == nil {
		return
	}
	expected := DeleteConfirmation(ref)
	if ar.IsPreview() {
		writeOK(w, map[string]any{"confirmation_text": expected, "strategy": ref})
		return
	}
	if err := ar.RequireTyped(expected); err != nil {
		_ = ar.Audit(ref.UserID, ref.StrategyID, nil, "DENIED", err.Error())
		writeErr(w, http.StatusPreconditionFailed, "E_ADMIN_CONFIRMATION", err.Error())
		return
	}
	if err := h.strategies.Delete(ar.Request.Context(), ref); err != nil {
		_ = ar.Audit(ref.UserID, ref.StrategyID, nil, "FAILED", err.Error())
		writeErr(w, http.StatusUnprocessableEntity, "E_ADMIN_STRATEGY_ACTION", err.Error())
		return
	}
	if aerr := ar.Audit(ref.UserID, ref.StrategyID,
		map[string]any{"open_positions_kept": ref.OpenPositions}, "OK",
		"strategy deleted — positions kept open under their standing stops"); aerr != nil {
		log.Printf("admin: CRITICAL — delete executed but audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "deleted, but audit write failed — check logs")
		return
	}
	writeOK(w, map[string]any{"status": "deleted", "strategy_id": ref.StrategyID,
		"user_id": ref.UserID, "open_positions_kept": ref.OpenPositions})
}

// ── M3 handlers ─────────────────────────────────────────────────────────

func (h *HTTP) handleCredential(w http.ResponseWriter, ar *AdminRequest) {
	userID := mux.Vars(ar.Request)["user_id"]
	facts, err := h.fleet.CredentialFacts(ar.Request.Context(), userID)
	if err != nil {
		log.Printf("admin: credential facts failed for %s: %v", userID, err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "credential lookup failed")
		return
	}
	verdict := h.prober.Probe(ar.Request.Context(), userID)
	if aerr := ar.Audit(userID, "", map[string]string{"verdict": verdict.Verdict}, "OK", ""); aerr != nil {
		log.Printf("admin: credential probe audit failed: %v", aerr)
	}
	writeOK(w, map[string]any{"stored": facts, "probe": verdict})
}

func (h *HTTP) handleCredentialExpire(w http.ResponseWriter, ar *AdminRequest) {
	userID := mux.Vars(ar.Request)["user_id"]
	// CONFIRM tier already enforced by Route(). Mutating action: the audit
	// row is part of the action — a failed audit fails the request.
	if err := h.fleet.ExpireCredential(ar.Request.Context(), userID); err != nil {
		_ = ar.Audit(userID, "", nil, "FAILED", err.Error())
		writeErr(w, http.StatusUnprocessableEntity, "E_ADMIN_EXPIRE_FAILED", err.Error())
		return
	}
	if aerr := ar.Audit(userID, "", nil, "OK", "credential force-expired — user must re-login via platform"); aerr != nil {
		log.Printf("admin: CRITICAL — expire executed but audit failed: %v", aerr)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "expired, but audit write failed — check logs")
		return
	}
	writeOK(w, map[string]string{"status": "expired", "user_id": userID})
}

func (h *HTTP) handleCredentialSweep(w http.ResponseWriter, ar *AdminRequest) {
	users, err := h.fleet.ActiveUserIDs(ar.Request.Context())
	if err != nil {
		log.Printf("admin: sweep user list failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "user list failed")
		return
	}
	results := h.prober.Sweep(ar.Request.Context(), users)
	if aerr := ar.Audit("", "", map[string]int{"users_probed": len(results)}, "OK", ""); aerr != nil {
		log.Printf("admin: sweep audit failed: %v", aerr)
	}
	writeOK(w, map[string]any{"results": results, "count": len(results)})
}

// ── M2 handlers ─────────────────────────────────────────────────────────

func (h *HTTP) handleFleet(w http.ResponseWriter, ar *AdminRequest) {
	rows, err := h.fleet.Fleet(ar.Request.Context())
	if err != nil {
		log.Printf("admin: fleet failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "fleet query failed")
		return
	}
	writeOK(w, map[string]any{"rows": rows, "count": len(rows)})
}

func (h *HTTP) handleAttention(w http.ResponseWriter, ar *AdminRequest) {
	items, notWired, err := h.fleet.Attention(ar.Request.Context())
	if err != nil {
		log.Printf("admin: attention failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "attention query failed")
		return
	}
	// M3: live-probe failures from the last sweep outrank the age heuristic —
	// prepend and keep CRITICAL-first ordering.
	if h.prober != nil {
		if fails := h.prober.sweepFailuresAsAttention(); len(fails) > 0 {
			items = append(fails, items...)
		}
	}
	// M5.4: out-of-band scale findings from the last check.
	if h.scale != nil {
		items = append(items, h.scale.findingsAsAttention()...)
	}
	writeOK(w, map[string]any{"items": items, "count": len(items), "not_wired": notWired})
}

// Required is the admin-session middleware: X-Admin-Token → live session →
// context. 403 with an audited ATTEMPT on anything else.
func (h *HTTP) Required(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.Header.Get(TokenHeader)
		if raw == "" {
			writeErr(w, http.StatusForbidden, "E_ADMIN_TOKEN_MISSING", "admin elevation required")
			return
		}
		sess, err := h.svc.Validate(r.Context(), raw)
		if err != nil {
			// Audit invalid-token probes: identity unknown, so record what
			// we have. Failure to audit here only loses probe telemetry —
			// nothing was authorized — so log, don't fail shut.
			if aerr := h.svc.store.Audit(r.Context(), AuditEntry{
				AdminID: "UNKNOWN", Action: "ADMIN_TOKEN_REJECTED", Tier: string(tierAuth),
				Result: "DENIED", Detail: r.Method + " " + r.URL.Path, IP: clientIP(r),
			}); aerr != nil {
				log.Printf("admin: audit of rejected token failed: %v", aerr)
			}
			writeErr(w, http.StatusForbidden, "E_ADMIN_SESSION_INVALID", "admin session invalid or expired")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), sessionKey, sess)))
	})
}

// Route registers one admin endpoint through the mandatory chain:
// session (from Required) → tier enforcement → audit → handler.
// action is the audit-log verb; handlers receive a ready AdminRequest.
func (h *HTTP) Route(r *mux.Router, method, path, action string, tier Tier,
	fn func(w http.ResponseWriter, req *AdminRequest)) {
	r.HandleFunc(path, func(w http.ResponseWriter, req *http.Request) {
		sess, ok := SessionFromContext(req.Context())
		if !ok { // structurally unreachable; guard anyway
			writeErr(w, http.StatusInternalServerError, "E_ADMIN_WIRING", "admin middleware missing")
			return
		}
		ar := &AdminRequest{Request: req, Session: sess, http: h, action: action, tier: tier}

		if tier == TierConfirm || tier == TierTyped {
			if err := ar.enforceTier(); err != nil {
				_ = h.svc.store.Audit(req.Context(), AuditEntry{
					AdminID: sess.AdminID, Action: action, Tier: string(tier),
					Result: "DENIED", Detail: err.Error(), IP: clientIP(req), SessionID: sess.ID,
				})
				writeErr(w, http.StatusPreconditionFailed, "E_ADMIN_CONFIRMATION", err.Error())
				return
			}
		}
		fn(w, ar)
	}).Methods(method)
}

// AdminRequest is what tiered handlers receive: the request, the verified
// session, and helpers that keep audit discipline out of handler bodies.
type AdminRequest struct {
	*http.Request
	Session *Session

	http    *HTTP
	action  string
	tier    Tier
	body    map[string]json.RawMessage // lazily parsed for tier checks
}

// parseBody reads the JSON body once (tier checks + handler share it).
func (ar *AdminRequest) parseBody() map[string]json.RawMessage {
	if ar.body != nil {
		return ar.body
	}
	ar.body = map[string]json.RawMessage{}
	if ar.Request.Body != nil {
		_ = json.NewDecoder(ar.Request.Body).Decode(&ar.body) // absent/invalid body → empty map
	}
	return ar.body
}

// BodyString returns a string field from the JSON body ("" if absent).
func (ar *AdminRequest) BodyString(key string) string {
	var s string
	if raw, ok := ar.parseBody()[key]; ok {
		_ = json.Unmarshal(raw, &s)
	}
	return s
}

// enforceTier implements spec M1.3 server-side.
//
//	CONFIRM: body must carry "confirmed": true.
//	TYPED:   body must carry "confirmation_text" EXACTLY equal to the
//	         server-computed blast-radius string, which the handler
//	         declares via header X-Expected-Confirmation set by the UI
//	         flow: the client first calls the endpoint with
//	         "preview": true to RECEIVE the required string, then
//	         resubmits with the human-retyped text.
func (ar *AdminRequest) enforceTier() error {
	body := ar.parseBody()
	switch ar.tier {
	case TierConfirm:
		var confirmed bool
		if raw, ok := body["confirmed"]; ok {
			_ = json.Unmarshal(raw, &confirmed)
		}
		if !confirmed {
			return errors.New(`confirmation required: resend with "confirmed": true`)
		}
	case TierTyped:
		// Preview requests pass tier enforcement; the HANDLER answers them
		// with the required confirmation text and performs nothing.
		var preview bool
		if raw, ok := body["preview"]; ok {
			_ = json.Unmarshal(raw, &preview)
		}
		if preview {
			return nil
		}
		if strings.TrimSpace(ar.BodyString("confirmation_text")) == "" {
			return errors.New(`typed confirmation required: call with "preview": true to get the confirmation text`)
		}
		// Verbatim comparison happens in the handler via RequireTyped,
		// because only the handler knows the blast radius.
	}
	return nil
}

// IsPreview reports whether a TYPED request only wants the confirmation text.
func (ar *AdminRequest) IsPreview() bool {
	var preview bool
	if raw, ok := ar.parseBody()["preview"]; ok {
		_ = json.Unmarshal(raw, &preview)
	}
	return preview
}

// RequireTyped compares the human-retyped text against the server-computed
// expectation. Handlers for TYPED endpoints MUST call this before acting.
func (ar *AdminRequest) RequireTyped(expected string) error {
	got := strings.TrimSpace(ar.BodyString("confirmation_text"))
	if got != expected {
		return errors.New("confirmation text does not match — expected exactly: " + expected)
	}
	return nil
}

// Audit writes the action's outcome row. Mutating handlers call it with
// their result; a returned error must fail the request.
func (ar *AdminRequest) Audit(targetUser, targetRef string, params any, result, detail string) error {
	return ar.http.svc.store.Audit(ar.Request.Context(), AuditEntry{
		AdminID: ar.Session.AdminID, Action: ar.action, Tier: string(ar.tier),
		TargetUser: targetUser, TargetRef: targetRef, Params: params,
		Result: result, Detail: detail,
		SelfAction: targetUser != "" && targetUser == ar.Session.AdminID,
		IP:         clientIP(ar.Request), SessionID: ar.Session.ID,
	})
}

// ── Handlers ────────────────────────────────────────────────────────────

func (h *HTTP) handleElevate(w http.ResponseWriter, r *http.Request) {
	userID, ok := auth.UserIDFromContext(r.Context())
	if !ok || userID == "" {
		writeErr(w, http.StatusUnauthorized, "E_AUTH_MISSING", "platform authentication required")
		return
	}
	res, err := h.svc.Elevate(r.Context(), userID, clientIP(r))
	switch {
	case errors.Is(err, ErrRateLimited):
		writeErr(w, http.StatusTooManyRequests, "E_ADMIN_RATE_LIMITED", err.Error())
	case errors.Is(err, ErrNotAdmin) || (err == nil && res == nil):
		// Same 403 whether the user exists off-list or is deactivated —
		// no oracle for probers.
		writeErr(w, http.StatusForbidden, "E_ADMIN_FORBIDDEN", "not authorized for admin access")
	case err != nil:
		log.Printf("admin: elevate failed for %s: %v", userID, err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "elevation failed")
	default:
		writeOK(w, res)
	}
}

func (h *HTTP) handleWhoami(w http.ResponseWriter, ar *AdminRequest) {
	writeOK(w, map[string]any{
		"admin_id":   ar.Session.AdminID,
		"session_id": ar.Session.ID,
		"expires_at": ar.Session.ExpiresAt,
	})
}

func (h *HTTP) handleLogout(w http.ResponseWriter, ar *AdminRequest) {
	if err := h.svc.Logout(ar.Request.Context(), ar.Session, clientIP(ar.Request)); err != nil {
		log.Printf("admin: logout failed for %s: %v", ar.Session.AdminID, err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "logout failed")
		return
	}
	writeOK(w, map[string]string{"status": "logged out"})
}

func (h *HTTP) handleAuditList(w http.ResponseWriter, ar *AdminRequest) {
	q := ar.Request.URL.Query()
	f := AuditFilter{AdminID: q.Get("admin_id"), TargetUser: q.Get("target_user")}
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			f.Limit = n
		}
	}
	if v := q.Get("since"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.Since = t
		}
	}
	rows, err := h.svc.store.ListAudit(ar.Request.Context(), f)
	if err != nil {
		log.Printf("admin: audit list failed: %v", err)
		writeErr(w, http.StatusInternalServerError, "E_ADMIN_INTERNAL", "audit query failed")
		return
	}
	// ?format=csv — compliance export (spec M1.2). Same filters, same rows,
	// RFC-4180 quoting via encoding/csv; UTC ISO timestamps for auditors.
	if q.Get("format") == "csv" {
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition",
			`attachment; filename="admin_audit_`+time.Now().UTC().Format("20060102T150405Z")+`.csv"`)
		cw := csv.NewWriter(w)
		_ = cw.Write([]string{"id", "created_at_utc", "admin_id", "action", "tier",
			"target_user", "target_ref", "result", "detail", "self_action", "ip", "params"})
		for _, r := range rows {
			params := ""
			if len(r.Params) > 0 && string(r.Params) != "null" {
				params = string(r.Params)
			}
			_ = cw.Write([]string{
				strconv.FormatInt(r.ID, 10), r.CreatedAt.UTC().Format(time.RFC3339),
				r.AdminID, r.Action, r.Tier, r.TargetUser, r.TargetRef,
				r.Result, r.Detail, strconv.FormatBool(r.SelfAction), r.IP, params,
			})
		}
		cw.Flush()
		if err := cw.Error(); err != nil {
			log.Printf("admin: audit csv write failed: %v", err)
		}
		return
	}
	writeOK(w, map[string]any{"rows": rows, "count": len(rows)})
}

// ── plumbing ────────────────────────────────────────────────────────────

func clientIP(r *http.Request) string {
	// Behind nginx the trustworthy hop is the LAST X-Forwarded-For entry —
	// nginx APPENDS the socket peer it actually saw, while earlier entries
	// are client-supplied and freely spoofable. Audit rows must not record
	// an attacker-chosen address.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[len(parts)-1])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func writeOK(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"infoID": "0", "infoMsg": "success",
		"timestamp": time.Now().UnixMilli(), "data": data,
	})
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"infoID": code, "infoMsg": msg, "timestamp": time.Now().UnixMilli(),
	})
}
