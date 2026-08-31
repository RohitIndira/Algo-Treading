package admin

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
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
