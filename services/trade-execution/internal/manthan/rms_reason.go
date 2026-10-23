package manthan

import (
	"regexp"
	"strconv"
	"strings"
)

// Indira's RMS rejects a SELL whose quantity is not free with a text like
//
//	"Limit exceeded Set= 239, N.Used= 239 ,Prev= 239, T.Free Qty= 0,today free qty=0 ..."
//
// Read live on 2026-10-23 against seven stop-outs and one ghost:
//
//	Set    = quantity the broker HOLDS for the scrip (0 → it holds nothing)
//	Prev   = quantity already committed by other sell orders (pending OR
//	         executed today — the text does not say which)
//	N.Used = the rejected order's own quantity
//	Free   = Set − Prev
//
// "Set= 0" is therefore the broker's own statement that the position does not
// exist at the broker (a ghost in our book). "Set= N, Prev= N" only says the
// quantity is committed elsewhere — the caller must look at OUR orders for the
// position (a stop that just filled, an exit still resting) before deciding.
type rmsLimit struct {
	Set  int
	Used int
	Prev int
}

var rmsLimitRe = regexp.MustCompile(`(?i)set\s*=\s*(\d+)\s*,\s*n\.?\s*used\s*=\s*(\d+)\s*,?\s*prev\s*=\s*(\d+)`)

// parseRMSLimit extracts Set / N.Used / Prev from an RMS quantity rejection.
// ok=false when the text is not that message.
func parseRMSLimit(reason string) (rmsLimit, bool) {
	m := rmsLimitRe.FindStringSubmatch(reason)
	if m == nil {
		return rmsLimit{}, false
	}
	set, e1 := strconv.Atoi(m[1])
	used, e2 := strconv.Atoi(m[2])
	prev, e3 := strconv.Atoi(m[3])
	if e1 != nil || e2 != nil || e3 != nil {
		return rmsLimit{}, false
	}
	return rmsLimit{Set: set, Used: used, Prev: prev}, true
}

// isRMSQtyRejection reports whether a broker/exchange rejection text is the
// RMS refusing the quantity (as opposed to a price band, a session or a
// validation error).
func isRMSQtyRejection(reason string) bool {
	r := strings.ToLower(reason)
	return strings.Contains(r, "free qty") || strings.Contains(r, "limit exceeded")
}

// rmsSaysNoHolding is true when the RMS text states the broker holds ZERO of
// the scrip ("Set= 0"): the position our book carries does not exist at the
// broker. Re-placing a stop or an exit for it can only be rejected again.
func rmsSaysNoHolding(reason string) bool {
	l, ok := parseRMSLimit(reason)
	return ok && l.Set == 0
}

// looksExecutedCancelError recognises the broker refusing to cancel an order
// because it has already executed ("FULLY_EXECUTED order not allowed to
// CANCEL" — FILATEX 2026-10-23): the quantity is SOLD, not locked.
func looksExecutedCancelError(err error) bool {
	if err == nil {
		return false
	}
	e := strings.ToLower(err.Error())
	return strings.Contains(e, "fully_executed") || strings.Contains(e, "fully executed") ||
		strings.Contains(e, "already executed") || strings.Contains(e, "executed order not allowed")
}

// lostStopAction is what the safety monitor does with a stop row whose broker
// order is gone (absent from the book, cancelled, rejected).
type lostStopAction int

const (
	// lostStopReprotect: live position, stop lost → reprotectLostSL (exit if
	// crossed, re-place inside the band, defer below it).
	lostStopReprotect lostStopAction = iota
	// lostStopOrphan: our own ledger says the position is fully sold (net ≤ 0)
	// — the row is a stale leftover. Close it; nothing to protect, nothing to
	// sell, nobody to page (FIEMIND/MPSLTD/HSCL 2026-10-23: three false
	// EXIT_CHASE_EXHAUSTED alerts came from stale rows of positions whose
	// stops had filled weeks earlier).
	lostStopOrphan
	// lostStopGhost: the broker's RMS rejected the stop with "Set= 0" — it
	// holds nothing. Park the row as cover (so the naked scan does not place
	// a stop for it every cycle), alert ONCE, never re-place (IOLCP
	// 2026-10-23: 79 rejected re-placements in 20 minutes).
	lostStopGhost
	// lostStopBudget: the row has already been re-placed reprotectBudgetPerDay
	// times today and keeps vanishing — something external keeps killing it.
	// Park + alert once instead of fighting the broker every 15 s.
	lostStopBudget
)

// reprotectBudgetPerDay caps how many times one stop row is re-placed in a
// session before the monitor stops and asks a human.
const reprotectBudgetPerDay = 3

// classifyLostStop decides the action for a lost stop from (a) our ledger's
// net holding for the position, (b) the broker's rejection text if the stop
// was rejected, (c) how many re-placements this row already had today.
//
// netKnown=false (ledger query failed) never blocks protection: the position
// is treated as live.
func classifyLostStop(net int, hasEntry, netKnown bool, rejReason string, replacedToday int) lostStopAction {
	if netKnown && (!hasEntry || net <= 0) {
		return lostStopOrphan
	}
	if rmsSaysNoHolding(rejReason) {
		return lostStopGhost
	}
	if replacedToday >= reprotectBudgetPerDay {
		return lostStopBudget
	}
	return lostStopReprotect
}
