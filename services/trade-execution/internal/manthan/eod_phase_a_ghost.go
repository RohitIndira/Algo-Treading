package manthan

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// eodGiveUpState is embedded in ProtectiveReplay (see protective_replay.go).
type eodGiveUpState struct {
	eodGiveUpMu sync.Mutex
	eodGiveUp   map[string]bool
}

// classifyAMOAttemptErrors sorts tonight's AMO rejection texts into
// hard (deterministic), lock (quantity still held by the swept day-stop — the
// expected evening state), ghost (broker holds nothing: "Set= 0") and auth
// (our own session was dead — the order never reached the broker; FIV99
// 2026-10-26: 60 h of AU004 attempts had spent the cap, so nothing was armed
// even after the client logged in).
//
// settled=false (the position's newest buy is within settlement) turns every
// "Set= 0" into a lock: an unsettled buy reads exactly like a ghost to the RMS
// (SHREEPUSHK 2026-10-28), and the spacing ladder — not a park — is the
// right response until the shares land in the demat.
func classifyAMOAttemptErrors(errs []string, settled bool) (hard, lock, ghost, auth int) {
	for _, e := range errs {
		if strings.TrimSpace(e) == "" {
			continue // row still pending / no rejection recorded
		}
		le := strings.ToLower(e)
		switch {
		case settled && rmsSaysNoHolding(e):
			ghost++
		case isRMSQtyRejection(e):
			lock++
		case strings.Contains(le, "au004") || strings.Contains(le, "session expired") || strings.Contains(le, "no broker auth"):
			auth++
		default:
			hard++
		}
	}
	return hard, lock, ghost, auth
}

// eodGiveUpOnce reports whether this is the first time tonight the cap verdict
// is reached for the position (per process): the retry worker re-enters every
// 5 minutes, and logging/publishing the same give-up each time produced 351
// identical error lines on 2026-10-26.
func (p *ProtectiveReplay) eodGiveUpOnce(entryOrderID int64, tradeDate time.Time) bool {
	key := fmt.Sprintf("%d|%s", entryOrderID, tradeDate.Format("2006-01-02"))
	p.eodGiveUpMu.Lock()
	defer p.eodGiveUpMu.Unlock()
	if p.eodGiveUp == nil {
		p.eodGiveUp = map[string]bool{}
	}
	if p.eodGiveUp[key] {
		return false
	}
	p.eodGiveUp[key] = true
	return true
}

// parkGhostAtEOD handles a position whose overnight stop the broker's RMS
// rejected with "Set= 0": the position does not exist at the broker. The
// latest stop row is parked GHOST_SUSPECT (ListPositionsNeedingProtection
// then excludes the position, so no further attempt tonight), the operator
// is paged once; the admin ghost endpoint heals the ledger.
func (p *ProtectiveReplay) parkGhostAtEOD(ctx context.Context, pos PositionNeedingProtection, errs []string) {
	reason := ""
	for _, e := range errs {
		if rmsSaysNoHolding(e) {
			reason = e
			break
		}
	}
	note := fmt.Sprintf("GHOST_SUSPECT: 0 sellable at the broker for %s (%s) while the ledger holds %d — sold, transferred or pledged; EOD arming stopped; the book reconciler decides (or heal via the admin ghost endpoint)", pos.Symbol, reason, pos.NetQty)
	parked, perr := p.repo.ParkLatestStopRowForPosition(ctx, pos.StrategyID, pos.Symbol, "GHOST_SUSPECT", note)
	if perr != nil || parked == 0 {
		p.logger.Error("EOD Phase A: ghost suspect but no stop row could be parked — the position will be re-attempted next cycle",
			zap.String("user_id", pos.UserID), zap.String("symbol", pos.Symbol), zap.NamedError("err", perr))
	} else {
		_ = p.repo.InsertEvent(ctx, parked, "SL_GHOST_SUSPECT", "", "SL_DEFERRED_BAND", "GHOST_SUSPECT", 0, pos.NetQty, note)
	}
	p.logger.Error("EOD Phase A: 0 sellable at the broker for this position (sold, transferred or pledged) — ghost suspect parked, not arming",
		zap.String("user_id", pos.UserID), zap.String("symbol", pos.Symbol), zap.Int("ledger_net", pos.NetQty), zap.String("rms", reason))
	notifyOps(p.authNotif, ctx, pos.UserID, pos.Symbol, "GHOST_SUSPECT",
		fmt.Sprintf("EOD: 0 sellable at the broker for %s but our ledger holds %d (RMS: %s) — sold, transferred or pledged; overnight stop not armed; the book reconciler decides", pos.Symbol, pos.NetQty, reason))
}
