package manthan

import (
	"context"
	"fmt"
	"strings"

	"go.uber.org/zap"
)

// classifyAMOAttemptErrors sorts tonight's AMO rejection texts into
// hard (deterministic), lock (quantity still held by the swept day-stop — the
// expected evening state) and ghost (broker holds nothing: "Set= 0").
func classifyAMOAttemptErrors(errs []string) (hard, lock, ghost int) {
	for _, e := range errs {
		if strings.TrimSpace(e) == "" {
			continue // row still pending / no rejection recorded
		}
		switch {
		case rmsSaysNoHolding(e):
			ghost++
		case isRMSQtyRejection(e):
			lock++
		default:
			hard++
		}
	}
	return hard, lock, ghost
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
	note := fmt.Sprintf("GHOST_SUSPECT: broker holds 0 of %s (%s) while the ledger holds %d — EOD arming stopped; heal via admin ghost endpoint", pos.Symbol, reason, pos.NetQty)
	parked, perr := p.repo.ParkLatestStopRowForPosition(ctx, pos.StrategyID, pos.Symbol, "GHOST_SUSPECT", note)
	if perr != nil || parked == 0 {
		p.logger.Error("EOD Phase A: ghost suspect but no stop row could be parked — the position will be re-attempted next cycle",
			zap.String("user_id", pos.UserID), zap.String("symbol", pos.Symbol), zap.NamedError("err", perr))
	} else {
		_ = p.repo.InsertEvent(ctx, parked, "SL_GHOST_SUSPECT", "", "SL_DEFERRED_BAND", "GHOST_SUSPECT", 0, pos.NetQty, note)
	}
	p.logger.Error("EOD Phase A: broker holds NOTHING of this position — ghost suspect parked, not arming",
		zap.String("user_id", pos.UserID), zap.String("symbol", pos.Symbol), zap.Int("ledger_net", pos.NetQty), zap.String("rms", reason))
	notifyOps(p.authNotif, ctx, pos.UserID, pos.Symbol, "GHOST_SUSPECT",
		fmt.Sprintf("EOD: broker holds 0 of %s but our ledger holds %d (RMS: %s) — overnight stop not armed; verify at the broker and heal via the admin ghost endpoint", pos.Symbol, pos.NetQty, reason))
}
