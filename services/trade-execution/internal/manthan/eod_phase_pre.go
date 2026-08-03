// EOD Pre-Flight (Layer 3) — 14:30 IST daily JWT-validity scan with
// pre-emptive SESSION_EXPIRED alerts.
//
// Why this exists:
//   EOD Phase A submits AMO+SL at 16:35 IST. If a user's broker JWT is
//   expired at that moment, Phase A skips them and Layer 4's retry queue
//   takes over — but only after the user re-logs in. Without a nudge, users
//   may not notice they need to re-login until the next morning, leaving
//   the position unprotected overnight. Layer 3 fires the nudge 65 minutes
//   before the EOD window opens so the user has time to log in via the
//   normal SSO flow and have their JWT ready when Phase A runs.
//
// What this does:
//   At 14:30 IST on every trading day, walk every user with OPEN Manthan
//   positions. For each user whose creds are absent OR whose session is
//   gated as expired by AuthExpiryNotifier, publish a targeted
//   SESSION_EXPIRED message to manthan.notifications. The frontend renders
//   it as a "re-login required" flash via the existing WebSocket bridge.
//
// What this does NOT do:
//   - Touch the broker. Layer 3 is a passive scan against in-memory state
//     (credentials cache + the AuthExpiryNotifier gate); the live JWT_EXPIRING
//     poll loop in jwt_expiry_notifier.go handles broker-side validation on
//     a separate 30-minute cadence with an 8-hour pre-warn window.
//   - Place orders. EOD Phase A at 16:35 IST does that.
package manthan

import (
	"context"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"go.uber.org/zap"
)

// StartEODPreFlight registers the 14:30 IST daily cron. Catch-up: if the
// service boots between 14:30 and 15:30 IST on a trading day, runs once
// immediately so a deploy mid-window doesn't skip a day.
func (p *ProtectiveReplay) StartEODPreFlight(ctx context.Context) {
	p.logger.Info("EOD pre-flight scheduler started",
		zap.String("schedule", "14:30 IST · JWT-validity scan for OPEN positions, alert on expiry"))

	now := p.now()
	if indiraClient.IsTradingDay(now) {
		minute := now.Hour()*60 + now.Minute()
		if minute >= 14*60+30 && minute < 15*60+30 {
			p.logger.Info("Startup-recovery: running missed EOD pre-flight cycle")
			go p.runEODPreFlight(ctx)
		}
	}
	go p.scheduleDaily(ctx, 14, 30, p.runEODPreFlight)
}

// runEODPreFlight is the 14:30 IST cron body: scan every user with OPEN
// positions, alert anyone whose JWT is missing or gated as expired.
func (p *ProtectiveReplay) runEODPreFlight(ctx context.Context) {
	cycleCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	positions, err := p.repo.ListPositionsNeedingProtection(cycleCtx)
	if err != nil {
		p.logger.Error("EOD pre-flight: list positions failed", zap.Error(err))
		return
	}
	if len(positions) == 0 {
		p.logger.Info("EOD pre-flight: no positions need protection — nothing to check")
		return
	}

	seen := map[string]struct{}{}
	var atRisk int
	for _, pos := range positions {
		if _, ok := seen[pos.UserID]; ok {
			continue
		}
		seen[pos.UserID] = struct{}{}

		auth := p.getAuth(pos.UserID)
		missingCreds := auth == nil
		expiredGate := p.authNotif != nil && p.authNotif.IsSessionExpired(pos.UserID)
		if !missingCreds && !expiredGate {
			continue
		}

		atRisk++
		reason := "EOD AMO at 16:35 IST — re-login required to enable overnight SL protection"
		if missingCreds {
			reason = "EOD AMO at 16:35 IST — no broker credentials on file; please log in to enable overnight SL protection"
		}
		if p.authNotif != nil {
			p.authNotif.PublishSessionExpired(cycleCtx, pos.UserID, reason)
		}
		p.logger.Warn("EOD pre-flight: user at risk for overnight protection — alerted",
			zap.String("user_id", pos.UserID),
			zap.Bool("missing_creds", missingCreds),
			zap.Bool("expired_gate", expiredGate))
	}

	p.logger.Info("EOD pre-flight: cycle complete",
		zap.Int("users_checked", len(seen)),
		zap.Int("users_at_risk", atRisk))
}
