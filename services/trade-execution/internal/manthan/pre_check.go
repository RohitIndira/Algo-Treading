package manthan

import (
	"context"
	"fmt"
	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"os"
	"time"

	"go.uber.org/zap"
)

// PreChecker validates preconditions before placing any order.
type PreChecker struct {
	repo   *Repository
	broker *BrokerAdapter
	logger *zap.Logger
}

func NewPreChecker(repo *Repository, broker *BrokerAdapter, logger *zap.Logger) *PreChecker {
	return &PreChecker{repo: repo, broker: broker, logger: logger}
}

// PreCheckResult tells the caller whether to proceed.
type PreCheckResult struct {
	CanProceed bool
	Reason     string
}

func pass() PreCheckResult              { return PreCheckResult{CanProceed: true} }
func fail(reason string) PreCheckResult { return PreCheckResult{CanProceed: false, Reason: reason} }

// CheckEntry runs all pre-checks before a LIMIT BUY.
func (p *PreChecker) CheckEntry(ctx context.Context, signal ManthanSignal, info *SymbolInfo) PreCheckResult {
	// 1. Market hours (9:15–15:20, no entries in last 10 min)
	if r := p.checkMarketHours(); !r.CanProceed {
		return r
	}

	// 2. Duplicate check (idempotency via signal_id)
	if signal.OrderID != "" {
		dup, err := p.repo.CheckDuplicate(ctx, signal.OrderID)
		if err != nil {
			p.logger.Warn("Duplicate check failed — proceeding", zap.Error(err))
		} else if dup {
			return fail("duplicate signal_id: " + signal.OrderID)
		}
	}

	// 3. Circuit check
	atUpper, _, err := p.broker.CheckCircuit(ctx, info.ExchangeToken)
	if err != nil {
		p.logger.Warn("Circuit check failed — proceeding", zap.Error(err))
	} else if atUpper {
		return fail(ReasonUpperCircuit)
	}

	// 4. Paper mode — skip margin check
	if signal.TradingMode == "PAPER" {
		return pass()
	}

	// 5. LIVE mode — could check margin here (deferred for now)
	// TODO: broker.GetFundLimits() and validate signal.InvestedAmt <= available

	return pass()
}

// CheckMargin verifies the user has enough available margin at the broker
// BEFORE we try to place the entry order. Skipping this doesn't break
// anything — the broker would reject with "Margin Limit exceeded" — but
// it's faster, doesn't pollute the order book with margin-rejected attempts,
// and gives us a clear early-skip reason in logs/dashboards.
//
// required = qty × limit_price × 1.005  (0.5% buffer for brokerage/taxes)
// Called after auth is resolved (separate from CheckEntry which runs
// before auth is available in the entry flow).
//
// Skipped for PAPER trading mode (no real margin involved).
func (p *PreChecker) CheckMargin(ctx context.Context, auth BrokerAuth, tradingMode string, qty int, limitPrice float64) PreCheckResult {
	if tradingMode == "PAPER" {
		return pass()
	}
	if qty <= 0 || limitPrice <= 0 {
		return pass()
	}

	required := float64(qty) * limitPrice * 1.005
	available, err := p.broker.GetAvailableMargin(ctx, auth)
	if err != nil {
		// Broker API failure — don't pre-skip; let broker enforce margin.
		// Worst case: broker rejects with clearer message than we'd produce.
		p.logger.Warn("Margin pre-check fetch failed — proceeding to broker",
			zap.String("user", auth.UserID), zap.Error(err))
		return pass()
	}

	if available < required {
		return fail(fmt.Sprintf("insufficient margin: available=₹%.2f required=₹%.2f (qty=%d × price=%.2f × 1.005)",
			available, required, qty, limitPrice))
	}
	p.logger.Debug("Margin check passed",
		zap.Float64("available", available),
		zap.Float64("required", required))
	return pass()
}

// CheckSLModify runs pre-checks before modifying a trailing SL.
func (p *PreChecker) CheckSLModify(ctx context.Context, signal SLModifySignal, info *SymbolInfo) PreCheckResult {
	// Paper mode — always OK
	if signal.TradingMode == "PAPER" {
		return pass()
	}

	// Lower circuit — SL modify won't help
	_, atLower, err := p.broker.CheckCircuit(ctx, info.ExchangeToken)
	if err == nil && atLower {
		return fail("stock at lower circuit — SL modify pointless")
	}

	return pass()
}

// ReasonUpperCircuit is the pre-check reason for an entry blocked ONLY by the
// upper price band. entry_handler matches on THIS constant to route the block
// into the UPPER_CIRCUIT retry class instead of POISON/DLQ — a reworded
// literal here would silently revert to the DLQ behavior, so keep them tied.
const ReasonUpperCircuit = "stock at upper circuit — skip entry"

// ReasonPreOpen is the pre-check reason for an entry that arrived BEFORE the
// 09:15 IST open. Like ReasonUpperCircuit it is a WAIT state, not a
// rejection: the daily 09:00 IST publish (and any sheet edit inside the
// 08:45–09:15 watch window) lands entries here every single morning. The
// entry handler matches on this literal to return ErrPreOpenHold, which the
// inbox classifier maps to the PRE_OPEN retry class (wake at 09:15:10, no
// attempt burn) instead of POISON/DLQ. 2026-08-17: all 26 of FIV99's
// Monday-morning entries were DLQ'd on attempt 1 by exactly this reason,
// and 2026-08-18's cron-time entries would have died the same way.
const ReasonPreOpen = "market not open yet (before 9:15)"

// checkMarketHours validates we're within trading hours.
// Entry orders rejected before 9:15 and after 15:20 (last 10 min buffer).
//
// DANGER-mode override (env MANTHAN_TEST_SKIP_CLOSE_CUTOFF=1): bypasses
// the 15:20 post-cutoff rejection. Weekend + before-open checks stay
// enforced. Use ONLY for controlled tests where you are actively
// monitoring positions manually and will manually place SL if needed.
// Never set in production — the 10-min buffer exists specifically so
// that a fill can be confirmed AND its SL placed before market close;
// bypass it and any position that fills after 15:20 goes into overnight
// unprotected.
func (p *PreChecker) checkMarketHours() PreCheckResult {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	if loc == nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}
	now := time.Now().In(loc)

	mockSession := os.Getenv("MANTHAN_MOCK_SESSION") == "1"
	weekday := now.Weekday()
	if (weekday == time.Saturday || weekday == time.Sunday) && !mockSession {
		return fail(fmt.Sprintf("market closed — %s", weekday))
	}
	// NSE holiday on a weekday: a hard close, not a pre-open wait. Without
	// this the PRE_OPEN hold would carry the 09:00 batch to 09:15 and send
	// LIMIT BUYs at stale prices to the broker on an exchange holiday.
	if !indiraClient.IsTradingDay(now) && !mockSession {
		return fail("market closed — exchange holiday")
	}
	if mockSession {
		p.logger.Warn("⚠️ MANTHAN_MOCK_SESSION=1 — weekend/holiday gates bypassed; other pre-checks active")
		return pass()
	}

	hour, min := now.Hour(), now.Minute()
	minuteOfDay := hour*60 + min

	marketOpen := 9*60 + 15   // 9:15
	marketClose := 15*60 + 20 // 15:20 (10 min buffer before 15:30)

	if minuteOfDay < marketOpen {
		return fail(ReasonPreOpen)
	}
	if minuteOfDay >= marketClose {
		if os.Getenv("MANTHAN_TEST_SKIP_CLOSE_CUTOFF") == "1" {
			p.logger.Warn("⚠️  MARKET-CLOSE CUTOFF BYPASSED (MANTHAN_TEST_SKIP_CLOSE_CUTOFF=1) — proceeding after 15:20",
				zap.Int("minute_of_day", minuteOfDay),
				zap.String("clock", fmt.Sprintf("%02d:%02d IST", hour, min)))
			return pass()
		}
		return fail("too close to market close (after 15:20)")
	}

	return pass()
}
