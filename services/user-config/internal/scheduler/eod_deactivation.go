package scheduler

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"time"
)

const istTimezone = "Asia/Kolkata"

// EOD deactivation times, kept aligned with the trade-execution square-off times.
// Defaults: paper 15:00, live 15:05 IST. Overridable via env so they can track an
// extended session close (e.g. 15:50 / 15:55 for a SEBI Saturday mock ending 16:00).
// Evaluated once at package init.
var paperDeactivationHour, paperDeactivationMinute = parseHHMM(envDefault("EOD_PAPER_DEACTIVATION_TIME", "15:00"), 15, 0)
var liveDeactivationHour, liveDeactivationMinute = parseHHMM(envDefault("EOD_LIVE_DEACTIVATION_TIME", "15:05"), 15, 5)

// envDefault returns the env var value for key, or def if unset/empty.
func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parseHHMM parses "HH:MM" into (hour, minute), returning (defH, defM) if the
// value is malformed or out of range.
func parseHHMM(s string, defH, defM int) (int, int) {
	var h, m int
	if n, err := fmt.Sscanf(s, "%d:%d", &h, &m); n != 2 || err != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return defH, defM
	}
	return h, m
}

// allowSaturdayMock, when true, treats Saturday as a normal trading day so EOD
// deactivation runs during SEBI's Saturday mock/special sessions. Sunday is
// always closed. Controlled by the ALLOW_SATURDAY_MOCK env var (default false),
// read once at package init.
var allowSaturdayMock = func() bool {
	v, _ := strconv.ParseBool(os.Getenv("ALLOW_SATURDAY_MOCK"))
	return v
}()

// isClosedWeekend reports whether t is a weekend day on which trading is closed.
// Sunday is always closed; Saturday is closed unless Saturday mock is enabled.
func isClosedWeekend(t time.Time) bool {
	switch t.Weekday() {
	case time.Sunday:
		return true
	case time.Saturday:
		return !allowSaturdayMock
	default:
		return false
	}
}

// StrategyDeactivator is the interface the scheduler needs from the service layer.
type StrategyDeactivator interface {
	// DeactivateAllActiveStrategiesByMode deactivates all active strategies of a given
	// trading mode ("PAPER" or "LIVE"). Used by the global EOD scheduler.
	DeactivateAllActiveStrategiesByMode(ctx context.Context, tradingMode string) (int, error)
	// DeactivateStrategiesAtAutoSquareOffTime deactivates all active strategies whose
	// enable_auto_square_off=true and auto_square_off_time matches squareOffTime (HH:MM).
	DeactivateStrategiesAtAutoSquareOffTime(ctx context.Context, squareOffTime string) (int, error)
}

// EODDeactivationScheduler deactivates active strategies at market close, aligned with
// the square-off times: paper at 15:00 IST, live at 15:05 IST (Monday–Friday).
type EODDeactivationScheduler struct {
	svc      StrategyDeactivator
	stopChan chan struct{}
}

// NewEODDeactivationScheduler creates a new end-of-day deactivation scheduler.
func NewEODDeactivationScheduler(svc StrategyDeactivator) *EODDeactivationScheduler {
	return &EODDeactivationScheduler{
		svc:      svc,
		stopChan: make(chan struct{}),
	}
}

// Start runs the scheduler loop, checking every minute on weekdays.
// Paper strategies deactivate at 15:00; live strategies at 15:05.
// Per-user custom auto_square_off_time strategies deactivate at their configured time.
func (s *EODDeactivationScheduler) Start(ctx context.Context) {
	loc, err := time.LoadLocation(istTimezone)
	if err != nil {
		log.Printf("[eod-scheduler] WARN: could not load IST timezone (%v), falling back to UTC+5:30 offset", err)
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	var lastPaperFiredDate string // prevents double-firing paper deactivation on same day
	var lastLiveFiredDate string  // prevents double-firing live deactivation on same day

	log.Printf("[eod-scheduler] Started — paper deactivation at %02d:%02d IST, live deactivation at %02d:%02d IST (weekdays); per-user deactivation at their auto-square-off time",
		paperDeactivationHour, paperDeactivationMinute, liveDeactivationHour, liveDeactivationMinute)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[eod-scheduler] Stopped via context")
			return
		case <-s.stopChan:
			log.Printf("[eod-scheduler] Stopped via stop channel")
			return
		case t := <-ticker.C:
			// Recover guards this single tick, so a panic at 15:00 doesn't
			// cancel every future minute's check — including 15:05 LIVE
			// deactivation and tomorrow's market close.
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						log.Printf("[eod-scheduler] PANIC recovered: %v\n%s", rec, debug.Stack())
					}
				}()

				now := t.In(loc)
				if isClosedWeekend(now) {
					return
				}

				currentTime := fmt.Sprintf("%02d:%02d", now.Hour(), now.Minute())
				today := now.Format("2006-01-02")

				// Per-user custom auto-square-off: deactivate strategies whose
				// auto_square_off_time matches the current minute.
				s.deactivateAtCustomSquareOffTime(ctx, currentTime)

				// Global paper deactivation at 15:00 IST.
				if now.Hour() == paperDeactivationHour && now.Minute() == paperDeactivationMinute && lastPaperFiredDate != today {
					lastPaperFiredDate = today
					s.deactivateByMode(ctx, "PAPER")
				}

				// Global live deactivation at 15:05 IST.
				if now.Hour() == liveDeactivationHour && now.Minute() == liveDeactivationMinute && lastLiveFiredDate != today {
					lastLiveFiredDate = today
					s.deactivateByMode(ctx, "LIVE")
				}
			}()
		}
	}
}

// Stop signals the scheduler to stop.
func (s *EODDeactivationScheduler) Stop() {
	close(s.stopChan)
}

func (s *EODDeactivationScheduler) deactivateByMode(ctx context.Context, tradingMode string) {
	deactivateCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	count, err := s.svc.DeactivateAllActiveStrategiesByMode(deactivateCtx, tradingMode)
	if err != nil {
		log.Printf("[eod-scheduler] ERROR: failed to deactivate %s strategies: %v", tradingMode, err)
		return
	}
	log.Printf("[eod-scheduler] ✓ Deactivated %d %s strategies at market close", count, tradingMode)
}

func (s *EODDeactivationScheduler) deactivateAtCustomSquareOffTime(ctx context.Context, squareOffTime string) {
	deactivateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	count, err := s.svc.DeactivateStrategiesAtAutoSquareOffTime(deactivateCtx, squareOffTime)
	if err != nil {
		log.Printf("[eod-scheduler] ERROR: failed to deactivate strategies at custom sq-off time %s: %v", squareOffTime, err)
		return
	}
	if count > 0 {
		log.Printf("[eod-scheduler] ✓ Deactivated %d strategies at custom auto-square-off time %s IST", count, squareOffTime)
	}
}
