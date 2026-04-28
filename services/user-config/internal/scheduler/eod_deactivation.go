package scheduler

import (
	"context"
	"log"
	"time"
)

const (
	eodDeactivationHour   = 15
	eodDeactivationMinute = 30
	istTimezone           = "Asia/Kolkata"
)

// StrategyDeactivator is the interface the scheduler needs from the service layer.
type StrategyDeactivator interface {
	DeactivateAllActiveStrategies(ctx context.Context) (int, error)
}

// EODDeactivationScheduler deactivates all active strategies at market close
// (15:30 IST, Monday–Friday).
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

// Start runs the scheduler loop. It checks every minute and fires once per day
// when the clock reaches 15:30 IST on a weekday.
func (s *EODDeactivationScheduler) Start(ctx context.Context) {
	loc, err := time.LoadLocation(istTimezone)
	if err != nil {
		log.Printf("[eod-scheduler] WARN: could not load IST timezone (%v), falling back to UTC+5:30 offset", err)
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	var lastFiredDate string // "YYYY-MM-DD" — prevents double-firing in the same minute

	log.Printf("[eod-scheduler] Started — will deactivate NEWS strategies at %02d:%02d IST (52W strategies stay active)",
		eodDeactivationHour, eodDeactivationMinute)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[eod-scheduler] Stopped via context")
			return
		case <-s.stopChan:
			log.Printf("[eod-scheduler] Stopped via stop channel")
			return
		case t := <-ticker.C:
			now := t.In(loc)
			if s.shouldFire(now, lastFiredDate) {
				lastFiredDate = now.Format("2006-01-02")
				s.deactivateAll(ctx)
			}
		}
	}
}

// Stop signals the scheduler to stop.
func (s *EODDeactivationScheduler) Stop() {
	close(s.stopChan)
}

// shouldFire returns true when now is a weekday at exactly 15:30 IST and has
// not already fired today.
func (s *EODDeactivationScheduler) shouldFire(now time.Time, lastFiredDate string) bool {
	weekday := now.Weekday()
	if weekday == time.Saturday || weekday == time.Sunday {
		return false
	}
	if now.Hour() != eodDeactivationHour || now.Minute() != eodDeactivationMinute {
		return false
	}
	today := now.Format("2006-01-02")
	return lastFiredDate != today
}

func (s *EODDeactivationScheduler) deactivateAll(ctx context.Context) {
	log.Printf("[eod-scheduler] 15:30 IST reached — deactivating NEWS strategies (52W stays active)")

	deactivateCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	count, err := s.svc.DeactivateAllActiveStrategies(deactivateCtx)
	if err != nil {
		log.Printf("[eod-scheduler] ERROR: failed to deactivate strategies: %v", err)
		return
	}

	log.Printf("[eod-scheduler] ✓ Deactivated %d strategies at market close", count)
}
