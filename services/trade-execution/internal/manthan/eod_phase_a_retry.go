// EOD Phase A Retry (Layer 4) — background worker that re-attempts EOD
// Phase A for users whose first try at 15:35 IST was skipped because their
// broker JWT was expired or absent.
//
// Why this exists:
//   Without Layer 4, a single missed EOD window leaves a user's positions
//   unprotected for the next overnight regardless of when they later log
//   in. The 09:14 IST morning cron is the only fallback — and it skips
//   the same way if the JWT is still expired at 09:14.
//
// What this does:
//   Two wake paths drain manthan_arm_retries:
//     1. 5-minute ticker — bounded latency between user re-login and AMO
//        submission, even if the on-login wake (path 2) drops.
//     2. OnCredentialsUpdated(userID) — invoked by the user-config-events
//        Kafka consumer (USER_CREDENTIALS_UPDATED event) the moment a fresh
//        JWT lands in user_credentials. Fires within ~1 second of SSO login.
//   For each user with PENDING rows + now-valid creds, the worker calls
//   ProtectiveReplay.RunEODPhaseANow which re-runs the full EOD cycle.
//   InsertAMOOrder is idempotent under the partial-unique index from
//   migration 011, so already-armed positions for other users skip cleanly.
//
// Give-up policy:
//   At the top of each tick, any PENDING row whose trade_date is strictly
//   before today (IST) is marked GIVEN_UP — the morning hot-SL cron has
//   already run or is too close to running, and a late AMO would land in
//   the wrong session. This keeps the queue bounded even if a user never
//   re-logs in.
package manthan

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// ArmRetryWorker is the Layer 4 retry worker. Construct via NewArmRetryWorker;
// start via Start(ctx). Wire OnCredentialsUpdated as the consumer's
// CredentialsObserver so SSO login triggers an immediate retry.
type ArmRetryWorker struct {
	replay *ProtectiveReplay
	repo   *Repository
	logger *zap.Logger
	ist    *time.Location

	wakeOnce sync.Once
	wakeCh   chan string // userID hint; "" = scan all PENDING rows
}

// NewArmRetryWorker constructs the retry worker. Start(ctx) launches its loop.
func NewArmRetryWorker(replay *ProtectiveReplay, repo *Repository, logger *zap.Logger) *ArmRetryWorker {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil || loc == nil {
		loc = time.FixedZone("IST", 5*60*60+30*60)
	}
	return &ArmRetryWorker{
		replay: replay,
		repo:   repo,
		logger: logger,
		ist:    loc,
		wakeCh: make(chan string, 32),
	}
}

// Start launches the worker's loop. Returns immediately. Idempotent.
func (w *ArmRetryWorker) Start(ctx context.Context) {
	if w == nil {
		return
	}
	w.wakeOnce.Do(func() {
		w.logger.Info("Arm-retry worker started",
			zap.String("schedule", "5-min poll + on-login wake via USER_CREDENTIALS_UPDATED"))
		go w.loop(ctx)
	})
}

// OnCredentialsUpdated implements the consumer.CredentialsObserver interface.
// The strategy-events consumer calls this whenever a user re-logs in via SSO.
// Non-blocking — if the wake channel is full (a wake for this user is already
// queued), the call is dropped silently because the queued wake will see any
// rows that landed since.
func (w *ArmRetryWorker) OnCredentialsUpdated(userID string) {
	if w == nil || userID == "" {
		return
	}
	select {
	case w.wakeCh <- userID:
		w.logger.Info("Arm-retry worker: wake-on-login queued",
			zap.String("user_id", userID))
	default:
	}
}

func (w *ArmRetryWorker) loop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	// Drain anything persisted by a prior process run.
	w.tick(ctx, "")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick(ctx, "")
		case uid := <-w.wakeCh:
			w.tick(ctx, uid)
		}
	}
}

// tick scans the retry queue + re-attempts EOD Phase A for users whose creds
// are now available. userHint, when non-empty, restricts the scan to one user
// (the on-login wake path); otherwise the full PENDING set is walked.
func (w *ArmRetryWorker) tick(ctx context.Context, userHint string) {
	// Cleanup: give up on rows whose trade_date has passed.
	today := w.todayIST()
	if err := w.repo.MarkArmRetriesGivenUpBefore(ctx, today); err != nil {
		w.logger.Warn("Arm-retry worker: cleanup failed", zap.Error(err))
	}

	rows, err := w.repo.ListPendingArmRetries(ctx, userHint)
	if err != nil {
		w.logger.Error("Arm-retry worker: list pending failed", zap.Error(err))
		return
	}
	if len(rows) == 0 {
		return
	}

	// Group by user — one RunEODPhaseANow per ready user. The retry path
	// uses the full-cycle method (not a single-user variant) because
	// InsertAMOOrder is idempotent: already-armed positions skip cleanly,
	// only positions whose user now has fresh auth actually get AMOs.
	byUser := map[string][]ArmRetryRow{}
	for _, r := range rows {
		byUser[r.UserID] = append(byUser[r.UserID], r)
	}

	cycleFired := false
	for uid, queued := range byUser {
		auth := w.replay.getAuth(uid)
		if auth == nil {
			for _, r := range queued {
				_ = w.repo.MarkArmRetryAttempted(ctx, r.ID, "still no broker auth")
			}
			continue
		}

		w.logger.Info("Arm-retry worker: user now has auth — re-attempting EOD",
			zap.String("user_id", uid),
			zap.Int("queued_rows", len(queued)))

		// Re-run the full EOD cycle once per tick. Multiple ready users in
		// the same tick share one cycle — InsertAMOOrder idempotency makes
		// it safe but wasteful to call repeatedly.
		if !cycleFired {
			w.replay.RunEODPhaseANow(ctx)
			cycleFired = true
		}

		// Per-row verification: only mark DONE if the position is genuinely
		// armed for its target trade_date now.
		for _, r := range queued {
			protected, perr := w.repo.HasActiveProtectionForToday(ctx, r.EntryOrderID, r.TradeDate)
			if perr != nil {
				_ = w.repo.MarkArmRetryAttempted(ctx, r.ID, "post-retry protection check errored: "+perr.Error())
				continue
			}
			if protected {
				_ = w.repo.MarkArmRetryDone(ctx, r.ID)
				continue
			}
			_ = w.repo.MarkArmRetryAttempted(ctx, r.ID, "post-retry: still no active protection — will retry next tick")
		}
	}
}

func (w *ArmRetryWorker) todayIST() time.Time {
	n := time.Now().In(w.ist)
	return time.Date(n.Year(), n.Month(), n.Day(), 0, 0, 0, 0, w.ist)
}
