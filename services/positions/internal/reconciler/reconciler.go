// Package reconciler is Chunk P.G of positions svc — periodic drift detection
// between broker holdings and positions_db.positions.
//
// Per docs/positions_service_design.md §11 Q2 and the 2026-06-12 S4450
// liquidation incident: this package NEVER auto-fixes. It observes and
// publishes to positions.drift.detected — a human decides what to do next.
//
// Broker-holdings fetch is deferred (see main.go). This package exposes a
// pure Detect() that takes (userID, brokerHoldings) as input, so callers
// can wire any fetch source (gRPC → trade-execution, Kafka snapshot, tests).
package reconciler

import (
	"context"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"

	"go.uber.org/zap"
)

// PositionSource is what the reconciler needs from the store — abstracting
// this as an interface keeps tests hermetic.
type PositionSource interface {
	FindAllActiveLotsForUser(ctx context.Context, userID string) ([]*store.Position, error)
}

// UsersSource returns the set of users to sweep each tick. The store's
// DistinctUsersWithActive satisfies this — one row per user_id with an
// ACTIVE lot.
type UsersSource interface {
	DistinctUsersWithActive(ctx context.Context) ([]string, error)
}

// HoldingsFetcher fetches the broker's per-symbol delivery quantity for
// one user. The tradeexec gRPC client satisfies this via GetBrokerHoldings.
type HoldingsFetcher interface {
	GetBrokerHoldings(ctx context.Context, userID string) (map[string]int, error)
}

// DriftEmitter is the fan-out surface. *publisher.DriftPublisher satisfies
// this; tests plug a capturing stub.
type DriftEmitter interface {
	PublishDrift(ctx context.Context, ev *publisher.DriftEvent)
}

// Reconciler is stateless — one instance can be reused across all users.
// Sweep-scoped state (list of users this tick, one user's holdings) lives
// on the stack per RunTicker iteration.
type Reconciler struct {
	store    PositionSource
	users    UsersSource
	holdings HoldingsFetcher
	drift    DriftEmitter
	logger   *zap.Logger
}

// New wires the reconciler for per-user Detect/DetectAndPublish only.
// Enough for tests + one-off invocations. See NewWithTicker for periodic
// sweeps that also need UsersSource + HoldingsFetcher.
func New(src PositionSource, emitter DriftEmitter, logger *zap.Logger) *Reconciler {
	return &Reconciler{store: src, drift: emitter, logger: logger}
}

// NewWithTicker wires the reconciler for periodic sweeps. All four
// dependencies must be non-nil for RunTicker to do anything.
func NewWithTicker(
	src PositionSource,
	users UsersSource,
	holdings HoldingsFetcher,
	emitter DriftEmitter,
	logger *zap.Logger,
) *Reconciler {
	return &Reconciler{store: src, users: users, holdings: holdings, drift: emitter, logger: logger}
}

// RunTicker sweeps every user with ACTIVE positions once per interval,
// fetches broker holdings, and publishes drift events. Blocks until ctx
// is cancelled. Safe to call once — spawns no goroutines of its own.
//
// Per-user error handling:
//   - Auth-lookup or broker-fetch failure ⇒ SKIP the user this tick. We
//     never publish false BROKER_ONLY / DB_ONLY drifts on transient
//     failure — the drift topic must be trustworthy for the ops human.
//   - Store failure listing users ⇒ log + skip the whole tick.
//
// Returns immediately (with a warn) if the three periodic-sweep deps
// weren't wired via NewWithTicker — supports the "enabled but no
// holdings source" boot path in main.go.
func (r *Reconciler) RunTicker(ctx context.Context, interval time.Duration) {
	if r.users == nil || r.holdings == nil {
		if r.logger != nil {
			r.logger.Warn("reconciler.RunTicker: users or holdings source missing — no-op")
		}
		return
	}
	if interval <= 0 {
		interval = 5 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if r.logger != nil {
		r.logger.Info("reconciler ticker started", zap.Duration("interval", interval))
		defer r.logger.Info("reconciler ticker stopped")
	}

	// Kick one sweep immediately so ops sees drift within seconds of boot,
	// not after the first interval.
	r.sweepOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.sweepOnce(ctx)
		}
	}
}

// sweepOnce runs one full drift-detection pass over all users with ACTIVE
// positions. Exposed as a method (not inlined into RunTicker) so tests can
// drive one iteration deterministically.
func (r *Reconciler) sweepOnce(ctx context.Context) {
	users, err := r.users.DistinctUsersWithActive(ctx)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("reconciler sweep: list users failed — skipping tick",
				zap.Error(err))
		}
		return
	}

	var totalDrifts, skippedUsers int
	for _, uid := range users {
		holdings, err := r.holdings.GetBrokerHoldings(ctx, uid)
		if err != nil {
			skippedUsers++
			if r.logger != nil {
				r.logger.Warn("reconciler sweep: broker fetch failed — skipping user",
					zap.String("user_id", uid), zap.Error(err))
			}
			continue
		}
		n, err := r.DetectAndPublish(ctx, uid, holdings)
		if err != nil {
			if r.logger != nil {
				r.logger.Warn("reconciler sweep: DetectAndPublish failed",
					zap.String("user_id", uid), zap.Error(err))
			}
			continue
		}
		totalDrifts += n
	}

	if r.logger != nil {
		r.logger.Info("reconciler sweep complete",
			zap.Int("users_scanned", len(users)),
			zap.Int("users_skipped", skippedUsers),
			zap.Int("total_drifts", totalDrifts))
	}
}

// DetectAndPublish is the main entry point per user. Loads ACTIVE lots,
// groups by symbol, compares against brokerHoldings, publishes one
// DriftEvent per mismatch. Returns the number of drifts emitted.
//
// brokerHoldings is a map[symbol]int — quantity the broker reports for
// this user. Missing symbols are treated as broker qty = 0.
func (r *Reconciler) DetectAndPublish(ctx context.Context, userID string, brokerHoldings map[string]int) (int, error) {
	if r.store == nil {
		return 0, fmt.Errorf("reconciler: store is nil")
	}
	lots, err := r.store.FindAllActiveLotsForUser(ctx, userID)
	if err != nil {
		return 0, fmt.Errorf("reconciler: load active lots: %w", err)
	}
	drifts := Detect(userID, lots, brokerHoldings)

	for i := range drifts {
		if r.drift != nil {
			r.drift.PublishDrift(ctx, &drifts[i])
		}
	}

	if r.logger != nil {
		r.logger.Info("reconciler sweep done",
			zap.String("user_id", userID),
			zap.Int("active_lots", len(lots)),
			zap.Int("broker_symbols", len(brokerHoldings)),
			zap.Int("drifts", len(drifts)))
	}
	return len(drifts), nil
}

// Detect is the pure comparison. Given the user's ACTIVE lots and the
// broker's per-symbol quantities, returns a DriftEvent per mismatch.
//
// Rules:
//   - Symbol only on broker (broker_qty > 0, our_qty = 0): BROKER_ONLY.
//   - Symbol only in our DB (broker_qty = 0, our_qty > 0):  DB_ONLY.
//   - Symbol on both, quantities differ:                    QTY_MISMATCH.
//   - Symbol on both, quantities match:                     NO drift.
//
// A single symbol with multiple ACTIVE lots aggregates them into one
// event. Origin split is included so ops can see whether the drift
// leans MANTHAN or USER_MANUAL.
func Detect(userID string, lots []*store.Position, brokerHoldings map[string]int) []publisher.DriftEvent {
	// Group our-side by symbol.
	type ourSide struct {
		total       int
		manthanQty  int
		userQty     int
		positionIDs []string
	}
	our := map[string]*ourSide{}
	for _, p := range lots {
		if p.Status != "ACTIVE" {
			continue // defense — store filter should have handled
		}
		s := our[p.Symbol]
		if s == nil {
			s = &ourSide{}
			our[p.Symbol] = s
		}
		s.total += p.Quantity
		if p.Origin == store.OriginManthan {
			s.manthanQty += p.Quantity
		} else {
			s.userQty += p.Quantity
		}
		s.positionIDs = append(s.positionIDs, p.PositionID.String())
	}

	var drifts []publisher.DriftEvent

	// 1) Walk our-side — either QTY_MISMATCH or DB_ONLY.
	for symbol, ours := range our {
		brokerQty := brokerHoldings[symbol]
		if brokerQty == ours.total {
			continue // clean
		}
		var driftType string
		if brokerQty == 0 {
			driftType = publisher.DriftDBOnly
		} else {
			driftType = publisher.DriftQtyMismatch
		}
		ev := publisher.DriftEvent{
			UserID:      userID,
			Symbol:      symbol,
			DriftType:   driftType,
			BrokerQty:   brokerQty,
			OurQty:      ours.total,
			PositionIDs: ours.positionIDs,
		}
		if ours.manthanQty > 0 && ours.userQty > 0 {
			ev.ManthanQty = ours.manthanQty
			ev.UserManualQty = ours.userQty
		}
		drifts = append(drifts, ev)
	}

	// 2) Walk broker-side — anything not in `our` is BROKER_ONLY.
	for symbol, brokerQty := range brokerHoldings {
		if brokerQty <= 0 {
			continue // broker "has 0" isn't drift; only positive holdings
		}
		if _, tracked := our[symbol]; tracked {
			continue // already handled above
		}
		drifts = append(drifts, publisher.DriftEvent{
			UserID:    userID,
			Symbol:    symbol,
			DriftType: publisher.DriftBrokerOnly,
			BrokerQty: brokerQty,
			OurQty:    0,
		})
	}

	return drifts
}
