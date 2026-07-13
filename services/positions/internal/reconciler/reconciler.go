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

	"github.com/RohitIndira/Algo-Treading/services/positions/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/positions/internal/store"

	"go.uber.org/zap"
)

// PositionSource is what the reconciler needs from the store — abstracting
// this as an interface keeps tests hermetic.
type PositionSource interface {
	FindAllActiveLotsForUser(ctx context.Context, userID string) ([]*store.Position, error)
}

// DriftEmitter is the fan-out surface. *publisher.DriftPublisher satisfies
// this; tests plug a capturing stub.
type DriftEmitter interface {
	PublishDrift(ctx context.Context, ev *publisher.DriftEvent)
}

// Reconciler is stateless — one instance can be reused across all users.
type Reconciler struct {
	store  PositionSource
	drift  DriftEmitter
	logger *zap.Logger
}

// New wires the reconciler. Both dependencies may be nil for smoke tests
// that only exercise Detect() (the pure function).
func New(src PositionSource, emitter DriftEmitter, logger *zap.Logger) *Reconciler {
	return &Reconciler{store: src, drift: emitter, logger: logger}
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
