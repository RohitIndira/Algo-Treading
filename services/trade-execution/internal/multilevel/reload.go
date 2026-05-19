package multilevel

import (
	"context"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// ReloadStats summarises the outcome of a startup reload pass.
type ReloadStats struct {
	EntriesScanned   int
	GroupsRestored   int
	GroupsSkipped    int // entry already present in memory, or unrecoverable state
	LevelsRestored   int
	WarnSingleSLLost int // FIXED/TRAILING live groups whose broker SL ID is unrecoverable
}

// Reload reconstructs in-memory Group state for every entry order that still
// has at least one PENDING/ACTIVE multi-level exit level in the database.
//
// Why this matters: without Reload, after a service restart price ticks and
// broker WS events for in-progress ML positions have no in-memory group to
// route to, so SL/TP levels effectively stop being managed. This method
// rebuilds the minimum state needed for the existing event paths to work:
//
//   - groupsByEntry: so HandleBrokerUpdate / CancelGroup / OnEntryFill find it
//   - shard subscription: so OnPriceUpdate enqueues eval jobs
//   - mlLevelIndex: so broker WS EXECUTED callbacks for per-level orders route correctly
//
// Limitations (logged, not failed):
//   - For SLMode=FIXED/TRAILING live groups, the SingleSLBrokerID is in-memory
//     only — the broker SL order still exists at Indira but we can't drive
//     trailing updates against it. Reported via WarnSingleSLLost so operators
//     can verify those positions out of band.
//   - This method does not place any new broker orders. It only rebuilds
//     in-memory routing state so EXISTING broker orders' fills are processed
//     correctly when they arrive.
//
// Idempotent: if a group is already registered (e.g. because RegisterEntry has
// already run for it in this process), it is skipped.
func (m *Manager) Reload(ctx context.Context) (*ReloadStats, error) {
	stats := &ReloadStats{}

	entries, err := m.repo.GetActiveMLEntries(ctx)
	if err != nil {
		return stats, fmt.Errorf("fetch active ML entries: %w", err)
	}
	stats.EntriesScanned = len(entries)
	if len(entries) == 0 {
		return stats, nil
	}

	entryIDs := make([]uuid.UUID, 0, len(entries))
	for _, e := range entries {
		entryIDs = append(entryIDs, e.OrderID)
	}
	// One batch query for all entries' levels keeps the reload O(1) DB calls.
	levelsByEntry, err := m.repo.GetMultiLevelExitLevelsBatch(ctx, entryIDs)
	if err != nil {
		return stats, fmt.Errorf("fetch ML levels batch: %w", err)
	}

	for _, entry := range entries {
		// Skip if a group is already in memory — RegisterEntry may have already
		// fired for this entry in this process (e.g. signal arrived between the
		// reload query and now).
		if _, exists := m.groupsByEntry.Load(entry.OrderID); exists {
			stats.GroupsSkipped++
			continue
		}

		levels := levelsByEntry[entry.OrderID]
		if len(levels) == 0 {
			// Race: query said active but no levels found now (e.g. all just
			// triggered). Skip safely.
			stats.GroupsSkipped++
			continue
		}

		g, levelCount, slLost := m.buildGroupFromDB(entry, levels)
		if g == nil {
			stats.GroupsSkipped++
			continue
		}
		m.groupsByEntry.Store(entry.OrderID, g)
		m.subscribeGroup(g)

		stats.GroupsRestored++
		stats.LevelsRestored += levelCount
		if slLost {
			stats.WarnSingleSLLost++
			m.logger.Warn("multilevel reload: single SL broker ID not recoverable for FIXED/TRAILING group; broker-side SL order remains active at Indira, but trailing updates from this service are paused until the position closes",
				zap.String("entry_order_id", entry.OrderID.String()),
				zap.String("symbol", entry.Symbol),
				zap.String("sl_mode", g.SLMode))
		}
	}

	m.logger.Info("multilevel reload complete",
		zap.Int("entries_scanned", stats.EntriesScanned),
		zap.Int("groups_restored", stats.GroupsRestored),
		zap.Int("groups_skipped", stats.GroupsSkipped),
		zap.Int("levels_restored", stats.LevelsRestored),
		zap.Int("warn_single_sl_lost", stats.WarnSingleSLLost))
	return stats, nil
}

// buildGroupFromDB reconstructs a Group from one entry order row + its level
// rows, mirroring the post-OnEntryFill in-memory layout. Returns nil if the
// row is unusable (e.g. missing fill data).
//
// Returns (group, restoredLevelCount, singleSLBrokerIDLost).
func (m *Manager) buildGroupFromDB(entry *models.Order, levels []*models.MultiLevelExitRecord) (*Group, int, bool) {
	if entry.FilledQuantity <= 0 || entry.FilledPrice == nil {
		// Entry not actually filled — can't reconstruct.
		return nil, 0, false
	}

	// Auth is per-user; we don't have a fresh bearer token at reload time. The
	// status service holds the latest auth, but Reload does NOT place any new
	// broker orders — it only restores routing state — so a nil Auth here is
	// acceptable. If downstream code later needs auth (e.g. when cancelling an
	// exit order during force-exit), it can fetch from the credentials repo.
	g := &Group{
		GroupID:      uuid.New(),
		EntryOrderID: entry.OrderID,
		UserID:       entry.UserID,
		Symbol:       entry.Symbol,
		Exchange:     string(entry.Exchange),
		StockCode:    entry.StockCode,
		OrderSide:    string(entry.OrderSide),
		ProductType:  entry.ProductType,
		Validity:     entry.Validity,
		FillPrice:    *entry.FilledPrice,
		TotalQty:     entry.FilledQuantity,
		RemainingQty: entry.FilledQuantity,
		State:        GroupStateActive,
		TradingMode:  entry.TradingMode,
		broker:       m.broker,
		StrategyID:   entry.StrategyID,
		StrategyName: entry.StrategyName,
		EventID:      entry.EventID,
		entryFilled:  true,
		HighestPrice: *entry.FilledPrice,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Infer SLMode / TPMode from the levels present.
	sawSL := false
	sawTP := false
	restored := 0
	for _, l := range levels {
		switch l.ExitType {
		case models.MLExitTypeSL:
			sawSL = true
		case models.MLExitTypeTP:
			sawTP = true
		}
	}
	if sawSL {
		g.SLMode = SLModeMultiLevel
	}
	if sawTP {
		g.TPMode = TPModeMultiLevel
	}

	for _, l := range levels {
		state := levelStateFromRecord(l)
		switch l.ExitType {
		case models.MLExitTypeSL:
			g.SLLevels = append(g.SLLevels, state)
			g.SLLevelConfigs = append(g.SLLevelConfigs, SLTPLevelConfig{
				LevelNum: l.LevelNum,
				PricePct: l.PricePct,
				QtyPct:   l.QtyPct,
			})
		case models.MLExitTypeTP:
			g.TPLevels = append(g.TPLevels, state)
			g.TPLevelConfigs = append(g.TPLevelConfigs, SLTPLevelConfig{
				LevelNum: l.LevelNum,
				PricePct: l.PricePct,
				QtyPct:   l.QtyPct,
			})
		}
		// Subtract triggered qty from RemainingQty so subsequent SL/TP eval is correct.
		if l.Status == models.MLStatusTriggered && l.ExitQty != nil {
			if *l.ExitQty <= g.RemainingQty {
				g.RemainingQty -= *l.ExitQty
			}
		}
		// Re-index live broker IDs so EXECUTED callbacks route correctly.
		if l.BrokerOrderID != nil && *l.BrokerOrderID != "" {
			m.mlLevelIndex.Store(*l.BrokerOrderID, &mlLevelRef{
				group:    g,
				exitType: l.ExitType,
				levelNum: l.LevelNum,
			})
		}
		restored++
	}

	// If a live group has no SL level rows it was running FIXED or TRAILING SL;
	// the single broker SL order ID is not persisted, so trailing updates from
	// this service cannot resume. Mark the loss so operators can be notified.
	singleSLLost := !sawSL && g.TradingMode == "LIVE"
	if singleSLLost {
		// Default to FIXED — without TrailingSLPct (also in-memory only) we
		// cannot drive trailing logic. The broker-side SL order remains.
		g.SLMode = SLModeFixed
	}
	return g, restored, singleSLLost
}

// levelStateFromRecord builds an ExitLevelState mirroring the DB row's state.
func levelStateFromRecord(r *models.MultiLevelExitRecord) *ExitLevelState {
	st := &ExitLevelState{
		LevelNum: r.LevelNum,
	}
	if r.TriggerPrice != nil {
		st.TriggerPrice = *r.TriggerPrice
	}
	if r.ExitQty != nil {
		st.ExitQty = *r.ExitQty
	}
	if r.OriginalExitQty != nil {
		st.OriginalExitQty = *r.OriginalExitQty
	} else if r.ExitQty != nil {
		st.OriginalExitQty = *r.ExitQty
	}
	if r.ExitOrderID != nil {
		st.ExitOrderID = *r.ExitOrderID
	}
	if r.BrokerOrderID != nil {
		st.BrokerOrderID = *r.BrokerOrderID
	}
	st.TriggeredAt = r.TriggeredAt
	if r.ExitPrice != nil {
		st.ExitPrice = *r.ExitPrice
	}
	// Map DB status → in-memory LevelStatus.
	switch r.Status {
	case models.MLStatusPending:
		st.Status = LevelPending
	case models.MLStatusActive:
		st.Status = LevelActive
	case models.MLStatusTriggered:
		st.Status = LevelTriggered
	case models.MLStatusCancelled:
		st.Status = LevelCancelled
	default:
		st.Status = LevelPending
	}
	return st
}
