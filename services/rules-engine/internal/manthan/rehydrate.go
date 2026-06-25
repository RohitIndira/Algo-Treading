package manthan

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

// StrategyAliveChecker is the subset of user-config gRPC we need for
// orphan detection. Kept as a tiny interface so:
//   - manthan package doesn't import the startup package (avoids cycles)
//   - tests can pass a fake without standing up a gRPC server
//
// The real implementation lives at
// services/rules-engine/internal/startup/userconfig_client.go and is
// satisfied by *startup.UserConfigClient.
//
// Contract — see UserConfigClient.IsStrategyAlive godoc:
//   - (true,  nil)        strategy is alive  → keep position
//   - (false, nil)        explicit NOT_FOUND → orphan, EXIT
//   - (false, non-nil)    uncertain          → caller MUST skip
type StrategyAliveChecker interface {
	IsStrategyAlive(ctx context.Context, userID, strategyID string) (bool, error)
}

// RehydrateActivePositions restores in-memory portfolio state from the
// authoritative source (Postgres `manthan_positions` WHERE status='ACTIVE')
// after a rules-engine restart.
//
// Side effects performed in one pass so DB, memory, and Redis stay in sync:
//
//  1. For each ACTIVE row whose strategy is loaded → populate in-memory
//     portfolio and upsert the Redis `manthan:position:{strategyID}:{symbol}`
//     cache key.
//  2. For ACTIVE rows whose strategy is orphaned (soft-deleted or missing)
//     → mark DB row as EXITED with reason='ORPHAN_CLEANUP', delete Redis key,
//     and remove from `manthan:positions:active` set. Broker SL cancellation
//     is handled separately by trade-execution's safety monitor.
//  3. Evict any `manthan:position:*` Redis key that was NOT seen in step 1
//     (stale cache from a previous run).
//
// strategyByID resolves a strategyID to its UserStrategy; nil return means
// orphan. Pass a closure bound to the rules-engine ConfigStore.
//
// Returns (restored, orphansCleaned, redisStaleEvicted, err).
//
// aliveChecker is consulted to confirm soft-deletion before EXITing a
// position whose strategy is missing from the configstore. Pass a non-nil
// *startup.UserConfigClient. If nil, the orphan path is disabled (rehydrate
// still works for the "strategy is loaded" path).
func (pm *PortfolioManager) RehydrateActivePositions(
	ctx context.Context,
	db *sql.DB,
	rdb *redis.Client,
	aliveChecker StrategyAliveChecker,
	strategyByID func(string) *UserStrategy,
) (int, int, int, error) {
	if db == nil {
		return 0, 0, 0, nil
	}

	rows, err := db.QueryContext(ctx, `
		SELECT strategy_id, user_id, symbol,
		       COALESCE(isin, ''),
		       COALESCE(industry, ''),
		       COALESCE(mcap_bucket, ''),
		       COALESCE(index_name, ''),
		       entry_price, quantity,
		       COALESCE(invested_amt, entry_price * quantity),
		       COALESCE(ema_alloc_pct, 0),
		       COALESCE(high_since_entry, entry_price),
		       current_sl,
		       COALESCE(last_trail_level, entry_price)
		FROM manthan_positions
		WHERE status = 'ACTIVE'`)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("query active positions: %w", err)
	}
	defer rows.Close()

	seenRedisKeys := make(map[string]struct{})
	restored := 0
	orphans := 0

	for rows.Next() {
		var strategyID, userID, symbol, isin, industry, mcapBucket, indexName string
		var entryPrice, invested, emaPct, high, sl, lastTrail float64
		var qty int32
		if err := rows.Scan(
			&strategyID, &userID, &symbol, &isin, &industry, &mcapBucket, &indexName,
			&entryPrice, &qty, &invested, &emaPct, &high, &sl, &lastTrail,
		); err != nil {
			pm.logger.Warn("Rehydrate: scan failed", zap.Error(err))
			continue
		}

		posKey := fmt.Sprintf("manthan:position:%s:%s", strategyID, symbol)
		setMember := fmt.Sprintf("%s:%s", strategyID, symbol)

		strategy := strategyByID(strategyID)
		if strategy == nil {
			// Configstore doesn't show this strategy as Active. Two causes:
			//   1. Genuine orphan — strategy was soft-deleted while the
			//      position was ACTIVE. Position should be cleaned up.
			//   2. Startup race — the config consumer is still replaying
			//      `user-config-events` from FirstOffset. The strategy is
			//      transiently in Paused (after a DEACTIVATED event, before
			//      the matching ACTIVATED catches up). Cleaning up here
			//      would WRONGLY mark a live position as orphan. This bit
			//      us 2026-05-11: three real positions got EXITED in DB
			//      because the configstore showed the strategy as
			//      not-Active for ~500ms during the replay window.
			//
			// Resolution: ask user-config via gRPC — it's the single owner
			// of the strategies table (see docs/architecture/data-ownership.md).
			// Same defensive posture as the legacy SQL check: an uncertain
			// answer (network failure, unknown error) skips silently — we
			// NEVER EXIT a real position on a flaky lookup.
			if aliveChecker == nil {
				pm.logger.Warn("Rehydrate: no strategy alive-checker wired — skipping orphan path",
					zap.String("strategy_id", strategyID), zap.String("symbol", symbol))
				continue
			}
			alive, rpcErr := aliveChecker.IsStrategyAlive(ctx, userID, strategyID)
			if rpcErr != nil {
				pm.logger.Warn("Rehydrate: strategies gRPC lookup failed — skipping (no orphan cleanup, no restore)",
					zap.String("strategy_id", strategyID),
					zap.String("symbol", symbol),
					zap.Error(rpcErr))
				continue
			}
			if alive {
				// Strategy is alive in DB; configstore just hasn't caught up.
				// Don't restore in-memory (the config consumer will populate
				// the configstore shortly), don't touch DB. The orphan
				// scanner reruns every 60s and will retry once the race
				// window has closed.
				pm.logger.Warn("Rehydrate: strategy not yet in configstore but alive in DB — skipping (config-consumer replay race)",
					zap.String("strategy_id", strategyID),
					zap.String("symbol", symbol))
				continue
			}

			// Genuine orphan — strategy is soft-deleted in DB.
			if _, err := db.ExecContext(ctx, `
				UPDATE manthan_positions
				SET status = 'EXITED', exit_reason = 'ORPHAN_CLEANUP',
				    exit_time = NOW(), updated_at = NOW()
				WHERE strategy_id = $1 AND symbol = $2 AND status = 'ACTIVE'`,
				strategyID, symbol); err != nil {
				pm.logger.Warn("Rehydrate: orphan DB update failed",
					zap.String("strategy_id", strategyID),
					zap.String("symbol", symbol),
					zap.Error(err))
			}
			if rdb != nil {
				if err := rdb.Del(ctx, posKey).Err(); err != nil {
					pm.logger.Warn("Rehydrate: Redis Del failed", zap.String("key", posKey), zap.Error(err))
				}
				if err := rdb.SRem(ctx, "manthan:positions:active", setMember).Err(); err != nil {
					pm.logger.Warn("Rehydrate: Redis SRem failed", zap.String("member", setMember), zap.Error(err))
				}
			}
			orphans++
			pm.logger.Warn("Rehydrate: orphan position cleaned up (strategy soft-deleted in DB)",
				zap.String("strategy_id", strategyID),
				zap.String("symbol", symbol))
			continue
		}

		// Live position: restore in-memory portfolio + upsert Redis cache.
		// The previous implementation took pm.mu (outer) — wrong; that lock
		// only protects the outer portfolios map. Inner Positions writes
		// must take the per-Portfolio Mu so concurrent LTPFeed poll +
		// allocator + fill consumer don't trip the concurrent-map panic.
		p := pm.GetOrCreate(*strategy)
		p.Mu.Lock()
		p.Positions[symbol] = &Position{
			Symbol:         symbol,
			ISIN:           isin,
			Industry:       industry,
			MCapBucket:     mcapBucket,
			IndexName:      indexName,
			EntryPrice:     entryPrice,
			EntryTime:      time.Now(), // true entry_time isn't in schema; use now as best-effort
			Quantity:       qty,
			InvestedAmt:    invested,
			HighSinceEntry: high,
			CurrentSL:      sl,
			LastTrailLevel: lastTrail,
			State:          StateActive,
			Active:         true,
		}
		p.Mu.Unlock()

		if rdb != nil {
			payload, _ := json.Marshal(map[string]any{
				"symbol":       symbol,
				"entry_price":  entryPrice,
				"quantity":     qty,
				"invested":     invested,
				"stop_loss":    sl,
				"high":         high,
				"status":       "ACTIVE",
				"entry_time":   time.Now().Format(time.RFC3339),
				"industry":     industry,
				"mcap_bucket":  mcapBucket,
				"ema_pct":      emaPct * 100,
				"trading_mode": strategy.TradingMode,
			})
			if err := rdb.Set(ctx, posKey, payload, 30*24*time.Hour).Err(); err != nil {
				pm.logger.Warn("Rehydrate: Redis Set failed", zap.String("key", posKey), zap.Error(err))
			}
			if err := rdb.SAdd(ctx, "manthan:positions:active", setMember).Err(); err != nil {
				pm.logger.Warn("Rehydrate: Redis SAdd failed", zap.String("member", setMember), zap.Error(err))
			}
			seenRedisKeys[posKey] = struct{}{}
		}

		restored++
		pm.logger.Info("Position rehydrated",
			zap.String("strategy_id", strategyID),
			zap.String("symbol", symbol),
			zap.Int32("qty", qty),
			zap.Float64("entry", entryPrice),
			zap.Float64("sl", sl),
			zap.Float64("high", high),
			zap.Float64("last_trail", lastTrail))
	}
	if err := rows.Err(); err != nil {
		return restored, orphans, 0, fmt.Errorf("iterate rows: %w", err)
	}

	// Evict stale Redis keys that have no corresponding ACTIVE DB row.
	staleEvicted := 0
	if rdb != nil {
		var cursor uint64
		for {
			keys, next, err := rdb.Scan(ctx, cursor, "manthan:position:*", 100).Result()
			if err != nil {
				pm.logger.Warn("Rehydrate: Redis scan failed", zap.Error(err))
				break
			}
			for _, k := range keys {
				if _, ok := seenRedisKeys[k]; ok {
					continue
				}
				if err := rdb.Del(ctx, k).Err(); err != nil {
					pm.logger.Warn("Rehydrate: stale Redis Del failed", zap.String("key", k), zap.Error(err))
					continue
				}
				staleEvicted++
			}
			if next == 0 {
				break
			}
			cursor = next
		}
	}

	pm.logger.Info("Rehydrate complete",
		zap.Int("restored", restored),
		zap.Int("orphans_cleaned", orphans),
		zap.Int("redis_stale_evicted", staleEvicted))

	return restored, orphans, staleEvicted, nil
}

// CleanupOrphans scans `manthan_positions` for ACTIVE rows whose strategy is
// no longer loaded (soft-deleted via strategies.deleted_at IS NOT NULL) and:
//
//  1. Marks the row EXITED with reason='ORPHAN_CLEANUP'
//  2. Evicts the Redis cache key + removes from the active set
//
// It does NOT cancel the broker SL or sell the position — that's intentional.
// A strategy soft-delete by a user doesn't necessarily mean they want the
// live position auto-closed (could be accidental deletion, renaming, etc.).
// The broker SL, if present, stays active so the position remains protected.
// If the user truly wants to exit, they do it explicitly via the UI.
//
// Designed to be called on a timer (every 60s) — fast and cheap (just one
// indexed query + a few updates per orphan).
func (pm *PortfolioManager) CleanupOrphans(
	ctx context.Context,
	db *sql.DB,
	rdb *redis.Client,
	aliveChecker StrategyAliveChecker,
	strategyByID func(string) *UserStrategy,
) (int, error) {
	if db == nil {
		return 0, nil
	}

	// user_id is required by the gRPC alive-checker (user-scoped authz).
	// Pulled here so we have it without a second per-row lookup.
	rows, err := db.QueryContext(ctx, `
		SELECT strategy_id, user_id, symbol
		FROM manthan_positions
		WHERE status = 'ACTIVE'`)
	if err != nil {
		return 0, fmt.Errorf("query active positions: %w", err)
	}
	type rowKey struct{ strategyID, userID, symbol string }
	var actives []rowKey
	for rows.Next() {
		var sid, uid, sym string
		if err := rows.Scan(&sid, &uid, &sym); err != nil {
			rows.Close()
			return 0, err
		}
		actives = append(actives, rowKey{sid, uid, sym})
	}
	rows.Close()

	cleaned := 0
	for _, rk := range actives {
		if strategyByID(rk.strategyID) != nil {
			continue // strategy still loaded — not an orphan
		}
		// Configstore-miss alone isn't enough to destroy a position. Confirm
		// with user-config via gRPC before EXITing. Same defensive posture
		// as the boot-time rehydrate — uncertain answer skips silently.
		if aliveChecker == nil {
			pm.logger.Warn("Orphan scanner: no alive-checker wired — skipping",
				zap.String("strategy_id", rk.strategyID), zap.String("symbol", rk.symbol))
			continue
		}
		alive, rpcErr := aliveChecker.IsStrategyAlive(ctx, rk.userID, rk.strategyID)
		if rpcErr != nil {
			pm.logger.Warn("Orphan scanner: gRPC lookup failed — skipping",
				zap.String("strategy_id", rk.strategyID),
				zap.String("symbol", rk.symbol), zap.Error(rpcErr))
			continue
		}
		if alive {
			// Configstore lag (e.g. operator-initiated pause that the
			// scanner observed in a transient window). Skip silently.
			continue
		}

		// Genuine orphan — also drop from in-memory portfolio if it was
		// rehydrated earlier. pm.mu (outer) only protects the portfolios
		// map; the inner Positions delete must take per-Portfolio Mu, same
		// pattern as the rest of PortfolioManager since 2026-06-25
		// (commit 73d418d). Race window had been open while the orphan
		// scanner overlapped the LTP poll on the same map.
		pm.mu.RLock()
		p, ok := pm.portfolios[rk.strategyID]
		pm.mu.RUnlock()
		if ok {
			p.Mu.Lock()
			delete(p.Positions, rk.symbol)
			p.Mu.Unlock()
		}

		if _, err := db.ExecContext(ctx, `
			UPDATE manthan_positions
			SET status = 'EXITED', exit_reason = 'ORPHAN_CLEANUP',
			    exit_time = NOW(), updated_at = NOW()
			WHERE strategy_id = $1 AND symbol = $2 AND status = 'ACTIVE'`,
			rk.strategyID, rk.symbol); err != nil {
			pm.logger.Warn("Orphan cleanup: DB update failed",
				zap.String("strategy_id", rk.strategyID),
				zap.String("symbol", rk.symbol), zap.Error(err))
			continue
		}
		if rdb != nil {
			posKey := fmt.Sprintf("manthan:position:%s:%s", rk.strategyID, rk.symbol)
			setMember := fmt.Sprintf("%s:%s", rk.strategyID, rk.symbol)
			if err := rdb.Del(ctx, posKey).Err(); err != nil {
				pm.logger.Warn("Orphan cleanup: Redis Del failed", zap.String("key", posKey), zap.Error(err))
			}
			if err := rdb.SRem(ctx, "manthan:positions:active", setMember).Err(); err != nil {
				pm.logger.Warn("Orphan cleanup: Redis SRem failed", zap.String("member", setMember), zap.Error(err))
			}
		}
		cleaned++
		pm.logger.Warn("Orphan position cleaned up (strategy soft-deleted in DB)",
			zap.String("strategy_id", rk.strategyID),
			zap.String("symbol", rk.symbol))
	}
	return cleaned, nil
}

// isStrategySoftDeleted was removed 2026-06-23 — the orphan scanner now
// asks user-config via gRPC (see StrategyAliveChecker at the top of this
// file). user-config is the single owner of the strategies table per
// docs/architecture/data-ownership.md.

// StartOrphanScanner runs CleanupOrphans on a ticker. Blocks until ctx
// cancelled. Intended to be launched once from main() in a goroutine.
func (pm *PortfolioManager) StartOrphanScanner(
	ctx context.Context,
	db *sql.DB,
	rdb *redis.Client,
	aliveChecker StrategyAliveChecker,
	strategyByID func(string) *UserStrategy,
	interval time.Duration,
) {
	if interval <= 0 {
		interval = 60 * time.Second
	}
	pm.logger.Info("Orphan scanner started", zap.Duration("interval", interval))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			pm.logger.Info("Orphan scanner stopped")
			return
		case <-ticker.C:
			if cleaned, err := pm.CleanupOrphans(ctx, db, rdb, aliveChecker, strategyByID); err != nil {
				pm.logger.Warn("Orphan scanner: cleanup pass failed", zap.Error(err))
			} else if cleaned > 0 {
				pm.logger.Info("Orphan scanner pass", zap.Int("cleaned", cleaned))
			}
		}
	}
}
