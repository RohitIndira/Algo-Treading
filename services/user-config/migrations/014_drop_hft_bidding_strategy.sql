-- 014_drop_hft_bidding_strategy.sql
--
-- Reverses migration 013 in full. hft-engine + api/proto/hft_engine were
-- deleted 2026-07-14 in commit a17884b. The strategy_type='HFT_BIDDING'
-- rows are now orphaned (no engine consumes them), so we delete them
-- along with the HFT-specific schema pieces.
--
-- Order matters (in one transaction):
--   1. Delete orphaned HFT trade_configs first (FK-safe if any exists —
--      the strategy row still has strategy_id referenced from tc.strategy_id
--      per the 011/012 schema).
--   2. Delete orphaned HFT strategy rows.
--   3. Drop the partial index that only made sense for HFT.
--   4. Drop the JSONB config_extra column that 013 added.
--   5. Tighten the strategies.strategy_type CHECK back to the 3 remaining
--      strategy types.
--
-- Idempotent: uses IF EXISTS everywhere; safe to re-run.
--
-- STAGING: apply during the next deploy window. If staging holds live
-- HFT strategies (unlikely — hft-engine was frozen since 2026-06-24
-- per the phase5 role audit), export them first via
--   SELECT strategy_id, user_id, strategy_name, config_extra
--   FROM strategies s
--   JOIN trade_configs tc USING (strategy_id)
--   WHERE s.strategy_type = 'HFT_BIDDING';
-- so the config isn't lost if the decision to remove HFT is reversed.

BEGIN;

-- Step 1: kill orphaned trade_configs for HFT strategies
DELETE FROM trade_configs
 WHERE strategy_id IN (
     SELECT strategy_id FROM strategies WHERE strategy_type = 'HFT_BIDDING'
 );

-- Step 2: kill orphaned strategy rows
DELETE FROM strategies WHERE strategy_type = 'HFT_BIDDING';

-- Step 3: drop the HFT-only partial index
DROP INDEX IF EXISTS idx_strategies_hft_active;

-- Step 4: drop the HFT-only JSONB column (was NULL on every non-HFT row)
ALTER TABLE trade_configs DROP COLUMN IF EXISTS config_extra;

-- Step 5: tighten the CHECK constraint back to the 3 remaining strategy types
ALTER TABLE strategies DROP CONSTRAINT IF EXISTS strategies_strategy_type_check;
ALTER TABLE strategies ADD CONSTRAINT strategies_strategy_type_check
    CHECK (strategy_type IN ('NEWS', '52W_BREAKOUT', 'MANTHAN'));

COMMIT;
