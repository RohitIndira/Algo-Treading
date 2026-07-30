-- Drop the 13 news-specific fields from strategy_conditions.
--
-- Context: MANTHAN is the only live strategy type as of 2026-07-20; it uses
-- none of these filter fields. All rows had them NULL/zero in prod. The
-- rules-engine already stopped consuming them on 2026-06-25 ("Cat B trim").
-- user-config, api-gateway, and proto were the last places still writing +
-- validating them.
--
-- Table itself is KEPT (one row per strategy, holds condition_id + strategy_id
-- + created_at) because the models.StrategyCondition struct still exists as a
-- placeholder and the outbox event schema still references it. Dropping the
-- table is a separate follow-up ("Option B" per the cleanup design).
--
-- ROLLBACK: to restore, run
--   ALTER TABLE strategy_conditions
--       ADD COLUMN match_all_news BOOLEAN DEFAULT FALSE,
--       ADD COLUMN impact_score_min INTEGER DEFAULT 0,
--       ...
-- The rows were empty in prod, so nothing is lost — recreating the columns
-- is enough.

ALTER TABLE strategy_conditions
    DROP COLUMN IF EXISTS match_all_news,
    DROP COLUMN IF EXISTS impact_score_min,
    DROP COLUMN IF EXISTS impact_score_max,
    DROP COLUMN IF EXISTS sentiments,
    DROP COLUMN IF EXISTS news_categories,
    DROP COLUMN IF EXISTS stock_codes,
    DROP COLUMN IF EXISTS min_market_cap,
    DROP COLUMN IF EXISTS max_market_cap,
    DROP COLUMN IF EXISTS market_cap_types,
    DROP COLUMN IF EXISTS min_price_change_pct,
    DROP COLUMN IF EXISTS max_price_change_pct,
    DROP COLUMN IF EXISTS min_volume,
    DROP COLUMN IF EXISTS exchanges;
