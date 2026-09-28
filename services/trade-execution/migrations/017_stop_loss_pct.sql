-- 017: per-order stop-loss percent (2026-09-28).
--
-- WHY: the stop distance is now bucket-aware (LARGE-cap trails at 10%,
-- MID/SMALL at 20%). rules-engine stamps the EFFECTIVE pct on each
-- trade-signal; trade-execution persists it here so later SL work
-- (safety-monitor re-placement, naked coverage) uses the position's own
-- distance instead of a hardcoded 20%. NULL = legacy row → code treats
-- as 20 (the only distance that existed before this migration).
ALTER TABLE manthan_orders
    ADD COLUMN IF NOT EXISTS stop_loss_pct NUMERIC(5,2);
