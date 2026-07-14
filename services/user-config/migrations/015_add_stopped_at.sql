-- STOP is a terminal status transition, NOT a soft-delete.
--
-- Before: DELETE /strategies/{id} set deleted_at = NOW() and every
--         read filtered `deleted_at IS NULL`, so a stopped strategy
--         vanished from the UI entirely.
-- After:  DELETE sets stopped_at = NOW() (deleted_at stays null); the
--         row keeps showing in reads with status=STOPPED so the user
--         can see history. RESUME rejects any row with stopped_at set
--         (stop is terminal — user redeploys to run again).
--
-- deleted_at is deliberately untouched — it stays as an escape hatch
-- for "actually purge from listings" operations (admin/compliance).

ALTER TABLE strategies
    ADD COLUMN IF NOT EXISTS stopped_at TIMESTAMPTZ DEFAULT NULL;

CREATE INDEX IF NOT EXISTS idx_strategies_stopped_at
    ON strategies(stopped_at);
