-- 016_pledged_status.sql
--
-- PLEDGED lifecycle state (2026-10-26, operator rule). The client pledged the
-- shares at the broker (margin): the strategy cannot sell them, so it must not
-- place stops or exits for the lot — but the lot is NOT exited: the client
-- still holds it. The api-gateway book reconciler flips the row
-- ACTIVE → PLEDGED when the broker reports the quantity pledged / not free,
-- and PLEDGED → ACTIVE when it is free again.
--
-- rules-engine semantics (types.Position.Occupies / occupyingStatuses):
--   PLEDGED does NOT occupy a book slot or count toward the sector/mcap caps
--   (new signals can be bought), is not trailed, and never emits an exit.
--   Flipping back re-occupies a slot even if that temporarily exceeds the
--   cap (logged).
--
-- The reason text is carried in the existing protective_last_status /
-- protective_last_reason forensics columns (status 'PLEDGED' / 'UNPLEDGED'),
-- so the client overview can render it without a new column.
--
-- Idempotent. Apply OFF-HOURS to trading_db (prod: docker Postgres, host port
-- 5442) with -v ON_ERROR_STOP=1 before deploying the api-gateway / rules-engine
-- builds that write or read the state:
--   psql -h localhost -p 5442 -U postgres -d trading_db -v ON_ERROR_STOP=1 \
--     -f services/rules-engine/migrations/016_pledged_status.sql

ALTER TABLE manthan_positions
    DROP CONSTRAINT IF EXISTS chk_manthan_pos_status;
ALTER TABLE manthan_positions
    ADD CONSTRAINT chk_manthan_pos_status CHECK (status IN (
        'ACTIVE', 'PARTIAL_ACTIVE', 'EXITED', 'COOLDOWN',
        'PENDING_ENTRY', 'EXPIRED',
        -- broker holds the shares pledged: held, not managed, slot free
        'PLEDGED'
    ));

-- Live-row lookups by the reconciler and the rehydrate.
CREATE INDEX IF NOT EXISTS idx_manthan_pos_pledged
    ON manthan_positions (status) WHERE status = 'PLEDGED';
