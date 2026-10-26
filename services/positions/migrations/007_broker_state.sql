-- 007: broker verdict per position + the NOT_HELD_AT_BROKER exit reason.
--
-- Book-vs-broker reconciliation (2026-10-26, operator rule): every Manthan lot
-- is compared with the client's live broker holdings — on login and on the
-- 15-minute intraday sweep — and the latest verdict is kept ON the row so the
-- admin console and the client app can show the same plain-language reason.
--
--   broker_state           HELD | PLEDGED | NOT_HELD | SOLD_TODAY | QTY_MISMATCH | UNVERIFIED
--   broker_state_reason    plain language ("Pledged: 2850 of 2850 pledged, 0 free —
--                          unpledge to let the strategy manage it")
--   broker_state_evidence  the broker facts the verdict rests on (qty / free /
--                          pledged / open SELL id / tradebook qty+price)
--   broker_checked_at      when the broker was last read for this row
--   broker_state_source    login | sweep | manual
--
-- A PLEDGED lot stays status=ACTIVE (it IS held) — the strategy slot is
-- released on the rules-engine side (manthan_positions.status='PLEDGED').
--
-- NOT_HELD_AT_BROKER: automated closure of a lot the broker no longer holds
-- (sold or transferred outside the system while no session was available);
-- exit_price stays NULL (unknown). The manual Postman heal keeps
-- ADMIN_GHOST_CLEANUP; a lot the tradebook shows sold today by hand is
-- closed as MANUAL_EXIT at the trade price.
--
-- History: every verdict change and every automated action is written to
-- trading_db.admin_audit (action BOOK_RECONCILE / GHOST_REAPER) by the
-- api-gateway reconciler; this table holds only the LATEST verdict per row.
--
-- Idempotent. Apply to positions_db:
--   psql -h localhost -p 5442 -U postgres -d positions_db -v ON_ERROR_STOP=1 \
--     -f services/positions/migrations/007_broker_state.sql

BEGIN;

ALTER TABLE positions ADD COLUMN IF NOT EXISTS broker_state          VARCHAR(16);
ALTER TABLE positions ADD COLUMN IF NOT EXISTS broker_state_reason   TEXT;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS broker_state_evidence JSONB;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS broker_checked_at     TIMESTAMPTZ;
ALTER TABLE positions ADD COLUMN IF NOT EXISTS broker_state_source   VARCHAR(8);
-- broker_state_stale: the broker could not be read on the latest run (dead
-- session / partial read) — broker_state keeps the LAST TRUTH and this flag
-- says it is not fresh (broker_checked_at is the last truthful read).
ALTER TABLE positions ADD COLUMN IF NOT EXISTS broker_state_stale    BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE positions DROP CONSTRAINT IF EXISTS chk_positions_broker_state;
ALTER TABLE positions ADD CONSTRAINT chk_positions_broker_state
    CHECK (broker_state IS NULL OR broker_state IN
        ('HELD','PLEDGED','NOT_HELD','SOLD_TODAY','QTY_MISMATCH','UNVERIFIED'));

ALTER TABLE positions DROP CONSTRAINT IF EXISTS chk_positions_exit_reason;
ALTER TABLE positions ADD CONSTRAINT chk_positions_exit_reason
    CHECK (exit_reason IS NULL OR exit_reason IN
        ('SL_TRIGGER','MANUAL_EXIT','STRATEGY_EXIT','LIQUIDATION',
         'ADMIN_GHOST_CLEANUP','NOT_HELD_AT_BROKER'));

-- The attention queue lists non-HELD verdicts on the open book.
CREATE INDEX IF NOT EXISTS idx_positions_broker_state
    ON positions (broker_state) WHERE status = 'ACTIVE' AND broker_state IS NOT NULL;

COMMENT ON COLUMN positions.broker_state IS
    'Latest broker-vs-book verdict: HELD | PLEDGED | NOT_HELD | SOLD_TODAY | QTY_MISMATCH | UNVERIFIED (api-gateway book reconciler).';
COMMENT ON COLUMN positions.broker_state_reason IS
    'Plain-language reason for broker_state, shown verbatim in the admin console and the client app.';

COMMIT;
