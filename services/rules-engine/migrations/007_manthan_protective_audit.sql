-- Migration 007: Protective replayer audit trail on manthan_positions
--
-- Adds three columns so the operator (and our /algo-trading/strategies/[id]
-- panel) can answer "did the protective replayer run this morning, and what
-- happened to each position" without diving through Kafka events or logs.
--
-- The `protective_trigger` snapshot column is intentionally OMITTED — the
-- live `current_sl` field already represents the latest TSL trail value
-- and is what the 09:14 cron reads. Adding a separate snapshot would
-- create a divergence risk; we'll surface the EOD trigger via a query
-- helper that just reads `current_sl` at 15:30 IST instead.
--
-- Rollback:
--   ALTER TABLE manthan_positions
--     DROP COLUMN protective_last_attempt,
--     DROP COLUMN protective_last_status,
--     DROP COLUMN protective_last_reason;

BEGIN;

ALTER TABLE manthan_positions
    ADD COLUMN IF NOT EXISTS protective_last_attempt TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS protective_last_status  VARCHAR(30),
    ADD COLUMN IF NOT EXISTS protective_last_reason  TEXT;

-- Helps the strategy detail page query "did protection fire today?" fast.
CREATE INDEX IF NOT EXISTS idx_mp_protective_attempt
    ON manthan_positions (protective_last_attempt DESC)
    WHERE status IN ('ACTIVE', 'PARTIAL_ACTIVE');

COMMENT ON COLUMN manthan_positions.protective_last_attempt IS
    'When the protective replayer last attempted to place an SL/MARKET order for this position at pre-open. NULL = never attempted.';

COMMENT ON COLUMN manthan_positions.protective_last_status IS
    'Result of last attempt: PLACED | FILLED | REJECTED | SKIPPED. PLACED means the order is alive on broker book; SKIPPED means we deliberately didn''t fire (freeQty=0, no auth, etc.).';

COMMENT ON COLUMN manthan_positions.protective_last_reason IS
    'Human-readable detail. For SKIPPED: why we skipped (e.g. "freeQty=0 < required=1 — CDSL/TPIN auth pending"). For REJECTED: granular broker rejReason from Order Trail API.';

COMMIT;
