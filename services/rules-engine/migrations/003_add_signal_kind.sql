-- signal_kind distinguishes an immediate trade from a price-monitor watch.
--   IMMEDIATE  → within_range / default: a real trade placed now (counts toward the cap).
--   MONITORING → below_min: parked in the price monitor, not a trade until it triggers.
-- Used by the durable reseed of the per-strategy daily trade counter (pkg/tradecap):
-- a committed trade = an IMMEDIATE signal, or a MONITORING watch whose status has
-- since advanced to EXECUTED/TRIGGERED. Backfilled to IMMEDIATE for existing rows so
-- historical counts stay conservative (never under-count a hard cap).
ALTER TABLE trade_signals
    ADD COLUMN IF NOT EXISTS signal_kind VARCHAR(20) NOT NULL DEFAULT 'IMMEDIATE';

COMMENT ON COLUMN trade_signals.signal_kind IS
    'IMMEDIATE = real trade placed now; MONITORING = price-monitor watch (not a trade until it triggers)';

-- Reseed counts committed trades for a strategy on today''s IST date; this partial-ish
-- composite index keeps that query and the per-strategy status rollups cheap.
CREATE INDEX IF NOT EXISTS idx_trade_signals_strategy_kind_status_date
    ON trade_signals(strategy_id, signal_kind, status, created_at);
