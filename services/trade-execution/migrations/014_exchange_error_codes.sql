-- Migration 014: Structured exchange error code tracking + SEBI algo tagging
--
-- Captures REAL Codifi/Indira rejection responses observed in the 2026-06-06
-- NSE Contingency Drill into structured columns so we can:
--   1. Auto-categorize rejections (retryable / terminal / DPR / margin / auth)
--   2. Filter / aggregate by NSE OE protocol code for SEBI audit trail
--   3. Tag every order with our registered algoID (SEBI 2022 algo circular)
--
-- See pkg/indira/error_codes.go for the canonical NSE tag → category mapping
-- and the full pattern catalog built from drill captures.

BEGIN;

-- Order-level columns: the FINAL reason for each order. When a rejection
-- happens we populate these alongside last_error so SEBI auditors can query
-- by NSE code (16247, 16307, 17179, …) without parsing text.
ALTER TABLE manthan_orders
    ADD COLUMN IF NOT EXISTS exchange_error_code INT,
    ADD COLUMN IF NOT EXISTS exchange_error_tag  TEXT,
    ADD COLUMN IF NOT EXISTS reject_category     TEXT,
    ADD COLUMN IF NOT EXISTS algo_id             INT;

-- Event-level columns: every state transition in the lifecycle. Important
-- for audit because a single order can hit multiple rejections (PlaceOrder
-- pre-trade → Modify → exchange release). We store the code on the event
-- row, not just the latest aggregate on the order row.
ALTER TABLE manthan_order_events
    ADD COLUMN IF NOT EXISTS exchange_error_code INT,
    ADD COLUMN IF NOT EXISTS exchange_error_tag  TEXT,
    ADD COLUMN IF NOT EXISTS reject_category     TEXT;

-- Index for SEBI audit queries that filter by category (e.g. "show me all
-- rejections last 7 days with category=DPR_BREACH").
CREATE INDEX IF NOT EXISTS idx_mord_reject_category
    ON manthan_orders(reject_category)
    WHERE reject_category IS NOT NULL;

-- Index for the algo-tagging audit: "show me all orders tagged with algo_id
-- 12345 in the last month." SEBI's algo audit cadence is monthly.
CREATE INDEX IF NOT EXISTS idx_mord_algo_id
    ON manthan_orders(algo_id, created_at)
    WHERE algo_id IS NOT NULL;

COMMENT ON COLUMN manthan_orders.exchange_error_code IS
    'NSE OE protocol code (16247=INVALID_PRICE, 16307=QTY_FREEZE, 17179=INVALID_ALGO_ID, ...). 0 when Codifi/exchange did not surface a numeric code.';
COMMENT ON COLUMN manthan_orders.exchange_error_tag IS
    'Canonical tag from pkg/indira/error_codes.go (e.g. INVALID_PRICE_TICK_MISMATCH, MARGIN_INSUFFICIENT, RMS_FREE_QTY_EXCEEDED_LIKELY_DDPI).';
COMMENT ON COLUMN manthan_orders.reject_category IS
    'PRE_TRADE_RETRYABLE / PRE_TRADE_TERMINAL / DPR_BREACH / AUTH / MARGIN_INSUFFICIENT / STALE_ORDER / UNKNOWN. Drives auto-retry vs alert decision.';
COMMENT ON COLUMN manthan_orders.algo_id IS
    'SEBI-registered algo ID (Manthan strategy). Sent on every PlaceOrder/ModifyOrder to satisfy SEBI 2022 algo audit circular.';

COMMENT ON TABLE manthan_orders IS
    'Manthan order ledger. exchange_error_code/tag/category populated from broker_adapter.go via pkg/indira.ParseCodifiResponse on every rejection. algo_id populated at order-submit time from MANTHAN_ALGO_ID env.';

COMMIT;
