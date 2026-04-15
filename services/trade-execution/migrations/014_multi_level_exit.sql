-- ============================================================================
-- Migration 014: Multi-Level Exit Levels Tracking
-- Stores the runtime state of each SL/TP exit level for an entry order.
-- Created after entry fills; each row tracks one partial exit.
-- ============================================================================

CREATE TABLE IF NOT EXISTS multi_level_exit_levels (
    id              SERIAL          PRIMARY KEY,

    -- Which entry order this level belongs to
    entry_order_id  UUID            NOT NULL,

    -- 'SL' or 'TP'
    exit_type       VARCHAR(5)      NOT NULL CHECK (exit_type IN ('SL', 'TP')),

    -- 1..5, unique per (entry_order_id, exit_type)
    level_num       INT             NOT NULL CHECK (level_num BETWEEN 1 AND 5),

    -- Original config values from strategy
    price_pct       DECIMAL(10,4)   NOT NULL CHECK (price_pct > 0), -- % move from entry (always positive)
    qty_pct         DECIMAL(10,4)   NOT NULL CHECK (qty_pct > 0),   -- % of total qty to exit here

    -- Computed after entry fills
    trigger_price   DECIMAL(15,2),   -- Absolute price to trigger this level
    exit_qty        INT,             -- Absolute qty to exit at this level

    -- Lifecycle
    -- PENDING  → level configured, awaiting activation
    -- ACTIVE   → TP order placed (live) or price monitor watching (SL/paper)
    -- TRIGGERED→ exit order filled / partial exit executed
    -- CANCELLED→ cancelled before triggering (e.g., SL cancelled when TP fills all qty)
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING',

    -- The exit order placed for this level (TP limit order or SL market order)
    exit_order_id   UUID,
    broker_order_id VARCHAR(50),

    -- When the level was triggered
    triggered_at    TIMESTAMPTZ,
    exit_price      DECIMAL(15,2),  -- Actual fill price when triggered

    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_ml_entry_type_level UNIQUE (entry_order_id, exit_type, level_num)
);

-- Fast lookup by entry order (e.g., "give me all levels for this entry")
CREATE INDEX IF NOT EXISTS idx_ml_entry_order
    ON multi_level_exit_levels(entry_order_id);

-- Find all active SL levels across all entries (for price monitor sweep)
CREATE INDEX IF NOT EXISTS idx_ml_active_sl
    ON multi_level_exit_levels(exit_type, status)
    WHERE exit_type = 'SL' AND status IN ('PENDING', 'ACTIVE');

-- Find pending TP levels (for placing limit orders after entry fills)
CREATE INDEX IF NOT EXISTS idx_ml_active_tp
    ON multi_level_exit_levels(exit_type, status)
    WHERE exit_type = 'TP' AND status IN ('PENDING', 'ACTIVE');

-- Lookup by broker order ID (for WS fill events on live TP orders)
CREATE INDEX IF NOT EXISTS idx_ml_broker_order
    ON multi_level_exit_levels(broker_order_id)
    WHERE broker_order_id IS NOT NULL;

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_ml_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_ml_updated_at
    BEFORE UPDATE ON multi_level_exit_levels
    FOR EACH ROW EXECUTE FUNCTION update_ml_updated_at();

COMMENT ON TABLE multi_level_exit_levels IS
    'Runtime state of each partial SL/TP exit level. One row per level per entry order. '
    'Populated after entry fills; status transitions: PENDING→ACTIVE→TRIGGERED or CANCELLED.';
