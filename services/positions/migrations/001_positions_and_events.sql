-- Migration 001: positions + position_events (positions svc genesis)
--
-- Owned by positions svc per docs/positions_service_design.md §4.
-- Every BUY fill creates a new positions row (lot). SELL fills touch specific
-- lots per §7.2 attribution rules. Never merge lots — full traceability from
-- signal → position → broker order.
--
-- Schema is append-mostly:
--   positions        rows are INSERTed on BUY, UPDATEd only on lifecycle
--                     transitions (status flip, exit fields, current_sl trail)
--   position_events  append-only audit log — every observation lands here first
--
-- Idempotency (position_events UNIQUE constraint) makes Kafka at-least-once
-- delivery safe: same order.events message replayed → same INSERT → no-op.

BEGIN;

-- =============================================================================
-- positions — one row per logical position lot
-- =============================================================================
CREATE TABLE IF NOT EXISTS positions (
    position_id           UUID PRIMARY KEY,

    -- Origin (segregation core — §2 of design doc)
    origin                VARCHAR(16) NOT NULL,
        -- MANTHAN      — placed by rules-engine, has signal_id
        -- USER_MANUAL  — placed by user via broker app, no signal_id

    user_id               VARCHAR(64) NOT NULL,
    strategy_id           UUID,                       -- NULL for USER_MANUAL
    signal_id             UUID,                       -- NULL for USER_MANUAL,
                                                       -- NOT NULL for MANTHAN
    symbol                VARCHAR(32) NOT NULL,
    exchange              VARCHAR(8)  NOT NULL,

    -- Lifecycle
    status                VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
        -- ACTIVE | EXITED
    entry_price           NUMERIC(14,4) NOT NULL,
    entry_time            TIMESTAMPTZ   NOT NULL,
    quantity              INTEGER       NOT NULL,     -- current net qty
                                                       -- (may drop on partial exits)
    invested_amount       NUMERIC(14,2) NOT NULL,     -- frozen at entry:
                                                       -- entry_price × initial_qty

    -- Exit fields — populated only when status flips to EXITED
    exit_price            NUMERIC(14,4),
    exit_time             TIMESTAMPTZ,
    exit_reason           VARCHAR(32),
        -- SL_TRIGGER | MANUAL_EXIT | STRATEGY_EXIT | LIQUIDATION
    realized_pnl          NUMERIC(14,2),

    -- Broker linkage
    entry_broker_order_id VARCHAR(50),                -- the BUY that created this row
    exit_broker_order_id  VARCHAR(50),                -- the SELL that closed this row

    -- Manthan trail state (unused for USER_MANUAL)
    current_sl            NUMERIC(14,4),
    high_since_entry      NUMERIC(14,4),
    last_trail_level      NUMERIC(14,4),

    -- Audit
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_positions_origin
        CHECK (origin IN ('MANTHAN','USER_MANUAL')),
    CONSTRAINT chk_positions_status
        CHECK (status IN ('ACTIVE','EXITED')),
    CONSTRAINT chk_positions_exit_reason
        CHECK (exit_reason IS NULL OR exit_reason IN
               ('SL_TRIGGER','MANUAL_EXIT','STRATEGY_EXIT','LIQUIDATION')),
    CONSTRAINT chk_positions_manthan_has_signal
        CHECK (origin != 'MANTHAN' OR signal_id IS NOT NULL),
    CONSTRAINT chk_positions_manual_no_signal
        CHECK (origin != 'USER_MANUAL' OR signal_id IS NULL),
    CONSTRAINT chk_positions_manthan_has_strategy
        CHECK (origin != 'MANTHAN' OR strategy_id IS NOT NULL),
    CONSTRAINT chk_positions_qty_nonneg
        CHECK (quantity >= 0),
    CONSTRAINT chk_positions_exit_consistency
        CHECK ((status = 'EXITED') = (exit_time IS NOT NULL))
);

-- "Show ACTIVE positions for user+symbol" — hot path for reconciler drift check
CREATE INDEX IF NOT EXISTS idx_positions_user_symbol_active
    ON positions (user_id, symbol) WHERE status = 'ACTIVE';

-- "Find the lot exited by this SL/exit signal"
CREATE INDEX IF NOT EXISTS idx_positions_manthan_signal
    ON positions (signal_id) WHERE origin = 'MANTHAN';

-- "Show me user X's holdings by origin"
CREATE INDEX IF NOT EXISTS idx_positions_user_origin
    ON positions (user_id, origin, status);

-- Locating a row from the BUY/SELL that touched it (reconciler + drift use)
CREATE INDEX IF NOT EXISTS idx_positions_entry_broker_order
    ON positions (entry_broker_order_id)
    WHERE entry_broker_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_positions_exit_broker_order
    ON positions (exit_broker_order_id)
    WHERE exit_broker_order_id IS NOT NULL;

COMMENT ON TABLE  positions IS
    'One row per logical position LOT. Never merged across BUYs — full signal→position traceability.';
COMMENT ON COLUMN positions.origin IS
    'MANTHAN = placed by rules-engine (signal_id NOT NULL). USER_MANUAL = user via broker app (signal_id NULL).';
COMMENT ON COLUMN positions.quantity IS
    'Current net qty. Drops on partial SELLs. When it hits 0, status flips to EXITED.';

-- =============================================================================
-- position_events — append-only audit log per §4.2
-- =============================================================================
CREATE TABLE IF NOT EXISTS position_events (
    id                    BIGSERIAL PRIMARY KEY,
    position_id           UUID NOT NULL REFERENCES positions(position_id),

    event_type            VARCHAR(32) NOT NULL,
        -- ENTRY_FILLED / USER_MANUAL_ENTRY / SL_MODIFIED / SL_FILLED /
        -- MANUAL_SELL_APPLIED / LIQUIDATION / RECONCILER_DRIFT_FIX

    broker_order_id       VARCHAR(50),
    signal_id             UUID,
    delta_qty             INTEGER,                    -- +N entry, -N exit/partial
    fill_price            NUMERIC(14,4),
    realized_pnl_delta    NUMERIC(14,2),              -- pro-rata for partial exits
    reason                TEXT,

    raw_source_event      JSONB NOT NULL,             -- full order.events envelope
    source_topic          VARCHAR(32) NOT NULL,       -- 'order.events' | 'reconciler' | ...
    source_offset         BIGINT,
    source_event_id       VARCHAR(80) NOT NULL,       -- for dedup

    observed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Idempotency key: same source event replayed → no-op via ON CONFLICT DO NOTHING
    CONSTRAINT uq_pe_source_event UNIQUE (position_id, source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_pe_position_time
    ON position_events (position_id, observed_at DESC);

CREATE INDEX IF NOT EXISTS idx_pe_event_type
    ON position_events (event_type, observed_at DESC);

COMMENT ON TABLE position_events IS
    'Append-only audit of every event that touched a position. UNIQUE (position_id, source_event_id) dedupes Kafka replays.';

COMMIT;

-- =============================================================================
-- Verification queries
-- =============================================================================
--   \d positions
--     -- expect: PK position_id, 4 partial indexes, 8 CHECK constraints
--   \d position_events
--     -- expect: PK id, FK position_id → positions, UNIQUE (position_id, source_event_id)
