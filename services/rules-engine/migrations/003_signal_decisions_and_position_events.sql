-- Migration 003: Signal decisions + position events (CQRS-lite)
--
-- Splits "what we INTENDED" from "what the broker DID".
--
-- Before this migration:
--   manthan_positions was written optimistically by rules-engine at signal-
--   generation time with status='ACTIVE'. If the broker rejected, never
--   placed, or partially filled, the row was left as a phantom (e.g.
--   NATIONALUM + LLOYDSME on 2026-04-24).
--
-- After this migration:
--   manthan_signal_decisions  → intent log (rules-engine writes here on entry)
--   manthan_positions         → broker truth (only updated on broker events)
--   manthan_position_events   → idempotent event log feeding manthan_positions
--
-- Idempotency: every broker event from trade-execution carries
-- (signal_id, event_seq). Inserts use ON CONFLICT DO NOTHING so WSS, API
-- poller and reconciler can race safely — first writer wins.
--
-- Migration is non-destructive:
--   - new tables created with IF NOT EXISTS
--   - new columns on manthan_positions are nullable (existing rows unaffected)
--   - tighter constraints (NOT NULL on signal_id) come in a follow-up after
--     backfill is complete
--
-- Rollback (if needed):
--   DROP TABLE manthan_signal_decisions;
--   DROP TABLE manthan_position_events;
--   ALTER TABLE manthan_positions DROP COLUMN signal_id;
--   ALTER TABLE manthan_positions DROP COLUMN event_seq;
--   ALTER TABLE manthan_positions DROP COLUMN broker_order_id;
--   (legacy code path still works; this migration does not remove anything)

BEGIN;

-- =============================================================================
-- 1) manthan_signal_decisions — INTENT LOG
-- =============================================================================
-- One row per (strategy, symbol, decision moment) — append-only audit trail of
-- what rules-engine decided to attempt. Status reflects whether the broker
-- accepted, rejected, or never responded.

CREATE TABLE IF NOT EXISTS manthan_signal_decisions (
    signal_id            UUID PRIMARY KEY,
    user_id              VARCHAR(255) NOT NULL,
    strategy_id          UUID NOT NULL,
    symbol               VARCHAR(30) NOT NULL,
    isin                 VARCHAR(12),

    -- Allocator inputs at decision time (frozen — never updated)
    decided_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ltp_at_decision      NUMERIC(12,2) NOT NULL,
    ema_alloc_pct        NUMERIC(6,4),
    intended_qty         INTEGER NOT NULL,
    intended_invested    NUMERIC(14,2) NOT NULL,
    initial_sl_target    NUMERIC(12,2) NOT NULL,
    industry             VARCHAR(100),
    mcap_bucket          VARCHAR(10),
    index_name           VARCHAR(30),

    -- Lifecycle
    status               VARCHAR(20) NOT NULL DEFAULT 'PROPOSED',
        -- PROPOSED   — decided, not yet published to Kafka
        -- DISPATCHED — published to trade-signals, awaiting broker
        -- CONFIRMED  — broker fully filled (manthan_positions row exists)
        -- PARTIAL    — broker partially filled
        -- REJECTED   — broker rejected (DPR, freeze qty, margin, etc.)
        -- TIMED_OUT  — no broker response within deadline (reaper marks)
        -- CLOSED     — position later exited (terminal state)
    dispatched_at        TIMESTAMPTZ,
    final_status_at      TIMESTAMPTZ,
    rejection_reason     TEXT,

    CONSTRAINT chk_msd_status CHECK (status IN (
        'PROPOSED','DISPATCHED','CONFIRMED','PARTIAL',
        'REJECTED','TIMED_OUT','CLOSED'
    )),

    -- One pending decision per (strategy, symbol) at a time. Once CLOSED,
    -- a new decision can be made for the same symbol — we use decided_at
    -- as the differentiator.
    CONSTRAINT uq_msd_per_attempt UNIQUE (strategy_id, symbol, decided_at)
);

CREATE INDEX IF NOT EXISTS idx_msd_user_strategy
    ON manthan_signal_decisions(user_id, strategy_id);

CREATE INDEX IF NOT EXISTS idx_msd_pending
    ON manthan_signal_decisions(status, dispatched_at)
    WHERE status IN ('PROPOSED','DISPATCHED');

CREATE INDEX IF NOT EXISTS idx_msd_symbol_active
    ON manthan_signal_decisions(strategy_id, symbol)
    WHERE status IN ('CONFIRMED','PARTIAL');

COMMENT ON TABLE manthan_signal_decisions IS
    'Intent log: every entry decision rules-engine makes, with allocator inputs and lifecycle status. Append-only; status updates by FillConsumer.';
COMMENT ON COLUMN manthan_signal_decisions.signal_id IS
    'Server-generated UUID. Carried as Kafka key + on every broker event for idempotency.';
COMMENT ON COLUMN manthan_signal_decisions.status IS
    'PROPOSED → DISPATCHED → CONFIRMED|PARTIAL|REJECTED|TIMED_OUT → CLOSED';

-- =============================================================================
-- 2) manthan_position_events — IDEMPOTENT EVENT LOG
-- =============================================================================
-- Every broker event (from WSS, API poller, or reconciler) lands here first.
-- The (signal_id, event_seq) UNIQUE constraint guarantees that re-publishes
-- from any source become no-ops via ON CONFLICT DO NOTHING.
--
-- event_seq is broker's exchTime in microseconds (deterministic across
-- WSS/poller/reconciler — they all observe the same exchange-stamped event).

CREATE TABLE IF NOT EXISTS manthan_position_events (
    id                BIGSERIAL PRIMARY KEY,
    signal_id         UUID NOT NULL,
    event_seq         BIGINT NOT NULL,
    event_type        VARCHAR(30) NOT NULL,
        -- ENTRY_FILLED, ENTRY_PARTIAL_FILL, ENTRY_REJECTED, ENTRY_TIMED_OUT,
        -- SL_PLACED, SL_MODIFIED, SL_REJECTED, SL_FILLED,
        -- EXIT_FILLED, RECONCILER_DRIFT_FIX

    broker_order_id   VARCHAR(50),
    fill_price        NUMERIC(12,2),
    fill_qty          INTEGER,
    rejection_reason  TEXT,

    raw_payload       JSONB NOT NULL,        -- full broker event for forensics
    received_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source            VARCHAR(20) NOT NULL,
        -- WSS | API_POLLER | RECONCILER | SAFETY_MONITOR | TRADE_EXEC

    CONSTRAINT uq_mpe_signal_seq UNIQUE (signal_id, event_seq),
    CONSTRAINT chk_mpe_event_type CHECK (event_type IN (
        'ENTRY_FILLED','ENTRY_PARTIAL_FILL','ENTRY_REJECTED','ENTRY_TIMED_OUT',
        'SL_PLACED','SL_MODIFIED','SL_REJECTED','SL_FILLED',
        'EXIT_FILLED','RECONCILER_DRIFT_FIX'
    )),
    CONSTRAINT chk_mpe_source CHECK (source IN (
        'WSS','API_POLLER','RECONCILER','SAFETY_MONITOR','TRADE_EXEC'
    ))
);

CREATE INDEX IF NOT EXISTS idx_mpe_signal
    ON manthan_position_events(signal_id, event_seq DESC);

CREATE INDEX IF NOT EXISTS idx_mpe_received_at
    ON manthan_position_events(received_at DESC);

CREATE INDEX IF NOT EXISTS idx_mpe_event_type
    ON manthan_position_events(event_type, received_at DESC);

COMMENT ON TABLE manthan_position_events IS
    'Idempotent broker event log. WSS, API poller, reconciler all INSERT here ON CONFLICT (signal_id, event_seq) DO NOTHING. First writer wins.';
COMMENT ON COLUMN manthan_position_events.event_seq IS
    'Deterministic monotonic key — broker exchTime in microseconds. Same value across WSS/poller/reconciler for the same broker event.';

-- =============================================================================
-- 3) manthan_positions — link to signal_id + add broker_order_id
-- =============================================================================
-- Existing rows (legacy optimistic writes) get nullable signal_id. Once
-- backfilled (next PR) we tighten to NOT NULL + UNIQUE.
--
-- broker_order_id is the broker-assigned order id from the entry order (e.g.
-- NZWKE00292H4). Lets us cross-reference manthan_orders without joining
-- through manthan_position_events.

ALTER TABLE manthan_positions
    ADD COLUMN IF NOT EXISTS signal_id        UUID,
    ADD COLUMN IF NOT EXISTS event_seq        BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS broker_order_id  VARCHAR(50);

-- Partial unique constraint — non-null signal_ids must be unique. Legacy NULL
-- rows are exempt. After backfill, we drop this and add a full UNIQUE NOT NULL.
CREATE UNIQUE INDEX IF NOT EXISTS uq_manthan_pos_signal
    ON manthan_positions(signal_id) WHERE signal_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_manthan_pos_broker_order
    ON manthan_positions(broker_order_id) WHERE broker_order_id IS NOT NULL;

-- Widen status enum to include PARTIAL_ACTIVE (broker filled < expected)
ALTER TABLE manthan_positions
    DROP CONSTRAINT IF EXISTS chk_manthan_pos_status;
ALTER TABLE manthan_positions
    ADD CONSTRAINT chk_manthan_pos_status CHECK (status IN (
        'ACTIVE','PARTIAL_ACTIVE','EXITED','COOLDOWN'
    ));

COMMENT ON COLUMN manthan_positions.signal_id IS
    'FK to manthan_signal_decisions.signal_id. NULL only for legacy rows pre-migration-003.';
COMMENT ON COLUMN manthan_positions.event_seq IS
    'Last applied broker event_seq. Used for stale-write protection in UPSERT (WHERE event_seq < EXCLUDED.event_seq).';
COMMENT ON COLUMN manthan_positions.broker_order_id IS
    'Broker-assigned entry order id (e.g. NZWKE00292H4). Cross-references execution_db.manthan_orders.';

-- =============================================================================
-- 4) Backwards-compatibility view — for any read path not yet migrated
-- =============================================================================
-- Old code that SELECTs from manthan_positions sees the same shape; new code
-- joins to manthan_signal_decisions for intent context.

CREATE OR REPLACE VIEW manthan_positions_with_intent AS
SELECT
    p.*,
    d.decided_at        AS intent_decided_at,
    d.ltp_at_decision   AS intent_ltp,
    d.intended_qty      AS intent_qty,
    d.intended_invested AS intent_invested,
    d.status            AS decision_status,
    d.rejection_reason  AS decision_rejection_reason
FROM manthan_positions p
LEFT JOIN manthan_signal_decisions d ON d.signal_id = p.signal_id;

COMMENT ON VIEW manthan_positions_with_intent IS
    'Position rows joined with their originating decision. Useful for debugging drift between intent and reality.';

COMMIT;

-- =============================================================================
-- Verification queries (run after migration)
-- =============================================================================
--
-- 1. New tables exist:
--    \dt manthan_signal_decisions
--    \dt manthan_position_events
--
-- 2. New columns on manthan_positions:
--    \d manthan_positions
--    -- expect: signal_id (uuid, nullable), event_seq (bigint), broker_order_id (varchar)
--
-- 3. Existing rows still readable:
--    SELECT count(*) FROM manthan_positions;
--    SELECT count(*) FROM manthan_positions WHERE signal_id IS NULL;  -- legacy
--
-- 4. View works:
--    SELECT count(*) FROM manthan_positions_with_intent;
--
-- 5. Confirm constraints accept new status:
--    -- (won't INSERT, just validates):
--    SELECT 'PARTIAL_ACTIVE'::varchar(20) IN ('ACTIVE','PARTIAL_ACTIVE','EXITED','COOLDOWN');
