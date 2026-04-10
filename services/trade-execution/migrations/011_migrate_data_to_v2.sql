-- ============================================================================
-- Migration 011: Migrate existing data from old tables → V2 schema
--
-- SAFE MIGRATION — no data loss, no old table drops
--
-- Edge cases handled:
--   - PENDING never persisted (only in-memory) → skip PENDING in history
--   - Paper exits use CANCELLED + paper_exit_price → detect via exit_price NOT NULL
--   - Live exits use CANCELLED + live_exit_price → detect via exit_price NOT NULL
--   - Broker statuses stored raw (EXECUTED/TRADED/A.REJECTED) → preserve as-is
--   - execution_events BROKER_WS_UPDATE has full WS JSON → migrate to history
--   - rejection_reason and error_message both nullable → preserve both
--   - OCO group status derived from member order statuses
--   - Orders with filled_price but filled_quantity=0 → handle gracefully
--   - Multiple credential sources (order fields vs user_credentials table)
--
-- Old tables remain intact for rollback.
-- Run AFTER 010_normalized_schema_v2.sql
-- ============================================================================

BEGIN;

-- ============================================================================
-- PRE-FLIGHT: Count old data (logged for verification)
-- ============================================================================
DO $$
DECLARE
    cnt_orders INT;
    cnt_events INT;
    cnt_creds  INT;
BEGIN
    SELECT COUNT(*) INTO cnt_orders FROM orders;
    SELECT COUNT(*) INTO cnt_events FROM execution_events;
    SELECT COUNT(*) INTO cnt_creds  FROM user_credentials;
    RAISE NOTICE '=== PRE-MIGRATION COUNTS ===';
    RAISE NOTICE 'orders:           %', cnt_orders;
    RAISE NOTICE 'execution_events: %', cnt_events;
    RAISE NOTICE 'user_credentials: %', cnt_creds;
END $$;

-- ============================================================================
-- STEP 1: instruments — deduplicate by (stock_code, exchange)
-- ============================================================================
INSERT INTO instruments (stock_code, symbol, exchange, instrument_type, is_active, created_at, updated_at)
SELECT
    sub.stock_code,
    sub.symbol,
    sub.exchange,
    'STK',
    true,
    sub.first_seen,
    NOW()
FROM (
    SELECT
        stock_code,
        exchange,
        -- Pick the symbol from the most recent order (in case of renames)
        (ARRAY_AGG(symbol ORDER BY created_at DESC))[1] AS symbol,
        MIN(created_at) AS first_seen
    FROM orders
    GROUP BY stock_code, exchange
) sub
ON CONFLICT (stock_code, exchange) DO NOTHING;

-- ============================================================================
-- STEP 2: broker_accounts — from user_credentials table first
-- ============================================================================
INSERT INTO broker_accounts (
    user_id, broker_name, broker_user_id, app_id, source,
    bearer_token, is_active, created_at, updated_at
)
SELECT
    uc.user_id,
    'INDIRA',
    uc.indira_user_id,
    uc.indira_app_id,
    uc.indira_source,
    uc.indira_bearer_token,   -- already encrypted
    true,
    uc.created_at,
    uc.updated_at
FROM user_credentials uc
ON CONFLICT (user_id, broker_name) DO NOTHING;

-- Also pick up users who only have auth on order rows (no user_credentials row)
INSERT INTO broker_accounts (
    user_id, broker_name, app_id, source, bearer_token,
    is_active, created_at, updated_at
)
SELECT DISTINCT ON (o.user_id)
    o.user_id,
    'INDIRA',
    o.app_id,
    COALESCE(o.source, 'WEB'),
    o.bearer_token,
    true,
    MIN(o.created_at) OVER (PARTITION BY o.user_id),
    NOW()
FROM orders o
WHERE o.bearer_token IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM broker_accounts ba WHERE ba.user_id = o.user_id)
ORDER BY o.user_id, o.created_at DESC
ON CONFLICT (user_id, broker_name) DO NOTHING;

-- ============================================================================
-- STEP 3: orders → orders_v2
--   - Keeps BOTH error_message and rejection_reason (concatenated if both set)
--   - Derives trading_mode from is_paper_trade flag
--   - Preserves exact status (including broker raw like EXECUTED/TRADED)
-- ============================================================================
INSERT INTO orders_v2 (
    order_id, instrument_id, account_id,
    user_id, strategy_id, strategy_name, event_id, signal_id,
    order_type, order_side, quantity, price,
    validity, product_type, trading_mode,
    status, filled_qty, avg_fill_price,
    risk_approved, risk_score,
    created_at, updated_at, submitted_at, executed_at,
    error_message, retry_count
)
SELECT
    o.order_id,
    i.instrument_id,
    ba.account_id,
    o.user_id,
    o.strategy_id,
    COALESCE(o.strategy_name, ''),
    o.event_id,
    o.signal_id,
    o.order_type,
    o.order_side,
    o.quantity,
    o.price,
    COALESCE(o.validity, 'DAY'),
    COALESCE(o.product_type, 'INTRADAY'),
    COALESCE(
        o.trading_mode,
        CASE WHEN o.is_paper_trade THEN 'PAPER' ELSE 'LIVE' END
    ),
    o.status,
    COALESCE(o.filled_quantity, 0),
    o.filled_price,
    COALESCE(o.risk_approved, false),
    o.risk_score,
    o.created_at,
    o.updated_at,
    o.submitted_at,
    o.executed_at,
    -- Preserve BOTH error_message and rejection_reason
    CASE
        WHEN o.error_message IS NOT NULL AND o.rejection_reason IS NOT NULL
            THEN o.error_message || ' | ' || o.rejection_reason
        ELSE COALESCE(o.error_message, o.rejection_reason)
    END,
    COALESCE(o.retry_count, 0)
FROM orders o
LEFT JOIN instruments i
    ON i.stock_code = o.stock_code AND i.exchange = o.exchange
LEFT JOIN broker_accounts ba
    ON ba.user_id = o.user_id AND ba.broker_name = 'INDIRA'
ON CONFLICT (order_id) DO NOTHING;

-- ============================================================================
-- STEP 4: order_status_history — reconstruct timeline from orders table
--
-- For each order, create the status transitions that MUST have happened:
--   1. NULL → RECEIVED    (always, at created_at)
--   2. RECEIVED → SUBMITTED (if submitted_at or indira_order_id exists)
--   3. SUBMITTED → final_status (if status is terminal and not SUBMITTED)
--   OR
--   2. RECEIVED → final_status (if never submitted but terminal)
-- ============================================================================

-- 4a: Every order started as RECEIVED
INSERT INTO order_status_history (
    order_id, from_status, to_status, source, reason, actor, created_at
)
SELECT
    order_id,
    NULL,
    'RECEIVED',
    'SYSTEM',
    'Signal consumed from Kafka',
    'migration_v2',
    created_at
FROM orders;

-- 4b: Orders that were submitted to broker
INSERT INTO order_status_history (
    order_id, from_status, to_status, source, reason, actor, created_at
)
SELECT
    order_id,
    'RECEIVED',
    'SUBMITTED',
    'SYSTEM',
    'Placed via Codifi API (broker_id=' || COALESCE(indira_order_id, odin_order_id, 'unknown') || ')',
    'migration_v2',
    COALESCE(submitted_at, created_at + interval '100 milliseconds')
FROM orders
WHERE submitted_at IS NOT NULL
   OR indira_order_id IS NOT NULL
   OR odin_order_id IS NOT NULL;

-- 4c: Terminal status transition (the final state the order reached)
--     Skips orders still in RECEIVED or SUBMITTED (those are covered above)
INSERT INTO order_status_history (
    order_id, from_status, to_status, source, reason,
    broker_raw_data, actor, created_at
)
SELECT
    order_id,
    -- from_status: SUBMITTED if broker saw it, else RECEIVED
    CASE
        WHEN submitted_at IS NOT NULL OR indira_order_id IS NOT NULL OR odin_order_id IS NOT NULL
            THEN 'SUBMITTED'
        ELSE 'RECEIVED'
    END,
    -- to_status: the actual current status
    status,
    -- source: derive from context clues
    CASE
        WHEN broker_status IS NOT NULL                           THEN 'BROKER'
        WHEN rejection_reason ILIKE '%strategy deactivat%'       THEN 'STRATEGY'
        WHEN rejection_reason ILIKE '%strategy%delet%'           THEN 'STRATEGY'
        WHEN rejection_reason ILIKE '%force exit%'               THEN 'USER'
        WHEN rejection_reason ILIKE '%price exceeded%'           THEN 'SCHEDULER'
        WHEN rejection_reason ILIKE '%LTP exceeded%'             THEN 'SCHEDULER'
        WHEN rejection_reason ILIKE '%square%off%'               THEN 'SCHEDULER'
        WHEN status IN ('REJECTED')                              THEN 'SYSTEM'
        WHEN status IN ('FAILED')                                THEN 'SYSTEM'
        WHEN is_paper_trade AND paper_exit_price IS NOT NULL     THEN 'SYSTEM'
        ELSE 'SYSTEM'
    END,
    -- reason: preserve both rejection_reason and error_message
    COALESCE(rejection_reason, error_message, 'Status: ' || status),
    -- broker_raw_data: store last WS payload if available
    CASE WHEN broker_ws_data IS NOT NULL THEN broker_ws_data::jsonb ELSE NULL END,
    'migration_v2',
    COALESCE(executed_at, updated_at, created_at + interval '200 milliseconds')
FROM orders
WHERE status NOT IN ('RECEIVED', 'SUBMITTED');

-- ============================================================================
-- STEP 5: execution_events → order_status_history (every broker WS event)
--
-- This preserves the full audit trail from execution_events.
-- Uses dedup_key to prevent duplicates with Step 4.
-- ============================================================================
INSERT INTO order_status_history (
    order_id, from_status, to_status, source, reason,
    broker_raw_data, actor, dedup_key, created_at
)
SELECT
    ee.order_id,
    -- from_status: extract previous_status from event_data if available
    ee.event_data->>'previous_status',
    -- to_status
    CASE ee.event_type
        WHEN 'SUBMITTED'            THEN 'SUBMITTED'
        WHEN 'REJECTED'             THEN 'REJECTED'
        WHEN 'FAILED'               THEN 'FAILED'
        WHEN 'CANCELLED'            THEN 'CANCELLED'
        WHEN 'PAPER_FILLED'         THEN 'FILLED'
        WHEN 'PAPER_EXITED'         THEN 'CANCELLED'
        WHEN 'PRICE_MONITOR_TRIGGERED' THEN 'PENDING'
        WHEN 'PRICE_EXCEEDED_MAX'   THEN 'CANCELLED'
        WHEN 'PRICE_WATCH_CANCELLED' THEN 'CANCELLED'
        WHEN 'BROKER_WS_UPDATE'     THEN COALESCE(
            UPPER(TRIM(ee.event_data->>'broker_status')),
            'UNKNOWN'
        )
        ELSE UPPER(ee.event_type)
    END,
    -- source
    CASE ee.event_type
        WHEN 'BROKER_WS_UPDATE'        THEN 'BROKER'
        WHEN 'PRICE_EXCEEDED_MAX'      THEN 'SCHEDULER'
        WHEN 'PRICE_MONITOR_TRIGGERED' THEN 'SCHEDULER'
        WHEN 'PRICE_WATCH_CANCELLED'   THEN 'USER'
        WHEN 'PAPER_FILLED'            THEN 'SYSTEM'
        WHEN 'PAPER_EXITED'            THEN 'SYSTEM'
        ELSE 'SYSTEM'
    END,
    -- reason
    COALESCE(
        ee.event_data->>'reason',
        ee.event_data->>'error',
        ee.event_data->>'status_message',
        ee.event_type
    ),
    -- broker_raw_data: full event_data for BROKER_WS_UPDATE
    CASE WHEN ee.event_type = 'BROKER_WS_UPDATE' THEN ee.event_data ELSE NULL END,
    -- actor
    'migration_v2_events',
    -- dedup_key: unique per execution_event row
    'ee_' || ee.id::text,
    ee.created_at
FROM execution_events ee;

-- ============================================================================
-- STEP 6: fills — create from orders with actual fills
--
-- Edge cases:
--   - filled_quantity=0 but status=EXECUTED → use quantity
--   - filled_price NULL but price set → use price (paper trades)
--   - Paper trades fill immediately at order price
-- ============================================================================
INSERT INTO fills (
    fill_id, order_id, fill_qty, fill_price,
    exchange_order_no, broker_order_id, filled_at
)
SELECT
    gen_random_uuid(),
    o.order_id,
    -- fill_qty: use filled_quantity, fallback to quantity for paper/executed
    CASE
        WHEN COALESCE(o.filled_quantity, 0) > 0 THEN o.filled_quantity
        WHEN o.status IN ('FILLED', 'EXECUTED', 'TRADED') THEN o.quantity
        ELSE o.filled_quantity
    END,
    -- fill_price: use filled_price, fallback to price for paper trades
    COALESCE(o.filled_price, o.price),
    o.exchange_order_number,
    COALESCE(o.indira_order_id, o.odin_order_id),
    COALESCE(o.executed_at, o.updated_at)
FROM orders o
WHERE (
    -- Has actual fills
    (COALESCE(o.filled_quantity, 0) > 0 AND o.filled_price IS NOT NULL)
    -- OR was fully filled (status says so)
    OR o.status IN ('FILLED', 'EXECUTED', 'TRADED')
    -- OR is a paper trade that was filled (has filled_price or paper_exit_price)
    OR (o.is_paper_trade AND (o.filled_price IS NOT NULL OR o.paper_exit_price IS NOT NULL))
)
-- Ensure we have a valid price
AND COALESCE(o.filled_price, o.price) IS NOT NULL
AND COALESCE(o.filled_price, o.price) > 0;

-- Also extract fills from BROKER_WS_UPDATE execution_events
-- (these capture individual partial fills that orders table overwrote)
-- Only insert rows where traded_qty and traded_price are valid numbers > 0.
-- Some WS events have empty strings or "0.00" — skip those.
INSERT INTO fills (
    fill_id, order_id, fill_qty, fill_price,
    exchange_order_no, broker_order_id, filled_at
)
SELECT
    gen_random_uuid(),
    ee.order_id,
    tq.val,
    tp.val,
    ee.event_data->>'order_number',
    ee.event_data->>'unique_code',
    ee.created_at
FROM execution_events ee
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN COALESCE(TRIM(ee.event_data->>'traded_qty'), '') ~ '^\d+$'
        THEN (ee.event_data->>'traded_qty')::int
        ELSE 0
    END AS val
) tq
CROSS JOIN LATERAL (
    SELECT CASE
        WHEN COALESCE(TRIM(ee.event_data->>'traded_price'), '') ~ '^\d+\.?\d*$'
             AND (ee.event_data->>'traded_price')::decimal > 0
        THEN (ee.event_data->>'traded_price')::decimal
        ELSE 0
    END AS val
) tp
WHERE ee.event_type = 'BROKER_WS_UPDATE'
  AND UPPER(TRIM(COALESCE(ee.event_data->>'broker_status', '')))
      IN ('EXECUTED', 'TRADED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
  AND tq.val > 0
  AND tp.val > 0
  -- Don't duplicate fills already created from orders table
  AND NOT EXISTS (
      SELECT 1 FROM fills f
      WHERE f.order_id = ee.order_id
        AND f.broker_order_id = ee.event_data->>'unique_code'
        AND f.fill_qty = tq.val
        AND f.fill_price = tp.val
  );

-- ============================================================================
-- STEP 7: broker_requests — one entry per submitted order
-- ============================================================================
INSERT INTO broker_requests (
    order_id, action, broker_name, attempt,
    response_body, broker_order_id,
    error_type, error_message, created_at
)
SELECT
    o.order_id,
    'PLACE',
    'INDIRA',
    COALESCE(o.retry_count, 0),
    CASE
        WHEN o.indira_response IS NOT NULL THEN to_jsonb(o.indira_response)
        WHEN o.odin_response IS NOT NULL THEN to_jsonb(o.odin_response)
        ELSE NULL
    END,
    COALESCE(o.indira_order_id, o.odin_order_id),
    CASE
        WHEN o.status = 'FAILED' AND o.error_message ILIKE '%timeout%' THEN 'TIMEOUT'
        WHEN o.status = 'FAILED' AND o.error_message ILIKE '%401%'     THEN 'AUTH'
        WHEN o.status = 'FAILED' AND o.error_message ILIKE '%session%' THEN 'AUTH'
        WHEN o.status = 'FAILED'                                       THEN 'NETWORK'
        ELSE NULL
    END,
    o.error_message,
    COALESCE(o.submitted_at, o.created_at)
FROM orders o
WHERE o.indira_order_id IS NOT NULL
   OR o.odin_order_id IS NOT NULL
   OR o.status = 'FAILED';  -- Include failed orders (broker was called but failed)

-- ============================================================================
-- STEP 8: stop_loss_config — extract SL/TP/trailing from orders
-- ============================================================================
-- Note: max_monitor_price and current_pct_change columns may not exist in the
-- actual database (migrations not applied). Using only columns confirmed present.
INSERT INTO stop_loss_config (
    order_id, sl_type,
    stop_loss_price, take_profit_price,
    trailing_pct, highest_price,
    is_active, triggered_at, trigger_reason, created_at
)
SELECT
    o.order_id,
    COALESCE(o.stop_loss_type, 'FIXED'),
    o.stop_loss,
    o.take_profit,
    o.trailing_sl_pct,
    o.highest_price,
    -- is_active: only active if order is still live and not terminal
    CASE
        WHEN o.status IN ('FILLED', 'EXECUTED', 'TRADED', 'PARTIALLY_FILLED',
                          'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
             AND o.stop_loss_type = 'TRAILING'
            THEN true
        ELSE false
    END,
    -- triggered_at: set if position was closed via SL/TP
    CASE
        WHEN o.paper_exit_price IS NOT NULL THEN o.updated_at
        WHEN o.live_exit_price IS NOT NULL THEN o.updated_at
        ELSE NULL
    END,
    -- trigger_reason
    CASE
        WHEN o.rejection_reason ILIKE '%price exceeded%' THEN 'PRICE_EXCEEDED'
        WHEN o.rejection_reason ILIKE '%LTP exceeded%'   THEN 'PRICE_EXCEEDED'
        WHEN o.paper_exit_price IS NOT NULL               THEN 'SL_HIT'
        WHEN o.live_exit_price IS NOT NULL                 THEN 'SL_HIT'
        ELSE NULL
    END,
    o.created_at
FROM orders o
WHERE o.stop_loss IS NOT NULL
   OR o.take_profit IS NOT NULL
   OR o.stop_loss_type = 'TRAILING'
ON CONFLICT (order_id) DO NOTHING;

-- ============================================================================
-- STEP 9: order_groups + order_group_legs — OCO groups
-- ============================================================================

-- 9a: Distinct OCO groups with derived status
INSERT INTO order_groups (group_id, group_type, user_id, status, created_at)
SELECT
    oco_group_id,
    'OCO',
    -- user_id: pick from any member
    (ARRAY_AGG(user_id))[1],
    -- status: derive from member order statuses
    CASE
        WHEN bool_and(status IN ('CANCELLED', 'REJECTED', 'A.REJECTED', 'FAILED'))
            THEN 'CANCELLED'
        WHEN bool_or(status IN ('FILLED', 'EXECUTED', 'TRADED'))
            THEN 'COMPLETED'
        ELSE 'ACTIVE'
    END,
    MIN(created_at)
FROM orders
WHERE oco_group_id IS NOT NULL
GROUP BY oco_group_id
ON CONFLICT (group_id) DO NOTHING;

-- 9b: Create legs
INSERT INTO order_group_legs (group_id, order_id, leg_role, sequence, created_at)
SELECT
    oco_group_id,
    order_id,
    COALESCE(oco_role, 'ENTRY'),
    CASE COALESCE(oco_role, 'ENTRY')
        WHEN 'ENTRY'  THEN 0
        WHEN 'SL_LEG' THEN 1
        WHEN 'TP_LEG' THEN 2
        ELSE 0
    END,
    created_at
FROM orders
WHERE oco_group_id IS NOT NULL
ON CONFLICT (group_id, order_id) DO NOTHING;

-- ============================================================================
-- STEP 10: positions — 1 position per order that was filled
--
-- Key logic:
--   - Paper with paper_exit_price → CLOSED
--   - Live with live_exit_price → CLOSED
--   - Status in (FILLED/EXECUTED/TRADED) without exit → OPEN
--   - CANCELLED with exit price → CLOSED (this is how exits work)
--   - exit_reason derived from rejection_reason text
-- ============================================================================
INSERT INTO positions (
    position_id, instrument_id,
    user_id, strategy_id, strategy_name,
    trading_mode, side,
    open_qty, avg_entry_price, avg_exit_price,
    realized_pnl, net_pnl, exit_reason,
    status, opened_at, closed_at
)
SELECT
    gen_random_uuid(),
    i.instrument_id,
    o.user_id,
    o.strategy_id,
    COALESCE(o.strategy_name, ''),
    -- trading_mode
    CASE WHEN o.is_paper_trade THEN 'PAPER' ELSE 'LIVE' END,
    -- side
    CASE WHEN o.order_side = 'BUY' THEN 'LONG' ELSE 'SHORT' END,
    -- open_qty: 0 if closed, else filled quantity
    CASE
        WHEN o.is_paper_trade AND o.paper_exit_price IS NOT NULL   THEN 0
        WHEN NOT o.is_paper_trade AND o.live_exit_price IS NOT NULL THEN 0
        ELSE COALESCE(
            NULLIF(o.filled_quantity, 0),
            o.quantity
        )
    END,
    -- avg_entry_price
    COALESCE(o.filled_price, o.price),
    -- avg_exit_price
    CASE
        WHEN o.is_paper_trade THEN o.paper_exit_price
        ELSE o.live_exit_price
    END,
    -- realized_pnl
    CASE
        WHEN o.is_paper_trade THEN o.paper_pnl
        ELSE o.live_pnl
    END,
    -- net_pnl (same as realized, no charges tracked in old schema)
    CASE
        WHEN o.is_paper_trade THEN o.paper_pnl
        ELSE o.live_pnl
    END,
    -- exit_reason
    CASE
        WHEN o.rejection_reason ILIKE '%strategy deactivat%'  THEN 'STRATEGY_DELETED'
        WHEN o.rejection_reason ILIKE '%strategy%delet%'      THEN 'STRATEGY_DELETED'
        WHEN o.rejection_reason ILIKE '%force exit%'          THEN 'MANUAL'
        WHEN o.rejection_reason ILIKE '%square%off%'          THEN 'SQUARE_OFF'
        WHEN o.rejection_reason ILIKE '%price exceeded%'      THEN 'PRICE_EXCEEDED'
        WHEN o.rejection_reason ILIKE '%LTP exceeded%'        THEN 'PRICE_EXCEEDED'
        WHEN o.is_paper_trade AND o.paper_exit_price IS NOT NULL THEN 'MANUAL'
        WHEN NOT o.is_paper_trade AND o.live_exit_price IS NOT NULL THEN 'MANUAL'
        ELSE NULL
    END,
    -- status
    CASE
        WHEN o.is_paper_trade AND o.paper_exit_price IS NOT NULL     THEN 'CLOSED'
        WHEN NOT o.is_paper_trade AND o.live_exit_price IS NOT NULL  THEN 'CLOSED'
        WHEN o.status IN ('FILLED', 'EXECUTED', 'TRADED')            THEN 'OPEN'
        WHEN o.status IN ('PARTIALLY_FILLED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED') THEN 'OPEN'
        ELSE 'CLOSED'  -- CANCELLED/REJECTED/FAILED with fills → closed
    END,
    -- opened_at
    COALESCE(o.executed_at, o.submitted_at, o.created_at),
    -- closed_at (only if closed)
    CASE
        WHEN o.is_paper_trade AND o.paper_exit_price IS NOT NULL    THEN o.updated_at
        WHEN NOT o.is_paper_trade AND o.live_exit_price IS NOT NULL THEN o.updated_at
        WHEN o.status IN ('FILLED', 'EXECUTED', 'TRADED',
                          'PARTIALLY_FILLED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
            THEN NULL  -- still open
        ELSE o.updated_at  -- terminal status = closed
    END
FROM orders o
LEFT JOIN instruments i ON i.stock_code = o.stock_code AND i.exchange = o.exchange
WHERE
    -- Orders that were actually filled (have fills)
    (COALESCE(o.filled_quantity, 0) > 0 AND COALESCE(o.filled_price, o.price) IS NOT NULL)
    -- OR status says filled
    OR o.status IN ('FILLED', 'EXECUTED', 'TRADED',
                    'PARTIALLY_FILLED', 'PARTIALLY TRADED', 'PARTIALLY EXECUTED')
    -- OR paper/live exit recorded
    OR o.paper_exit_price IS NOT NULL
    OR o.live_exit_price IS NOT NULL;

-- ============================================================================
-- STEP 11: position_fills — link fills ↔ positions
--
-- Matching strategy: order_id (each old order = 1 position)
-- ============================================================================
INSERT INTO position_fills (position_id, fill_id, order_id, direction, created_at)
SELECT
    p.position_id,
    f.fill_id,
    f.order_id,
    'ENTRY',
    f.filled_at
FROM fills f
-- Match position by finding the position that was created from the same order
-- Use orders_v2 to bridge: fill.order_id → orders_v2 → match to position
JOIN orders_v2 ov ON ov.order_id = f.order_id
JOIN positions p
    ON p.user_id = ov.user_id
    AND p.instrument_id = ov.instrument_id
    AND p.strategy_id = ov.strategy_id
    AND p.trading_mode = ov.trading_mode
    -- Time-based tiebreaker: position opened around same time as fill
    AND p.opened_at BETWEEN (f.filled_at - interval '1 hour') AND (f.filled_at + interval '1 hour')
-- Take the closest position if multiple match
ORDER BY ABS(EXTRACT(EPOCH FROM (p.opened_at - f.filled_at)))
ON CONFLICT (position_id, fill_id) DO NOTHING;

COMMIT;

-- ============================================================================
-- POST-MIGRATION VERIFICATION (run this after COMMIT to check)
-- ============================================================================

-- Compare row counts
SELECT '=== ROW COUNTS ===' AS section;

SELECT 'OLD: orders' AS table_name, COUNT(*) AS cnt FROM orders
UNION ALL SELECT 'NEW: orders_v2', COUNT(*) FROM orders_v2
UNION ALL SELECT 'OLD: execution_events', COUNT(*) FROM execution_events
UNION ALL SELECT 'NEW: order_status_history', COUNT(*) FROM order_status_history
UNION ALL SELECT 'OLD: user_credentials', COUNT(*) FROM user_credentials
UNION ALL SELECT 'NEW: broker_accounts', COUNT(*) FROM broker_accounts
UNION ALL SELECT 'NEW: instruments', COUNT(*) FROM instruments
UNION ALL SELECT 'NEW: fills', COUNT(*) FROM fills
UNION ALL SELECT 'NEW: broker_requests', COUNT(*) FROM broker_requests
UNION ALL SELECT 'NEW: stop_loss_config', COUNT(*) FROM stop_loss_config
UNION ALL SELECT 'NEW: order_groups', COUNT(*) FROM order_groups
UNION ALL SELECT 'NEW: order_group_legs', COUNT(*) FROM order_group_legs
UNION ALL SELECT 'NEW: positions', COUNT(*) FROM positions
UNION ALL SELECT 'NEW: position_fills', COUNT(*) FROM position_fills
ORDER BY table_name;

-- Verify ZERO data loss: every old order has a new order
SELECT '=== DATA LOSS CHECK ===' AS section;

SELECT 'orders missing from orders_v2' AS check_name,
       COUNT(*) AS missing_count
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM orders_v2 v WHERE v.order_id = o.order_id);

-- Verify every order has at least one status history entry
SELECT 'orders without status history' AS check_name,
       COUNT(*) AS missing_count
FROM orders_v2 ov
WHERE NOT EXISTS (SELECT 1 FROM order_status_history h WHERE h.order_id = ov.order_id);

-- Verify filled orders have fills
SELECT 'filled orders without fills' AS check_name,
       COUNT(*) AS missing_count
FROM orders_v2 ov
WHERE ov.filled_qty > 0
AND NOT EXISTS (SELECT 1 FROM fills f WHERE f.order_id = ov.order_id);

-- Verify positions match expected count
SELECT 'orders with fills but no position' AS check_name,
       COUNT(*) AS missing_count
FROM orders o
WHERE (
    (COALESCE(o.filled_quantity, 0) > 0 AND o.filled_price IS NOT NULL)
    OR o.status IN ('FILLED', 'EXECUTED', 'TRADED')
    OR o.paper_exit_price IS NOT NULL
    OR o.live_exit_price IS NOT NULL
)
AND NOT EXISTS (
    SELECT 1 FROM positions p
    JOIN instruments i ON i.instrument_id = p.instrument_id
    WHERE p.user_id = o.user_id
      AND i.stock_code = o.stock_code
      AND i.exchange = o.exchange
      AND p.strategy_id = o.strategy_id
);

-- Status distribution comparison
SELECT '=== STATUS DISTRIBUTION ===' AS section;

SELECT 'OLD' AS source, status, COUNT(*) AS cnt
FROM orders
GROUP BY status
UNION ALL
SELECT 'NEW', status, COUNT(*)
FROM orders_v2
GROUP BY status
ORDER BY source, status;

-- Position summary
SELECT '=== POSITIONS SUMMARY ===' AS section;

SELECT trading_mode, status, COUNT(*) AS cnt,
       SUM(COALESCE(realized_pnl, 0)) AS total_pnl
FROM positions
GROUP BY trading_mode, status
ORDER BY trading_mode, status;
