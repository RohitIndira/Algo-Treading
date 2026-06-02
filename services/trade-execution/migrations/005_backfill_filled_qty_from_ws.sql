-- Migration 005: backfill filled_quantity from broker WS data (one-time data fix)
--
-- Bug: handleStatusUpdate derived the cumulative fill only from the broker's
-- TradedQTY. Indira sometimes reports OrderStatus=EXECUTED/TRADED with an EMPTY
-- TradedQTY (the qty rides in OrderOriginalQty/PendingQty), so a genuinely-filled
-- order was recorded with filled_quantity = 0. That zero then:
--   * defeated the deactivation bulk-cancel guard (filled_quantity > 0), so the
--     filled position was CANCELLED instead of squared off, and
--   * made enrichPositions treat the (real) broker position as broker-direct,
--     showing it under "USER DIRECT" instead of its strategy.
--
-- The code fix (resolveFilledQty) prevents new occurrences. This migration repairs
-- existing rows by recomputing filled_quantity from the stored broker_ws_data,
-- using the same precedence as resolveFilledQty:
--   OrderOriginalQty - PendingQty  ->  OrderOriginalQty  ->  placed quantity.
--
-- Scope guard: only rows the broker marked EXECUTED/TRADED, that we recorded with
-- filled_quantity = 0, and whose TradedQTY was empty/non-numeric (the exact bug
-- signature). An explicit numeric TradedQTY — including "0" — is left untouched.
--
-- Entries only: we restrict to entry orders (not square-off orders, not OCO
-- SL_LEG/TP_LEG exit legs) — the same orders enrichPositions uses for attribution.
-- Backfilling an exit leg would not change attribution (enrichPositions skips
-- them) and could make a closed exit leg look like an open position to
-- GetOpenLiveEntriesByUserSymbol.
--
-- Idempotent: after the update filled_quantity > 0, so a re-run matches nothing.
-- Note: this only restores strategy attribution. It does NOT reconstruct exit
-- price/time/reason — that data was never captured for these closed round-trips.

BEGIN;

UPDATE orders o
SET    filled_quantity = CASE
           WHEN (ws->>'OrderOriginalQty')::int
                - COALESCE(NULLIF(ws->>'PendingQty', '')::int, 0) > 0
             THEN (ws->>'OrderOriginalQty')::int
                - COALESCE(NULLIF(ws->>'PendingQty', '')::int, 0)
           WHEN (ws->>'OrderOriginalQty')::int > 0
             THEN (ws->>'OrderOriginalQty')::int
           ELSE o.quantity
       END,
       updated_at = NOW()
FROM   (SELECT order_id, broker_ws_data::jsonb AS ws FROM orders) j
WHERE  o.order_id = j.order_id
  AND  o.is_paper_trade = false
  AND  o.filled_quantity = 0
  AND  o.is_square_off_order = false
  AND  (o.oco_role IS NULL OR o.oco_role NOT IN ('SL_LEG', 'TP_LEG'))
  AND  o.broker_ws_data IS NOT NULL
  AND  (j.ws->>'OrderStatus') IN ('EXECUTED', 'TRADED')
  AND  COALESCE(j.ws->>'TradedQTY', '') !~ '^[0-9]+$';

COMMIT;
