-- Migration 002: widen buy_sell to accommodate REST-source events
--
-- Migration 001 sized `buy_sell CHAR(1)` for WSS payloads which use "1"/"2".
-- Codify's REST orderbook returns "BUY"/"SELL" instead — 3-4 characters,
-- CHAR(1) rejects them. Rather than translate one side to match the other,
-- we widen the column and let each writer store whatever the broker gave us.
-- Consumers normalise on read.
--
-- Non-destructive: existing WSS-sourced "1"/"2" values fit VARCHAR(8) fine.

BEGIN;

ALTER TABLE broker_events
    ALTER COLUMN buy_sell TYPE VARCHAR(8);

COMMENT ON COLUMN broker_events.buy_sell IS
    'Direction. WSS path stores "1" (BUY) or "2" (SELL). REST orderbook path stores "BUY"/"SELL". Consumers should normalise.';

COMMIT;
