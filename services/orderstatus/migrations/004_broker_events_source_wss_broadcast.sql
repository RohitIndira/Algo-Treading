-- Migration 004: allow source = 'WSS_BROADCAST' on broker_events
--
-- The dealer broadcast listener (services/orderstatus/internal/wss/broadcast.go)
-- writes frames it receives on wss://.../order-notify/websocket/broadcast with
-- source = 'WSS_BROADCAST' so forensics can tell which socket saw an event
-- first. Dedup is unchanged: UNIQUE (broker_order_id, event_seq) covers the
-- per-user socket, the REST polls and the broadcast alike — the same broker
-- event arriving on two sockets is one row.
--
-- Idempotent: drops and re-creates the CHECK constraint. No data change.
-- Needed only when BROADCAST_FEED_MODE is own_orders or all; shadow mode
-- writes nothing and does not require it.

BEGIN;

ALTER TABLE broker_events DROP CONSTRAINT IF EXISTS chk_broker_events_source;

-- NOT VALID: new rows are checked at once, existing rows are not scanned under
-- the ACCESS EXCLUSIVE lock. VALIDATE then scans with only a SHARE UPDATE
-- EXCLUSIVE lock, so the live writers (per-user socket, REST polls) are not
-- blocked for the duration of the table scan.
ALTER TABLE broker_events ADD CONSTRAINT chk_broker_events_source CHECK (source IN (
    'WSS', 'WSS_BROADCAST', 'REST_ORDERBOOK', 'REST_RECONCILER', 'REST_TRADEBOOK'
)) NOT VALID;

ALTER TABLE broker_events VALIDATE CONSTRAINT chk_broker_events_source;

COMMENT ON COLUMN broker_events.source IS
    'Where the event was observed: WSS (per-user socket), WSS_BROADCAST (dealer broadcast socket), REST_ORDERBOOK, REST_RECONCILER, REST_TRADEBOOK.';

COMMIT;
