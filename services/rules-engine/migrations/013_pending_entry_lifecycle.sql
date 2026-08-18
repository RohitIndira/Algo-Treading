-- 013_pending_entry_lifecycle.sql
--
-- Confirmation-driven position lifecycle (2026-08-18 phantom fix):
--   PENDING_ENTRY — persisted at entry DISPATCH (was ACTIVE: dispatches that
--                   died at trade-execution left phantom ACTIVE rows that
--                   polluted sector/mcap caps and were resurrected by every
--                   rehydrate — 29 phantoms blocked both users' SMALL bucket).
--   ACTIVE        — set ONLY by PersistFillConfirmed on a broker-confirmed
--                   fill (position.events POSITION_OPENED).
--   EXPIRED       — orphan scanner retires PENDING_ENTRY rows >24h old
--                   (dispatches whose fills never confirmed).
-- Exits are likewise booked only on confirmation (POSITION_EXITED) — the
-- trail-cross now just ORDERS the exit (EXIT_PENDING is in-memory only).

ALTER TABLE manthan_positions
    DROP CONSTRAINT IF EXISTS chk_manthan_pos_status;
ALTER TABLE manthan_positions
    ADD CONSTRAINT chk_manthan_pos_status CHECK (status IN (
        -- pre-existing set (migration 003) …
        'ACTIVE', 'PARTIAL_ACTIVE', 'EXITED', 'COOLDOWN',
        -- … plus the confirmation-driven lifecycle states
        'PENDING_ENTRY', 'EXPIRED'
    ));
