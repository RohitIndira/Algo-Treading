-- 011_drop_manthan_position_events.sql
--
-- Drops the legacy `manthan_position_events` table created by
-- 003_signal_decisions_and_position_events.sql and altered by
-- 005_manual_interference_event_types.sql.
--
-- Context: rules-engine used to write manthan_position_events from the
-- PositionProjector (deleted 2026-07-10 in the "signal-engine-only"
-- refactor). Positions svc — shipped in the design/positions-cqrs
-- branch (P.A → P.G) — is the new authoritative writer of
-- position lifecycle events, into `positions_db.position_events`.
--
-- Grep across services/, pkg/ finds ZERO runtime INSERT/UPDATE/SELECT on
-- manthan_position_events. Every remaining reference is either
--   - A docstring in rules-engine/internal/manthan/publisher.go
--     documenting what the file NO LONGER does.
--   - A comment in rules-engine/internal/manthan/wire.go pointing at
--     the new positions svc as the successor writer.
--   - A comment in trade-execution/internal/manthan/event_publisher.go
--     describing the OLD event_type CHECK constraint (its own
--     execution_events table now serves that purpose).
--   - The migrations 003 (CREATE TABLE) and 005 (ALTER CHECK) that
--     the current audit is retiring.
--
-- Safety: the table was empty locally (0 rows). On any environment where
-- it holds data, that data corresponds to historical events for the
-- deleted projector code path and is superseded by
-- positions_db.position_events. Cross-reference before running on
-- production if any consumer still depends on it.
--
-- Apply order: AFTER 010. Idempotent — re-runs are no-ops.

BEGIN;

DROP TABLE IF EXISTS manthan_position_events CASCADE;

COMMIT;
