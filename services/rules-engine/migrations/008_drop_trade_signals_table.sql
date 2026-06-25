-- 008_drop_trade_signals_table.sql
--
-- Drops the legacy `trade_signals` table created by 001_create_trade_signals_table.sql.
--
-- Context: 001 was created for the news-event-driven path
-- (consumer/handler.go → publisher/kafka_publisher.go →
--  repository/trade_signal_repository.go → INSERT INTO trade_signals).
--
-- That entire chain was deleted on 2026-06-25 ("Cat B trim" — see commit
-- 671f970). With no writers and no readers, the table is dead weight on
-- every database where 001 was applied. This migration removes it.
--
-- Safety: the table held legacy news-path orders only — never any Manthan
-- data. Manthan lives in manthan_positions / manthan_signal_decisions /
-- manthan_position_events (migrations 002, 003). Dropping trade_signals
-- has zero impact on Manthan state.
--
-- Apply order: AFTER 007. Idempotent — re-runs are no-ops.

BEGIN;

DROP TABLE IF EXISTS trade_signals CASCADE;

COMMIT;
