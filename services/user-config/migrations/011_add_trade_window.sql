-- ============================================================================
-- Migration 011: Trade Window
-- Adds per-strategy trade window: orders are only placed when the current
-- IST time falls within [trade_window_start, trade_window_end].
--
-- Format: "HH:MM" 24-hour (e.g. "09:15", "15:00")
-- Empty string / NULL means no window restriction (always fire).
-- Valid range: 09:15 to 15:00 (NSE/BSE market hours).
-- ============================================================================

ALTER TABLE trade_configs
    ADD COLUMN IF NOT EXISTS trade_window_start VARCHAR(5) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS trade_window_end   VARCHAR(5) NOT NULL DEFAULT '';

COMMENT ON COLUMN trade_configs.trade_window_start IS
    'Start of trade window in HH:MM (IST, 24h). Empty = no restriction.';
COMMENT ON COLUMN trade_configs.trade_window_end IS
    'End of trade window in HH:MM (IST, 24h). Empty = no restriction.';
