-- 002_hft_runtime_state.sql
-- Durable per-strategy runtime/terminal state for HFT strategies.
--
-- Problem this solves: when an HFT strategy finishes, the engine only kept
-- its final snapshot in an in-memory cache with a 10-minute TTL. After the
-- TTL (or an engine restart) GET /state returned 404 "strategy not running"
-- and the dashboard fell back to showing "Start Engine" — as if a completed
-- strategy had never run.
--
-- This table is the source of truth for "what happened to strategy X":
--   - written RUNNING on Entry
--   - written COMPLETED | HALTED | STOPPED on runner exit, with the full
--     final snapshot as JSONB so /state can replay it forever.

CREATE TABLE IF NOT EXISTS hft_runtime_state (
    strategy_id   UUID         PRIMARY KEY,
    user_id       VARCHAR(50)  NOT NULL,
    symbol        VARCHAR(30)  NOT NULL,
    mode          VARCHAR(10)  NOT NULL,            -- PAPER | LIVE
    status        VARCHAR(20)  NOT NULL,            -- RUNNING | COMPLETED | HALTED | STOPPED
    buy_position  INT          NOT NULL DEFAULT 0,
    sell_position INT          NOT NULL DEFAULT 0,
    snapshot      JSONB        NOT NULL,            -- full state.Strategy, replayed by /state
    started_at    TIMESTAMPTZ,
    ended_at      TIMESTAMPTZ,                      -- set on terminal transition
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Dashboard "my strategies" listing by user.
CREATE INDEX IF NOT EXISTS idx_hft_runtime_user
    ON hft_runtime_state(user_id, updated_at DESC);
