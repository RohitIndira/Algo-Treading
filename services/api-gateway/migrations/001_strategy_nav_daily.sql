-- 001_strategy_nav_daily.sql  (stockk_market)
--
-- WHY
--   The deployed-strategy details page needs the TRUE equity curve of the
--   user's own deployment. Nothing recorded per-deployment daily NAV, so the
--   chart (and drawdown/Sharpe/CAGR derived from it) could only show the
--   reference track-record rebased at the deploy date. Historical per-symbol
--   closes don't exist anywhere in the stack, so the true curve cannot be
--   backfilled — it accrues from the day this table starts filling.
--
-- WRITER: api-gateway's NAV snapshot job (cmd/nav_snapshot.go) — the same
--   NAV math the live page serves (positions_db lots + live LTPs), upserted
--   per (strategy_id, date). Intraday runs overwrite today's row; the last
--   write after close is the settled point (same semantics as
--   sync_nifty_benchmark.sh's partial-candle rule).
--
-- IDEMPOTENT: re-runnable.

BEGIN;

CREATE TABLE IF NOT EXISTS strategy_nav_daily (
    strategy_id       TEXT   NOT NULL,
    user_id           TEXT   NOT NULL,
    date              DATE   NOT NULL,
    deployed_capital  BIGINT NOT NULL,
    net_pnl_amount    BIGINT NOT NULL,
    net_pnl_pct       NUMERIC(10,4) NOT NULL,
    realized_amount   BIGINT NOT NULL DEFAULT 0,
    unrealized_amount BIGINT NOT NULL DEFAULT 0,
    open_positions    INT    NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy_id, date)
);

CREATE INDEX IF NOT EXISTS idx_strategy_nav_daily_user ON strategy_nav_daily (user_id, date DESC);

COMMIT;
