-- 004_signal_decisions.sql
--
-- Durable audit trail of every strategy-evaluation decision.
--
-- Before this table the only record of a rejected signal was a log line, so
-- "why didn't my strategy fire on that news?" could only be answered by grepping
-- pm2 logs — and those logs are masked (STRATEGY_NAME_LOG_OVERRIDE strips
-- strategy_id and rewrites strategy_name), so even that was often impossible.
--
-- One row per (news event x strategy) evaluation, whether it matched or not.
-- Lives in trading_db next to trade_signals so a decision and the signal it
-- produced can be joined without a cross-database query.

CREATE TABLE IF NOT EXISTS signal_decisions (
    decision_id   BIGSERIAL PRIMARY KEY,
    event_id      VARCHAR(255) NOT NULL,
    user_id       VARCHAR(255) NOT NULL,
    strategy_id   UUID         NOT NULL,
    strategy_name VARCHAR(255) NOT NULL DEFAULT '',

    symbol        VARCHAR(50),
    stock_code    BIGINT,
    exchange      VARCHAR(20),

    -- MATCHED | REJECTED
    outcome       VARCHAR(20)  NOT NULL,
    -- Pipeline stage that decided: MATCH | COMPLIANCE | SIZING | RISK | CAP | WINDOW
    stage         VARCHAR(20)  NOT NULL,
    -- Machine-readable code, e.g. PCT_CHANGE_ABOVE_MAX, DPR_UPPER_BREACH.
    -- Empty for a clean match.
    reason_code   VARCHAR(64)  NOT NULL DEFAULT '',
    -- Human-readable sentence rendered directly in the admin UI.
    reason_detail TEXT         NOT NULL DEFAULT '',

    -- What was compared, so the UI can render "limit X, actual Y" without
    -- having to re-derive it from the strategy config.
    limit_value   NUMERIC(20,4),
    actual_value  NUMERIC(20,4),

    -- Full evaluator output. Cheap to store and the fastest way to see which
    -- specific condition killed a match.
    matched_conditions TEXT[],
    failed_conditions  TEXT[],
    condition_scores   JSONB,
    match_score        NUMERIC(5,2),

    -- News snapshot exactly as evaluated. Stored rather than re-read later so
    -- the audit row stays true even if the feed corrects the event afterwards.
    impact_score  INT,
    sentiment     VARCHAR(50),
    news_category VARCHAR(255),
    pct_change    NUMERIC(10,4),
    ltp           NUMERIC(15,2),

    -- Set only when outcome = MATCHED and an order was actually published.
    order_id      VARCHAR(255),

    correlation_id VARCHAR(64),
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Admin "show me this user's day" — the primary access path.
CREATE INDEX IF NOT EXISTS idx_sigdec_user_date
    ON signal_decisions (user_id, created_at DESC);

-- Per-strategy drill-down and match-rate analytics.
CREATE INDEX IF NOT EXISTS idx_sigdec_strategy_date
    ON signal_decisions (strategy_id, created_at DESC);

-- "What did every strategy do with this one news event?"
CREATE INDEX IF NOT EXISTS idx_sigdec_event
    ON signal_decisions (event_id);

-- Reason histogram / funnel in the decisions explorer.
CREATE INDEX IF NOT EXISTS idx_sigdec_outcome
    ON signal_decisions (outcome, reason_code, created_at DESC);

-- Order timeline join. Partial: most rows are rejections with no order.
CREATE INDEX IF NOT EXISTS idx_sigdec_order
    ON signal_decisions (order_id) WHERE order_id IS NOT NULL;
