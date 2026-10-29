-- 018: client login history (2026-10-29).
--
-- WHY: user_credentials keeps ONE row per client — only the LATEST broker
-- token and its updated_at. The admin console could answer "when did this
-- client last log in?" but never "how often, from where, and did the
-- session cover the market hours?". FIV99's dead session on 2026-10-29
-- (10 rejected buys, last login 27 Oct 09:14 IST) was only visible as a
-- stale timestamp. This table keeps one row per credential refresh.
--
-- WRITER: user-config (the single writer of user_credentials) inserts a
-- row inside StoreIndiraCredentials — both login paths funnel there:
--   * POST /api/v1/auth/credentials (SSO login flow in the web app)
--   * api-gateway token auto-capture (a newer JWT seen on any request)
-- READER: api-gateway admin console (GET /admin/logins, /admin/activity).
--
-- token_issued_at is the JWT "iat" claim = the moment the broker issued
-- the session, i.e. the real login time; recorded_at is when WE saw it.
-- token_fingerprint (sha256 prefix of the plaintext token) de-duplicates
-- the same token being pushed twice — the web app re-posts on every page
-- load; those are not logins.
--
-- Idempotent. Apply to execution_db:
--   docker exec -i algo-dev-postgres psql -U postgres -d execution_db \
--     -v ON_ERROR_STOP=1 < services/trade-execution/migrations/018_user_login_events.sql

CREATE TABLE IF NOT EXISTS user_login_events (
    id                BIGSERIAL PRIMARY KEY,
    user_id           VARCHAR(50)  NOT NULL,
    indira_user_id    VARCHAR(100),
    app_id            VARCHAR(100),
    source            VARCHAR(20),           -- what the caller declared: SSO | WEB | IOS | AND
    login_source      VARCHAR(20),           -- JWT loginSource claim (SSO | APP | ...), '' when absent
    kind              VARCHAR(16)  NOT NULL DEFAULT 'LOGIN',  -- LOGIN | BACKFILL
    token_issued_at   TIMESTAMPTZ,           -- JWT iat (broker login time); NULL when the token has none
    token_expires_at  TIMESTAMPTZ,           -- JWT exp
    token_fingerprint VARCHAR(32) NOT NULL,  -- sha256(token) hex prefix; 'legacy:' + md5 prefix for backfill
    recorded_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_user_login_events_kind CHECK (kind IN ('LOGIN','BACKFILL')),
    CONSTRAINT uq_user_login_events_token UNIQUE (user_id, token_fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_user_login_events_user_time
    ON user_login_events (user_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_login_events_time
    ON user_login_events (recorded_at DESC);

-- Backfill: the last known login of every client as of this migration, so
-- the history does not start empty. updated_at is timestamp WITHOUT tz
-- written by the DB's now() — the server runs on UTC, so read it as UTC.
INSERT INTO user_login_events
    (user_id, indira_user_id, app_id, source, login_source, kind,
     token_issued_at, token_expires_at, token_fingerprint, recorded_at)
SELECT user_id, indira_user_id, indira_app_id, indira_source, '', 'BACKFILL',
       NULL, NULL, 'legacy:' || left(md5(indira_bearer_token), 24),
       updated_at AT TIME ZONE 'UTC'
  FROM user_credentials
 WHERE indira_bearer_token <> ''
ON CONFLICT (user_id, token_fingerprint) DO NOTHING;

-- The console reads through the least-privilege gateway role where present.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'gateway_admin') THEN
        GRANT SELECT ON user_login_events TO gateway_admin;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'qa_readonly') THEN
        GRANT SELECT ON user_login_events TO qa_readonly;
    END IF;
END $$;

COMMENT ON TABLE user_login_events IS
    'One row per broker-credential refresh seen by the platform (client login history). Written by user-config.';
