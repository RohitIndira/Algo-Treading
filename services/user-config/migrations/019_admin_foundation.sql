-- Migration 019: Admin Console foundation (M1) — identity, sessions, audit.
--
-- Design (2026-08-31, spec M1.1/M1.2):
--   * admin_users     — server-side allow-list. Being a platform user is NOT
--                       enough; a row here (active=true) is what makes an
--                       admin. Seeded with S4450 (founding admin, dual-role:
--                       remains a normal trading user).
--   * admin_sessions  — opaque elevation tokens, stored HASHED (sha256).
--                       There is no signing secret to steal and nothing to
--                       forge: a valid token is 32 random bytes that exist
--                       only in the admin's client and (hashed) here.
--                       Expiry 30 min; revocation = set revoked_at.
--   * admin_audit     — append-only trail of every admin action, including
--                       DENIED attempts. The gateway's DB role must get
--                       INSERT+SELECT only (no UPDATE/DELETE) on this table
--                       so history cannot be rewritten even by a compromised
--                       gateway process — see GRANT note at the bottom.

BEGIN;

CREATE TABLE IF NOT EXISTS admin_users (
    user_id     TEXT PRIMARY KEY,
    active      BOOLEAN     NOT NULL DEFAULT true,
    added_by    TEXT        NOT NULL,
    note        TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO admin_users (user_id, added_by, note)
VALUES ('S4450', 'BOOTSTRAP', 'founding admin (dual-role: also a live trading user)')
ON CONFLICT (user_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS admin_sessions (
    id          BIGSERIAL   PRIMARY KEY,
    admin_id    TEXT        NOT NULL REFERENCES admin_users(user_id),
    token_hash  TEXT        NOT NULL UNIQUE,      -- hex(sha256(raw token)); raw token never stored
    ip          TEXT        NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_admin_sessions_live
    ON admin_sessions (token_hash)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS admin_audit (
    id           BIGSERIAL   PRIMARY KEY,
    admin_id     TEXT        NOT NULL,             -- who (claimed) — recorded even on DENIED
    action       TEXT        NOT NULL,             -- ELEVATE / ELEVATE_DENIED / REVOKE / <endpoint action>
    tier         TEXT        NOT NULL,             -- READ | CONFIRM | TYPED | AUTH
    target_user  TEXT        NOT NULL DEFAULT '',
    target_ref   TEXT        NOT NULL DEFAULT '',  -- strategy/order/position id
    params       JSONB,
    result       TEXT        NOT NULL,             -- OK | DENIED | FAILED
    detail       TEXT        NOT NULL DEFAULT '',
    self_action  BOOLEAN     NOT NULL DEFAULT false, -- admin acting on their own account
    ip           TEXT        NOT NULL DEFAULT '',
    session_id   BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_admin_audit_admin_time ON admin_audit (admin_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_admin_audit_target     ON admin_audit (target_user, created_at DESC);

-- ── Immutability hardening (run manually as superuser AFTER creating the
--    dedicated gateway role; documented here, not executed by migration
--    because role management is environment-specific):
--
--   CREATE ROLE gateway_admin LOGIN PASSWORD '<env-specific>';
--   GRANT CONNECT ON DATABASE trading_db TO gateway_admin;
--   GRANT SELECT, INSERT               ON admin_audit    TO gateway_admin;
--   GRANT SELECT                       ON admin_users    TO gateway_admin;
--   GRANT SELECT, INSERT, UPDATE       ON admin_sessions TO gateway_admin;
--   GRANT USAGE ON ALL SEQUENCES IN SCHEMA public        TO gateway_admin;
--   -- deliberately NO UPDATE/DELETE on admin_audit, NO INSERT/UPDATE on admin_users.

COMMIT;
