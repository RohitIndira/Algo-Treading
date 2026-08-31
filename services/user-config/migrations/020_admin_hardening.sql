-- Migration 020: Admin foundation hardening — immutability by physics.
--
-- Two layers (spec M1.2 guardrail):
--   1. TRIGGER guard on admin_audit: UPDATE/DELETE raise an exception for
--      EVERY role, superuser included. Effective immediately, everywhere —
--      an app bug or a careless psql session cannot rewrite history without
--      first deliberately dropping the trigger (which is itself loud).
--   2. Dedicated least-privilege role `gateway_admin` for the gateway's
--      admin-store connection: INSERT+SELECT only on admin_audit, no
--      UPDATE/DELETE anywhere it doesn't need. Password is set OUTSIDE the
--      migration (ALTER ROLE ... PASSWORD, environment-specific — never in
--      the repo). The gateway uses it when ADMIN_DB_USER/ADMIN_DB_PASSWORD
--      are present; otherwise it falls back to the shared handle with a
--      loud warning at boot.

BEGIN;

-- ── Layer 1: append-only by trigger ─────────────────────────────────────
CREATE OR REPLACE FUNCTION admin_audit_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'admin_audit is append-only: % refused (id=%)', TG_OP, OLD.id
        USING HINT = 'The admin audit trail is immutable by design (spec M1.2).';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_admin_audit_append_only ON admin_audit;
CREATE TRIGGER trg_admin_audit_append_only
    BEFORE UPDATE OR DELETE ON admin_audit
    FOR EACH ROW EXECUTE FUNCTION admin_audit_append_only();

-- ── Layer 2: least-privilege role ───────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'gateway_admin') THEN
        CREATE ROLE gateway_admin LOGIN;
    END IF;
END $$;

GRANT CONNECT ON DATABASE trading_db TO gateway_admin;
GRANT USAGE   ON SCHEMA public       TO gateway_admin;
GRANT SELECT                 ON admin_users    TO gateway_admin;
GRANT SELECT, INSERT, UPDATE ON admin_sessions TO gateway_admin;  -- UPDATE = revocation only
GRANT SELECT, INSERT         ON admin_audit    TO gateway_admin;  -- NO update/delete
GRANT USAGE ON SEQUENCE admin_audit_id_seq, admin_sessions_id_seq TO gateway_admin;
-- deliberately NOTHING else: no strategies, no orders, no credentials tables.

COMMIT;

-- Post-migration, run once per environment (password never in repo):
--   ALTER ROLE gateway_admin PASSWORD '<generated>';
