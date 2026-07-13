-- Portfolio svc — read-only role on positions_db.
--
-- Defence in depth per §7 of docs/portfolio_service_design.md: the query
-- side of the CQRS split must not carry INSERT/UPDATE privileges on the
-- canonical positions_db. positions svc owns writes; portfolio svc reads.
--
-- Idempotent: safe to re-run. Uses DO blocks + IF NOT EXISTS to avoid
-- ROLE / GRANT churn on repeat migrations.

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'positions_reader') THEN
        -- LOGIN + no CREATEDB / no CREATEROLE. Password set out-of-band via
        -- ALTER ROLE (deployment secret, not in source).
        CREATE ROLE positions_reader LOGIN;
    END IF;
END
$$;

-- Connect + read-only privileges on positions_db objects. Ownership of
-- tables stays with the migration owner (postgres); positions_reader
-- gets SELECT only.
GRANT CONNECT ON DATABASE positions_db TO positions_reader;
GRANT USAGE   ON SCHEMA public         TO positions_reader;
GRANT SELECT  ON ALL TABLES  IN SCHEMA public TO positions_reader;
GRANT SELECT  ON ALL SEQUENCES IN SCHEMA public TO positions_reader;

-- Future tables (position_events snapshots, materialised views, etc.)
-- auto-grant SELECT to positions_reader on CREATE.
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES    TO positions_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON SEQUENCES TO positions_reader;

-- Belt + braces: explicitly REVOKE writes on the two tables we know about.
-- Redundant with the SELECT-only grants above but documents intent for
-- reviewers and catches drift if someone GRANTs ALL by mistake later.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON positions      FROM positions_reader;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON position_events FROM positions_reader;
