-- 021: read-only admin role (2026-09-25).
--
-- WHY: external testers need to verify every admin READ endpoint against
-- production data without any ability to mutate (square-off, ghost-heal,
-- pause/resume all move real money on funded accounts). Role is enforced
-- centrally in the gateway's Route() wrapper: role='read_only' + any
-- TierConfirm/TierTyped route → 403 E_ADMIN_READ_ONLY + audit row.
--
-- Idempotent: safe to re-run (the admin test suite applies it on boot).

ALTER TABLE admin_users
    ADD COLUMN IF NOT EXISTS role text NOT NULL DEFAULT 'full';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'chk_admin_users_role'
    ) THEN
        ALTER TABLE admin_users
            ADD CONSTRAINT chk_admin_users_role CHECK (role IN ('full', 'read_only'));
    END IF;
END $$;
