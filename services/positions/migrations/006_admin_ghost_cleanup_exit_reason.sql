-- 006: allow ADMIN_GHOST_CLEANUP as an exit_reason.
--
-- M7.5 (admin console) heals confirmed ghost positions — book ACTIVE,
-- broker empty (the IOLCP class) — by closing the book rows from
-- broker-verified evidence. Those exits need their own reason so they
-- are never mistaken for strategy or manual exits in P&L attribution.
ALTER TABLE positions DROP CONSTRAINT IF EXISTS chk_positions_exit_reason;
ALTER TABLE positions ADD CONSTRAINT chk_positions_exit_reason
    CHECK (exit_reason IS NULL OR exit_reason IN
        ('SL_TRIGGER','MANUAL_EXIT','STRATEGY_EXIT','LIQUIDATION','ADMIN_GHOST_CLEANUP'));
