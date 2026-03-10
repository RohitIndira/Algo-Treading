-- Migration: Remove deprecated Odin API fields (optional - run after complete migration)
-- Description: Removes legacy Odin fields once migration to Indira is complete
-- Date: 2025-12-12
-- WARNING: Only run this after confirming Indira migration is complete and stable!

-- Drop Odin-specific columns (optional cleanup after migration is stable)
-- ALTER TABLE IF EXISTS orders DROP COLUMN IF EXISTS odin_order_id;
-- ALTER TABLE IF EXISTS orders DROP COLUMN IF EXISTS odin_response;

-- Drop related indexes
-- DROP INDEX IF EXISTS idx_orders_odin_order_id;

-- Note: Keep these commented until Indira migration is fully tested and validated in production
