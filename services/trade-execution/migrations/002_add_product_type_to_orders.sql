-- Migration: 002_add_product_type_to_orders.sql
-- Purpose: Ensure `product_type` column exists on `orders` table
-- Created: 2026-01-02

BEGIN;

-- Add product_type column if it doesn't exist (safe to run multiple times)
ALTER TABLE IF EXISTS orders
ADD COLUMN IF NOT EXISTS product_type VARCHAR(20) DEFAULT 'INTRADAY';

COMMIT;

-- Notes:
-- - This migration is idempotent and compatible with PostgreSQL 11+.
-- - Run using your migration runner (e.g., golang-migrate, goose) or psql against
--   the target database:
--     psql -d <db> -f services/trade-execution/migrations/002_add_product_type_to_orders.sql
