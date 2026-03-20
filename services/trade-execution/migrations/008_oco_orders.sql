-- OCO (One-Cancels-the-Other) order support
-- Version: 8.0
-- Adds fields to link orders within an OCO group.

-- oco_group_id: UUID linking all orders (entry + SL leg + TP leg) in one OCO group.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS oco_group_id UUID;

-- oco_role: the order's role within the OCO group (ENTRY, SL_LEG, TP_LEG).
ALTER TABLE orders ADD COLUMN IF NOT EXISTS oco_role VARCHAR(20);

-- parent_order_id: child leg orders point back to their entry order.
ALTER TABLE orders ADD COLUMN IF NOT EXISTS parent_order_id UUID;

-- Index for fast lookup of all orders in an OCO group (used for restart recovery).
CREATE INDEX IF NOT EXISTS idx_orders_oco_group ON orders(oco_group_id)
    WHERE oco_group_id IS NOT NULL;

-- Index for finding active OCO orders (non-terminal with oco_group_id set).
CREATE INDEX IF NOT EXISTS idx_orders_oco_active ON orders(oco_group_id, status)
    WHERE oco_group_id IS NOT NULL
    AND status NOT IN ('CANCELLED', 'REJECTED', 'A.REJECTED', 'FAILED');
