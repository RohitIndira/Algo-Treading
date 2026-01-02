-- Increase size of all VARCHAR columns in the orders table to prevent truncation errors
-- This migration sets all string columns to VARCHAR(100) for consistency and to avoid future issues

DO $$
DECLARE
    col_record RECORD;
    alter_sql TEXT;
    target_columns TEXT[] := ARRAY[
        'source', 'exchange', 'order_type', 'order_side', 'validity', 
        'status', 'rejection_reason', 'error_message', 'product_type',
        'strategy_id', 'event_id', 'strategy_name', 'symbol', 'indira_order_id',
        'indira_exchange_order_id', 'indira_parent_order_id', 'exchange_order_id',
        'parent_order_id', 'disclosed_quantity', 'trigger_price', 'price',
        'average_price', 'filled_quantity', 'pending_quantity', 'cancelled_quantity',
        'market_protection', 'tag', 'validity_ttl', 'validity_ttl_unit', 'order_timestamp',
        'exchange_timestamp', 'exchange_update_timestamp', 'variety', 'validity_ttl_quantity',
        'validity_ttl_quantity_unit', 'validity_start_time', 'validity_end_time',
        'order_identifier', 'exchange_order_identifier', 'parent_order_identifier',
        'order_source', 'exchange_order_source', 'order_identifier_type',
        'exchange_order_identifier_type', 'parent_order_identifier_type',
        'order_identifier_value', 'exchange_order_identifier_value',
        'parent_order_identifier_value', 'order_identifier_source',
        'exchange_order_identifier_source', 'parent_order_identifier_source',
        'order_identifier_updated_at', 'exchange_order_identifier_updated_at',
        'parent_order_identifier_updated_at', 'order_identifier_created_at',
        'exchange_order_identifier_created_at', 'parent_order_identifier_created_at',
        'order_identifier_metadata', 'exchange_order_identifier_metadata',
        'parent_order_identifier_metadata', 'order_identifier_status',
        'exchange_order_identifier_status', 'parent_order_identifier_status',
        'order_identifier_notes', 'exchange_order_identifier_notes',
        'parent_order_identifier_notes'
    ];
    col_name TEXT;
BEGIN
    -- First, handle all known columns with specific sizes
    FOR col_name IN SELECT unnest(target_columns) LOOP
        IF EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_name = 'orders' 
            AND column_name = col_name
            AND data_type = 'character varying'
            AND (character_maximum_length < 100 OR character_maximum_length IS NULL)
        ) THEN
            alter_sql := format('ALTER TABLE orders ALTER COLUMN %I TYPE VARCHAR(100)', col_name);
            EXECUTE alter_sql;
            RAISE NOTICE 'Altered column orders.%. Changed to VARCHAR(100)', col_name;
        END IF;
    END LOOP;

    -- Then handle any remaining VARCHAR columns that might have been missed
    FOR col_record IN 
        SELECT column_name
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND data_type = 'character varying'
        AND (character_maximum_length < 100 OR character_maximum_length IS NULL)
        AND column_name != ALL(target_columns)
    LOOP
        alter_sql := format('ALTER TABLE orders ALTER COLUMN %I TYPE VARCHAR(100)', col_record.column_name);
        EXECUTE alter_sql;
        RAISE NOTICE 'Altered column orders.%. Changed to VARCHAR(100)', col_record.column_name;
    END LOOP;
END $$;

-- Also update any enum-like columns that might be stored as VARCHAR
-- These are common candidates for being too small
DO $$
BEGIN
    -- Status field often needs more space for various states
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND column_name = 'status'
        AND data_type = 'character varying'
        AND (character_maximum_length < 50 OR character_maximum_length IS NULL)
    ) THEN
        EXECUTE 'ALTER TABLE orders ALTER COLUMN status TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.status to VARCHAR(50)';
    END IF;

    -- Order type and side often have longer values than expected
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND column_name = 'order_type'
        AND data_type = 'character varying'
        AND (character_maximum_length < 50 OR character_maximum_length IS NULL)
    ) THEN
        EXECUTE 'ALTER TABLE orders ALTER COLUMN order_type TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.order_type to VARCHAR(50)';
    END IF;

    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND column_name = 'order_side'
        AND data_type = 'character varying'
        AND (character_maximum_length < 50 OR character_maximum_length IS NULL)
    ) THEN
        EXECUTE 'ALTER TABLE orders ALTER COLUMN order_side TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.order_side to VARCHAR(50)';
    END IF;
END $$;
