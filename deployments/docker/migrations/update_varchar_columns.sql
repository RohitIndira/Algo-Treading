-- Update specific columns that need larger VARCHAR sizes
DO $$
BEGIN
    -- Increase size for status field
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND column_name = 'status'
        AND data_type = 'character varying'
        AND character_maximum_length < 50
    ) THEN
        EXECUTE 'ALTER TABLE orders ALTER COLUMN status TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.status to VARCHAR(50)';
    END IF;

    -- Increase size for product_type field
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND column_name = 'product_type'
        AND data_type = 'character varying'
        AND character_maximum_length < 50
    ) THEN
        EXECUTE 'ALTER TABLE orders ALTER COLUMN product_type TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.product_type to VARCHAR(50)';
    END IF;

    -- Increase size for source field
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND column_name = 'source'
        AND data_type = 'character varying'
        AND character_maximum_length < 50
    ) THEN
        EXECUTE 'ALTER TABLE orders ALTER COLUMN source TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.source to VARCHAR(50)';
    END IF;

    -- Increase size for stop_loss_type field (from 10 to 20)
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND column_name = 'stop_loss_type'
        AND data_type = 'character varying'
        AND character_maximum_length < 20
    ) THEN
        EXECUTE 'ALTER TABLE orders ALTER COLUMN stop_loss_type TYPE VARCHAR(20)';
        RAISE NOTICE 'Altered column orders.stop_loss_type to VARCHAR(20)';
    END IF;

    -- Increase size for indira_order_id field
    IF EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'orders' 
        AND column_name = 'indira_order_id'
        AND data_type = 'character varying'
        AND character_maximum_length < 100
    ) THEN
        EXECUTE 'ALTER TABLE orders ALTER COLUMN indira_order_id TYPE VARCHAR(100)';
        RAISE NOTICE 'Altered column orders.indira_order_id to VARCHAR(100)';
    END IF;
END $$;
