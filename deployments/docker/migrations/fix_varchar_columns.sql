-- Fix for 'value too long for type character varying(10)' error
-- This migration increases the size of the source column in the orders table

-- First, check if the column exists and its current definition
DO $$
BEGIN
    -- Check if the column exists
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'orders' AND column_name = 'source') THEN
        
        -- Check if the column needs to be altered
        IF EXISTS (SELECT 1 FROM information_schema.columns 
                  WHERE table_name = 'orders' 
                  AND column_name = 'source' 
                  AND character_maximum_length = 10) THEN
            
            -- Alter the column to increase its size
            EXECUTE 'ALTER TABLE orders ALTER COLUMN source TYPE VARCHAR(50)';
            RAISE NOTICE 'Altered column orders.source to VARCHAR(50)';
            
        END IF;
    END IF;
END $$;

-- Also check and update any other potentially problematic columns
DO $$
BEGIN
    -- Check and update exchange column if needed
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'orders' 
              AND column_name = 'exchange' 
              AND character_maximum_length = 20) THEN
        
        EXECUTE 'ALTER TABLE orders ALTER COLUMN exchange TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.exchange to VARCHAR(50)';
    END IF;
    
    -- Check and update order_type column if needed
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'orders' 
              AND column_name = 'order_type' 
              AND character_maximum_length = 20) THEN
        
        EXECUTE 'ALTER TABLE orders ALTER COLUMN order_type TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.order_type to VARCHAR(50)';
    END IF;
    
    -- Check and update order_side column if needed
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'orders' 
              AND column_name = 'order_side' 
              AND character_maximum_length = 20) THEN
        
        EXECUTE 'ALTER TABLE orders ALTER COLUMN order_side TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.order_side to VARCHAR(50)';
    END IF;
    
    -- Check and update status column if needed
    IF EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'orders' 
              AND column_name = 'status' 
              AND character_maximum_length = 20) THEN
        
        EXECUTE 'ALTER TABLE orders ALTER COLUMN status TYPE VARCHAR(50)';
        RAISE NOTICE 'Altered column orders.status to VARCHAR(50)';
    END IF;
END $$;
