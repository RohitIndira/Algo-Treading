-- Fix for remaining VARCHAR columns that might be too small
-- This migration increases the size of several columns in the orders table
-- to prevent potential data truncation errors

DO $$
DECLARE
    alter_sql TEXT;
    col_name TEXT;
    col_def TEXT;
    target_columns TEXT[][] := ARRAY[
        ['order_side', '20'],
        ['validity', '20'],
        ['exchange', '10'],  -- Exchange codes are typically short (NSE, BSE, etc.)
        ['order_type', '20']
    ];
BEGIN
    -- Process each target column
    FOR i IN 1..array_length(target_columns, 1) LOOP
        col_name := target_columns[i][1];
        col_def := target_columns[i][2];
        
        -- Check if the column exists and has a length less than or equal to 10
        IF EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_name = 'orders' 
            AND column_name = col_name
            AND data_type = 'character varying' 
            AND character_maximum_length <= 10
        ) THEN
            alter_sql := format('ALTER TABLE orders ALTER COLUMN %I TYPE VARCHAR(%s)', col_name, col_def);
            EXECUTE alter_sql;
            RAISE NOTICE 'Altered column orders.%. Changed to VARCHAR(%)', col_name, col_def;
        END IF;
    END LOOP;
END $$;
