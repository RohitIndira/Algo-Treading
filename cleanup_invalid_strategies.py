#!/usr/bin/env python3
import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        database="trading_db",
        user="postgres",
        password="postgres",
        port="5432"
    )
    
    cur = conn.cursor()
    
    # Find strategies with no trade_config (incomplete)
    cur.execute("""
        SELECT s.strategy_id
        FROM strategies s
        WHERE s.user_id = 'ISPL19027'
        AND NOT EXISTS (SELECT 1 FROM trade_configs WHERE strategy_id = s.strategy_id)
    """)
    
    invalid_strategies = cur.fetchall()
    
    print(f"Found {len(invalid_strategies)} incomplete strategies to delete:")
    
    for (strategy_id,) in invalid_strategies:
        print(f"  Deleting: {strategy_id}")
        # Delete from trade_configs first (if any)
        cur.execute("DELETE FROM trade_configs WHERE strategy_id = %s", (strategy_id,))
        # Delete from strategy_conditions
        cur.execute("DELETE FROM strategy_conditions WHERE strategy_id = %s", (strategy_id,))
        # Delete from risk_limits
        cur.execute("DELETE FROM risk_limits WHERE strategy_id = %s", (strategy_id,))
        # Delete from strategies
        cur.execute("DELETE FROM strategies WHERE strategy_id = %s", (strategy_id,))
    
    conn.commit()
    
    print(f"\n✓ Successfully deleted {len(invalid_strategies)} incomplete strategies")
    print("\nRemaining valid strategies:")
    
    cur.execute("""
        SELECT 
            s.strategy_id, 
            s.strategy_name,
            tc.quantity,
            s.active
        FROM strategies s
        LEFT JOIN trade_configs tc ON s.strategy_id = tc.strategy_id
        WHERE s.user_id = 'ISPL19027'
        ORDER BY s.created_at DESC
    """)
    
    for strategy_id, name, quantity, active in cur.fetchall():
        print(f"  ✓ {name} - Qty: {quantity}, Active: {active}")
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
