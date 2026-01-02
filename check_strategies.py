#!/usr/bin/env python3
import psycopg2
import json

try:
    conn = psycopg2.connect(
        host="localhost",
        database="trading_db",
        user="postgres",
        password="postgres",
        port="5432"
    )
    
    cur = conn.cursor()
    
    # Check all strategies for user
    cur.execute("""
        SELECT 
            s.strategy_id, 
            s.user_id, 
            s.strategy_name,
            tc.quantity,
            tc.order_type,
            tc.exchange,
            s.active
        FROM strategies s
        LEFT JOIN trade_configs tc ON s.strategy_id = tc.strategy_id
        WHERE s.user_id = 'ISPL19027'
        ORDER BY s.created_at DESC
    """)
    
    strategies = cur.fetchall()
    
    print("=" * 100)
    print("STRATEGIES FOR USER ISPL19027")
    print("=" * 100)
    
    for row in strategies:
        strategy_id, user_id, name, quantity, order_type, exchange, active = row
        print(f"Strategy ID: {strategy_id}")
        print(f"  Name: {name}")
        print(f"  Quantity: {quantity}")
        print(f"  Order Type: {order_type}")
        print(f"  Exchange: {exchange}")
        print(f"  Active: {active}")
        print()
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
