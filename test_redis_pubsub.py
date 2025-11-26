#!/usr/bin/env python3
"""
Test script to monitor Redis Pub/Sub channels for matching events
"""
import redis
import json
import sys
from datetime import datetime

def main():
    # Connect to Redis
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        print("✓ Connected to Redis successfully\n")
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        print("Make sure Redis is running on localhost:6379")
        sys.exit(1)
    
    # Subscribe to all user match channels
    pubsub = r.pubsub()
    
    # Subscribe to pattern for all users
    pubsub.psubscribe('user:*:matches')
    
    print("=" * 70)
    print("MONITORING REDIS PUB/SUB CHANNELS")
    print("=" * 70)
    print("Pattern: user:*:matches")
    print("Waiting for matching events...\n")
    print("Press Ctrl+C to stop\n")
    
    message_count = 0
    
    try:
        for message in pubsub.listen():
            if message['type'] == 'pmessage':
                message_count += 1
                print(f"\n{'=' * 70}")
                print(f"MESSAGE #{message_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'=' * 70}")
                print(f"Channel: {message['channel']}")
                print(f"Pattern: {message['pattern']}")
                
                try:
                    # Parse and pretty print JSON
                    data = json.loads(message['data'])
                    print("\nMessage Content:")
                    print(json.dumps(data, indent=2))
                    
                    # Highlight key fields
                    print(f"\n📊 Key Details:")
                    print(f"   Order ID: {data.get('order_id', 'N/A')}")
                    print(f"   User ID: {data.get('user_id', 'N/A')}")
                    print(f"   Stock Code: {data.get('stock_code', 'N/A')}")
                    print(f"   Symbol: {data.get('symbol', 'N/A')}")
                    print(f"   Match Score: {data.get('match_score', 'N/A')}")
                    print(f"   Price: {data.get('order_price', 'N/A')}")
                    print(f"   Strategy: {data.get('strategy_name', 'N/A')}")
                    
                except json.JSONDecodeError:
                    print(f"\nRaw Data: {message['data']}")
                
                print(f"{'=' * 70}")
                
            elif message['type'] == 'psubscribe':
                print(f"✓ Subscribed to pattern: {message['pattern']}")
    
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print(f"Monitoring stopped. Total messages received: {message_count}")
        print("=" * 70)
        pubsub.punsubscribe()
        pubsub.close()

if __name__ == "__main__":
    main()
