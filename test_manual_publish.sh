#!/bin/bash
# Test script to manually publish to Redis and verify pub/sub is working

echo "======================================================================"
echo "Redis Pub/Sub Manual Test"
echo "======================================================================"
echo ""

# Check if redis-cli is available
if ! command -v redis-cli &> /dev/null; then
    echo "❌ redis-cli not found. Please install Redis CLI tools."
    exit 1
fi

# Test Redis connection
echo "1. Testing Redis connection..."
if redis-cli ping > /dev/null 2>&1; then
    echo "   ✓ Redis is running and responding"
else
    echo "   ❌ Redis is not responding. Make sure it's running."
    exit 1
fi

echo ""
echo "2. Publishing test message to channel: user:IS14415:matches"
echo ""

# Create test JSON payload
TEST_MESSAGE='{
  "order_id": "test-12345",
  "user_id": "IS14415",
  "strategy_id": "test-strategy",
  "strategy_name": "Test Strategy",
  "stock_code": 500570,
  "symbol": "TATAMOTORS",
  "exchange": "NSE",
  "match_score": 99.5,
  "order_price": 718.15,
  "order_status": "PENDING",
  "timestamp": '$(date +%s)',
  "message": "This is a TEST message to verify Redis Pub/Sub is working"
}'

# Publish message
RESULT=$(redis-cli PUBLISH "user:IS14415:matches" "$TEST_MESSAGE")

echo "   Published to: user:IS14415:matches"
echo "   Subscribers received: $RESULT"
echo ""

if [ "$RESULT" -eq 0 ]; then
    echo "   ⚠️  WARNING: No subscribers listening on this channel!"
    echo "   Make sure to run the monitoring script in another terminal:"
    echo "   python test_redis_pubsub.py"
else
    echo "   ✓ Message delivered to $RESULT subscriber(s)"
fi

echo ""
echo "3. Checking active Pub/Sub channels..."
redis-cli PUBSUB CHANNELS "user:*:matches"

echo ""
echo "4. Checking number of subscribers per channel..."
redis-cli PUBSUB NUMSUB "user:IS14415:matches"

echo ""
echo "======================================================================"
echo "Test complete!"
echo ""
echo "Next steps:"
echo "1. Run 'python test_redis_pubsub.py' in another terminal"
echo "2. Run this script again to see the message received"
echo "3. Or trigger a real match in your rules engine"
echo "======================================================================"
