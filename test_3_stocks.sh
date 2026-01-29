#!/bin/bash

echo "=== Testing 3 Different Stocks ==="
echo "Sending ICICIBANK..."
echo '{"symbol":"ICICIBANK","token":"1270","exchange":"NSE","ltp":1245.80,"week_52_high":1245.80,"week_52_high_date":"2026-01-28","timestamp":'$(date +%s)000',"is_new_week_52_high":true}' | kcat -b localhost:9092 -t market.data.52w_breakouts -P
echo "✅ ICICIBANK sent"

sleep 3

echo "Sending SBIN..."
echo '{"symbol":"SBIN","token":"3045","exchange":"NSE","ltp":895.60,"week_52_high":895.60,"week_52_high_date":"2026-01-28","timestamp":'$(date +%s)000',"is_new_week_52_high":true}' | kcat -b localhost:9092 -t market.data.52w_breakouts -P
echo "✅ SBIN sent"

sleep 3

echo "Sending LT..."
echo '{"symbol":"LT","token":"11483","exchange":"NSE","ltp":3685.25,"week_52_high":3685.25,"week_52_high_date":"2026-01-28","timestamp":'$(date +%s)000',"is_new_week_52_high":true}' | kcat -b localhost:9092 -t market.data.52w_breakouts -P
echo "✅ LT sent"

echo ""
echo "=== All 3 stocks sent! Watch rules-engine logs ==="
EOF
