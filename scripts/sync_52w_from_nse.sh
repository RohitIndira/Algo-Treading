#!/bin/bash
# One-time script: Fetch correct 52W high/low from NSE API for all stocks
# and update Redis. Fixes any stale bhavcopy values.
#
# Usage: bash scripts/sync_52w_from_nse.sh
#
# Takes ~50-60 minutes for all ~3300 NSE stocks (1 req/sec rate limit)
# Safe to Ctrl+C and re-run — only updates, doesn't delete.

echo "================================================"
echo "Syncing Redis 52W data from NSE India API"
echo "================================================"
echo ""

# Get all NSE symbols from Redis
SYMBOLS=$(redis-cli KEYS "52w:token:*" 2>/dev/null | \
    sed 's/52w:token://' | \
    grep -v '^[0-9]*$' | \
    grep -v '(BSE)' | \
    sort -u)

TOTAL=$(echo "$SYMBOLS" | wc -l)
echo "Found $TOTAL NSE symbols to sync"
echo ""

# Get NSE session cookie
COOKIE_FILE="/tmp/nse_sync_cookie.txt"
curl -s -c "$COOKIE_FILE" -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  "https://www.nseindia.com" -o /dev/null 2>/dev/null
sleep 1

UPDATED=0
SKIPPED=0
FAILED=0
COUNT=0

for SYMBOL in $SYMBOLS; do
    COUNT=$((COUNT+1))

    # Refresh cookie every 100 requests
    if [ $((COUNT % 100)) -eq 0 ]; then
        curl -s -c "$COOKIE_FILE" -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
          "https://www.nseindia.com" -o /dev/null 2>/dev/null
        sleep 1
    fi

    sleep 1  # Rate limit

    NSE_JSON=$(curl -s -b "$COOKIE_FILE" \
      -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
      -H "Referer: https://www.nseindia.com/" \
      "https://www.nseindia.com/api/quote-equity?symbol=$SYMBOL" 2>/dev/null)

    NSE_HIGH=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('weekHighLow',{}).get('max',0))" 2>/dev/null)
    NSE_LOW=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('weekHighLow',{}).get('min',0))" 2>/dev/null)
    NSE_LTP=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('lastPrice',0))" 2>/dev/null)

    if [ -z "$NSE_HIGH" ] || [ "$NSE_HIGH" = "0" ] || [ "$NSE_HIGH" = "None" ]; then
        FAILED=$((FAILED+1))
        [ $((COUNT % 50)) -eq 0 ] && echo "  [$COUNT/$TOTAL] $SYMBOL — FAILED (no data from NSE)"
        continue
    fi

    # Get current Redis value
    OUR_HIGH=$(redis-cli GET "52w:token:$SYMBOL" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('high',0))" 2>/dev/null)

    # Check if update needed (>₹1 difference)
    NEEDS_UPDATE=$(python3 -c "print('yes' if abs(float('${OUR_HIGH:-0}') - float('$NSE_HIGH')) > 1.0 or abs(float('${OUR_LOW:-0}') - float('$NSE_LOW')) > 1.0 else 'no')" 2>/dev/null)

    if [ "$NEEDS_UPDATE" = "yes" ]; then
        # Build JSON value
        VAL=$(python3 -c "
import json
print(json.dumps({
    'high': $NSE_HIGH,
    'low': $NSE_LOW,
    'last_close': $NSE_LTP,
    'symbol': '$SYMBOL',
    'token': '$SYMBOL',
    'exchange': 'NSE',
    'as_of': '$(date +%Y-%m-%d)',
    'source': 'nse_api'
}))
" 2>/dev/null)

        # Update Redis
        redis-cli SET "52w:token:$SYMBOL" "$VAL" EX 432000 > /dev/null 2>&1  # 5 day TTL
        UPDATED=$((UPDATED+1))
        echo "  [$COUNT/$TOTAL] $SYMBOL — UPDATED (our: $OUR_HIGH → NSE: $NSE_HIGH)"
    else
        SKIPPED=$((SKIPPED+1))
        [ $((COUNT % 100)) -eq 0 ] && echo "  [$COUNT/$TOTAL] Progress... ($UPDATED updated, $SKIPPED ok)"
    fi
done

echo ""
echo "================================================"
echo "Sync complete!"
echo "  Updated: $UPDATED"
echo "  Already correct: $SKIPPED"
echo "  Failed: $FAILED"
echo "  Total: $COUNT"
echo "================================================"
