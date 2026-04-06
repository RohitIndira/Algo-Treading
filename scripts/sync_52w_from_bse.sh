#!/bin/bash
# One-time script: Fetch correct 52W high/low from BSE API for all BSE stocks
# and update Redis + DB materialized view.
#
# Usage: bash scripts/sync_52w_from_bse.sh
#
# BSE API: https://api.bseindia.com/BseIndiaAPI/api/HighLow/w?Type=EQ&flag=C&scripcode={code}
# Returns: Fifty2WkHigh_adj, Fifty2WkLow_adj

echo "================================================"
echo "Syncing Redis 52W data from BSE India API"
echo "================================================"
echo ""

# Get all BSE scrip codes from our DB
BSE_CODES=$(PGPASSWORD=postgres psql -U postgres -d market_data -tAc \
  "SELECT token FROM instruments WHERE exchange='BSE' AND is_active=TRUE AND token ~ '^[0-9]+$' ORDER BY token;" 2>/dev/null)

TOTAL=$(echo "$BSE_CODES" | wc -l)
echo "Found $TOTAL BSE scrip codes to sync"
echo ""

UPDATED=0
SKIPPED=0
FAILED=0
COUNT=0

for CODE in $BSE_CODES; do
    COUNT=$((COUNT+1))

    sleep 0.5  # BSE is more lenient on rate limits

    BSE_JSON=$(curl -s \
      -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
      -H "Referer: https://www.bseindia.com/" \
      -H "Origin: https://www.bseindia.com" \
      "https://api.bseindia.com/BseIndiaAPI/api/HighLow/w?Type=EQ&flag=C&scripcode=$CODE" 2>/dev/null)

    RESULT=$(echo "$BSE_JSON" | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    high = d.get('Fifty2WkHigh_adj', '0')
    low = d.get('Fifty2WkLow_adj', '0')
    # Clean: remove date part if present
    high = high.split('(')[0].strip() if '(' in str(high) else str(high).strip()
    low = low.split('(')[0].strip() if '(' in str(low) else str(low).strip()
    if float(high) > 0 and float(low) > 0:
        print(f'{high}|{low}')
    else:
        print('SKIP')
except:
    print('FAIL')
" 2>/dev/null)

    if [ "$RESULT" = "FAIL" ] || [ -z "$RESULT" ]; then
        FAILED=$((FAILED+1))
        [ $((COUNT % 100)) -eq 0 ] && echo "  [$COUNT/$TOTAL] $CODE — FAILED"
        continue
    fi

    if [ "$RESULT" = "SKIP" ]; then
        SKIPPED=$((SKIPPED+1))
        continue
    fi

    BSE_HIGH=$(echo "$RESULT" | cut -d'|' -f1)
    BSE_LOW=$(echo "$RESULT" | cut -d'|' -f2)

    # Get symbol for this scrip code
    SYMBOL=$(PGPASSWORD=postgres psql -U postgres -d market_data -tAc \
      "SELECT symbol FROM instruments WHERE token='$CODE' AND exchange='BSE' LIMIT 1;" 2>/dev/null)

    [ -z "$SYMBOL" ] && { SKIPPED=$((SKIPPED+1)); continue; }

    # Update Redis
    VAL=$(python3 -c "
import json
print(json.dumps({
    'high': float('$BSE_HIGH'),
    'low': float('$BSE_LOW'),
    'last_close': 0,
    'symbol': '$SYMBOL',
    'token': '$CODE',
    'exchange': 'BSE',
    'as_of': '$(date +%Y-%m-%d)',
    'source': 'bse_api'
}))
" 2>/dev/null)

    redis-cli SET "52w:token:$CODE" "$VAL" EX 432000 > /dev/null 2>&1
    UPDATED=$((UPDATED+1))

    [ $((COUNT % 50)) -eq 0 ] && echo "  [$COUNT/$TOTAL] $SYMBOL — H:$BSE_HIGH L:$BSE_LOW ($UPDATED updated)"
done

echo ""
echo "================================================"
echo "BSE Sync complete!"
echo "  Updated: $UPDATED"
echo "  Skipped: $SKIPPED"
echo "  Failed: $FAILED"
echo "  Total: $COUNT"
echo "================================================"
