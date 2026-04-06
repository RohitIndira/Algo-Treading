#!/bin/bash
# Verify our Redis 52W data against NSE India's official API
# Also reports missing NSE stocks
#
# Usage:
#   ./scripts/verify_52w.sh              # Check 15 random stocks
#   ./scripts/verify_52w.sh 30           # Check 30 random stocks
#   ./scripts/verify_52w.sh RELIANCE TCS # Check specific stocks

SAMPLE_SIZE=${1:-15}
SPECIFIC_STOCKS="${@:2}"

echo "================================================"
echo "52W Data Verification vs NSE India API"
echo "================================================"
echo ""

# Get NSE session cookie
COOKIE_FILE="/tmp/nse_verify_cookie.txt"
curl -s -c "$COOKIE_FILE" -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  "https://www.nseindia.com" -o /dev/null 2>/dev/null
sleep 1

# Determine which stocks to check
if [ -n "$SPECIFIC_STOCKS" ]; then
    SYMBOLS="$SPECIFIC_STOCKS"
    echo "Checking specific stocks: $SYMBOLS"
elif [[ "$1" =~ ^[A-Z] ]]; then
    SYMBOLS="$@"
    echo "Checking specific stocks: $SYMBOLS"
else
    # Get random NSE symbols from Redis (skip numeric BSE tokens)
    SYMBOLS=$(redis-cli KEYS "52w:token:*" 2>/dev/null | \
        sed 's/52w:token://' | \
        grep -v '^[0-9]*$' | \
        grep -v '(BSE)' | \
        shuf | head -"$SAMPLE_SIZE")
    echo "Checking $SAMPLE_SIZE random NSE stocks..."
fi
echo ""

# --- Part 1: Accuracy Check ---
echo "=== ACCURACY CHECK ==="
printf "%-14s %10s %10s %10s %10s %7s\n" "SYMBOL" "OUR_52WH" "NSE_52WH" "OUR_52WL" "NSE_52WL" "MATCH"
echo "─────────────────────────────────────────────────────────────────────────────"

MATCH=0
MISMATCH=0
SKIP=0
MISMATCH_LIST=""

for SYMBOL in $SYMBOLS; do
    # Get our Redis data
    REDIS_DATA=$(redis-cli GET "52w:token:$SYMBOL" 2>/dev/null)
    [ -z "$REDIS_DATA" ] && { SKIP=$((SKIP+1)); continue; }

    OUR_HIGH=$(echo "$REDIS_DATA" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('high',0))" 2>/dev/null)
    OUR_LOW=$(echo "$REDIS_DATA" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('low',0))" 2>/dev/null)
    SRC=$(echo "$REDIS_DATA" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('source','bhavcopy'))" 2>/dev/null)

    [ "$OUR_HIGH" = "0" ] && { SKIP=$((SKIP+1)); continue; }

    sleep 1  # Rate limit NSE API
    NSE_JSON=$(curl -s -b "$COOKIE_FILE" \
      -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
      -H "Referer: https://www.nseindia.com/" \
      "https://www.nseindia.com/api/quote-equity?symbol=$SYMBOL" 2>/dev/null)

    NSE_HIGH=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('weekHighLow',{}).get('max',0))" 2>/dev/null)
    NSE_LOW=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('weekHighLow',{}).get('min',0))" 2>/dev/null)

    if [ -z "$NSE_HIGH" ] || [ "$NSE_HIGH" = "0" ]; then
        SKIP=$((SKIP+1))
        printf "%-14s %10s %10s %10s %10s %7s\n" "$SYMBOL" "$OUR_HIGH" "ERROR" "$OUR_LOW" "ERROR" "⏭"
        continue
    fi

    RESULT=$(python3 -c "
h_ok = abs($OUR_HIGH - $NSE_HIGH) <= 1.0
l_ok = abs($OUR_LOW - $NSE_LOW) <= 1.0
print('✅' if h_ok and l_ok else '❌')
" 2>/dev/null)

    if [ "$RESULT" = "✅" ]; then
        MATCH=$((MATCH+1))
    else
        MISMATCH=$((MISMATCH+1))
        MISMATCH_LIST="$MISMATCH_LIST $SYMBOL(our:$OUR_HIGH/$OUR_LOW nse:$NSE_HIGH/$NSE_LOW)"
    fi

    printf "%-14s %10.2f %10.2f %10.2f %10.2f %7s\n" "$SYMBOL" "$OUR_HIGH" "$NSE_HIGH" "$OUR_LOW" "$NSE_LOW" "$RESULT"
done

echo "─────────────────────────────────────────────────────────────────────────────"
TOTAL=$((MATCH+MISMATCH))
PCT=$( [ $TOTAL -gt 0 ] && echo "$((MATCH*100/TOTAL))%" || echo "N/A" )
echo "✅ Match: $MATCH  ❌ Mismatch: $MISMATCH  ⏭ Skipped: $SKIP  Accuracy: $PCT"

if [ -n "$MISMATCH_LIST" ]; then
    echo ""
    echo "Mismatched stocks:$MISMATCH_LIST"
fi

echo ""

# --- Part 2: Coverage Check ---
echo "=== COVERAGE CHECK ==="

# Count our stocks
OUR_NSE=$(redis-cli KEYS "52w:token:*" 2>/dev/null | sed 's/52w:token://' | grep -v '^[0-9]*$' | wc -l)
OUR_BSE=$(redis-cli KEYS "52w:*" 2>/dev/null | grep -v "token" | grep -v "breakouts" | wc -l)
DB_INSTRUMENTS=$(PGPASSWORD=postgres psql -U postgres -d market_data -tAc "SELECT COUNT(DISTINCT symbol) FROM instruments WHERE exchange='NSE' AND is_active=TRUE;" 2>/dev/null)
BREAKOUTS_HIGH=$(PGPASSWORD=postgres psql -U postgres -d market_data -tAc "SELECT COUNT(*) FROM breakout_events WHERE trade_date=CURRENT_DATE AND breakout_type='52W_HIGH';" 2>/dev/null)
BREAKOUTS_LOW=$(PGPASSWORD=postgres psql -U postgres -d market_data -tAc "SELECT COUNT(*) FROM breakout_events WHERE trade_date=CURRENT_DATE AND breakout_type='52W_LOW';" 2>/dev/null)

echo "Redis 52W keys (NSE tokens):  $OUR_NSE"
echo "Redis 52W keys (instrument):  $OUR_BSE"
echo "DB instruments (NSE active):  $DB_INSTRUMENTS"
echo "Today's 52W HIGH breakouts:   $BREAKOUTS_HIGH"
echo "Today's 52W LOW breakouts:    $BREAKOUTS_LOW"
echo ""

# --- Part 3: Today's Breakouts ---
echo "=== TODAY'S BREAKOUTS ==="
echo ""
echo "--- 52W HIGH ---"
PGPASSWORD=postgres psql -U postgres -d market_data -c "
SELECT symbol, breakout_price as price, volume,
  ROUND(pct_change::numeric,2) as \"chg%\",
  detected_at::time(0) as time
FROM breakout_events
WHERE trade_date = CURRENT_DATE AND breakout_type = '52W_HIGH'
ORDER BY detected_at;" 2>/dev/null

echo ""
echo "--- 52W LOW ---"
PGPASSWORD=postgres psql -U postgres -d market_data -c "
SELECT symbol, breakout_price as price, volume,
  ROUND(pct_change::numeric,2) as \"chg%\",
  detected_at::time(0) as time
FROM breakout_events
WHERE trade_date = CURRENT_DATE AND breakout_type = '52W_LOW'
ORDER BY detected_at;" 2>/dev/null

echo ""

# --- Part 4: Missed Breakouts Check ---
echo "=== MISSED BREAKOUTS CHECK ==="
echo "Checking 30 random stocks from NSE to find any 52W breakouts we missed..."
echo ""

MISSED_HIGH=0
MISSED_LOW=0
FOUND_HIGH=0
FOUND_LOW=0

CHECK_SYMBOLS=$(redis-cli KEYS "52w:token:*" 2>/dev/null | \
    sed 's/52w:token://' | \
    grep -v '^[0-9]*$' | \
    grep -v '(BSE)' | \
    shuf | head -30)

# Refresh NSE cookie
curl -s -c "$COOKIE_FILE" -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
  "https://www.nseindia.com" -o /dev/null 2>/dev/null
sleep 1

TODAY=$(date +%d-%b-%Y) # Format: 06-Apr-2026

for SYM in $CHECK_SYMBOLS; do
    sleep 1
    NSE_JSON=$(curl -s -b "$COOKIE_FILE" \
      -A "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36" \
      -H "Referer: https://www.nseindia.com/" \
      "https://www.nseindia.com/api/quote-equity?symbol=$SYM" 2>/dev/null)

    NSE_HIGH_DATE=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('weekHighLow',{}).get('maxDate',''))" 2>/dev/null)
    NSE_LOW_DATE=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('weekHighLow',{}).get('minDate',''))" 2>/dev/null)

    TODAY_SHORT=$(date +%d-%b-%Y)

    # Check if this stock made 52W high TODAY
    if [ "$NSE_HIGH_DATE" = "$TODAY_SHORT" ]; then
        # Did we detect it?
        OUR_DETECT=$(PGPASSWORD=postgres psql -U postgres -d market_data -tAc \
            "SELECT COUNT(*) FROM breakout_events WHERE symbol='$SYM' AND breakout_type='52W_HIGH' AND trade_date=CURRENT_DATE;" 2>/dev/null)
        if [ "$OUR_DETECT" = "0" ]; then
            NSE_HIGH=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('weekHighLow',{}).get('max',0))" 2>/dev/null)
            echo "  ❌ MISSED 52W HIGH: $SYM (NSE high=$NSE_HIGH, date=$NSE_HIGH_DATE)"
            MISSED_HIGH=$((MISSED_HIGH+1))
        else
            FOUND_HIGH=$((FOUND_HIGH+1))
        fi
    fi

    # Check if this stock made 52W low TODAY
    if [ "$NSE_LOW_DATE" = "$TODAY_SHORT" ]; then
        OUR_DETECT=$(PGPASSWORD=postgres psql -U postgres -d market_data -tAc \
            "SELECT COUNT(*) FROM breakout_events WHERE symbol='$SYM' AND breakout_type='52W_LOW' AND trade_date=CURRENT_DATE;" 2>/dev/null)
        if [ "$OUR_DETECT" = "0" ]; then
            NSE_LOW=$(echo "$NSE_JSON" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('priceInfo',{}).get('weekHighLow',{}).get('min',0))" 2>/dev/null)
            echo "  ❌ MISSED 52W LOW: $SYM (NSE low=$NSE_LOW, date=$NSE_LOW_DATE)"
            MISSED_LOW=$((MISSED_LOW+1))
        else
            FOUND_LOW=$((FOUND_LOW+1))
        fi
    fi
done

echo ""
echo "Breakout detection: found HIGH=$FOUND_HIGH LOW=$FOUND_LOW | missed HIGH=$MISSED_HIGH LOW=$MISSED_LOW"

echo ""
echo "================================================"
echo "Verification complete."
echo "================================================"
