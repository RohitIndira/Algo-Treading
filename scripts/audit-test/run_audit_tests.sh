#!/usr/bin/env bash
#
# run_audit_tests.sh — ONE script that drives every audit case by publishing a
# mock news event to Kafka. The rules-engine prices each order off the LIVE LTP
# already in Redis — you do NOT seed Redis for most cases.
#
#   qty | value | trade  → real stock (REAL_TOKEN in config.sh), NO Redis needed
#   dpr | velocity       → fake token + one crafted Redis value (the ONLY cases
#                          that need it: a real stock never leaves its DPR band
#                          or jumps 1% in 1s on demand)
#
# Each mock news carries a strategy's unique category, so exactly one AUDIT-*
# strategy matches (create them first with ./create_test_strategies.sh).
#
# Watch:
#   pm2 logs rules-engine     | grep -E 'ORDER_AUDIT|ORDER_REJECTED'
#   pm2 logs trade-execution  | grep -E 'Placing order|order.*response|EXECUTED'   # for the trade case
#
# Usage:  ./run_audit_tests.sh            # all
#         ./run_audit_tests.sh trade      # one: trade|qty|value|dpr|velocity
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config.sh"
EXCH="nse"
R="redis-cli -h ${REDIS_HOST} -p ${REDIS_PORT}"

producer() {
  if command -v kcat >/dev/null 2>&1; then kcat -P -b "$KAFKA_BROKER" -t "$TOPIC"
  elif command -v kafkacat >/dev/null 2>&1; then kafkacat -P -b "$KAFKA_BROKER" -t "$TOPIC"
  else echo "ERROR: kcat/kafkacat not found" >&2; return 1; fi
}

# publish_news <token> <symbol> <category> <ltp_hint>
publish_news() {
  local tok="$1" sym="$2" cat="$3" ltp="$4"; local nid="AUDIT-$(date +%s%N)"
  printf '{"news_id":"%s","newsid":"%s","symbol":"%s","exchange":"NSE","code":%s,"token":%s,"company":"INE000TEST01","companyname":"%s","impact":"High","impact score":5,"sentiment":"POSITIVE","category":"%s","mcap":1500000,"mcaptype":"Large","pct_change":0.5,"LastTradedPrice":%s,"dt_tm":"%s","document_date":"%s"}' \
    "$nid" "$nid" "$sym" "$tok" "$tok" "$sym" "$cat" "$ltp" \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(date -u +'%Y-%m-%d %H:%M:%S')" | producer
  echo "  → news_id=${nid} sym=${sym} token=${tok} category=${cat}"
}

# Warn if the real stock has no live market data (order would be skipped).
check_real_ltp() {
  local v; v=$($R -n "$REDIS_DB" GET "market:${EXCH}:${REAL_TOKEN}" 2>/dev/null || true)
  if [ -z "$v" ]; then
    echo "  WARNING: market:${EXCH}:${REAL_TOKEN} is empty in Redis DB ${REDIS_DB}."
    echo "           No live LTP → the order will be SKIPPED (no log). Set REAL_TOKEN"
    echo "           in config.sh to an actively-traded stock, or fix REDIS_DB."
  fi
}

# ── Cases that need NO Redis (price off live LTP) ─────────────────────────────
case_trade() {
  echo "[TRADE] ${REAL_SYMBOL} (token ${REAL_TOKEN}) qty 1 → passes all checks → order PLACED"
  echo "        ** In LIVE mode this places a REAL 1-share order. Watch trade-execution logs. **"
  check_real_ltp
  publish_news "$REAL_TOKEN" "$REAL_SYMBOL" AUDIT_TRADE 0
  echo "  expect: ORDER_AUDIT (approved) in rules-engine; 'Placing order ... payload=...' + broker"
  echo "          response in trade-execution logs."
}
case_qty() {
  echo "[#2 QTY] ${REAL_SYMBOL} (token ${REAL_TOKEN}) qty 60000 > 50000"
  check_real_ltp
  publish_news "$REAL_TOKEN" "$REAL_SYMBOL" AUDIT_QTY 0
  echo "  expect: ORDER_REJECTED reason=QTY_LIMIT_EXCEEDED"
}
case_value() {
  echo "[#3 VALUE] ${REAL_SYMBOL} (token ${REAL_TOKEN}) qty 50000 × LTP > ₹2cr (needs LTP > ₹400)"
  check_real_ltp
  publish_news "$REAL_TOKEN" "$REAL_SYMBOL" AUDIT_VALUE 0
  echo "  expect: ORDER_REJECTED reason=ORDER_VALUE_LIMIT_EXCEEDED"
}

# ── Cases that REQUIRE a crafted Redis value (fake token, no live overwrite) ──
case_dpr() {
  echo "[#1 DPR] ACC (fake token 9990011): ltp 2500, tight band [2400,2450] → price>upper"
  local j; j=$(printf '{"symbol":"ACC","token":"9990011","exchange":"nse","ltp":2500,"prev_close":2490,"volume":1000,"timestamp":%s,"tick_size":0.05,"percent_change":0.3,"dpr_lower":2400,"dpr_upper":2450}' "$(date +%s)")
  $R -n "$REDIS_DB" SET "market:${EXCH}:9990011" "$j" >/dev/null
  publish_news 9990011 ACC AUDIT_DPR 2500
  echo "  expect: ORDER_REJECTED reason=DPR_UPPER_BREACH"
}
case_velocity() {
  echo "[#4 VELOCITY] TATAMOTORS (fake token 9990014): +1.5% ticks in last ~1s"
  local j; j=$(printf '{"symbol":"TATAMOTORS","token":"9990014","exchange":"nse","ltp":1000,"prev_close":995,"volume":1000,"timestamp":%s,"tick_size":0.05,"percent_change":0.4,"dpr_lower":900,"dpr_upper":1100}' "$(date +%s)")
  $R -n "$REDIS_DB" SET "market:${EXCH}:9990014" "$j" >/dev/null
  local key="ticks:${EXCH}:9990014"; $R -n "$TICKSTORE_DB" DEL "$key" >/dev/null
  local now; now=$(date +%s%N); local i=0
  for m in 1.000 1.005 1.010 1.015; do
    local p; p=$(awk -v m="$m" 'BEGIN{printf "%.2f", 1000*m}')
    $R -n "$TICKSTORE_DB" RPUSH "$key" \
      "$(printf '{"symbol":"TATAMOTORS","token":"9990014","exchange":"nse","ltp":%s,"ts":%s}' "$p" "$(( now - (3-i)*250000000 ))")" >/dev/null
    i=$(( i + 1 ))
  done
  $R -n "$TICKSTORE_DB" EXPIRE "$key" 600 >/dev/null
  publish_news 9990014 TATAMOTORS AUDIT_VELOCITY 1000
  echo "  expect: ORDER_REJECTED reason=VELOCITY_BREACH"
  echo "  (1s window — if it misses due to Kafka lag, set VELOCITY_WINDOW_MS=5000 and re-run velocity)"
}

run_one() { echo ""; "$@"; sleep 4; }
target="${1:-all}"
case "$target" in
  trade)    run_one case_trade ;;
  qty)      run_one case_qty ;;
  value)    run_one case_value ;;
  dpr)      run_one case_dpr ;;
  velocity) run_one case_velocity ;;
  all)      run_one case_qty; run_one case_value; run_one case_dpr; run_one case_velocity; run_one case_trade ;;
  *) echo "usage: $0 [trade|qty|value|dpr|velocity|all]"; exit 1 ;;
esac
echo ""
echo "Done. Every built order also logs ORDER_AUDIT (case #5)."
