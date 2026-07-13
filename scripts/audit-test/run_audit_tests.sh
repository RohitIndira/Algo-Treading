#!/usr/bin/env bash
#
# run_audit_tests.sh — ONE script that drives every audit case by publishing a
# mock news event to Kafka. The rules-engine prices each order off the LIVE LTP
# already in Redis — you do NOT seed Redis for most cases.
#
#   qty | value | trade    → real stock (REAL_TOKEN in config.sh), prices off
#                            its live LTP already in Redis — NO seeding. TRADE
#                            additionally reads the order back from the broker
#                            (GET .../live-orders/indira-positions) after a 6s
#                            wait, to prove rules-engine really handed it to
#                            trade-execution and trade-execution really placed
#                            it — not just that the logs claim so.
#   dpr | exposure | ban   → real token, but the script SETs Redis with a
#                            crafted value (price outside the DPR band, a
#                            controlled LTP, or just enough LTP to not get
#                            skipped) immediately before publishing the news —
#                            the compliance check reads whatever's in Redis at
#                            that instant. A live feed updating the same token
#                            mid-market-hours can in theory win that race; if
#                            a case misses, just re-run it.
#                            EXPOSURE/BAN also need rules-engine env vars:
#                            MAX_EXPOSURE_LIMIT > 0, BANNED_TOKENS must
#                            include 14366 — otherwise nothing rejects them.
#
# Each mock news carries a strategy's unique category, so exactly one AUDIT-*
# strategy matches (create them first with ./create_test_strategies.sh).
#
# Watch:
#   pm2 logs rules-engine     | grep -E 'ORDER_AUDIT|ORDER_REJECTED'
#   pm2 logs trade-execution  | grep -E 'Placing order|order.*response|EXECUTED'   # for the trade case
#
# Usage:  ./run_audit_tests.sh            # all
#         ./run_audit_tests.sh trade      # one: trade|qty|value|dpr|exposure|ban
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config.sh"
EXCH="nse"
R="redis-cli -h ${REDIS_HOST} -p ${REDIS_PORT}"
[ -n "${REDIS_PASSWORD:-}" ] && R="${R} -a ${REDIS_PASSWORD} --no-auth-warning"

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

  # Read the order back from the BROKER (not our own DB) to prove rules-engine
  # really handed it to trade-execution and trade-execution really executed it,
  # not just that the logs say so. 6s covers Kafka → rules-engine → trade-execution
  # → broker; bump it if this comes back empty on a slower box.
  echo "  waiting 6s, then confirming execution at the broker (indira-positions)..."
  sleep 6
  local resp; resp=$(curl -sS "${GATEWAY_URL}/api/v1/live-orders/indira-positions?user_id=${CLIENT_ID}" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" -H "appId: ${APP_ID}" -H "userId: ${CLIENT_ID}" -H "source: ${SOURCE}")
  local match; match=$(echo "$resp" | jq -c --arg sym "$REAL_SYMBOL" \
    '.positions[]? | select(.symbol == $sym) | {symbol, net_qty, order_side, filled_price, strategy_name, indira_order_id}' 2>/dev/null)
  if [ -n "$match" ]; then
    echo "  ✔ CONFIRMED at broker: ${match}"
  else
    echo "  ✘ NOT FOUND in broker positions yet — either it's still in flight (re-check"
    echo "    manually) or the pipeline didn't hand it off. Raw response: ${resp}"
  fi
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

# ── Cases that REQUIRE a crafted Redis value (real token, seeded right before
#    publish — the check reads whatever's in Redis at that moment; same
#    pattern for dpr/exposure/ban below) ──────────────────────────────────────
case_dpr() {
  echo "[#1 DPR] ACC (token 22): seed ltp just above ACC's REAL dpr_upper → price>upper"
  local key="market:${EXCH}:22"
  # Use ACC's REAL circuit band, not made-up numbers. Prefer the live feed's band
  # when it's present (the feed writes rich docs with a "high" field; our own seed
  # doesn't), so the test tracks ACC's actual daily band. Fall back to ACC's known
  # band when only a stale seed / nothing is there (e.g. after hours).
  local cur lo up pc tick
  cur=$($R -n "$REDIS_DB" GET "$key" 2>/dev/null || true)
  if [ -n "$cur" ] && [ "$(echo "$cur" | jq -r 'has("high")' 2>/dev/null)" = "true" ]; then
    lo=$(echo "$cur" | jq -r '.dpr_lower'); up=$(echo "$cur" | jq -r '.dpr_upper')
    pc=$(echo "$cur" | jq -r '.prev_close'); tick=$(echo "$cur" | jq -r '.tick_size')
    echo "  using ACC's live band from Redis: [${lo}, ${up}]"
  else
    lo=1065.8; up=1598.6; pc=1360; tick=0.05
    echo "  live band not present; using ACC fallback band: [${lo}, ${up}]"
  fi
  # ltp ~0.7% above the real upper → the LIMIT order prices above the band.
  local newltp; newltp=$(awk -v u="$up" 'BEGIN{printf "%.2f", u*1.007}')
  local j; j=$(printf '{"symbol":"ACC","token":"22","exchange":"nse","ltp":%s,"prev_close":%s,"volume":1000,"timestamp":%s,"tick_size":%s,"percent_change":0.3,"dpr_lower":%s,"dpr_upper":%s}' "$newltp" "$pc" "$(date +%s%3N)" "$tick" "$lo" "$up")
  $R -n "$REDIS_DB" SET "$key" "$j" >/dev/null
  echo "  seeded ltp=${newltp} (real band kept: [${lo}, ${up}])"
  publish_news 22 ACC AUDIT_DPR "$newltp"
  echo "  expect: ORDER_REJECTED reason=DPR_UPPER_BREACH"
}

case_exposure() {
  echo "[EXPOSURE] RELIANCE (2885, qty 6500 @ ltp 1334.5 ~ 87L) then ONGC (2475, qty 6500 @ ltp 245 ~ 16L, seeded)"
  echo "        ** requires MAX_EXPOSURE_LIMIT>0 on rules-engine (e.g. 10000000 = ₹1cr) **"
  local ts; ts=$(date +%s)
  local jr; jr=$(printf '{"symbol":"RELIANCE","token":"2885","exchange":"nse","ltp":1334.5,"prev_close":1309.5,"volume":500,"timestamp":%s,"tick_size":0.1,"percent_change":1.9,"dpr_lower":1219.1,"dpr_upper":1489.9}' "$ts")
  # tick_size 0.05 (not the live feed's reported 0.01 — the broker's real tick for ONGC
  # at this price rejects 0.01-multiple prices with EG003 "Price Not in multiple of PriceTick").
  local jo; jo=$(printf '{"symbol":"ONGC","token":"2475","exchange":"nse","ltp":245,"prev_close":246.25,"volume":500,"timestamp":%s,"tick_size":0.05,"percent_change":-0.5,"dpr_lower":230,"dpr_upper":319}' "$ts")
  $R -n "$REDIS_DB" SET "market:${EXCH}:2885" "$jr" >/dev/null
  $R -n "$REDIS_DB" SET "market:${EXCH}:2475" "$jo" >/dev/null
  $R -n "$REDIS_DB" DEL "exposure:${CLIENT_ID}" >/dev/null   # start each run with a clean exposure counter
  echo "  order 1 (RELIANCE): expect ORDER_AUDIT (approved); exposure -> ~87L"
  publish_news 2885 RELIANCE AUDIT_EXPOSURE 1334.5
  sleep 5
  echo "  order 2 (ONGC):     expect ORDER_REJECTED reason=EXPOSURE_LIMIT_EXCEEDED (87L+16L>1cr limit)"
  publish_news 2475 ONGC AUDIT_EXPOSURE 245
}
case_ban() {
  echo "[BAN] IDEA (token 14366): must be listed in rules-engine's BANNED_TOKENS env"
  local j; j=$(printf '{"symbol":"IDEA","token":"14366","exchange":"nse","ltp":8.5,"prev_close":8.3,"volume":10000,"timestamp":%s,"tick_size":0.05,"percent_change":0.2,"dpr_lower":7.5,"dpr_upper":9.5}' "$(date +%s)")
  $R -n "$REDIS_DB" SET "market:${EXCH}:14366" "$j" >/dev/null
  publish_news 14366 IDEA AUDIT_BAN 8.5
  echo "  expect: ORDER_REJECTED reason=BANNED_TOKEN"
}

run_one() { echo ""; "$@"; sleep 4; }
target="${1:-all}"
case "$target" in
  trade)    run_one case_trade ;;
  qty)      run_one case_qty ;;
  value)    run_one case_value ;;
  dpr)      run_one case_dpr ;;
  exposure) run_one case_exposure ;;
  ban)      run_one case_ban ;;
  all)      run_one case_qty; run_one case_value; run_one case_dpr; run_one case_exposure; run_one case_ban; run_one case_trade ;;
  *) echo "usage: $0 [trade|qty|value|dpr|exposure|ban|all]"; exit 1 ;;
esac
echo ""
echo "Done. Every built order also logs ORDER_AUDIT (case #5)."
