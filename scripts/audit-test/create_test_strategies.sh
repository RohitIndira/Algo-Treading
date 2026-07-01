#!/usr/bin/env bash
#
# create_test_strategies.sh — creates 4 PAPER strategies, one per audit case.
#
# Isolation trick: each strategy has match_all_news=false and a UNIQUE category.
# The rules-engine only matches a strategy when the news category is in its list,
# so each mock news (see run_audit_tests.sh) triggers EXACTLY one strategy, and
# real live news (other categories) never triggers these test strategies.
#
#   AUDIT-DPR       cat=AUDIT_DPR       qty 10     → DPR_UPPER_BREACH
#   AUDIT-QTY       cat=AUDIT_QTY       qty 60000  → QTY_LIMIT_EXCEEDED
#   AUDIT-VALUE     cat=AUDIT_VALUE     qty 50000  → ORDER_VALUE_LIMIT_EXCEEDED
#   AUDIT-VELOCITY  cat=AUDIT_VELOCITY  qty 10     → VELOCITY_BREACH
#
# All also emit ORDER_AUDIT (per order) + STRATEGY_INITIALIZED (on create).
# PAPER mode → no real broker orders (checks run identically to LIVE).
#
# Usage:  GATEWAY_URL=http://uat:8081 ./create_test_strategies.sh
set -euo pipefail

# Paste your values in config.sh (same folder) — this sources them.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config.sh"
API="${GATEWAY_URL}/api/v1/strategies"

# create_strategy <name> <qty> <category> <desc>
create_strategy() {
  local name="$1" qty="$2" cat="$3" desc="$4"
  echo ""
  echo "── ${name} (qty=${qty}, category=${cat}, mode=${TRADING_MODE}) ──"
  curl -sS -X POST "${API}" \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    -H "appId: ${APP_ID}" -H "userId: ${CLIENT_ID}" -H "source: ${SOURCE}" \
    -d @- <<JSON
{
  "user_id": "${CLIENT_ID}",
  "strategy_name": "${name}",
  "description": "${desc}",
  "trading_mode": "${TRADING_MODE}",
  "activate_immediately": true,
  "conditions": {
    "match_all_news": false,
    "impact_score_min": 0,
    "impact_score_max": 10,
    "sentiments": [],
    "categories": ["${cat}"],
    "market_cap_types": [],
    "exchanges": ["NSE"]
  },
  "trade_config": {
    "order_type": "LIMIT",
    "product_type": "INTRADAY",
    "validity": "DAY",
    "quantity": ${qty},
    "exchange": "NSE",
    "order_side": "BUY",
    "stop_loss_pct": 1.0,
    "take_profit_pct": 2.0,
    "stop_loss_type": "FIXED",
    "take_profit_type": "FIXED",
    "trade_window_start": "",
    "trade_window_end": ""
  },
  "risk_limits": {
    "max_daily_trades": 1000,
    "max_per_trade_risk": 100000,
    "max_loss_per_day": 100000000,
    "enable_risk_checks": true,
    "enable_auto_square_off": true,
    "auto_square_off_time": "15:05",
    "position_sizing": "FIXED",
    "max_amount_per_stock": 0,
    "max_trades_per_strategy": 1000
  }
}
JSON
  echo ""
}

echo "Gateway: ${GATEWAY_URL}   Client: ${CLIENT_ID}   Mode: ${TRADING_MODE}"
create_strategy "AUDIT-TRADE-1"    1     "AUDIT_TRADE"    "Passes all checks; ONE order placed → order req/res in trade-execution logs"
create_strategy "AUDIT-DPR-1"      10    "AUDIT_DPR"      "DPR breach (seeded); expect DPR_UPPER_BREACH"
create_strategy "AUDIT-QTY-1"      60000 "AUDIT_QTY"      "Expect QTY_LIMIT_EXCEEDED"
create_strategy "AUDIT-VALUE-1"    50000 "AUDIT_VALUE"    "Expect ORDER_VALUE_LIMIT_EXCEEDED"
create_strategy "AUDIT-VELOCITY-1" 10    "AUDIT_VELOCITY" "Velocity breach (seeded ticks); expect VELOCITY_BREACH"

echo ""
echo "Strategies created. Give Kafka a few seconds to propagate CONFIG_CREATED to"
echo "the rules-engine config store, then run:  ./run_audit_tests.sh"
