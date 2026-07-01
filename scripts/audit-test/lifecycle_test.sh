#!/usr/bin/env bash
#
# lifecycle_test.sh — exercises the strategy lifecycle so the config-event logs
# appear: CREATE → ACTIVATE → DEACTIVATE. No Kafka/Redis needed; all via the
# gateway API. Uses a category ("AUDIT_LIFECYCLE") no live news matches, so it
# never fires an order during the test.
#
# Watch:
#   pm2 logs user-config    | grep -Ei 'strategy|activate|deactivate'
#   pm2 logs rules-engine   | grep -E 'STRATEGY_INITIALIZED|config consumer'
#
# Usage:  ./lifecycle_test.sh
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config.sh"
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

API="${GATEWAY_URL}/api/v1"
auth=(-H "Authorization: Bearer ${ACCESS_TOKEN}" -H "appId: ${APP_ID}" -H "userId: ${CLIENT_ID}" -H "source: ${SOURCE}" -H "Content-Type: application/json")

echo "── 1) CREATE (inactive) ─────────────────────────────────────────────"
resp=$(curl -sS -X POST "${API}/strategies" "${auth[@]}" -d @- <<JSON
{
  "user_id": "${CLIENT_ID}",
  "strategy_name": "AUDIT-LIFECYCLE",
  "description": "lifecycle demo: create/activate/deactivate",
  "trading_mode": "${TRADING_MODE}",
  "activate_immediately": false,
  "conditions": { "match_all_news": false, "impact_score_min": 0, "impact_score_max": 10, "sentiments": [], "categories": ["AUDIT_LIFECYCLE"], "market_cap_types": [], "exchanges": ["NSE"] },
  "trade_config": { "order_type": "LIMIT", "product_type": "INTRADAY", "validity": "DAY", "quantity": 1, "exchange": "NSE", "order_side": "BUY", "stop_loss_pct": 1.0, "take_profit_pct": 2.0, "stop_loss_type": "FIXED", "take_profit_type": "FIXED", "trade_window_start": "", "trade_window_end": "" },
  "risk_limits": { "max_daily_trades": 10, "max_per_trade_risk": 100000, "max_loss_per_day": 1000000, "enable_risk_checks": true, "enable_auto_square_off": true, "auto_square_off_time": "15:05", "position_sizing": "FIXED", "max_amount_per_stock": 0, "max_trades_per_strategy": 10 }
}
JSON
)
echo "$resp" | jq -c '{success, strategy_id: .strategy.strategy_id, name: .strategy.strategy_name}' 2>/dev/null || echo "$resp"
SID=$(echo "$resp" | jq -r '.strategy.strategy_id // empty')
[ -z "$SID" ] && { echo "Could not get strategy_id from create response; aborting."; exit 1; }
echo "  strategy_id=${SID}"
echo "  expect: user-config 'strategy created' log; rules-engine CONFIG event"
sleep 3

echo ""
echo "── 2) ACTIVATE ──────────────────────────────────────────────────────"
curl -sS -X POST "${API}/strategies/${SID}/activate" "${auth[@]}" -d "{\"user_id\":\"${CLIENT_ID}\"}" | jq -c '.' 2>/dev/null || true
echo "  expect: user-config activation log; rules-engine STRATEGY_INITIALIZED / CONFIG_CREATED"
sleep 3

echo ""
echo "── 3) DEACTIVATE ────────────────────────────────────────────────────"
curl -sS -X POST "${API}/strategies/${SID}/deactivate" "${auth[@]}" -d "{\"user_id\":\"${CLIENT_ID}\"}" | jq -c '.' 2>/dev/null || true
echo "  expect: user-config deactivation log; rules-engine CONFIG_PAUSED;"
echo "          trade-execution 'Closing all positions for strategy' (if any were open)"
echo ""
echo "Done. Delete it with:  ACTION=delete ./cleanup_test_strategies.sh"
