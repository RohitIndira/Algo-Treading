#!/usr/bin/env bash
#
# Manthan test harness: flushes runtime state + creates a fresh LIVE Manthan strategy.
# Leaves reference data intact (today's manthan_signals, instruments, daily_ohlcv, EMA
# weights, external Redis ISIN mappings). Credentials are upserted fresh from $JWT.
#
# Usage:
#   JWT='<fresh-HS512-userSession-from-browser>' ./scripts/manthan-reset.sh
#
# Optional env overrides:
#   USER_ID        (default ND03920)
#   APP_ID         (default 1e7f02cd...)
#   TOTAL_CAPITAL  (default 700000)
#   TRADING_MODE   (default LIVE)
#   GATEWAY        (default http://localhost:8080)
#   STRATEGY_NAME  (default "Manthan Live")

set -euo pipefail

: "${JWT:?JWT is required (paste the HS512 userSession JWT from browser)}"
USER_ID="${USER_ID:-ND03920}"
APP_ID="${APP_ID:-1e7f02cdcedfa8d28f0f912bd1a441931775033677466}"
TOTAL_CAPITAL="${TOTAL_CAPITAL:-700000}"
TRADING_MODE="${TRADING_MODE:-LIVE}"
GATEWAY="${GATEWAY:-http://localhost:8080}"
STRATEGY_NAME="${STRATEGY_NAME:-Manthan Live}"

PG="PGPASSWORD=postgres psql -h localhost -U postgres"

echo "=== 1. Flushing Manthan runtime tables (trading_db = source of truth) ==="
eval "$PG -d trading_db -c \"
TRUNCATE manthan_positions, manthan_orders, manthan_order_events, manthan_cooldown, manthan_portfolio_state, execution_outbox, trade_signals RESTART IDENTITY CASCADE;
TRUNCATE strategies CASCADE;\""
eval "$PG -d trading_execution -c \"
TRUNCATE orders, execution_events, manthan_orders, manthan_order_events RESTART IDENTITY CASCADE;\""
# market_data.manthan_positions/_cooldown/_portfolio_state were dropped as
# duplicates of trading_db — keep manthan_signals/manthan_stocks (reference data).

echo
echo "=== 2. Flushing Manthan runtime Redis keys (keep signals + EMA) ==="
redis-cli --no-auth-warning EVAL "
for _,k in ipairs(redis.call('keys','manthan:portfolio:*')) do redis.call('del',k) end
for _,k in ipairs(redis.call('keys','manthan:position:*'))  do redis.call('del',k) end
redis.call('del','manthan:positions:active')
return 'ok'" 0

echo
echo "=== 3. Creating fresh Manthan strategy ($TRADING_MODE) ==="
BODY=$(cat <<EOF
{
  "user_id": "$USER_ID",
  "strategy_name": "$STRATEGY_NAME",
  "description": "Manthan fresh run via scripts/manthan-reset.sh",
  "strategy_type": "MANTHAN",
  "trading_mode": "$TRADING_MODE",
  "activate_immediately": true,
  "trade_config": {
    "order_type": "LIMIT",
    "product_type": "DELIVERY",
    "validity": "DAY",
    "quantity": 1,
    "exchange": "NSE",
    "order_side": "BUY",
    "stop_loss_type": "PERCENTAGE",
    "position_sizing_mode": "EMA_ALLOCATION",
    "total_capital": $TOTAL_CAPITAL
  },
  "risk_limits": {
    "enable_risk_checks": false,
    "enable_auto_square_off": false
  },
  "indira_auth": {
    "user_id": "$USER_ID",
    "app_id": "$APP_ID",
    "source": "WEB",
    "bearer_token": "$JWT"
  }
}
EOF
)

echo "$BODY" > /tmp/manthan-strategy.json

HTTP_STATUS=$(curl -s -o /tmp/manthan-resp.json -w "%{http_code}" \
  -X POST "$GATEWAY/api/v1/strategies" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $JWT" \
  -H "appId: $APP_ID" \
  -H "source: WEB" \
  -H "userId: $USER_ID" \
  -d @/tmp/manthan-strategy.json)

echo "HTTP $HTTP_STATUS"
cat /tmp/manthan-resp.json
echo

echo
echo "=== 4. Verify strategy + credentials ==="
eval "$PG -d trading_db -c \"
SELECT s.strategy_id, s.strategy_name, s.active, s.trading_mode,
       tc.total_capital, tc.max_positions, tc.trailing_sl_pct, tc.stop_loss_pct
FROM strategies s JOIN trade_configs tc ON tc.strategy_id = s.strategy_id
WHERE s.user_id='$USER_ID' AND s.deleted_at IS NULL;\""
eval "$PG -d trading_execution -c \"
SELECT user_id, LEFT(indira_bearer_token, 15) AS tkn, updated_at
FROM user_credentials WHERE user_id='$USER_ID';\""

echo
echo "✓ Done. Watch rules-engine + trade-execution logs for the new entry."
