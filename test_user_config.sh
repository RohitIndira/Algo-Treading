#!/bin/bash

USER_ID="test-user-$(date +%s)"

echo "=== Testing User Config Service ==="
echo "User ID: $USER_ID"

# 1. Create Strategy
echo -e "\n1. Creating strategy..."
RESPONSE=$(grpcurl -plaintext -d "{
  \"user_id\": \"$USER_ID\",
  \"strategy_name\": \"Test Strategy-2\",
  \"description\": \"Automated test-2\",
  \"activate_immediately\": true,
  \"conditions\": {
    \"impact_score_threshold\": 1,
    \"sentiments\": [],
    \"stock_codes\": [],
    \"exchanges\": []
  },
  \"trade_config\": {
    \"order_type\": \"ORDER_TYPE_MARKET\",
    \"order_side\": \"ORDER_SIDE_BUY\",
    \"quantity\": 10,
    \"exchange\": \"EXCHANGE_NSE\",
    \"validity\": \"DAY\"
  },
  \"risk_limits\": {
    \"max_daily_trades\": 5,
    \"max_loss_per_day\": 5000.00,
    \"position_sizing\": \"POSITION_SIZING_FIXED\",
    \"enable_risk_checks\": true
  }
}" localhost:50051 user_config.UserConfigService/CreateStrategy)

STRATEGY_ID=$(echo $RESPONSE | jq -r '.strategy.strategy_id')
echo "Strategy ID: $STRATEGY_ID"

# 2. Get Strategy
echo -e "\n2. Getting strategy..."
grpcurl -plaintext -d "{
  \"strategy_id\": \"$STRATEGY_ID\",
  \"user_id\": \"$USER_ID\"
}" localhost:50051 user_config.UserConfigService/GetStrategy

# 3. List Strategies
echo -e "\n3. Listing strategies..."
grpcurl -plaintext -d "{
  \"user_id\": \"$USER_ID\",
  \"active_only\": false
}" localhost:50051 user_config.UserConfigService/ListUserStrategies

# # 4. Deactivate
# echo -e "\n4. Deactivating strategy..."
# grpcurl -plaintext -d "{
#   \"strategy_id\": \"$STRATEGY_ID\",
#   \"user_id\": \"$USER_ID\"
# }" localhost:50051 user_config.UserConfigService/DeactivateStrategy

echo -e "\n=== Test Complete ==="
