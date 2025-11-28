#!/bin/bash

# API Gateway Test Script
# Base URL for all requests
BASE_URL="http://localhost:8081/api/v1"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}API Gateway REST API Test Suite${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Test 1: Health Check
echo -e "${GREEN}1. Health Check${NC}"
echo "GET $BASE_URL/health"
curl -X GET "$BASE_URL/health" | jq .
echo -e "\n"

# Test 2: Create Strategy
echo -e "${GREEN}2. Create Strategy${NC}"
echo "POST $BASE_URL/strategies"
curl -X POST "$BASE_URL/strategies" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "High Impact News Trader",
    "description": "Trades on high-impact news with positive sentiment",
    "activate_immediately": true,
    "conditions": {
      "match_all_news": false,
      "impact_score_threshold": 7,
      "sentiments": [1, 2],
      "categories": ["Results", "Board Meeting"],
      "exchanges": [1, 2],
      "price_range": {
        "min": 10.0,
        "max": 1000.0
      },
      "volume_threshold": 100000,
      "pct_change_threshold": 2.0
    },
    "trade_config": {
      "order_type": 1,
      "quantity": 100,
      "exchange": 1,
      "order_side": 1,
      "validity": "DAY",
      "max_position_size": 50000.0,
      "stop_loss_pct": 2.0,
      "take_profit_pct": 5.0
    },
    "risk_limits": {
      "max_daily_trades": 10,
      "max_loss_per_day": 10000.0,
      "position_sizing": 1,
      "max_portfolio_exposure_pct": 25.0,
      "enable_risk_checks": true
    }
  }' | jq .
echo -e "\n"

# Note: Save the strategy_id from the response above to use in the next tests
echo -e "${RED}Note: Copy the strategy_id from the response above for the following tests${NC}\n"
read -p "Enter the strategy_id from the response above: " STRATEGY_ID

# Test 3: List User Strategies
echo -e "${GREEN}3. List User Strategies (All)${NC}"
echo "GET $BASE_URL/users/IS14415/strategies"
curl -X GET "$BASE_URL/users/IS14415/strategies" | jq .
echo -e "\n"

# Test 4: List Active Strategies Only
echo -e "${GREEN}4. List User Strategies (Active Only)${NC}"
echo "GET $BASE_URL/users/IS14415/strategies?active_only=true"
curl -X GET "$BASE_URL/users/IS14415/strategies?active_only=true" | jq .
echo -e "\n"

# Test 5: List with Pagination
echo -e "${GREEN}5. List User Strategies (With Pagination)${NC}"
echo "GET $BASE_URL/users/IS14415/strategies?page=1&page_size=5"
curl -X GET "$BASE_URL/users/IS14415/strategies?page=1&page_size=5" | jq .
echo -e "\n"

# Test 6: Get Specific Strategy
if [ ! -z "$STRATEGY_ID" ]; then
  echo -e "${GREEN}6. Get Specific Strategy${NC}"
  echo "GET $BASE_URL/strategies/$STRATEGY_ID?user_id=IS14415"
  curl -X GET "$BASE_URL/strategies/$STRATEGY_ID?user_id=IS14415" | jq .
  echo -e "\n"
else
  echo -e "${RED}6. Skipping Get Strategy (no strategy_id provided)${NC}\n"
fi

# Test 7: Update Strategy
if [ ! -z "$STRATEGY_ID" ]; then
  echo -e "${GREEN}7. Update Strategy${NC}"
  echo "PUT $BASE_URL/strategies/$STRATEGY_ID"
  curl -X PUT "$BASE_URL/strategies/$STRATEGY_ID" \
    -H "Content-Type: application/json" \
    -d '{
      "user_id": "IS14415",
      "strategy_name": "Updated High Impact News Trader",
      "description": "Updated description for testing",
      "version": 1
    }' | jq .
  echo -e "\n"
else
  echo -e "${RED}7. Skipping Update Strategy (no strategy_id provided)${NC}\n"
fi

# Test 8: Deactivate Strategy
if [ ! -z "$STRATEGY_ID" ]; then
  echo -e "${GREEN}8. Deactivate Strategy${NC}"
  echo "POST $BASE_URL/strategies/$STRATEGY_ID/deactivate"
  curl -X POST "$BASE_URL/strategies/$STRATEGY_ID/deactivate" \
    -H "Content-Type: application/json" \
    -d '{"user_id": "IS14415"}' | jq .
  echo -e "\n"
else
  echo -e "${RED}8. Skipping Deactivate Strategy (no strategy_id provided)${NC}\n"
fi

# Test 9: Activate Strategy
if [ ! -z "$STRATEGY_ID" ]; then
  echo -e "${GREEN}9. Activate Strategy${NC}"
  echo "POST $BASE_URL/strategies/$STRATEGY_ID/activate"
  curl -X POST "$BASE_URL/strategies/$STRATEGY_ID/activate" \
    -H "Content-Type: application/json" \
    -d '{"user_id": "IS14415"}' | jq .
  echo -e "\n"
else
  echo -e "${RED}9. Skipping Activate Strategy (no strategy_id provided)${NC}\n"
fi

# Test 10: Delete Strategy (Optional - uncomment to test)
# if [ ! -z "$STRATEGY_ID" ]; then
#   echo -e "${GREEN}10. Delete Strategy${NC}"
#   echo "DELETE $BASE_URL/strategies/$STRATEGY_ID?user_id=IS14415"
#   curl -X DELETE "$BASE_URL/strategies/$STRATEGY_ID?user_id=IS14415" | jq .
#   echo -e "\n"
# else
#   echo -e "${RED}10. Skipping Delete Strategy (no strategy_id provided)${NC}\n"
# fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Test Suite Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
