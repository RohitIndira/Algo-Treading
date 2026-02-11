#!/bin/bash
# Bash script to test Jobbing Strategy API endpoints

set -e

echo "========================================="
echo "Jobbing Strategy API Test Suite"
echo "========================================="
echo ""

# Configuration
API_BASE_URL="${API_BASE_URL:-http://localhost:8080}"
USER_ID="${TEST_USER_ID:-ISPL19027}"

echo "Configuration:"
echo "  API Base URL: $API_BASE_URL"
echo "  Test User ID: $USER_ID"
echo ""

# Test counter
test_count=0
passed_tests=0
failed_tests=0

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

function test_endpoint() {
    local name="$1"
    local method="$2"
    local url="$3"
    local body="$4"
    
    test_count=$((test_count + 1))
    echo -e "${CYAN}Test $test_count: $name${NC}"
    echo "  Method: $method"
    echo "  URL: $url"
    
    if [ -n "$body" ]; then
        echo "  Body: $body"
    fi
    
    if [ -n "$body" ]; then
        response=$(curl -s -X "$method" "$url" \
            -H "Content-Type: application/json" \
            -d "$body" \
            -w "\n%{http_code}")
    else
        response=$(curl -s -X "$method" "$url" \
            -w "\n%{http_code}")
    fi
    
    # Split response and status code
    http_code=$(echo "$response" | tail -n1)
    response_body=$(echo "$response" | sed '$d')
    
    if [ "$http_code" = "200" ] || [ "$http_code" = "201" ]; then
        echo -e "  Response:"
        echo "$response_body" | jq '.' 2>/dev/null || echo "$response_body"
        passed_tests=$((passed_tests + 1))
        echo -e "  ${GREEN}✓ PASSED${NC}"
    else
        echo -e "  ${RED}✗ FAILED (HTTP $http_code)${NC}"
        echo "  Response: $response_body"
        failed_tests=$((failed_tests + 1))
    fi
    echo ""
}

# Test 1: Configure Jobbing Strategy (Create)
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 1: Configure Jobbing Strategy${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

config_body='{
  "user_id": "'$USER_ID'",
  "configs": [
    {
      "token": "30274",
      "symbol": "SILVERCASE",
      "exchange": "NSE",
      "lower_range": 10.0,
      "higher_range": 15.0,
      "initial_buy_offset": 0.01,
      "distance_continue": 0.01,
      "quantity_per_order": 1,
      "max_quantity": 10,
      "trading_mode": "PAPER",
      "enabled": true
    },
    {
      "token": "500325",
      "symbol": "RELIANCE",
      "exchange": "NSE",
      "lower_range": 2400.0,
      "higher_range": 2600.0,
      "initial_buy_offset": 0.50,
      "distance_continue": 0.50,
      "quantity_per_order": 1,
      "max_quantity": 5,
      "trading_mode": "PAPER",
      "enabled": true
    }
  ]
}'

test_endpoint \
    "Configure jobbing strategy for 2 tokens" \
    "POST" \
    "$API_BASE_URL/api/v1/strategies/jobbing/configure" \
    "$config_body"

sleep 1

# Test 2: Get All Jobbing Configs
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 2: Get All Jobbing Configs${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

test_endpoint \
    "Get all jobbing configs for user" \
    "GET" \
    "$API_BASE_URL/api/v1/strategies/jobbing?user_id=$USER_ID"

sleep 1

# Test 3: Get Single Jobbing Config
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 3: Get Single Jobbing Config${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

test_endpoint \
    "Get jobbing config for specific token" \
    "GET" \
    "$API_BASE_URL/api/v1/strategies/jobbing/30274?user_id=$USER_ID"

sleep 1

# Test 4: Update Jobbing Config
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 4: Update Jobbing Config${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

update_body='{
  "user_id": "'$USER_ID'",
  "lower_range": 11.0,
  "higher_range": 14.0,
  "max_quantity": 15
}'

test_endpoint \
    "Update jobbing config parameters" \
    "PUT" \
    "$API_BASE_URL/api/v1/strategies/jobbing/30274" \
    "$update_body"

sleep 1

# Test 5: Disable Jobbing Config
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 5: Disable Jobbing Config${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

disable_body='{
  "user_id": "'$USER_ID'"
}'

test_endpoint \
    "Disable jobbing config" \
    "POST" \
    "$API_BASE_URL/api/v1/strategies/jobbing/30274/disable" \
    "$disable_body"

sleep 1

# Test 6: Get Enabled Only
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 6: Get Enabled Configs Only${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

test_endpoint \
    "Get only enabled jobbing configs" \
    "GET" \
    "$API_BASE_URL/api/v1/strategies/jobbing?user_id=$USER_ID&enabled_only=true"

sleep 1

# Test 7: Enable Jobbing Config
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 7: Enable Jobbing Config${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

enable_body='{
  "user_id": "'$USER_ID'"
}'

test_endpoint \
    "Enable jobbing config" \
    "POST" \
    "$API_BASE_URL/api/v1/strategies/jobbing/30274/enable" \
    "$enable_body"

sleep 1

# Test 8: Update to LIVE mode
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 8: Update Trading Mode to LIVE${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

live_mode_body='{
  "user_id": "'$USER_ID'",
  "trading_mode": "LIVE"
}'

test_endpoint \
    "Update trading mode to LIVE" \
    "PUT" \
    "$API_BASE_URL/api/v1/strategies/jobbing/30274" \
    "$live_mode_body"

sleep 1

# Test 9: Delete Jobbing Config (Optional - Commented)
echo -e "${YELLOW}=========================================${NC}"
echo -e "${YELLOW}Test 9: Delete Jobbing Config (Optional)${NC}"
echo -e "${YELLOW}=========================================${NC}"
echo ""

echo "Skipping delete test to preserve test data"
echo "To delete, uncomment the following:"
echo "# test_endpoint \"Delete jobbing config\" \"DELETE\" \"$API_BASE_URL/api/v1/strategies/jobbing/500325?user_id=$USER_ID\""
echo ""

# Summary
echo -e "${CYAN}=========================================${NC}"
echo -e "${CYAN}Test Summary${NC}"
echo -e "${CYAN}=========================================${NC}"
echo "Total Tests: $test_count"
echo -e "${GREEN}Passed: $passed_tests${NC}"

if [ $failed_tests -gt 0 ]; then
    echo -e "${RED}Failed: $failed_tests${NC}"
    echo ""
    echo -e "${RED}✗ Some tests failed!${NC}"
    exit 1
else
    echo -e "${GREEN}Failed: $failed_tests${NC}"
    echo ""
    echo -e "${GREEN}✓ All tests passed!${NC}"
    exit 0
fi
