#!/bin/bash

# Test script to create user config strategies for testing
# This script uses grpcurl to create test strategies

echo "=========================================="
echo "Creating Test User Config Strategies"
echo "=========================================="
echo ""

# Check if grpcurl is installed
if ! command -v grpcurl &> /dev/null; then
    echo "❌ grpcurl is not installed. Install it with:"
    echo "   go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest"
    exit 1
fi

# Test 1: Match Most News (with broad filters)
echo "📝 Test 1: Creating 'Match Most News' strategy..."
grpcurl -plaintext -d @ localhost:50051 user_config.UserConfigService/CreateStrategy <<EOF
{
  "user_id": "TEST_USER_001",
  "strategy_name": "Match All News - Test Strategy",
  "description": "Simple test strategy that matches most news events with minimal filters",
  "activate_immediately": true,
  "conditions": {
    "impact_score_threshold": 1,
    "sentiments": [
      "SENTIMENT_POSITIVE",
      "SENTIMENT_NEUTRAL",
      "SENTIMENT_NEGATIVE"
    ],
    "categories": [
      "Results",
      "Board Meeting",
      "Announcements",
      "Corporate Actions",
      "Dividends",
      "Mergers & Acquisitions",
      "Regulatory",
      "Financial Results",
      "Management Changes",
      "Market News"
    ],
    "stock_codes": [],
    "exchanges": [
      "EXCHANGE_NSE",
      "EXCHANGE_BSE"
    ]
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "quantity": 1,
    "exchange": "EXCHANGE_NSE",
    "order_side": "ORDER_SIDE_BUY",
    "validity": "DAY",
    "max_position_size": 10000.0,
    "stop_loss_pct": 1.0,
    "take_profit_pct": 2.0
  },
  "risk_limits": {
    "max_daily_trades": 100,
    "max_loss_per_day": 50000.0,
    "position_sizing": "POSITION_SIZING_FIXED",
    "max_portfolio_exposure_pct": 50.0,
    "max_per_trade_risk": 5000.0,
    "enable_risk_checks": true
  }
}
EOF

echo ""
echo "✅ Test 1 completed"
echo ""
echo "=========================================="
echo ""

# Test 2: Match ALL News (using match_all_news flag)
echo "📝 Test 2: Creating 'Match ALL News' strategy (with match_all_news flag)..."
grpcurl -plaintext -d @ localhost:50051 user_config.UserConfigService/CreateStrategy <<EOF
{
  "user_id": "TEST_USER_002",
  "strategy_name": "Match ALL News Events",
  "description": "Matches every single news event regardless of sentiment, category, or stock",
  "activate_immediately": true,
  "conditions": {
    "match_all_news": true,
    "impact_score_threshold": 1
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "quantity": 1,
    "exchange": "EXCHANGE_NSE",
    "order_side": "ORDER_SIDE_BUY",
    "validity": "DAY",
    "max_position_size": 5000.0,
    "stop_loss_pct": 0.5,
    "take_profit_pct": 1.0
  },
  "risk_limits": {
    "max_daily_trades": 200,
    "max_loss_per_day": 100000.0,
    "position_sizing": "POSITION_SIZING_FIXED",
    "max_portfolio_exposure_pct": 75.0,
    "max_per_trade_risk": 2000.0,
    "enable_risk_checks": true
  }
}
EOF

echo ""
echo "✅ Test 2 completed"
echo ""
echo "=========================================="
echo ""

# Test 3: GOLDEN STRATEGY (empty arrays - should match everything)
echo "📝 Test 3: Creating 'GOLDEN STRATEGY' (empty filters, no restrictions)..."
grpcurl -plaintext -d @ localhost:50051 user_config.UserConfigService/CreateStrategy <<EOF
{
  "user_id": "user-123",
  "strategy_name": "GOLDEN-STRATEGY",
  "description": "Trade everything - no restrictions",
  "activate_immediately": true,
  "conditions": {
    "match_all_news": false,
    "impact_score_threshold": 1,
    "sentiments": [],
    "categories": [],
    "stock_codes": [],
    "exchanges": [],
    "price_range": {
      "min_price": 0,
      "max_price": 999999999
    },
    "volume_threshold": 0,
    "pct_change_threshold": 0
  },
  "trade_config": {
    "order_type": "ORDER_TYPE_MARKET",
    "order_side": "ORDER_SIDE_BUY",
    "quantity": 1,
    "exchange": "EXCHANGE_NSE",
    "validity": "DAY"
  },
  "risk_limits": {
    "max_daily_trades": 100,
    "max_loss_per_day": 50000.0,
    "enable_risk_checks": true,
    "position_sizing": "POSITION_SIZING_FIXED"
  }
}
EOF

echo ""
echo "✅ Test 3 completed"
echo ""
echo "=========================================="
echo "✅ All test strategies created successfully!"
echo "=========================================="
echo ""
echo "📊 To list all strategies, run:"
echo "   grpcurl -plaintext -d '{\"user_id\": \"TEST_USER_001\"}' localhost:50051 user_config.UserConfigService/ListUserStrategies"
echo ""
echo "   grpcurl -plaintext -d '{\"user_id\": \"TEST_USER_002\"}' localhost:50051 user_config.UserConfigService/ListUserStrategies"
echo ""
echo "   grpcurl -plaintext -d '{\"user_id\": \"user-123\"}' localhost:50051 user_config.UserConfigService/ListUserStrategies"
