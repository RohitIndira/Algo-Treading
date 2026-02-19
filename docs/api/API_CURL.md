User Config Service - API Testing Commands
These cURL commands are designed to test the api-gateway which routes requests to the user-config-service.

Prerequisites
Base URL: http://localhost:8081 (assuming Gateway runs on 8081)
Headers:
Authorization: Bearer <token> (Required, can be dummy for now if backend doesn't validate signature yet)
userId: <user_id> (Required, must match body user_id for write operations)
appId: AlgoTradingApp (Required)
source: Web (Required)
1. Create Strategy
Note: user_id in body MUST match userId header.

bash
curl --location 'http://localhost:8081/api/v1/strategies' \
--header 'Content-Type: application/json' \
--header 'Authorization: Bearer dummy-token' \
--header 'userId: user-123' \
--header 'appId: AlgoTradingApp' \
--header 'source: Web' \
--data '{
    "user_id": "user-123",
    "strategy_name": "My First Strategy",
    "description": "A test strategy for momentum trading",
    "trading_mode": "PAPER",
    "activate_immediately": true,
    "conditions": {
        "match_all_news": false,
        "impact_score_min": 5,
        "impact_score_max": 9,
        "sentiments": ["POSITIVE"],
        "categories": ["EARNINGS", "MERGERS"],
        "stock_codes": [500325, 532540],
        "market_cap_types": ["LARGE", "MID"],
        "min_market_cap": 1000.0,
        "max_market_cap": 50000.0,
        "min_price_change_pct": 1.5,
        "max_price_change_pct": 5.0,
        "min_volume": 100000,
        "exchanges": ["NSE"]
    },
    "trade_config": {
        "order_type": "MARKET",
        "product_type": "INTRADAY",
        "validity": "DAY",
        "quantity": 10,
        "exchange": "NSE",
        "order_side": "BUY",
        "stop_loss_pct": 2.0,
        "take_profit_pct": 5.0,
        "stop_loss_type": "FIXED"
    },
    "risk_limits": {
        "max_daily_trades": 5,
        "max_per_trade_risk": 500.0,
        "max_portfolio_exposure_pct": 10.0,
        "max_loss_per_day": 2000.0,
        "enable_risk_checks": true,
        "enable_auto_square_off": true,
        "auto_square_off_time": "15:15",
        "position_sizing": "FIXED"
    }
}'
2. Get Strategy
Replace {strategy_id} with the ID returned from creation.

bash
curl --location 'http://localhost:8081/api/v1/strategies/{strategy_id}?user_id=user-123' \
--header 'Authorization: Bearer dummy-token' \
--header 'userId: user-123' \
--header 'appId: AlgoTradingApp' \
--header 'source: Web'
3. Update Strategy
Replace {strategy_id} with the actual ID. Header/Body user match required.

bash
curl --location --request PUT 'http://localhost:8081/api/v1/strategies/{strategy_id}' \
--header 'Content-Type: application/json' \
--header 'Authorization: Bearer dummy-token' \
--header 'userId: user-123' \
--header 'appId: AlgoTradingApp' \
--header 'source: Web' \
--data '{
    "user_id": "user-123",
    "strategy_name": "Updated Strategy Name",
    "version": 1,
    "conditions": {
        "impact_score_min": 7,
        "impact_score_max": 10
    }
}'
4. List User Strategies
bash
curl --location 'http://localhost:8081/api/v1/users/user-123/strategies?page=1&page_size=10' \
--header 'Authorization: Bearer dummy-token' \
--header 'userId: user-123' \
--header 'appId: AlgoTradingApp' \
--header 'source: Web'
5. Activate Strategy
bash
curl --location --request POST 'http://localhost:8081/api/v1/strategies/{strategy_id}/activate' \
--header 'Content-Type: application/json' \
--header 'Authorization: Bearer dummy-token' \
--header 'userId: user-123' \
--header 'appId: AlgoTradingApp' \
--header 'source: Web' \
--data '{
    "user_id": "user-123"
}'
6. Deactivate Strategy
bash
curl --location --request POST 'http://localhost:8081/api/v1/strategies/{strategy_id}/deactivate' \
--header 'Content-Type: application/json' \
--header 'Authorization: Bearer dummy-token' \
--header 'userId: user-123' \
--header 'appId: AlgoTradingApp' \
--header 'source: Web' \
--data '{
    "user_id": "user-123"
}'
7. Delete Strategy
bash
curl --location --request DELETE 'http://localhost:8081/api/v1/strategies/{strategy_id}?user_id=user-123' \
--header 'Authorization: Bearer dummy-token' \
--header 'userId: user-123' \
--header 'appId: AlgoTradingApp' \
--header 'source: Web'
8. Health Check
bash
curl --location 'http://localhost:8081/api/v1/health'