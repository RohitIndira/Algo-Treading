# Test User Configurations for News Matching

This directory contains simple test configurations designed to match mostly all news events for testing the news pipeline.

## Test Configurations

### 1. `test_news_config.json` - Match Most News
**User ID**: `TEST_USER_001`

This configuration uses broad filters to match almost all news:
- **Impact Score**: 1 (minimum threshold - matches everything)
- **Sentiments**: ALL (positive, neutral, negative)
- **Categories**: 10 broad categories covering most news types
- **Stock Codes**: Empty array (matches all stocks)
- **Exchanges**: NSE and BSE (covers all major exchanges)

**Perfect for**: Testing if news events are being properly ingested and matched

### 2. `test_all_news_config.json` - Match EVERY News Event
**User ID**: `TEST_USER_002`

This configuration uses the `match_all_news` flag:
- **match_all_news**: `true` (overrides all filters)
- **Impact Score**: 1 (minimum threshold)
- All other conditions are ignored when `match_all_news` is enabled

**Perfect for**: Testing the entire news-to-trade pipeline without any filtering

## How to Use

### Option 1: Using PowerShell Script (Recommended for Windows)
```powershell
.\test_create_strategy.ps1
```

### Option 2: Using Bash Script (Git Bash or WSL)
```bash
./test_create_strategy.sh
```

### Option 3: Manual Creation with grpcurl

**Create Test Strategy 1:**
```bash
grpcurl -plaintext -d @test_news_config.json localhost:50051 user_config.UserConfigService/CreateStrategy
```

**Create Test Strategy 2:**
```bash
grpcurl -plaintext -d @test_all_news_config.json localhost:50051 user_config.UserConfigService/CreateStrategy
```

### Option 4: Using Postman
1. Import the service: `localhost:50051`
2. Select method: `user_config.UserConfigService/CreateStrategy`
3. Copy-paste JSON from `test_news_config.json` or `test_all_news_config.json`
4. Send request

## Verify Creation

List strategies for Test User 1:
```bash
grpcurl -plaintext -d '{"user_id": "TEST_USER_001"}' localhost:50051 user_config.UserConfigService/ListUserStrategies
```

List strategies for Test User 2:
```bash
grpcurl -plaintext -d '{"user_id": "TEST_USER_002"}' localhost:50051 user_config.UserConfigService/ListUserStrategies
```

## Key Differences

| Feature | Test Config 1 | Test Config 2 |
|---------|--------------|--------------|
| Match Method | Broad filters | `match_all_news` flag |
| Impact Score | 1 (min) | 1 (min) |
| Sentiments | All 3 types | Ignored |
| Categories | 10 categories | Ignored |
| Stock Codes | Empty (all) | Ignored |
| Use Case | Testing with some control | Testing everything |

## Trade Configuration (Both Strategies)

Both test configs use minimal, safe trading parameters:
- **Order Type**: MARKET
- **Quantity**: 1 share (minimal exposure)
- **Exchange**: NSE
- **Order Side**: BUY
- **Max Daily Trades**: 100-200
- **Position Sizing**: FIXED

## Risk Limits

Both configs have generous risk limits for testing:
- High max daily trades (100-200)
- High max daily loss (50k-100k INR)
- High portfolio exposure (50%-75%)
- Risk checks enabled

## Monitoring Test Results

After creating these strategies and sending news events, monitor:
1. **Kafka Topics**: Check `strategy-matches` topic for matched events
2. **Database**: Query `strategies` table to see active strategies
3. **Logs**: Watch rules-engine logs for matching activity
4. **Trade Execution**: Monitor if trades are being generated

## Cleanup

To deactivate test strategies:
```bash
# Get strategy ID from list command, then:
grpcurl -plaintext -d '{"strategy_id": "YOUR_STRATEGY_ID"}' localhost:50051 user_config.UserConfigService/DeactivateStrategy
```

To delete test strategies:
```bash
grpcurl -plaintext -d '{"strategy_id": "YOUR_STRATEGY_ID", "user_id": "TEST_USER_001"}' localhost:50051 user_config.UserConfigService/DeleteStrategy
```

## Prerequisites

Ensure these services are running:
1. User Config Service (port 50051)
2. PostgreSQL database
3. Kafka (for publishing strategy events)

## Troubleshooting

**If strategy creation fails:**
1. Check if user-config service is running: `grpcurl -plaintext localhost:50051 list`
2. Verify database connection in `.env` file
3. Check service logs for errors
4. Ensure Kafka is running if publish fails

**If news isn't matching:**
1. Verify strategy is ACTIVE in database
2. Check rules-engine is consuming from Kafka
3. Verify news events are being published
4. Check rules-engine logs for matching logic
