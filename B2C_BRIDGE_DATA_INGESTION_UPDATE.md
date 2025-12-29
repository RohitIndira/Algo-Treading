# B2C Bridge & Data Ingestion Service Update Summary

## Changes Completed

### 1. B2C Bridge Updates (b2c-api-python/b2c_bridge.py)

#### Changed Subscription Method: Touchline → Bestfive
- **_on_websocket_open()**: Updated to use `bestfive_subscription()` instead of `touchline_subscription()`
- **_resubscribe_tokens()**: Updated resubscription logic to use bestfive subscription
- **_start_heartbeat()**: Updated heartbeat ping mechanism to use bestfive instead of touchline

#### Implemented Bestfive Data Processing
- **_on_bestfive() callback**: Completely redesigned to process and output market data
  - Extracts 5-level bid-ask depth data (BidPrice1-5, AskPrice1-5)
  - Extracts bid and ask quantities for each level
  - Streams structured JSON data with bid-ask depth to stdout:
    ```json
    {
      "token": "476",
      "ltp": 150.25,
      "bid_prices": [150.20, 150.15, 150.10, 150.05, 150.00],
      "bid_quantities": [1000, 2000, 1500, 3000, 500],
      "ask_prices": [150.30, 150.35, 150.40, 150.45, 150.50],
      "ask_quantities": [1500, 1000, 2000, 1000, 3000],
      "timestamp": 1735564234000,
      ...
    }
    ```

### 2. Data Ingestion Service Refactoring

#### Removed MongoDB Dependency
- **Removed imports**:
  - `github.com/RohitIndira/Algo-Treading/pkg/database/mongodb`
  - MongoDB client initialization and connection handling

- **Config changes** (`config/config.go`):
  - Removed: `MongoURI`, `MongoDatabase`, `MongoCollection`, `MongoConnectTimeout`
  - Added: `B2CBridgePath`, `B2CTokens` (comma-separated list)
  - Updated topic from `market.data.news` → `market.data.live`
  - Environment variables:
    - `B2C_BRIDGE_PATH`: Path to b2c_bridge.py (default: `/app/b2c-api-python/b2c_bridge.py`)
    - `B2C_TOKENS`: Comma-separated list of tokens to subscribe (e.g., `"476:NSE,500410:BSE"`)
    - `KAFKA_TOPIC_MARKET_DATA`: Kafka topic for live market data

#### Created B2C Watcher (`internal/watcher/b2c_watcher.go`)
- **B2CWatcher**: New struct that manages the B2C bridge subprocess
  - Starts Python B2C bridge as subprocess with tokens as arguments
  - Reads JSON-formatted market data from stdout
  - Validates market data (checks token, LTP > 0, timestamp within 1 minute)
  - Publishes to Kafka with token as key
  - Handles graceful shutdown and process cleanup

- **B2CMarketData struct**: Represents live market price data with:
  - Basic OHLCV data (open, high, low, close, volume)
  - Best 5 bid-ask depth (prices and quantities)
  - 52-week high/low
  - Percentage change and timestamp

#### Updated Main Service (`cmd/main.go`)
- Removed MongoDB initialization
- Updated to:
  1. Load B2C configuration (bridge path and tokens)
  2. Validate B2C configuration existence
  3. Create B2CWatcher instead of MongoWatcher
  4. Start B2C bridge and stream market data to Kafka

## Data Flow

```
Python B2C Bridge (bestfive subscription)
    ↓
Market Data JSON (stdout)
    ↓
Data-Ingestion Service (reads subprocess stdout)
    ↓
Validation & Processing
    ↓
Kafka Topic (market.data.live)
    ↓
Downstream Services (Trade Execution, Risk Management, etc.)
```

## Environment Variables Required

```bash
# B2C Configuration
B2C_BRIDGE_PATH="/app/b2c-api-python/b2c_bridge.py"
B2C_TOKENS="476:NSE,500410:BSE,..."

# Kafka Configuration
KAFKA_BROKERS="localhost:9092"
KAFKA_TOPIC_MARKET_DATA="market.data.live"

# (No MongoDB variables needed anymore)
```

## Backward Compatibility Notes

- **Removed**: All MongoDB dependencies from data-ingestion service
- **Changed**: Kafka topic name from `market.data.news` to `market.data.live`
- **Changed**: Message format - now contains live market prices with bid-ask depth instead of news documents
- **Changed**: Message key - token instead of MongoDB document ID

## Integration Points

1. **B2C Bridge → Data Ingestion**: Python bridge runs as subprocess, outputs JSON via stdout
2. **Data Ingestion → Kafka**: Market data published with token as key
3. **Kafka Consumers**: Any service subscribing to `market.data.live` topic receives live market updates

## Testing Checklist

- [ ] B2C bridge successfully subscribes to bestfive for given tokens
- [ ] Bestfive callback receives and processes market data with bid-ask depth
- [ ] Data-ingestion service starts B2C bridge subprocess correctly
- [ ] Market data is validated (token exists, LTP > 0, timestamp recent)
- [ ] Messages are published to Kafka with correct format
- [ ] Graceful shutdown kills B2C bridge process cleanly
- [ ] Error handling for missing B2C_TOKENS or B2C_BRIDGE_PATH environment variables
