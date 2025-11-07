# Protocol Buffer Definitions

## Overview

This document contains all Protocol Buffer (protobuf) definitions for the trading system's gRPC services. These definitions form the contract between all microservices.

---

## Common Types (`api/proto/common/common.proto`)

```protobuf
syntax = "proto3";

package common;

option go_package = "github.com/trading-system/api/proto/common";

import "google/protobuf/timestamp.proto";

// Money represents a monetary value
message Money {
  string currency = 1;  // e.g., "INR"
  double amount = 2;
}

// Decimal represents a high-precision decimal number
message Decimal {
  string value = 1;  // String representation for precision
}

// PriceRange represents a price range filter
message PriceRange {
  double min_price = 1;
  double max_price = 2;
}

// TimeRange represents a time range filter
message TimeRange {
  google.protobuf.Timestamp start_time = 1;
  google.protobuf.Timestamp end_time = 2;
}

// Pagination for list requests
message Pagination {
  int32 page = 1;
  int32 page_size = 2;
}

// Sort order for list results
message SortOrder {
  string field = 1;
  bool ascending = 2;
}

// Exchange enum
enum Exchange {
  EXCHANGE_UNSPECIFIED = 0;
  NSE = 1;
  BSE = 2;
}

// Sentiment enum
enum Sentiment {
  SENTIMENT_UNSPECIFIED = 0;
  POSITIVE = 1;
  NEUTRAL = 2;
  NEGATIVE = 3;
}

// Order type enum
enum OrderType {
  ORDER_TYPE_UNSPECIFIED = 0;
  MARKET = 1;
  LIMIT = 2;
  STOP_LOSS = 3;
  STOP_LOSS_MARKET = 4;
}

// Order status enum
enum OrderStatus {
  ORDER_STATUS_UNSPECIFIED = 0;
  PENDING = 1;
  SUBMITTED = 2;
  PARTIALLY_FILLED = 3;
  FILLED = 4;
  CANCELLED = 5;
  REJECTED = 6;
  EXPIRED = 7;
}

// Generic empty response
message Empty {}

// Generic success response
message SuccessResponse {
  bool success = 1;
  string message = 2;
}
```

---

## Error Definitions (`api/proto/common/errors.proto`)

```protobuf
syntax = "proto3";

package common;

option go_package = "github.com/trading-system/api/proto/common";

// Error codes
enum ErrorCode {
  ERROR_CODE_UNSPECIFIED = 0;
  
  // Client errors (4xx)
  INVALID_REQUEST = 400;
  UNAUTHORIZED = 401;
  FORBIDDEN = 403;
  NOT_FOUND = 404;
  CONFLICT = 409;
  VALIDATION_ERROR = 422;
  RATE_LIMIT_EXCEEDED = 429;
  
  // Server errors (5xx)
  INTERNAL_ERROR = 500;
  SERVICE_UNAVAILABLE = 503;
  TIMEOUT = 504;
  
  // Business logic errors
  INSUFFICIENT_BALANCE = 1001;
  RISK_LIMIT_EXCEEDED = 1002;
  DUPLICATE_ORDER = 1003;
  MARKET_CLOSED = 1004;
  INVALID_SYMBOL = 1005;
}

// Error details
message ErrorDetails {
  ErrorCode code = 1;
  string message = 2;
  string field = 3;  // For validation errors
  map<string, string> metadata = 4;
}

// Error response
message ErrorResponse {
  ErrorDetails error = 1;
  string request_id = 2;
}
```

---

## User Configuration Service (`api/proto/user_config/user_config.proto`)

```protobuf
syntax = "proto3";

package user_config;

option go_package = "github.com/trading-system/api/proto/user_config";

import "google/protobuf/timestamp.proto";
import "common/common.proto";
import "common/errors.proto";

// ========== Data Models ==========

// Trading conditions for a strategy
message Conditions {
  int32 impact_score_threshold = 1;  // Minimum impact score (1-10)
  repeated common.Sentiment sentiments = 2;  // Allowed sentiments
  repeated string categories = 3;  // News categories to monitor
  repeated int64 stocks = 4;  // Stock codes to monitor (empty = all)
  common.PriceRange price_range = 5;  // Price filter
  int64 volume_threshold = 6;  // Minimum trading volume
  double pct_change_threshold = 7;  // Minimum percentage change
}

// Trade configuration
message TradeConfig {
  common.OrderType order_type = 1;
  int32 quantity = 2;  // Number of shares
  int64 max_position_size = 3;  // Max position value in INR
  double stop_loss_pct = 4;  // Stop loss percentage
  double take_profit_pct = 5;  // Take profit percentage
  common.Exchange exchange = 6;  // Preferred exchange
}

// Risk limits
message RiskLimits {
  int32 max_daily_trades = 1;  // Max trades per day
  int64 max_loss_per_day = 2;  // Max loss in INR per day
  string position_sizing = 3;  // "FIXED" or "PERCENTAGE"
  double max_portfolio_exposure = 4;  // Max % of portfolio
}

// Complete strategy definition
message Strategy {
  string strategy_id = 1;
  string user_id = 2;
  string strategy_name = 3;
  string description = 4;
  bool active = 5;
  Conditions conditions = 6;
  TradeConfig trade_config = 7;
  RiskLimits risk_limits = 8;
  google.protobuf.Timestamp created_at = 9;
  google.protobuf.Timestamp updated_at = 10;
  int32 version = 11;  // For optimistic locking
}

// ========== Request/Response Messages ==========

// Create strategy request
message CreateStrategyRequest {
  string user_id = 1;
  string strategy_name = 2;
  string description = 3;
  Conditions conditions = 4;
  TradeConfig trade_config = 5;
  RiskLimits risk_limits = 6;
}

message CreateStrategyResponse {
  Strategy strategy = 1;
}

// Update strategy request
message UpdateStrategyRequest {
  string strategy_id = 1;
  string user_id = 2;
  optional string strategy_name = 3;
  optional string description = 4;
  optional Conditions conditions = 5;
  optional TradeConfig trade_config = 6;
  optional RiskLimits risk_limits = 7;
  int32 version = 8;  // For optimistic locking
}

message UpdateStrategyResponse {
  Strategy strategy = 1;
}

// Delete strategy request
message DeleteStrategyRequest {
  string strategy_id = 1;
  string user_id = 2;
}

message DeleteStrategyResponse {
  common.SuccessResponse result = 1;
}

// Get strategy request
message GetStrategyRequest {
  string strategy_id = 1;
  string user_id = 2;
}

message GetStrategyResponse {
  Strategy strategy = 1;
}

// List strategies request
message ListStrategiesRequest {
  string user_id = 1;
  optional bool active_only = 2;
  optional common.Pagination pagination = 3;
  optional common.SortOrder sort = 4;
}

message ListStrategiesResponse {
  repeated Strategy strategies = 1;
  int32 total_count = 2;
  int32 page = 3;
  int32 page_size = 4;
}

// Activate strategy
message ActivateStrategyRequest {
  string strategy_id = 1;
  string user_id = 2;
}

message ActivateStrategyResponse {
  Strategy strategy = 1;
}

// Deactivate strategy
message DeactivateStrategyRequest {
  string strategy_id = 1;
  string user_id = 2;
}

message DeactivateStrategyResponse {
  Strategy strategy = 1;
}

// Get active user strategies (for Rules Engine)
message GetActiveUserStrategiesRequest {
  repeated string user_ids = 1;  // Empty = all users
}

message GetActiveUserStrategiesResponse {
  repeated Strategy strategies = 1;
}

// Bulk update strategy status (internal)
message BulkUpdateStatusRequest {
  repeated string strategy_ids = 1;
  bool active = 2;
  string reason = 3;
}

message BulkUpdateStatusResponse {
  int32 updated_count = 1;
}

// ========== Service Definition ==========

service UserConfigService {
  // Strategy CRUD operations
  rpc CreateStrategy(CreateStrategyRequest) returns (CreateStrategyResponse);
  rpc UpdateStrategy(UpdateStrategyRequest) returns (UpdateStrategyResponse);
  rpc DeleteStrategy(DeleteStrategyRequest) returns (DeleteStrategyResponse);
  rpc GetStrategy(GetStrategyRequest) returns (GetStrategyResponse);
  rpc ListStrategies(ListStrategiesRequest) returns (ListStrategiesResponse);
  
  // Strategy activation
  rpc ActivateStrategy(ActivateStrategyRequest) returns (ActivateStrategyResponse);
  rpc DeactivateStrategy(DeactivateStrategyRequest) returns (DeactivateStrategyResponse);
  
  // Bulk operations (for internal services)
  rpc GetActiveUserStrategies(GetActiveUserStrategiesRequest) returns (GetActiveUserStrategiesResponse);
  rpc BulkUpdateStatus(BulkUpdateStatusRequest) returns (BulkUpdateStatusResponse);
}
```

---

## Trade Execution Service (`api/proto/trade_execution/trade_execution.proto`)

```protobuf
syntax = "proto3";

package trade_execution;

option go_package = "github.com/trading-system/api/proto/trade_execution";

import "google/protobuf/timestamp.proto";
import "common/common.proto";
import "common/errors.proto";

// ========== Data Models ==========

// Order details
message Order {
  string order_id = 1;
  string user_id = 2;
  string strategy_id = 3;
  string event_id = 4;  // Market event that triggered the order
  
  // Stock details
  int64 stock_code = 5;
  string symbol = 6;
  common.Exchange exchange = 7;
  
  // Order details
  common.OrderType order_type = 8;
  int32 quantity = 9;
  double price = 10;  // For limit orders
  
  // Risk management
  optional double stop_loss = 11;
  optional double take_profit = 12;
  
  // Status
  common.OrderStatus status = 13;
  string odin_order_id = 14;  // External order ID from Odin
  
  // Execution details
  int32 filled_quantity = 15;
  double filled_price = 16;
  double commission = 17;
  
  // Timestamps
  google.protobuf.Timestamp created_at = 18;
  google.protobuf.Timestamp updated_at = 19;
  google.protobuf.Timestamp executed_at = 20;
  
  // Error info
  optional string error_message = 21;
  optional int32 retry_count = 22;
}

// Execution details
message Execution {
  string execution_id = 1;
  string order_id = 2;
  int32 quantity = 3;
  double price = 4;
  double commission = 5;
  google.protobuf.Timestamp timestamp = 6;
  string exchange_trade_id = 7;
}

// Order statistics
message OrderStats {
  int32 total_orders = 1;
  int32 filled_orders = 2;
  int32 pending_orders = 3;
  int32 rejected_orders = 4;
  double total_value = 5;
  double total_commission = 6;
}

// ========== Request/Response Messages ==========

// Get order status
message GetOrderStatusRequest {
  string order_id = 1;
  string user_id = 2;
}

message GetOrderStatusResponse {
  Order order = 1;
  repeated Execution executions = 2;
}

// Get user orders
message GetUserOrdersRequest {
  string user_id = 1;
  optional common.OrderStatus status = 2;
  optional common.TimeRange time_range = 3;
  optional common.Pagination pagination = 4;
}

message GetUserOrdersResponse {
  repeated Order orders = 1;
  int32 total_count = 2;
  OrderStats stats = 3;
}

// Cancel order
message CancelOrderRequest {
  string order_id = 1;
  string user_id = 2;
  string reason = 3;
}

message CancelOrderResponse {
  Order order = 1;
  common.SuccessResponse result = 2;
}

// Get order history
message GetOrderHistoryRequest {
  string user_id = 1;
  optional string strategy_id = 2;
  common.TimeRange time_range = 3;
  optional common.Pagination pagination = 4;
}

message GetOrderHistoryResponse {
  repeated Order orders = 1;
  int32 total_count = 2;
  OrderStats stats = 3;
}

// Get order by event (internal)
message GetOrderByEventRequest {
  string event_id = 1;
}

message GetOrderByEventResponse {
  repeated Order orders = 1;
}

// Update order status (internal)
message UpdateOrderStatusRequest {
  string order_id = 1;
  common.OrderStatus status = 2;
  optional string error_message = 3;
  optional int32 filled_quantity = 4;
  optional double filled_price = 5;
}

message UpdateOrderStatusResponse {
  Order order = 1;
}

// Get pending orders (internal)
message GetPendingOrdersRequest {
  optional string user_id = 1;
  int32 limit = 2;
}

message GetPendingOrdersResponse {
  repeated Order orders = 1;
}

// ========== Service Definition ==========

service TradeExecutionService {
  // Public APIs
  rpc GetOrderStatus(GetOrderStatusRequest) returns (GetOrderStatusResponse);
  rpc GetUserOrders(GetUserOrdersRequest) returns (GetUserOrdersResponse);
  rpc CancelOrder(CancelOrderRequest) returns (CancelOrderResponse);
  rpc GetOrderHistory(GetOrderHistoryRequest) returns (GetOrderHistoryResponse);
  
  // Internal APIs
  rpc GetOrderByEvent(GetOrderByEventRequest) returns (GetOrderByEventResponse);
  rpc UpdateOrderStatus(UpdateOrderStatusRequest) returns (UpdateOrderStatusResponse);
  rpc GetPendingOrders(GetPendingOrdersRequest) returns (GetPendingOrdersResponse);
}
```

---

## Risk Management Service (`api/proto/risk_management/risk_management.proto`)

```protobuf
syntax = "proto3";

package risk_management;

option go_package = "github.com/trading-system/api/proto/risk_management";

import "google/protobuf/timestamp.proto";
import "common/common.proto";
import "trade_execution/trade_execution.proto";

// ========== Data Models ==========

// Risk check result
message RiskCheckResult {
  bool approved = 1;
  repeated string violations = 2;  // List of violated rules
  string reason = 3;
  RiskMetrics current_metrics = 4;
}

// Risk metrics
message RiskMetrics {
  string user_id = 1;
  
  // Daily metrics
  int32 daily_trade_count = 2;
  double daily_loss = 3;
  double daily_profit = 4;
  double daily_net_pnl = 5;
  
  // Position metrics
  map<string, Position> positions = 6;  // stock_code -> position
  double total_exposure = 7;
  double portfolio_value = 8;
  double margin_used = 9;
  
  // Risk indicators
  double max_drawdown = 10;
  double var_95 = 11;  // Value at Risk 95%
  double sharpe_ratio = 12;
  
  google.protobuf.Timestamp updated_at = 13;
}

// Position details
message Position {
  string stock_code = 1;
  int32 quantity = 2;
  double avg_price = 3;
  double current_price = 4;
  double unrealized_pnl = 5;
  double realized_pnl = 6;
  common.Exchange exchange = 7;
}

// Risk limits
message RiskLimitsConfig {
  string user_id = 1;
  int32 max_daily_trades = 2;
  double max_daily_loss = 3;
  double max_position_size = 4;
  double max_portfolio_exposure = 5;
  double margin_limit = 6;
  repeated string blocked_stocks = 7;
}

// Risk event
message RiskEvent {
  string event_id = 1;
  string user_id = 2;
  string event_type = 3;  // LIMIT_BREACH, CIRCUIT_BREAKER, etc.
  string severity = 4;  // LOW, MEDIUM, HIGH, CRITICAL
  string description = 5;
  map<string, string> metadata = 6;
  google.protobuf.Timestamp timestamp = 7;
  bool resolved = 8;
}

// ========== Request/Response Messages ==========

// Pre-trade risk check
message CheckPreTradeRiskRequest {
  string user_id = 1;
  string strategy_id = 2;
  int64 stock_code = 3;
  common.OrderType order_type = 4;
  int32 quantity = 5;
  double price = 6;
  common.Exchange exchange = 7;
}

message CheckPreTradeRiskResponse {
  RiskCheckResult result = 1;
}

// Update post-trade metrics
message UpdatePostTradeMetricsRequest {
  string user_id = 1;
  trade_execution.Order order = 2;
  double realized_pnl = 3;
}

message UpdatePostTradeMetricsResponse {
  RiskMetrics metrics = 1;
}

// Get risk metrics
message GetRiskMetricsRequest {
  string user_id = 1;
  optional google.protobuf.Timestamp as_of_date = 2;
}

message GetRiskMetricsResponse {
  RiskMetrics metrics = 1;
}

// Set risk limits
message SetRiskLimitsRequest {
  string user_id = 1;
  RiskLimitsConfig limits = 2;
}

message SetRiskLimitsResponse {
  common.SuccessResponse result = 1;
}

// Get risk limits
message GetRiskLimitsRequest {
  string user_id = 1;
}

message GetRiskLimitsResponse {
  RiskLimitsConfig limits = 1;
}

// Get risk events
message GetRiskEventsRequest {
  string user_id = 1;
  optional common.TimeRange time_range = 2;
  optional bool unresolved_only = 3;
  optional common.Pagination pagination = 4;
}

message GetRiskEventsResponse {
  repeated RiskEvent events = 1;
  int32 total_count = 2;
}

// Reset daily counters (internal, scheduled)
message ResetDailyCountersRequest {
  optional string user_id = 1;  // Empty = all users
}

message ResetDailyCountersResponse {
  int32 users_reset = 1;
}

// Calculate portfolio metrics (internal)
message CalculatePortfolioMetricsRequest {
  string user_id = 1;
}

message CalculatePortfolioMetricsResponse {
  RiskMetrics metrics = 1;
}

// ========== Service Definition ==========

service RiskManagementService {
  // Public APIs
  rpc CheckPreTradeRisk(CheckPreTradeRiskRequest) returns (CheckPreTradeRiskResponse);
  rpc GetRiskMetrics(GetRiskMetricsRequest) returns (GetRiskMetricsResponse);
  rpc SetRiskLimits(SetRiskLimitsRequest) returns (SetRiskLimitsResponse);
  rpc GetRiskLimits(GetRiskLimitsRequest) returns (GetRiskLimitsResponse);
  rpc GetRiskEvents(GetRiskEventsRequest) returns (GetRiskEventsResponse);
  
  // Internal APIs
  rpc UpdatePostTradeMetrics(UpdatePostTradeMetricsRequest) returns (UpdatePostTradeMetricsResponse);
  rpc ResetDailyCounters(ResetDailyCountersRequest) returns (ResetDailyCountersResponse);
  rpc CalculatePortfolioMetrics(CalculatePortfolioMetricsRequest) returns (CalculatePortfolioMetricsResponse);
}
```

---

## Rules Engine Service (`api/proto/rules_engine/rules_engine.proto`)

```protobuf
syntax = "proto3";

package rules_engine;

option go_package = "github.com/trading-system/api/proto/rules_engine";

import "google/protobuf/timestamp.proto";
import "common/common.proto";

// ========== Data Models ==========

// Market event from Kafka
message MarketEvent {
  string event_id = 1;
  string event_type = 2;  // NEWS_UPDATE, PRICE_CHANGE, etc.
  google.protobuf.Timestamp timestamp = 3;
  
  // Stock data
  int64 stock_code = 4;
  common.Exchange exchange = 5;
  string symbol = 6;
  string company_code = 7;
  string company_name = 8;
  
  // News data
  optional string news_id = 9;
  optional string news_link = 10;
  optional string category = 11;
  optional string short_summary = 12;
  
  // Analysis
  common.Sentiment sentiment = 13;
  string impact_description = 14;
  int32 impact_score = 15;
  
  // Market data
  double last_traded_price = 16;
  double pct_change = 17;
  double news_first_price = 18;
  double news_pct_change = 19;
  
  // Price map
  PriceMap price_map = 20;
}

// Price map
message PriceMap {
  double open = 1;
  double high = 2;
  double low = 3;
  int64 volume = 4;
  optional double week_52_high = 5;
  optional double week_52_low = 6;
}

// Matching result
message MatchResult {
  bool matched = 1;
  string user_id = 2;
  string strategy_id = 3;
  double match_score = 4;  // 0-100
  repeated string matched_conditions = 5;
  map<string, string> metadata = 6;
}

// Trade signal
message TradeSignal {
  string signal_id = 1;
  string user_id = 2;
  string strategy_id = 3;
  string event_id = 4;
  
  // Stock details
  int64 stock_code = 5;
  string symbol = 6;
  common.Exchange exchange = 7;
  
  // Trade details
  common.OrderType order_type = 8;
  int32 quantity = 9;
  double price = 10;
  optional double stop_loss = 11;
  optional double take_profit = 12;
  
  // Signal metadata
  double confidence = 13;  // 0-100
  string reason = 14;
  google.protobuf.Timestamp generated_at = 15;
}

// ========== Request/Response Messages ==========

// Evaluate conditions (internal)
message EvaluateConditionsRequest {
  MarketEvent event = 1;
  repeated string strategy_ids = 2;  // Empty = check all
}

message EvaluateConditionsResponse {
  repeated MatchResult matches = 1;
  int32 total_evaluated = 2;
  int32 total_matched = 3;
}

// Reload user rules (when strategy updated)
message ReloadUserRulesRequest {
  repeated string user_ids = 1;  // Empty = reload all
}

message ReloadUserRulesResponse {
  int32 rules_reloaded = 1;
  common.SuccessResponse result = 2;
}

// Get matching statistics
message GetMatchingStatsRequest {
  optional string user_id = 1;
  optional common.TimeRange time_range = 2;
}

message GetMatchingStatsResponse {
  int32 total_events_processed = 1;
  int32 total_matches = 2;
  int32 total_signals_generated = 3;
  double avg_matching_latency_ms = 4;
  map<string, int32> matches_by_strategy = 5;
}

// Test strategy matching (for user to test their config)
message TestStrategyMatchingRequest {
  string strategy_id = 1;
  MarketEvent test_event = 2;
}

message TestStrategyMatchingResponse {
  MatchResult result = 1;
  string explanation = 2;
}

// ========== Service Definition ==========

service RulesEngineService {
  // Internal APIs
  rpc EvaluateConditions(EvaluateConditionsRequest) returns (EvaluateConditionsResponse);
  rpc ReloadUserRules(ReloadUserRulesRequest) returns (ReloadUserRulesResponse);
  
  // Public APIs
  rpc GetMatchingStats(GetMatchingStatsRequest) returns (GetMatchingStatsResponse);
  rpc TestStrategyMatching(TestStrategyMatchingRequest) returns (TestStrategyMatchingResponse);
}
```

---

## Compilation Instructions

### Prerequisites

```bash
# Install Protocol Buffer compiler
sudo apt-get install -y protobuf-compiler

# Install Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Compilation Script (`scripts/proto-gen.sh`)

```bash
#!/bin/bash

set -e

PROTO_DIR="api/proto"
OUT_DIR="."

echo "Generating Go code from proto files..."

# Common types
protoc --go_out=$OUT_DIR --go_opt=paths=source_relative \
       --go-grpc_out=$OUT_DIR --go-grpc_opt=paths=source_relative \
       $PROTO_DIR/common/*.proto

# User Config Service
protoc --go_out=$OUT_DIR --go_opt=paths=source_relative \
       --go-grpc_out=$OUT_DIR --go-grpc_opt=paths=source_relative \
       -I$PROTO_DIR \
       $PROTO_DIR/user_config/*.proto

# Trade Execution Service
protoc --go_out=$OUT_DIR --go_opt=paths=source_relative \
       --go-grpc_out=$OUT_DIR --go-grpc_opt=paths=source_relative \
       -I$PROTO_DIR \
       $PROTO_DIR/trade_execution/*.proto

# Risk Management Service
protoc --go_out=$OUT_DIR --go_opt=paths=source_relative \
       --go-grpc_out=$OUT_DIR --go-grpc_opt=paths=source_relative \
       -I$PROTO_DIR \
       $PROTO_DIR/risk_management/*.proto

# Rules Engine Service
protoc --go_out=$OUT_DIR --go_opt=paths=source_relative \
       --go-grpc_out=$OUT_DIR --go-grpc_opt=paths=source_relative \
       -I$PROTO_DIR \
       $PROTO_DIR/rules_engine/*.proto

echo "Proto compilation complete!"
```

---

## Usage Examples

### User Config Service

```go
// Create a new strategy
client := user_config.NewUserConfigServiceClient(conn)

strategy, err := client.CreateStrategy(ctx, &user_config.CreateStrategyRequest{
    UserId:       "user123",
    StrategyName: "High Impact News Trading",
    Conditions: &user_config.Conditions{
        ImpactScoreThreshold: 7,
        Sentiments:           []common.Sentiment{common.Sentiment_POSITIVE},
        PctChangeThreshold:   2.0,
    },
    TradeConfig: &user_config.TradeConfig{
        OrderType:    common.OrderType_MARKET,
        Quantity:     1000,
        StopLossPct:  2.0,
        TakeProfitPct: 5.0,
    },
})
```

### Trade Execution Service

```go
// Get order status
client := trade_execution.NewTradeExecutionServiceClient(conn)

response, err := client.GetOrderStatus(ctx, &trade_execution.GetOrderStatusRequest{
    OrderId: "order123",
    UserId:  "user123",
})
```

### Risk Management Service

```go
// Pre-trade risk check
client := risk_management.NewRiskManagementServiceClient(conn)

result, err := client.CheckPreTradeRisk(ctx, &risk_management.CheckPreTradeRiskRequest{
    UserId:    "user123",
    StockCode: 517170,
    OrderType: common.OrderType_MARKET,
    Quantity:  1000,
    Price:     47.75,
})

if !result.Result.Approved {
    log.Printf("Risk check failed: %v", result.Result.Violations)
}
```

---

## Protocol Buffer Best Practices

1. **Use optional fields sparingly**: Only for truly optional data
2. **Never remove or change field numbers**: This breaks compatibility
3. **Use enums for fixed sets**: Better than strings for type safety
4. **Include timestamps**: For audit trails
5. **Use nested messages**: For complex data structures
6. **Add metadata fields**: For extensibility
7. **Version your APIs**: Through separate proto files or packages
8. **Document with comments**: Explain business logic
9. **Use common types**: Reuse definitions across services
10. **Test serialization**: Ensure data integrity

---

## Next Steps

1. ✅ Protocol Buffer definitions complete
2. ⏭️ Create actual directory structure
3. ⏭️ Implement core services
4. ⏭️ Create Docker configurations
5. ⏭️ Setup development environment
