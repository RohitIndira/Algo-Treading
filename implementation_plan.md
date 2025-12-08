# Implementation Plan: Dynamic Trade Configuration from Kafka

## [Overview]

Replace hardcoded trade configuration values in the rules-engine with dynamic values from user strategies stored in Kafka.

Currently, the rules-engine's `handler.go` (line 204) creates orders with hardcoded values (quantity: 1, stop_loss_pct: 2.0, take_profit_pct: 5.0) instead of using the actual user-configured trade settings from their strategy. This implementation will modify the order generation flow to fetch complete strategy details including trade_config and risk_limits from the existing Kafka-based strategy sync system, ensuring that all user-configured parameters (quantity, order_type, order_side, exchange, validity, stop_loss_pct, take_profit_pct, limit_price, max_position_size) are properly applied to generated orders.

The system already has the infrastructure in place: strategies are synced from the user-config service to the rules-engine via the "user-configs" Kafka topic and stored in both Elasticsearch (for matching) and Redis cache (for fast lookup). The fix requires modifying the `processMatch()` function to retrieve the full strategy from cache instead of creating a stub with hardcoded values.

## [Types]

No new type definitions required - all necessary types already exist in the codebase.

The existing type structures support dynamic configuration:
- `models.Strategy` (in rules-engine/internal/models/strategy.go) - Contains complete strategy definition
- `models.TradeConfig` - Contains all trade execution parameters (order_type, quantity, stop_loss_pct, take_profit_pct, exchange, order_side, limit_price, validity, max_position_size)
- `models.RiskLimits` - Contains risk management parameters (max_daily_trades, max_loss_per_day, position_sizing, etc.)
- `models.OrderRequest` (in rules-engine/internal/models/order.go) - Already supports all dynamic fields from TradeConfig

The Kafka "user-configs" topic payload already includes the complete trade_config section with all required fields as shown in the user's example JSON.

## [Files]

Modifications required to existing files only - no new files needed.

**Existing Files to Modify:**

1. **services/rules-engine/internal/consumer/handler.go**
   - Modify `processMatch()` function (around line 204) to fetch full strategy from cache
   - Remove hardcoded TradeConfig creation
   - Add logic to retrieve complete strategy from StrategyCache
   - Add fallback mechanism if cache lookup fails
   - Preserve all existing validation and order creation logic

2. **services/rules-engine/internal/consumer/handler.go** (same file, additional changes)
   - Update `NewHandler()` constructor to ensure StrategyCache is properly passed and available
   - Modify error handling to account for strategy fetch failures

3. **services/rules-engine/cmd/main.go** (verification only)
   - Verify that StrategyCache is properly initialized and passed to Handler
   - No changes needed - already properly wired

**No Files to Create, Delete, or Move**

Configuration file updates: None required - all existing environment variables remain unchanged.

## [Functions]

Modification of existing functions to support dynamic trade configuration from cached strategies.

**Modified Functions:**

1. **Function**: `processMatch` 
   - **File**: services/rules-engine/internal/consumer/handler.go
   - **Current Signature**: `func (h *Handler) processMatch(ctx context.Context, match *models.RuleMatch, event *models.MarketEvent) error`
   - **Changes Required**:
     - Replace hardcoded strategy stub creation (lines 203-212) with strategy cache lookup
     - Add call to `h.strategyCache.GetStrategy(ctx, match.StrategyID)` to fetch full strategy
     - Implement fallback logic: if cache miss, attempt to reconstruct from Elasticsearch data in match
     - Add error handling for missing or invalid strategy data
     - Ensure default values are used only as absolute last resort (with warning logs)
     - Preserve all downstream order creation, risk checking, and publishing logic
   
2. **Function**: `NewHandler`
   - **File**: services/rules-engine/internal/consumer/handler.go
   - **Current Signature**: `func NewHandler(...) *Handler`
   - **Changes Required**:
     - Add `strategyCache *cache.StrategyCache` parameter to constructor
     - Store strategyCache in Handler struct for use in processMatch
     - Update all call sites (cmd/main.go) to pass strategyCache instance

**New Functions**: None required

**Removed Functions**: None

## [Classes]

Modification of Handler struct to include strategy cache reference.

**Modified Classes:**

1. **Struct**: `Handler`
   - **File**: services/rules-engine/internal/consumer/handler.go
   - **Current Fields**: matcher, rabbitPubl, kafkaPubl, signalRepo, riskClient, redisCache, stats, logger
   - **Modifications**:
     - Add field: `strategyCache *cache.StrategyCache` to enable strategy lookups
     - This provides typed access to strategy retrieval methods
     - Keeps existing redisCache field for LTP lookups and pub/sub
   - **Reason**: Separates concerns between raw Redis cache operations and strategy-specific caching logic

**New Classes**: None

**Removed Classes**: None

## [Dependencies]

No new external dependencies required - all necessary packages already imported.

**Existing Dependencies Used:**
- `github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/cache` - Already imported, provides StrategyCache
- `github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models` - Already imported, provides Strategy types
- `context` - Already imported for context propagation
- `go.uber.org/zap` - Already imported for logging

**No New Packages Required**

**Integration Points:**
- StrategyCache already integrated with Redis
- Strategy sync from Kafka "user-configs" topic already operational
- Elasticsearch indexing already functional
- All infrastructure components properly initialized in cmd/main.go

## [Testing]

Comprehensive testing approach to validate dynamic configuration retrieval and fallback mechanisms.

**Test Scenarios:**

1. **Cache Hit Scenario**:
   - Test strategy successfully retrieved from Redis cache
   - Verify all TradeConfig fields properly applied to OrderRequest
   - Confirm quantity, stop_loss_pct, take_profit_pct, order_type, exchange, order_side, validity from strategy

2. **Cache Miss Scenario**:
   - Test behavior when strategy not in cache
   - Verify fallback to reconstructed strategy from match data
   - Ensure warning logs are generated

3. **Missing Strategy Data**:
   - Test when strategy cannot be found anywhere
   - Verify error is returned and order is not generated
   - Confirm appropriate error logging

4. **Validation**:
   - Test with various trade_config values from Kafka
   - Verify order_side (BUY/SELL) correctly applied
   - Test ORDER_TYPE_MARKET and ORDER_TYPE_LIMIT handling
   - Confirm exchange normalization (EXCHANGE_NSE -> NSE)

**Existing Test Modifications**: None - this is a bug fix, not new functionality

**Manual Testing Commands**:
```bash
# 1. Check strategy in Redis cache
redis-cli GET "strategy:5244dd9c-a0d7-4383-8f9b-8f7ae9e75281"

# 2. Check Elasticsearch index
curl -X GET "localhost:9200/strategies/_doc/5244dd9c-a0d7-4383-8f9b-8f7ae9e75281"

# 3. Monitor Kafka user-configs topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic user-configs --from-beginning

# 4. Monitor rules-engine logs for strategy lookups
docker logs -f rules-engine | grep -i "strategy"

# 5. Verify generated orders have correct quantity
# Check PostgreSQL trade_signals table
psql -U postgres -d trading_system -c "SELECT order_id, quantity, order_type, order_side FROM trade_signals ORDER BY created_at DESC LIMIT 5;"

# 6. Verify RabbitMQ orders have correct config
# Check RabbitMQ management UI or consume from queue
```

**Validation Strategy**:
- Deploy to development environment first
- Create test strategy with quantity=5, custom stop_loss_pct
- Trigger market event that matches strategy
- Verify generated order has quantity=5 and custom stop_loss
- Monitor for any errors or cache misses in logs

## [Implementation Order]

Sequential implementation steps to minimize conflicts and ensure successful integration.

**Step-by-Step Implementation:**

1. **Add StrategyCache field to Handler struct** (services/rules-engine/internal/consumer/handler.go)
   - Add `strategyCache *cache.StrategyCache` field to Handler struct definition
   - Ensures field is available for subsequent changes

2. **Update NewHandler constructor** (services/rules-engine/internal/consumer/handler.go)
   - Add `strategyCache *cache.StrategyCache` parameter
   - Initialize handler.strategyCache field
   - Return updated Handler instance

3. **Update Handler initialization in main.go** (services/rules-engine/cmd/main.go)
   - Pass strategyCache to consumer.NewHandler() call (around line 171)
   - Verify strategyCache is already initialized earlier in main function (around line 101)

4. **Modify processMatch function - Strategy Retrieval** (services/rules-engine/internal/consumer/handler.go)
   - Replace lines 203-212 (hardcoded strategy creation)
   - Add strategy fetch from cache: `strategy, err := h.strategyCache.GetStrategy(ctx, match.StrategyID)`
   - Add error handling for cache miss with warning log
   - Implement fallback: reconstruct strategy from Elasticsearch data if cache miss
   - Add final fallback: return error if strategy cannot be retrieved

5. **Add comprehensive logging** (services/rules-engine/internal/consumer/handler.go)
   - Log successful strategy retrieval with key fields (quantity, order_type, etc.)
   - Log cache misses with strategy_id
   - Log fallback usage
   - Log when using default values (should be rare)

6. **Test with sample strategy** 
   - Create test strategy via API with specific quantity value
   - Trigger market event to generate order
   - Verify order has correct quantity from strategy
   - Check logs for proper strategy retrieval

7. **Deploy and monitor**
   - Deploy to development environment
   - Monitor logs for any errors or unexpected fallbacks
   - Verify all orders use strategy-configured values
   - Check for any performance impact from cache lookups

**Dependencies Between Steps:**
- Steps 1-3 must be completed before step 4
- Step 4 is the core change and must be completed before testing
- Steps 5-7 are validation and can be done after core implementation

**Rollback Strategy:**
- If issues occur, revert handler.go changes
- System will fall back to hardcoded values
- No data loss or corruption risk
- Cache remains functional for future retry

**Estimated Time:** 2-3 hours for implementation and testing

---

## Additional Notes

**Key Benefits:**
1. Users get full control over their trade parameters
2. No code changes needed for parameter adjustments
3. Leverages existing Kafka-based configuration system
4. Maintains backward compatibility

**Performance Considerations:**
- Strategy cache lookup adds ~1-5ms per order generation
- Redis cache hit rate should be >95% after warm-up
- Elasticsearch fallback adds ~50-100ms but should be rare
- Overall impact negligible compared to order execution latency

**Risk Mitigation:**
- Comprehensive error handling prevents order generation failures
- Fallback mechanisms ensure system continues functioning
- Extensive logging enables quick troubleshooting
- No database schema changes reduce deployment risk

**Future Enhancements (Out of Scope):**
- Add strategy version tracking for audit trail
- Implement strategy warmup on service startup
- Add metrics for cache hit/miss rates
- Create strategy validation service
