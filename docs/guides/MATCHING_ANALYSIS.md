# News Matching Analysis Report

## Problem Summary
The news event for TIPSMUSIC (impact score: 1, sentiment: Neutral) did not match with any of the three user strategies, even though Strategy 2 and Strategy 3 appeared to have matching criteria.

---

## News Event Details

```json
{
  "stock": "TIPSMUSIC",
  "impact_score": 1,
  "sentiment": "Neutral",
  "category": "Earnings Call Transcript",
  "symbolmap": {
    "NSE": "TIPSMUSIC",
    "BSE": 532375,
    "Company_Name": "Tips Music Ltd"
  },
  "LastTradedPrice": null,
  "pct_change": null
}
```

**Parsed Market Event:**
- **Stock Code**: 532375 (from BSE)
- **Exchange**: NSE (derived from symbolmap logic)
- **Impact Score**: 1
- **Sentiment**: Neutral
- **Category**: Earnings Call Transcript

---

## Strategy Configurations

### Strategy 1 (test-user-1763110782)
```
✗ impact_score_threshold: 7 (News has 1 - FAIL)
✗ sentiments: ["SENTIMENT_POSITIVE"] (News is "Neutral" - FAIL)
✗ stock_codes: [2885] (News has 532375 - FAIL)
✓ exchanges: ["EXCHANGE_NSE"]
```
**Result**: ❌ **NO MATCH** - Multiple conditions failed

---

### Strategy 2 (test-user-1763112157)
```
✓ impact_score_threshold: 1 (News has 1 - PASS)
✓ sentiments: [] (Empty = accepts all - PASS)
✓ stock_codes: [] (Empty = accepts all - PASS)
✓ exchanges: ["EXCHANGE_NSE"] (News has NSE - PASS)
```
**Expected**: ✅ Should Match
**Actual**: ❌ Did Not Match

---

### Strategy 3 (test-user-1763112320)
```
✓ impact_score_threshold: 1 (News has 1 - PASS)
✓ sentiments: [] (Empty = accepts all - PASS)
✓ stock_codes: [] (Empty = accepts all - PASS)
✓ exchanges: [] (Empty = accepts all - PASS)
```
**Expected**: ✅ Should Match
**Actual**: ❌ Did Not Match

---

## Root Cause Analysis

### Issue 1: Exchange Format Mismatch ⚠️ **CRITICAL**

**In User Config:**
```protobuf
exchanges: ["EXCHANGE_NSE"]  // Using enum format
```

**In Rules Engine:**
```go
// query.go - Elasticsearch query
"exchange": "EXCHANGE_NSE"  // Stored with enum prefix

// evaluator.go - Exchange evaluation
if event.StockData.Exchange == strategy.TradeConfig.Exchange {
    // Comparing "NSE" with "EXCHANGE_NSE" ❌
}
```

**In MongoDB Event Parsing:**
```go
// mongodb_event.go
if nse, ok := m.SymbolMap["NSE"]; ok && nse != nil && nse != "" {
    sd.Exchange = "NSE"  // Sets as "NSE" not "EXCHANGE_NSE"
}
```

**Problem**: The event has `Exchange = "NSE"` but strategies expect `"EXCHANGE_NSE"`, causing mismatch in evaluator.

---

### Issue 2: Missing Data in News Event ⚠️

The news event has:
```json
"LastTradedPrice": null,
"pct_change": null
```

This affects:
- **Elasticsearch Candidate Selection**: The query uses price and pct_change filters which might exclude strategies
- **Risk Management**: Needs price data for position sizing
- **Order Execution**: Cannot place orders without current price

---

### Issue 3: Elasticsearch Query Logic 🔍

From `query.go`, the Elasticsearch query requires:
```go
"minimum_should_match": 1  // At least one "should" clause must match
```

The "should" clauses include:
1. Sentiment match (boost: 2.0)
2. Category match (boost: 2.0)
3. Stock match (boost: 3.0)
4. Exchange match (boost: 1.5)
5. Price range filter
6. Volume filter
7. Pct change filter

**If the news has null values** for price, volume, and pct_change, and the strategy has:
- Empty stock_codes (no stock match)
- Empty sentiments (but still checked in "should")
- Exchange format mismatch

Then **minimum_should_match: 1** might not be satisfied, preventing the strategy from being selected as a candidate!

---

## Why Strategies 2 & 3 Failed

### Strategy 2 Analysis
```
ElasticSearch Candidate Selection:
- Active: ✓ (required)
- Impact score min <= 1: ✓ (passes filter)
- Should clauses need at least 1 match:
  × Sentiment: Not in "should" (empty array doesn't boost)
  × Category: Not indexed or doesn't match
  × Stock: Empty array doesn't match stock 532375
  × Exchange: "EXCHANGE_NSE" vs "NSE" mismatch
  × Price: NULL - cannot evaluate
  × Volume: NULL - cannot evaluate
  × Pct Change: NULL - cannot evaluate
```
**Result**: ❌ Fails `minimum_should_match: 1` → Not selected as candidate

### Strategy 3 Analysis
```
ElasticSearch Candidate Selection:
- Active: ✓ (required)
- Impact score min <= 1: ✓ (passes filter)
- Should clauses need at least 1 match:
  × Exchange: Empty array in conditions (but TradeConfig.Exchange = "EXCHANGE_NSE")
  × All other fields: Same issues as Strategy 2
```
**Result**: ❌ Fails `minimum_should_match: 1` → Not selected as candidate

---

## Solutions

### Solution 1: Fix Exchange Format Consistency ✅ **HIGH PRIORITY**

**Option A**: Store exchanges without enum prefix in Elasticsearch
```go
// In indexer.go - when indexing strategies
func (i *Indexer) mapStrategyToES(strategy *models.Strategy) *models.ElasticsearchStrategy {
    // ...
    Exchange: normalizeExchange(strategy.TradeConfig.Exchange), // "NSE" or "BSE"
}

func normalizeExchange(exchange string) string {
    // Remove EXCHANGE_ prefix if present
    return strings.TrimPrefix(exchange, "EXCHANGE_")
}
```

**Option B**: Parse MongoDB events with enum format
```go
// In mongodb_event.go
if nse, ok := m.SymbolMap["NSE"]; ok && nse != nil && nse != "" {
    sd.Exchange = "EXCHANGE_NSE"  // Use enum format
}
```

**Recommendation**: Use **Option A** - Store normalized exchange values ("NSE", "BSE") everywhere.

---

### Solution 2: Relax Elasticsearch Query Logic ✅ **HIGH PRIORITY**

Update `query.go` to be more inclusive for strategies with empty conditions:

```go
func (q *QueryEngine) buildQuery(event *models.MarketEvent) map[string]interface{} {
    // ... existing code ...
    
    // Don't require minimum_should_match if we have broad strategies
    // Let the evaluator handle precise matching
    boolQuery["minimum_should_match"] = 0  // Change from 1 to 0
    
    // OR: Only add to "should" if the condition is actually set
    // Don't add empty/null conditions to "should" clauses
}
```

**Impact**: More strategies will be selected as candidates, but the evaluator will still filter them correctly.

---

### Solution 3: Handle NULL Market Data Gracefully ✅

In `evaluator.go`, ensure NULL values don't cause failures:

```go
// evaluatePriceRange - already handles this correctly
if minPrice == 0 && maxPrice == 0 {
    result.MatchedConditions = append(result.MatchedConditions, condition)
    return
}

// evaluateVolume - already handles this correctly
if minVolume == 0 {
    result.MatchedConditions = append(result.MatchedConditions, condition)
    return
}

// evaluatePctChange - already handles this correctly
if threshold == 0 {
    result.MatchedConditions = append(result.MatchedConditions, condition)
    return
}
```

**Status**: ✅ Already implemented correctly

---

### Solution 4: Verify Strategy Indexing in Elasticsearch 🔍

Check if strategies are properly indexed:

```bash
# Check if strategies exist in Elasticsearch
curl -X GET "localhost:9200/trading-strategies/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "term": {
      "active": true
    }
  }
}'

# Check specific strategy
curl -X GET "localhost:9200/trading-strategies/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "query": {
    "term": {
      "strategy_id": "868b1bc7-072c-4a3c-9f9d-67cf2149f941"
    }
  }
}'
```

---

## Immediate Action Items

### 1. Fix Exchange Format (CRITICAL)
- [ ] Update `services/rules-engine/internal/index/indexer.go` to normalize exchange values
- [ ] Update existing Elasticsearch index to fix exchange values
- [ ] Verify exchange comparison in evaluator

### 2. Relax ES Query (HIGH)
- [ ] Change `minimum_should_match` from 1 to 0 in `query.go`
- [ ] OR: Only add non-empty conditions to "should" clauses

### 3. Verify Data Flow (MEDIUM)
- [ ] Check if strategies are indexed in Elasticsearch
- [ ] Check if news events are being consumed from Kafka/MongoDB
- [ ] Add logging to see candidate selection results

### 4. Testing (HIGH)
- [ ] Create unit tests for exchange matching
- [ ] Create integration tests for end-to-end matching
- [ ] Test with NULL market data

---

## Testing Plan

### Test Case 1: Exchange Matching
```go
event := &MarketEvent{
    StockData: StockData{Exchange: "NSE"},
    Analysis: Analysis{ImpactScore: 1},
}

strategy := &Strategy{
    Conditions: Conditions{ImpactScoreThreshold: 1},
    TradeConfig: TradeConfig{Exchange: "EXCHANGE_NSE"},
}

// Should match after fix
```

### Test Case 2: Empty Conditions (Catch-All Strategy)
```go
strategy := &Strategy{
    Conditions: Conditions{
        ImpactScoreThreshold: 1,
        Sentiments: [],      // Empty = accept all
        Stocks: [],          // Empty = accept all
    },
}

// Should match any event with impact >= 1
```

### Test Case 3: NULL Market Data
```go
event := &MarketEvent{
    MarketData: MarketData{
        LastTradedPrice: 0,  // NULL/not available
        PctChange: 0,        // NULL/not available
    },
}

// Should still match if other conditions pass
```

---

## Monitoring Recommendations

1. **Add Debug Logging** in matcher.go:
   ```go
   m.logger.Debug("Candidate selection",
       zap.Int("total_candidates", len(candidates)),
       zap.String("event_id", event.EventID))
   ```

2. **Add ES Query Logging** in query.go:
   ```go
   q.logger.Debug("Elasticsearch query",
       zap.String("query", string(buf.Bytes())))
   ```

3. **Add Evaluation Result Logging** in evaluator.go:
   ```go
   e.logger.Debug("Evaluation result",
       zap.Strings("matched", result.MatchedConditions),
       zap.Strings("failed", result.FailedConditions))
   ```

---

## Conclusion

The matching failed primarily due to:

1. **Exchange Format Mismatch**: News has "NSE", strategies have "EXCHANGE_NSE"
2. **Strict Elasticsearch Query**: `minimum_should_match: 1` excludes strategies with empty conditions
3. **NULL Market Data**: Missing price/volume data affects query boosting

**Fix Priority**: 
1. Exchange format normalization (CRITICAL)
2. Relax ES query logic (HIGH)
3. Verify strategy indexing (HIGH)

After implementing these fixes, Strategy 2 and Strategy 3 should correctly match the TIPSMUSIC news event.
