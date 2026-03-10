# Indira Securities API Migration Analysis

## Executive Summary

This document provides a comprehensive analysis of the current Odin API wrapper and b2c-api-python implementation, and outlines the migration strategy to use the Indira Securities API directly.

---

## Current Architecture Analysis

### 1. Current Components

#### A. **odin-api-wrapper** (Python FastAPI Service)
- **Location**: `services/odin-api-wrapper/`
- **Purpose**: HTTP wrapper around the b2c-api-python SDK
- **Technology**: Python, FastAPI
- **Key Features**:
  - Session management per user
  - TOTP-based authentication
  - RESTful endpoints for order management
  - Environment-based configuration

#### B. **b2c-api-python** (Python SDK)
- **Location**: `b2c-api-python/`
- **Purpose**: Python SDK for broker API (generic SDK, not Indira-specific)
- **Key Components**:
  - `IBTConnect` class for API interactions
  - Request/response parsing
  - WebSocket support for broadcasts
  - Session management

#### C. **pkg/odin** (Go Client)
- **Location**: `pkg/odin/`
- **Purpose**: Go client for the odin-api-wrapper service
- **Key Features**:
  - HTTP client for wrapper service
  - Type-safe Go structs
  - Order placement, modification, cancellation
  - Portfolio and position management

#### D. **services/trade-execution/internal/odin** (Go Integration)
- **Location**: `services/trade-execution/internal/odin/`
- **Purpose**: Integration layer between trade-execution and odin pkg
- **Key Features**:
  - Credential management
  - Order conversion (internal model → Odin request)
  - Authentication handling

### 2. Current Data Flow

```
┌─────────────────┐
│ Trade Execution │
│   Service (Go)  │
└────────┬────────┘
         │
         │ 1. Uses pkg/odin client
         ▼
┌─────────────────┐
│   pkg/odin      │
│   (Go Client)   │
└────────┬────────┘
         │
         │ 2. HTTP calls to wrapper
         ▼
┌─────────────────┐
│ odin-api-wrapper│
│  (Python/FastAPI)│
└────────┬────────┘
         │
         │ 3. Uses b2c-api-python SDK
         ▼
┌─────────────────┐
│ b2c-api-python  │
│  (Python SDK)   │
└────────┬────────┘
         │
         │ 4. HTTP calls to broker
         ▼
┌─────────────────┐
│  Broker API     │
│  (Unknown)      │
└─────────────────┘
```

### 3. Problems with Current Architecture

1. **Multiple Abstraction Layers**: 
   - Trade-execution → pkg/odin → odin-api-wrapper → b2c-api-python → Broker API
   - Each layer adds latency and complexity

2. **Generic SDK**: 
   - b2c-api-python is not Indira Securities-specific
   - May not support all Indira Securities features
   - Extra overhead for unsupported features

3. **Language Barriers**: 
   - Go ↔ Python communication adds overhead
   - Type conversions at multiple levels
   - Difficult debugging across language boundaries

4. **Maintenance Overhead**: 
   - Multiple services to maintain
   - Python dependencies to manage
   - Session management complexity

5. **Performance**: 
   - Network latency from multiple hops
   - JSON serialization/deserialization at each layer
   - Python interpreter overhead

---

## Indira Securities API Analysis

### 1. API Endpoints from Postman Collection

Based on the provided Postman collection, Indira Securities provides these endpoints:

#### Authentication & User Services
```
Base URL: 
```

1. **Profile**
   - GET `/user-services/api/user/v1/AccountInfo`
   - Headers: `userId`, `source`, `appId`, Bearer token

#### Order Management Services

2. **Place Order**
   - POST `/order-services/api/order/v1/place-order`
   - Headers: `userId`, `source`, `appId`, Bearer token
   - Body:
   ```json
   {
     "symbol": "STK_APOLLOHOSP_EQ_NSE_157",
     "excToken": "157",
     "exc": "NSE",
     "ordAction": "BUY",
     "ordValidity": "DAY",
     "ordType": "Market",
     "prdType": "INTRADAY",
     "limitPrice": 0.0,
     "triggerPrice": 0.0,
     "qty": 1,
     "disQty": 0,
     "lotSize": 1,
     "instrument": "STK",
     "amo": true,
     "boStpLoss": null,
     "boTgtPrice": null
   }
   ```

3. **Modify Order**
   - POST `/order-services/api/order/v1/modify-order`
   - Headers: `userId`, `source`, `appId`, Bearer token
   - Body:
   ```json
   {
     "ordId": "NZULR1000AI2",
     "symbol": "STK_TCS_EQ_NSE_11536",
     "ordAction": "BUY",
     "ordValidity": "DAY",
     "exchangeToken": "11536",
     "exc": "NSE",
     "qty": 11,
     "tradedQty": 0,
     "limitPrice": 3961.0,
     "triggerPrice": 0.0,
     "ordType": "Limit",
     "prdType": "DELIVERY",
     "instrument": "STK",
     "lotSize": 1,
     "disQty": 0,
     "offMktFlag": false
   }
   ```

4. **Cancel Order**
   - POST `/order-services/api/order/v1/cancel-order`
   - Headers: `userId`, `source`, `appId`, Bearer token
   - Body:
   ```json
   {
     "symbol": "STK_TCS_EQ_NSE_11536",
     "exc": "NSE",
     "ordId": "NZULR1000AI2"
   }
   ```

5. **Brokerage Charges**
   - POST `/order-services/api/order/v1/brokerage-charges`
   - Headers: `userId`, `source`, `appId`, Bearer token

#### Portfolio Services

6. **Order Book**
   - GET `/portfolio-services/api/order/v1/order-book`
   - Headers: `userId`, `source`, `appId`, Bearer token

7. **Order Trail**
   - POST `/portfolio-services/api/order/v2/order-trail`
   - Headers: `userId`, `source`, `appId`, Bearer token
   - Body:
   ```json
   {
     "ordId": "NZYXM00001N",
     "instrument": "STK"
   }
   ```

8. **Trade Book**
   - POST `/portfolio-services/api/order/v1/trade-book?orderIds=NZUMV000028`
   - Headers: `userId`, `source`, `appId`, Bearer token

9. **Holdings**
   - GET `/portfolio-services/api/portfolio/v2/holdings`
   - Headers: `userId`, `source`, `appId`, Bearer token

10. **Positions**
    - GET `/portfolio-services/api/portfolio/v1/position-book`
    - Headers: `userId`, `source`, `appId`, Bearer token

11. **Position Conversion**
    - POST `/portfolio-services/api/portfolio/v1/convert-position`
    - Headers: `userId`, `source`, `appId`, Bearer token
    - Body:
    ```json
    {
      "ordAction": "BUY",
      "prdType": "CASH",
      "toPrdType": "INTRADAY",
      "qty": 7,
      "symbol": "STK_TCS_EQ_NSE_11536",
      "excToken": "11536",
      "exc": "NSE",
      "lotSize": 0,
      "instrument": "IDX",
      "type": "DAY1"
    }
    ```

#### Payment Services

12. **Fund Limit**
    - GET `/payments/api/v1/get-fund-limit`
    - Headers: `userId`, `source`, `appId`, Bearer token

### 2. Key API Characteristics

#### Authentication Pattern
- **Bearer Token**: All requests require a Bearer token in Authorization header
- **Headers Required**:
  - `userId`: User identifier (e.g., "ISPL19122")
  - `source`: Platform identifier (e.g., "IOS", "AND")
  - `appId`: Application identifier (unique per session)
  - `Authorization`: Bearer token
  - `Content-Type`: application/json

#### Symbol Format
- Format: `{INSTRUMENT}_{SYMBOL}_{SERIES}_{EXCHANGE}_{TOKEN}`
- Example: `STK_TCS_EQ_NSE_11536`
  - Instrument: STK (Stock)
  - Symbol: TCS
  - Series: EQ (Equity)
  - Exchange: NSE
  - Token: 11536

#### Order Types
- `Market`: Market order
- `Limit`: Limit order
- `SL`: Stop Loss
- `SL-M`: Stop Loss Market

#### Product Types
- `INTRADAY`: Intraday/MIS
- `DELIVERY`: Delivery/CNC
- `CASH`: Cash
- `MTF`: Margin Trading Facility

#### Order Actions
- `BUY`: Buy order
- `SELL`: Sell order

#### Validity
- `DAY`: Day order
- `IOC`: Immediate or Cancel

---

## API Mapping: Current vs Indira

### Order Placement

| Current (Odin Wrapper) | Indira Securities API |
|------------------------|----------------------|
| `POST /orders/place` | `POST /order-services/api/order/v1/place-order` |
| **Request Format** | **Request Format** |
| `scrip_info.exchange` → "NSE_EQ" | `exc` → "NSE" |
| `scrip_info.symbol` → "TCS" | `symbol` → "STK_TCS_EQ_NSE_11536" |
| `scrip_info.scrip_token` → 11536 | `excToken` → "11536" |
| `transaction_type` → "BUY" | `ordAction` → "BUY" |
| `product_type` → "INTRADAY" | `prdType` → "INTRADAY" |
| `order_type` → "RL-MKT" | `ordType` → "Market" |
| `quantity` → 10 | `qty` → 10 |
| `price` → 100.0 | `limitPrice` → 100.0 |
| `trigger_price` → 95.0 | `triggerPrice` → 95.0 |
| `validity` → "DAY" | `ordValidity` → "DAY" |
| `disclosed_quantity` → 0 | `disQty` → 0 |
| `is_amo` → false | `amo` → false |

### Order Modification

| Current (Odin Wrapper) | Indira Securities API |
|------------------------|----------------------|
| `PUT /orders/modify` | `POST /order-services/api/order/v1/modify-order` |
| **Request Format** | **Request Format** |
| `exchange` → "NSE" | `exc` → "NSE" |
| `order_id` → "ABC123" | `ordId` → "ABC123" |
| `quantity` → 20 | `qty` → 20 |
| `price` → 110.0 | `limitPrice` → 110.0 |
| `trigger_price` → 105.0 | `triggerPrice` → 105.0 |
| `order_type` → "RL" | `ordType` → "Limit" |

### Order Cancellation

| Current (Odin Wrapper) | Indira Securities API |
|------------------------|----------------------|
| `DELETE /orders/cancel` | `POST /order-services/api/order/v1/cancel-order` |
| **Request Format** | **Request Format** |
| `exchange` → "NSE" | `exc` → "NSE" |
| `order_id` → "ABC123" | `ordId` → "ABC123" |
| - | `symbol` → "STK_TCS_EQ_NSE_11536" |

### Portfolio & Positions

| Current (Odin Wrapper) | Indira Securities API |
|------------------------|----------------------|
| `GET /portfolio/positions?position_type=DAY` | `GET /portfolio-services/api/portfolio/v1/position-book` |
| `GET /portfolio/holdings` | `GET /portfolio-services/api/portfolio/v2/holdings` |
| `GET /orders/book` | `GET /portfolio-services/api/order/v1/order-book` |
| `GET /orders/trades` | `POST /portfolio-services/api/order/v1/trade-book?orderIds=XXX` |

---

## Migration Strategy

### Phase 1: Create Indira API Client (Go)

**Goal**: Build a native Go client for Indira Securities API

**Components**:
1. **New package**: `pkg/indira/`
   - Client implementation
   - Type definitions
   - Authentication handler
   - HTTP client with retry logic

2. **Features**:
   - JWT Bearer token management
   - Session management
   - Auto-refresh tokens
   - Error handling
   - Rate limiting

3. **API Coverage**:
   - Authentication (login/logout)
   - Order placement
   - Order modification
   - Order cancellation
   - Order book retrieval
   - Trade book retrieval
   - Position retrieval
   - Holdings retrieval
   - Brokerage calculation

### Phase 2: Update Trade Execution Service

**Goal**: Replace odin client with indira client in trade-execution service

**Changes**:
1. Update `services/trade-execution/internal/broker/` (create new package)
   - Replace `odin/client.go` with `indira/client.go`
   - Update request/response mapping
   - Handle symbol format conversion

2. Update credential management
   - Store Indira-specific credentials (userId, appId, source)
   - Handle Bearer token lifecycle

3. Update order processing
   - Map internal order model to Indira format
   - Handle Indira-specific response formats

### Phase 3: Remove Legacy Components

**Goal**: Clean up unused code and dependencies

**Actions**:
1. Remove `services/odin-api-wrapper/`
2. Remove `b2c-api-python/`
3. Remove `pkg/odin/`
4. Remove RabbitMQ dependencies (if only used for odin-wrapper)
5. Update documentation

### Phase 4: Testing & Validation

**Goal**: Ensure all functionality works correctly

**Tests**:
1. Unit tests for Indira client
2. Integration tests for trade-execution
3. End-to-end order flow tests
4. Error handling validation
5. Performance benchmarks

---

## New Architecture

```
┌─────────────────────┐
│ Trade Execution     │
│   Service (Go)      │
└──────────┬──────────┘
           │
           │ 1. Direct API calls
           ▼
┌─────────────────────┐
│   pkg/indira        │
│   (Go Client)       │
└──────────┬──────────┘
           │
           │ 2. HTTPS with Bearer token
           ▼
┌─────────────────────┐
│ Indira Securities   │
│       API           │
│ (livemiddleware)    │
└─────────────────────┘
```

**Benefits**:
- ✅ Reduced latency (2 hops instead of 5)
- ✅ Simpler architecture (1 language, Go)
- ✅ Native type safety
- ✅ Better error handling
- ✅ Easier debugging
- ✅ Lower operational overhead
- ✅ Better performance

---

## Implementation Details

### Symbol Format Conversion

**Current Internal Format**: 
- Exchange: `EXCHANGE_NSE`
- Symbol: `TCS`
- StockCode: `11536`

**Indira Format**:
- Symbol: `STK_TCS_EQ_NSE_11536`
- Exchange: `NSE`
- ExcToken: `11536`

**Conversion Function**:
```go
func BuildIndiraSymbol(symbol string, exchange string, token string, instrument string) string {
    // instrument: STK, IDX, OPT, FUT
    // series: EQ, FO, etc.
    series := "EQ" // default to equity
    if instrument == "OPT" || instrument == "FUT" {
        series = "FO"
    }
    
    // Clean exchange name
    exc := strings.TrimPrefix(exchange, "EXCHANGE_")
    
    return fmt.Sprintf("%s_%s_%s_%s_%s", instrument, symbol, series, exc, token)
}
```

### Order Type Mapping

```go
func MapOrderType(internalType models.OrderType) string {
    switch internalType {
    case models.OrderTypeMarket:
        return "Market"
    case models.OrderTypeLimit:
        return "Limit"
    case models.OrderTypeStopLoss:
        return "SL"
    case models.OrderTypeStopLossMarket:
        return "SL-M"
    default:
        return "Market"
    }
}
```

### Authentication Flow

```go
type IndiraAuth struct {
    userId      string
    appId       string
    source      string
    bearerToken string
    tokenExpiry time.Time
}

func (c *Client) Login(ctx context.Context, userId, password string) error {
    // Call login API (needs to be discovered from docs)
    // Store bearer token
    // Set expiry time
}

func (c *Client) ensureAuthenticated(ctx context.Context) error {
    if time.Now().After(c.auth.tokenExpiry) {
        return c.refreshToken(ctx)
    }
    return nil
}
```

---

## Migration Checklist

### Pre-Migration
- [ ] Review complete Indira Securities API documentation
- [ ] Identify authentication/login endpoint
- [ ] Test all endpoints in Postman
- [ ] Document all request/response formats
- [ ] Identify rate limits and constraints

### Development
- [ ] Create `pkg/indira/` package
- [ ] Implement authentication
- [ ] Implement order management APIs
- [ ] Implement portfolio APIs
- [ ] Add comprehensive error handling
- [ ] Add retry logic
- [ ] Add logging
- [ ] Write unit tests

### Integration
- [ ] Create new broker interface in trade-execution
- [ ] Implement Indira client wrapper
- [ ] Update order conversion logic
- [ ] Update credential management
- [ ] Update configuration
- [ ] Test in development environment

### Testing
- [ ] Unit tests for all client methods
- [ ] Integration tests for order flow
- [ ] Test error scenarios
- [ ] Performance testing
- [ ] Load testing

### Deployment
- [ ] Update environment variables
- [ ] Update deployment configs
- [ ] Deploy to staging
- [ ] Validate in staging
- [ ] Deploy to production
- [ ] Monitor for errors

### Cleanup
- [ ] Remove odin-api-wrapper service
- [ ] Remove b2c-api-python
- [ ] Remove pkg/odin
- [ ] Update documentation
- [ ] Archive old code

---

## Risk Mitigation

### 1. Incomplete API Documentation
**Risk**: Indira API might have undocumented behaviors
**Mitigation**: 
- Extensive testing with real API
- Maintain fallback to wrapper if needed initially
- Gradual rollout

### 2. Authentication Issues
**Risk**: Login/token management might be complex
**Mitigation**:
- Thorough testing of auth flow
- Implement robust token refresh logic
- Add comprehensive error logging

### 3. Symbol Format Issues
**Risk**: Symbol format conversion might fail for some instruments
**Mitigation**:
- Create comprehensive symbol mapping
- Add validation logic
- Test with various instrument types

### 4. Performance Concerns
**Risk**: Direct API might have rate limits
**Mitigation**:
- Implement rate limiting
- Add request queuing
- Monitor API usage

---

## Next Steps

1. **Discover Authentication Endpoint**: 
   - The Postman collection shows Bearer tokens but not the login endpoint
   - Need to find authentication/token generation API

2. **Create pkg/indira**:
   - Start with basic client structure
   - Implement authentication
   - Add order management

3. **Parallel Testing**:
   - Keep odin-wrapper running
   - Test indira client in parallel
   - Compare results

4. **Gradual Migration**:
   - Use feature flags to toggle between implementations
   - Start with small percentage of traffic
   - Gradually increase

5. **Documentation**:
   - Document all API endpoints
   - Create migration guide
   - Update architecture diagrams

---

## Conclusion

Migrating from the multi-layer Odin API wrapper architecture to direct Indira Securities API integration will:
- **Simplify** the architecture (5 layers → 2 layers)
- **Improve** performance (eliminate multiple network hops)
- **Reduce** operational overhead (no Python service to maintain)
- **Enhance** maintainability (single language, Go)
- **Enable** better error handling and debugging

The migration is straightforward given that:
- Indira API is RESTful and well-structured
- Request/response formats are clear
- Most current functionality maps directly to Indira endpoints

**Recommended Approach**: Implement pkg/indira first, test thoroughly in parallel with existing system, then gradually migrate.
