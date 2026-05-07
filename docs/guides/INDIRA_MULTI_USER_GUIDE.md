# Indira API Multi-User Usage Guide

## Overview

The Indira API client now supports **multiple concurrent users** with per-request authentication. The frontend sends bearer token, appId, and source for each request.

---

## Key Changes

### Before (Single User)
```go
// Old way - client stored auth globally
client := indira.NewClient(indira.Config{
    UserId: "USER1",
    AppId: "APP1",
    Source: "WEB",
})
client.SetBearerToken(token, expiry)
client.PlaceOrder(ctx, orderReq)
```

### After (Multi-User)
```go
// New way - auth passed per request
client := indira.NewDefaultClient() // Stateless, reusable

// Each request includes auth from frontend
auth := &indira.AuthContext{
    UserId:      "USER1",
    BearerToken: "token-from-frontend",
    AppId:       "app-id-from-frontend",
    Source:      "WEB", // or "IOS", "AND"
}

client.PlaceOrder(ctx, auth, orderReq)
```

---

## Architecture

### Frontend → Backend Flow

```
┌─────────────┐
│  Frontend   │
│  (React/    │
│   Vue/etc)  │
└──────┬──────┘
       │
       │ HTTP Request with:
       │ - Authorization: Bearer <token>
       │ - X-App-Id: <appId>
       │ - X-Source: <source>
       │ - X-User-Id: <userId>
       │
       ▼
┌──────────────────┐
│   API Gateway    │
│  /gRPC Service   │
└──────┬───────────┘
       │
       │ Extract auth from headers
       │ Create AuthContext
       │
       ▼
┌──────────────────┐
│ Trade Execution  │
│    Service       │
└──────┬───────────┘
       │
       │ Pass auth to Indira client
       │
       ▼
┌──────────────────┐
│  Indira Client   │
│  (pkg/indira)    │
└──────┬───────────┘
       │
       │ HTTP with auth headers
       │
       ▼
┌──────────────────┐
│ Indira Securities│
│       API        │
└──────────────────┘
```

---

## Implementation Guide

### 1. Initialize Client (Once, Globally)

```go
// In main.go or service initialization
var indiraClient *indira.Client

func init() {
    // Create a single shared client - it's stateless and thread-safe
    indiraClient = indira.NewDefaultClient()
}
```

### 2. Extract Auth from Frontend Request

#### In gRPC Service

```go
func (s *TradeExecutionService) PlaceOrder(ctx context.Context, req *proto.PlaceOrderRequest) (*proto.PlaceOrderResponse, error) {
    // Extract auth from gRPC metadata
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return nil, status.Error(codes.Unauthenticated, "missing auth metadata")
    }
    
    // Build auth context from metadata
    auth := &indira.AuthContext{
        UserId:      getMetadataValue(md, "user-id"),
        BearerToken: getMetadataValue(md, "authorization"), // Strip "Bearer " prefix
        AppId:       getMetadataValue(md, "app-id"),
        Source:      getMetadataValue(md, "source"),
    }
    
    // Validate auth
    if auth.BearerToken == "" || auth.UserId == "" {
        return nil, status.Error(codes.Unauthenticated, "missing authentication")
    }
    
    // Use Indira client with per-request auth
    orderID, err := s.indiraClient.PlaceOrder(ctx, auth, &indira.PlaceOrderRequest{
        Symbol: req.Symbol,
        // ... other fields
    })
    
    return &proto.PlaceOrderResponse{
        OrderId: orderID,
    }, nil
}
```

#### In HTTP/REST API

```go
func PlaceOrderHandler(w http.ResponseWriter, r *http.Request) {
    // Extract auth from HTTP headers
    auth := &indira.AuthContext{
        UserId:      r.Header.Get("X-User-Id"),
        BearerToken: strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "),
        AppId:       r.Header.Get("X-App-Id"),
        Source:      r.Header.Get("X-Source"),
    }
    
    // Validate
    if auth.BearerToken == "" || auth.UserId == "" {
        http.Error(w, "Unauthorized", http.StatusUnauthorized)
        return
    }
    
    // Parse request body
    var orderReq indira.PlaceOrderRequest
    json.NewDecoder(r.Body).Decode(&orderReq)
    
    // Call Indira API with per-request auth
    resp, err := indiraClient.PlaceOrder(r.Context(), auth, &orderReq)
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    
    json.NewEncoder(w).Encode(resp)
}
```

### 3. Frontend Integration

#### React/JavaScript Example

```javascript
// Frontend auth service
class AuthService {
    getBearerToken() {
        // Get from localStorage, sessionStorage, or auth provider
        return localStorage.getItem('bearerToken');
    }
    
    getAppId() {
        return localStorage.getItem('appId');
    }
    
    getUserId() {
        return localStorage.getItem('userId');
    }
    
    getSource() {
        return 'WEB'; // or detect: IOS, AND
    }
}

// API client
class TradingAPI {
    constructor(authService) {
        this.authService = authService;
        this.baseUrl = 'https://api.yourbackend.com';
    }
    
    async placeOrder(orderData) {
        const response = await fetch(`${this.baseUrl}/api/v1/orders/place`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${this.authService.getBearerToken()}`,
                'X-User-Id': this.authService.getUserId(),
                'X-App-Id': this.authService.getAppId(),
                'X-Source': this.authService.getSource(),
            },
            body: JSON.stringify(orderData),
        });
        
        if (!response.ok) {
            throw new Error('Order placement failed');
        }
        
        return response.json();
    }
    
    async getOrderBook() {
        const response = await fetch(`${this.baseUrl}/api/v1/orders/book`, {
            headers: {
                'Authorization': `Bearer ${this.authService.getBearerToken()}`,
                'X-User-Id': this.authService.getUserId(),
                'X-App-Id': this.authService.getAppId(),
                'X-Source': this.authService.getSource(),
            },
        });
        
        return response.json();
    }
}

// Usage
const authService = new AuthService();
const tradingAPI = new TradingAPI(authService);

// Place order
await tradingAPI.placeOrder({
    symbol: 'STK_TCS_EQ_NSE_11536',
    qty: 10,
    price: 3950.0,
    orderType: 'Limit',
    // ...
});
```

---

## Complete Example

### Backend gRPC Service

```go
package main

import (
    "context"
    "log"
    "net"
    
    "google.golang.org/grpc"
    "google.golang.org/grpc/metadata"
    
    "github.com/RohitIndira/Algo-Treading/pkg/indira"
    pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
)

type TradeService struct {
    pb.UnimplementedTradeExecutionServer
    indiraClient *indira.Client
}

func NewTradeService() *TradeService {
    return &TradeService{
        indiraClient: indira.NewDefaultClient(),
    }
}

func (s *TradeService) PlaceOrder(ctx context.Context, req *pb.PlaceOrderRequest) (*pb.PlaceOrderResponse, error) {
    // Extract auth from gRPC metadata
    auth, err := extractAuth(ctx)
    if err != nil {
        return nil, err
    }
    
    // Build symbol
    sb := indira.NewSymbolBuilder()
    sb.Symbol = req.Symbol
    sb.Exchange = req.Exchange
    sb.Token = req.ExchangeToken
    sb.Instrument = req.Instrument
    
    // Build order request
    orderReq := &indira.PlaceOrderRequest{
        Symbol:       sb.BuildSymbol(),
        ExcToken:     sb.Token,
        Exc:          sb.Exchange,
        OrdAction:    req.OrderSide,
        OrdType:      req.OrderType,
        PrdType:      req.ProductType,
        Qty:          int(req.Quantity),
        LimitPrice:   req.Price,
        TriggerPrice: req.TriggerPrice,
        OrdValidity:  "DAY",
        Instrument:   sb.Instrument,
        LotSize:      1,
    }
    
    // Place order with Indira API
    resp, err := s.indiraClient.PlaceOrder(ctx, auth, orderReq)
    if err != nil {
        return nil, err
    }
    
    return &pb.PlaceOrderResponse{
        OrderId: resp.OrderId,
        Success: true,
        Message: "Order placed successfully",
    }, nil
}

func extractAuth(ctx context.Context) (*indira.AuthContext, error) {
    md, ok := metadata.FromIncomingContext(ctx)
    if !ok {
        return nil, fmt.Errorf("missing metadata")
    }
    
    auth := &indira.AuthContext{
        UserId:      getFirst(md.Get("user-id")),
        BearerToken: strings.TrimPrefix(getFirst(md.Get("authorization")), "Bearer "),
        AppId:       getFirst(md.Get("app-id")),
        Source:      getFirst(md.Get("source")),
    }
    
    if auth.UserId == "" || auth.BearerToken == "" {
        return nil, fmt.Errorf("missing required auth fields")
    }
    
    return auth, nil
}

func getFirst(values []string) string {
    if len(values) > 0 {
        return values[0]
    }
    return ""
}

func main() {
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        log.Fatalf("failed to listen: %v", err)
    }
    
    grpcServer := grpc.NewServer()
    pb.RegisterTradeExecutionServer(grpcServer, NewTradeService())
    
    log.Println("Trade execution service listening on :50051")
    grpcServer.Serve(lis)
}
```

---

## Security Best Practices

### 1. Token Validation

```go
func validateBearerToken(token string) error {
    // Verify JWT signature
    claims, err := parseJWT(token)
    if err != nil {
        return fmt.Errorf("invalid token: %w", err)
    }
    
    // Check expiry
    if claims.ExpiresAt.Before(time.Now()) {
        return fmt.Errorf("token expired")
    }
    
    return nil
}
```

### 2. Don't Store Tokens in Database

```go
// ❌ Bad - storing bearer token in DB
order.BearerToken = auth.BearerToken
db.Save(order)

// ✅ Good - only use token for API call
auth := extractAuthFromRequest(req)
orderID, err := indiraClient.PlaceOrder(ctx, auth, orderReq)
// Token is discarded after use
```

### 3. Use HTTPS/TLS

```go
// Always use TLS for gRPC
creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
grpcServer := grpc.NewServer(grpc.Creds(creds))
```

### 4. Rate Limiting Per User

```go
// Implement rate limiting based on userId
if !rateLimiter.Allow(auth.UserId) {
    return status.Error(codes.ResourceExhausted, "rate limit exceeded")
}
```

---

## Testing

### Unit Test Example

```go
func TestPlaceOrder_MultipleUsers(t *testing.T) {
    client := indira.NewDefaultClient()
    
    // Test User 1
    auth1 := &indira.AuthContext{
        UserId:      "USER1",
        BearerToken: "token1",
        AppId:       "app1",
        Source:      "WEB",
    }
    
    // Test User 2
    auth2 := &indira.AuthContext{
        UserId:      "USER2",
        BearerToken: "token2",
        AppId:       "app2",
        Source:      "IOS",
    }
    
    // Both users can use same client concurrently
    var wg sync.WaitGroup
    wg.Add(2)
    
    go func() {
        defer wg.Done()
        resp, err := client.PlaceOrder(context.Background(), auth1, orderReq1)
        assert.NoError(t, err)
        assert.NotEmpty(t, resp.OrderId)
    }()
    
    go func() {
        defer wg.Done()
        resp, err := client.PlaceOrder(context.Background(), auth2, orderReq2)
        assert.NoError(t, err)
        assert.NotEmpty(t, resp.OrderId)
    }()
    
    wg.Wait()
}
```

---

## Migration Checklist

- [x] Update pkg/indira to accept per-request auth
- [x] Update trade-execution integration layer
- [x] Remove old Odin code
- [ ] Update gRPC service to extract auth from metadata
- [ ] Update database migration scripts
- [ ] Update frontend to send auth headers
- [ ] Test with multiple concurrent users
- [ ] Deploy to staging
- [ ] Validate in production

---

## FAQ

### Q: Do I need to create a new client for each user?
**A:** No! The client is stateless and thread-safe. Create one client and share it across all users.

### Q: Where does the bearer token come from?
**A:** The frontend obtains it from Indira's login API and sends it with each request.

### Q: Should I store the bearer token in the database?
**A:** No, for security reasons. Only use it for the API call and discard it.

### Q: How do I handle token expiry?
**A:** The frontend should monitor token expiry and refresh it. Backend just uses what frontend sends.

### Q: Can multiple users place orders at the same time?
**A:** Yes! The client is designed for concurrent use. Each request is independent.

---

## Support

For questions or issues:
- Check the main docs: `docs/guides/INDIRA_MIGRATION_GUIDE.md`
- Review the client code: `pkg/indira/`
- Contact: backend-team@company.com
