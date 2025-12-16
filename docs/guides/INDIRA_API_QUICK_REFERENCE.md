# Indira Securities API - Quick Reference

## 🔑 Authentication

```go
client := indira.NewClient(indira.Config{
    UserId: "ISPL19122",
    AppId:  "your-app-id",
    Source: "WEB", // or "IOS", "AND"
})

client.SetBearerToken("bearer-token", expiryTime)
```

## 📝 Symbol Format

```
{INSTRUMENT}_{SYMBOL}_{SERIES}_{EXCHANGE}_{TOKEN}

Example: STK_TCS_EQ_NSE_11536
```

**Building Symbols**:
```go
sb := indira.NewSymbolBuilder()
sb.Instrument = "STK"  // STK, OPT, FUT, IDX
sb.Symbol = "TCS"
sb.Series = "EQ"       // EQ (equity) or FO (futures/options)
sb.Exchange = "NSE"    // NSE, BSE
sb.Token = "11536"
symbol := sb.BuildSymbol()
```

## 📊 Order Placement

```go
resp, err := client.PlaceOrder(ctx, &indira.PlaceOrderRequest{
    Symbol:       "STK_TCS_EQ_NSE_11536",
    ExcToken:     "11536",
    Exc:          "NSE",
    OrdAction:    "BUY",        // BUY, SELL
    OrdValidity:  "DAY",        // DAY, IOC
    OrdType:      "Limit",      // Market, Limit, SL, SL-M
    PrdType:      "INTRADAY",   // INTRADAY, DELIVERY, CASH, MTF
    LimitPrice:   3950.0,
    TriggerPrice: 0,
    Qty:          1,
    DisQty:       0,
    LotSize:      1,
    Instrument:   "STK",
    Amo:          false,
})
```

## ✏️ Order Modification

```go
err := client.ModifyOrder(ctx, &indira.ModifyOrderRequest{
    OrdId:         "NZULR1000AI2",
    Symbol:        "STK_TCS_EQ_NSE_11536",
    OrdAction:     "BUY",
    OrdValidity:   "DAY",
    ExchangeToken: "11536",
    Exc:           "NSE",
    Qty:           11,
    LimitPrice:    3961.0,
    OrdType:       "Limit",
    PrdType:       "DELIVERY",
    Instrument:    "STK",
})
```

## ❌ Order Cancellation

```go
err := client.CancelOrder(ctx, &indira.CancelOrderRequest{
    Symbol: "STK_TCS_EQ_NSE_11536",
    Exc:    "NSE",
    OrdId:  "NZULR1000AI2",
})
```

## 📚 Order Book

```go
orders, err := client.GetOrderBook(ctx)
for _, order := range orders {
    fmt.Printf("%s: %s %d @ %.2f - %s\n", 
        order.OrdId, order.Symbol, order.Qty, 
        order.LimitPrice, order.Status)
}
```

## 📈 Positions

```go
positions, err := client.GetPositions(ctx)
for _, pos := range positions {
    fmt.Printf("%s: Qty=%d, PnL=%.2f\n", 
        pos.Symbol, pos.NetQty, pos.PnL)
}
```

## 🏦 Holdings

```go
holdings, err := client.GetHoldings(ctx)
for _, h := range holdings {
    fmt.Printf("%s: %d @ %.2f (%.2f%%)\n", 
        h.Symbol, h.Qty, h.AvgPrice, h.PnLPercentage)
}
```

## 💰 Fund Limits

```go
funds, err := client.GetFundLimit(ctx)
fmt.Printf("Available: %.2f, Used: %.2f\n", 
    funds.AvailableCash, funds.UsedMargin)
```

## 🔄 Type Mapping

### Order Types
- `Market` - Market order
- `Limit` - Limit order
- `SL` - Stop Loss
- `SL-M` - Stop Loss Market

### Product Types
- `INTRADAY` - Intraday/MIS
- `DELIVERY` - Delivery/CNC
- `CASH` - Cash
- `MTF` - Margin Trading

### Order Actions
- `BUY` - Buy
- `SELL` - Sell

### Validity
- `DAY` - Day order
- `IOC` - Immediate or Cancel

### Instruments
- `STK` - Stock
- `OPT` - Option
- `FUT` - Future
- `IDX` - Index

## 🛠️ Utility Functions

```go
// Clean exchange name
exchange := indira.CleanExchangeName("EXCHANGE_NSE") // Returns "NSE"

// Map order types
orderType := indira.MapOrderType("MARKET") // Returns "Market"

// Map product types
productType := indira.MapProductType("MIS") // Returns "INTRADAY"

// Map order sides
side := indira.MapOrderSide("B") // Returns "BUY"

// Determine instrument
instrument := indira.DetermineInstrument("NSE", "TCS") // Returns "STK"
```

## 📍 API Endpoints

| Function | Endpoint |
|----------|----------|
| Place Order | POST `/order-services/api/order/v1/place-order` |
| Modify Order | POST `/order-services/api/order/v1/modify-order` |
| Cancel Order | POST `/order-services/api/order/v1/cancel-order` |
| Order Book | GET `/portfolio-services/api/order/v1/order-book` |
| Trade Book | POST `/portfolio-services/api/order/v1/trade-book` |
| Holdings | GET `/portfolio-services/api/portfolio/v2/holdings` |
| Positions | GET `/portfolio-services/api/portfolio/v1/position-book` |
| Account Info | GET `/user-services/api/user/v1/AccountInfo` |
| Fund Limit | GET `/payments/api/v1/get-fund-limit` |

**Base URL**: 

## ⚡ Error Handling

```go
resp, err := client.PlaceOrder(ctx, orderReq)
if err != nil {
    // Handle error
    log.Printf("Order failed: %v", err)
    return
}

// Check response status
if resp.Status == "error" {
    log.Printf("Broker rejected: %s", resp.Message)
    return
}

// Success
fmt.Printf("Order ID: %s\n", resp.OrderId)
```

## 🔐 Authentication Check

```go
if !client.IsAuthenticated() {
    // Token expired or not set
    token, expiry := getNewToken()
    client.SetBearerToken(token, expiry)
}
```

## 📦 Import

```go
import "github.com/RohitIndira/Algo-Treading/pkg/indira"
```

## 📖 Full Documentation

- **Migration Guide**: `docs/guides/INDIRA_MIGRATION_GUIDE.md`
- **Analysis**: `docs/guides/INDIRA_API_MIGRATION_ANALYSIS.md`
- **Client README**: `pkg/indira/README.md`
- **Summary**: `docs/guides/MIGRATION_SUMMARY.md`
