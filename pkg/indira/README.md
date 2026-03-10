# Indira Securities API Client

A native Go client for the Indira Securities trading API.

## Features

- ✅ Order Management (Place, Modify, Cancel)
- ✅ Portfolio Management (Holdings, Positions)
- ✅ Order Book & Trade Book
- ✅ Account Information & Fund Limits
- ✅ Brokerage Calculation
- ✅ Type-safe API with comprehensive error handling
- ✅ Context support for request cancellation
- ✅ Thread-safe authentication management

## Installation

```bash
go get github.com/RohitIndira/Algo-Treading/pkg/indira
```

## Usage

### Initialize Client

```go
import "github.com/RohitIndira/Algo-Treading/pkg/indira"

// Create client
client := indira.NewClient(indira.Config{
    UserId: "ISPL19122",
    AppId:  "your-app-id",
    Source: "IOS", // or "AND", "WEB"
})

// Set bearer token (obtained from authentication service)
client.SetBearerToken("your-bearer-token", expiryTime)
```

### Place Order

```go
// Build symbol
sb := indira.NewSymbolBuilder()
sb.Instrument = "STK"
sb.Symbol = "TCS"
sb.Series = "EQ"
sb.Exchange = "NSE"
sb.Token = "11536"

// Create order request
orderReq := &indira.PlaceOrderRequest{
    Symbol:       sb.BuildSymbol(),
    ExcToken:     sb.Token,
    Exc:          sb.Exchange,
    OrdAction:    "BUY",
    OrdValidity:  "DAY",
    OrdType:      "Limit",
    PrdType:      "INTRADAY",
    LimitPrice:   3950.0,
    TriggerPrice: 0,
    Qty:          1,
    DisQty:       0,
    LotSize:      1,
    Instrument:   sb.Instrument,
    Amo:          false,
}

// Place order
resp, err := client.PlaceOrder(context.Background(), orderReq)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Order placed: %s\n", resp.OrderId)
```

### Modify Order

```go
modifyReq := &indira.ModifyOrderRequest{
    OrdId:         "NZULR1000AI2",
    Symbol:        "STK_TCS_EQ_NSE_11536",
    OrdAction:     "BUY",
    OrdValidity:   "DAY",
    ExchangeToken: "11536",
    Exc:           "NSE",
    Qty:           11,
    TradedQty:     0,
    LimitPrice:    3961.0,
    TriggerPrice:  0,
    OrdType:       "Limit",
    PrdType:       "DELIVERY",
    Instrument:    "STK",
    LotSize:       1,
    DisQty:        0,
    OffMktFlag:    false,
}

err := client.ModifyOrder(context.Background(), modifyReq)
if err != nil {
    log.Fatal(err)
}
```

### Cancel Order

```go
cancelReq := &indira.CancelOrderRequest{
    Symbol: "STK_TCS_EQ_NSE_11536",
    Exc:    "NSE",
    OrdId:  "NZULR1000AI2",
}

err := client.CancelOrder(context.Background(), cancelReq)
if err != nil {
    log.Fatal(err)
}
```

### Get Order Book

```go
orders, err := client.GetOrderBook(context.Background())
if err != nil {
    log.Fatal(err)
}

for _, order := range orders {
    fmt.Printf("Order: %s, Symbol: %s, Status: %s\n", 
        order.OrdId, order.Symbol, order.Status)
}
```

### Get Positions

```go
positions, err := client.GetPositions(context.Background())
if err != nil {
    log.Fatal(err)
}

for _, pos := range positions {
    fmt.Printf("Position: %s, Qty: %d, PnL: %.2f\n", 
        pos.Symbol, pos.NetQty, pos.PnL)
}
```

### Get Holdings

```go
holdings, err := client.GetHoldings(context.Background())
if err != nil {
    log.Fatal(err)
}

for _, holding := range holdings {
    fmt.Printf("Holding: %s, Qty: %d, PnL: %.2f%%\n", 
        holding.Symbol, holding.Qty, holding.PnLPercentage)
}
```

### Get Account Info

```go
accountInfo, err := client.GetAccountInfo(context.Background())
if err != nil {
    log.Fatal(err)
}

fmt.Printf("User: %s, Client ID: %s\n", 
    accountInfo.UserName, accountInfo.ClientId)
```

### Get Fund Limit

```go
fundLimit, err := client.GetFundLimit(context.Background())
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Available Cash: %.2f, Used Margin: %.2f\n", 
    fundLimit.AvailableCash, fundLimit.UsedMargin)
```

## Symbol Format

Indira Securities uses a specific symbol format:

```
{INSTRUMENT}_{SYMBOL}_{SERIES}_{EXCHANGE}_{TOKEN}
```

Examples:
- `STK_TCS_EQ_NSE_11536` - TCS stock on NSE
- `STK_RELIANCE_EQ_BSE_500325` - Reliance stock on BSE
- `OPT_NIFTY_FO_NSE_12345` - Nifty option

Use the `SymbolBuilder` to construct symbols correctly:

```go
sb := indira.NewSymbolBuilder()
sb.Instrument = "STK"  // STK, OPT, FUT, IDX
sb.Symbol = "TCS"
sb.Series = "EQ"       // EQ, FO
sb.Exchange = "NSE"    // NSE, BSE
sb.Token = "11536"
symbol := sb.BuildSymbol()
```

Or parse existing symbols:

```go
sb, err := indira.ParseSymbol("STK_TCS_EQ_NSE_11536")
```

## Order Types

- `Market` - Market order
- `Limit` - Limit order
- `SL` - Stop Loss
- `SL-M` - Stop Loss Market

## Product Types

- `INTRADAY` - Intraday/MIS
- `DELIVERY` - Delivery/CNC
- `CASH` - Cash
- `MTF` - Margin Trading Facility

## Order Actions

- `BUY` - Buy order
- `SELL` - Sell order

## Validity

- `DAY` - Day order (valid until end of trading day)
- `IOC` - Immediate or Cancel

## Error Handling

All methods return errors that should be checked:

```go
resp, err := client.PlaceOrder(ctx, orderReq)
if err != nil {
    // Handle error
    log.Printf("Order placement failed: %v", err)
    return
}

// Use resp
fmt.Printf("Order ID: %s\n", resp.OrderId)
```

## Authentication

This client does not handle authentication/login directly. You must obtain a bearer token from your authentication service and set it using `SetBearerToken()`.

The token should be refreshed before expiry. Check authentication status:

```go
if !client.IsAuthenticated() {
    // Token expired or not set, need to refresh
    // ... obtain new token from auth service ...
    client.SetBearerToken(newToken, newExpiry)
}
```

## Thread Safety

The client is thread-safe for concurrent use. Authentication information is protected by mutexes.


## License

Part of the Algo-Trading system.
