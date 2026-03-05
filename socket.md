# Enhanced Stream WebSocket – SMG Format Documentation

**Endpoint:** `wss://localhost/enhanced-stream`

---

## 1. Connection

### URL Parameters

| Parameter   | Type   | Required | Description                                                       |
|-------------|--------|----------|-------------------------------------------------------------------|
| `client_id` | string | Yes      | Unique identifier for this client. Used to restore subscriptions on reconnect. |
| `format`    | string | No       | Set to `binary` to receive binary frames (~80% smaller). Default: JSON. |

**JSON mode (default)**
```
wss://localhost/enhanced-stream?client_id=YOUR_CLIENT_ID
```

**Binary mode**
```
wss://localhost/enhanced-stream?client_id=YOUR_CLIENT_ID&format=binary
```

> Source: `enhanced_websocket.go:260-278` – `client_id` extracted from query string; `format=binary` sets `useBinary=true`.

---

## 2. Market Hours

Live data is only streamed when the Indian stock market is open.

| Setting       | Value               |
|---------------|---------------------|
| Market Open   | 09:15 IST           |
| Market Close  | 15:30 IST           |
| Timezone      | Asia/Kolkata (IST)  |

Outside these hours subscribing is still valid, but no `market_data` frames are delivered.
When the market opens/closes the server automatically broadcasts a status event to all clients.

> Source: `enhanced_websocket.go:207-246`

---

## 3. Message Types Overview

### Server → Client

| `type` value           | When sent                                          |
|------------------------|----------------------------------------------------|
| `welcome`              | Immediately on connect                              |
| `subscription_response`| After every `subscribe` / `unsubscribe` request    |
| `pong`                 | In response to a `ping` request                    |
| `subscriptions`        | In response to `get_subscriptions` request         |
| `market_status`        | In response to `market_status` request             |
| `market_data`          | Live tick data for subscribed tokens (market open) |
| `periodic_52week_data` | Every 2 minutes for subscribed tokens               |
| `market_opened`        | Broadcast when market transitions to open state    |
| `market_closed`        | Broadcast when market transitions to closed state  |
| `error`                | Any request error                                  |

### Client → Server

| `action` value      | Purpose                         |
|---------------------|---------------------------------|
| `subscribe`         | Subscribe to token/symbol list  |
| `unsubscribe`       | Unsubscribe from token/symbols  |
| `ping`              | Keep-alive heartbeat            |
| `get_subscriptions` | List current subscriptions      |
| `market_status`     | Get current market status       |

---

## 4. Client → Server Request Format

All client messages are JSON with the following schema:

```jsonc
{
  "type":      "request",          // Always "request"
  "action":    "<action>",         // See actions table above
  "stocks":    ["<token_or_symbol>", ...],  // For subscribe/unsubscribe
  "client_id": "<client_id>",      // Optional, informational
  "timestamp": 1700000000000       // Optional, Unix ms
}
```

The `stocks` array accepts **either** numeric token IDs or symbol strings (e.g. `"11536"` or `"RELIANCE"`).
Symbols are case-insensitive; they are upper-cased internally.

> Source: `enhanced_websocket.go:132-138`, `handleSubscribe:411-485`

---

## 5. Server → Client Message Schemas

### 5.1 Welcome (on connect)

```jsonc
{
  "type":      "welcome",
  "status":    "connected",
  "client_id": "YOUR_CLIENT_ID",
  "data": {
    "message":           "Connected to Odin Streamer Enhanced WebSocket",
    "client_id":         "YOUR_CLIENT_ID",
    "format":            "json",                 // or "binary"
    "features":          ["dynamic_subscriptions", "market_hours_control",
                          "persistent_connection", "real_time_data",
                          "binary_format_support"],
    "market_hours":      "09:15 - 15:30 IST",
    "market_open":       true,                   // bool
    "current_time":      "10:30:45",
    "subscribed_stocks": 0,                      // restored subscription count on reconnect
    "instructions": {
      "subscribe":     "{\"type\": \"request\", \"action\": \"subscribe\", \"stocks\": [\"11536\", \"476\"]}",
      "unsubscribe":   "{\"type\": \"request\", \"action\": \"unsubscribe\", \"stocks\": [\"11536\"]}",
      "ping":          "{\"type\": \"request\", \"action\": \"ping\"}",
      "binary_format": "Add ?format=binary to URL for 80% bandwidth reduction"
    }
  },
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:346-368`

---

### 5.2 Subscribe Request

**Send:**
```json
{
  "type":   "request",
  "action": "subscribe",
  "stocks": ["11536", "476", "RELIANCE"]
}
```

**Receive:**
```jsonc
{
  "type":      "subscription_response",
  "status":    "success",
  "client_id": "YOUR_CLIENT_ID",
  "data": {
    "action":            "subscribe",
    "subscribed_stocks": ["RELIANCE (NSE)", "INFY (NSE)"],   // successfully subscribed
    "failed_stocks":     ["BADTOKEN"],                         // not found in database
    "total_subscribed":  2                                     // running total
  },
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:467-485`

---

### 5.3 Unsubscribe Request

**Send:**
```json
{
  "type":   "request",
  "action": "unsubscribe",
  "stocks": ["11536"]
}
```

**Receive:**
```jsonc
{
  "type":      "subscription_response",
  "status":    "success",
  "client_id": "YOUR_CLIENT_ID",
  "data": {
    "action":              "unsubscribe",
    "unsubscribed_stocks": ["RELIANCE (NSE)"],
    "total_subscribed":    1
  },
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:547-565`

---

### 5.4 Ping / Pong

**Send:**
```json
{ "type": "request", "action": "ping" }
```

**Receive:**
```jsonc
{
  "type":      "pong",
  "status":    "alive",
  "client_id": "YOUR_CLIENT_ID",
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:568-579`

---

### 5.5 Get Subscriptions

**Send:**
```json
{ "type": "request", "action": "get_subscriptions" }
```

**Receive:**
```jsonc
{
  "type":      "subscriptions",
  "status":    "success",
  "client_id": "YOUR_CLIENT_ID",
  "data": {
    "subscribed_tokens":  ["11536", "476"],
    "subscribed_symbols": ["RELIANCE", "INFY"],
    "total_subscribed":   2
  },
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:582-613`

---

### 5.6 Market Status

**Send:**
```json
{ "type": "request", "action": "market_status" }
```

**Receive:**
```jsonc
{
  "type":      "market_status",
  "status":    "success",
  "client_id": "YOUR_CLIENT_ID",
  "data": {
    "market_open":    true,
    "market_hours":   "09:15 - 15:30 IST",
    "current_time":   "10:30:45",
    "next_open":      "2026-03-03 09:15:00",
    "streaming_data": true
  },
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:616-640`

---

### 5.7 Live Market Data (`market_data`)

Pushed to subscribed clients on every tick during market hours.

```jsonc
{
  "type":      "market_data",
  "symbol":    "RELIANCE",
  "token":     "11536",
  "exchange":  "NSE",
  "data": {
    // ── Identity ──────────────────────────────────────────
    "symbol":   "RELIANCE",
    "token":    "11536",
    "exchange": "NSE",              // "NSE" | "BSE" | "NFO" | "MCX"

    // ── Price ─────────────────────────────────────────────
    "ltp":           2450.75,       // Last Traded Price
    "open":          2430.00,
    "high":          2460.00,
    "low":           2420.50,
    "close":         2440.00,       // Closing price (last session)
    "prev_close":    2440.00,       // Previous close (frozen after 15:15 IST)

    // ── Change ────────────────────────────────────────────
    "percent_change": 0.44,         // % change vs prev_close
    "percent_value":  10.75,        // Absolute change vs prev_close (in ₹)

    // ── Volume ────────────────────────────────────────────
    "volume":       3842000,        // int64
    "avg_volume_5d": 3842000,       // int64

    // ── 52-Week Range ─────────────────────────────────────
    "week_52_high":           2800.00,
    "week_52_low":            1950.00,
    "week_52_high_date":      "2025-09-15",
    "week_52_low_date":       "2025-03-10",
    "week_52_high_timestamp": "2025-09-15T09:30:00Z",
    "week_52_low_timestamp":  "2025-03-10T09:30:00Z",

    // ── Day Range ─────────────────────────────────────────
    "day_high": 2460.00,
    "day_low":  2420.50,

    // ── Record Flags ──────────────────────────────────────
    "is_new_week_52_high": false,   // true if this tick set a new 52w high
    "is_new_week_52_low":  false,   // true if this tick set a new 52w low

    // ── Timing ────────────────────────────────────────────
    "timestamp":    1700000000000,  // Unix ms from exchange
    "last_updated": "2026-03-02T13:04:39Z"  // Server ISO8601
  },
  "timestamp": 1700000000000       // Unix ms (outer envelope)
}
```

> Source: `main.go:98-132` (LiveMarketData struct), `main.go:1266-1292` (field assignment)

---

### 5.8 Periodic 52-Week Data (`periodic_52week_data`)

Sent every **2 minutes** for all subscribed tokens, regardless of live ticks.
Useful for keeping UI fresh when market activity is low.

```jsonc
{
  "type":      "periodic_52week_data",
  "symbol":    "RELIANCE",
  "token":     "11536",
  "exchange":  "NSE",
  "data": {
    "symbol":            "RELIANCE",
    "token":             "11536",
    "exchange":          "NSE",

    "week_52_high":      2800.00,
    "week_52_low":       1950.00,
    "week_52_high_date": "2025-09-15",
    "week_52_low_date":  "2025-03-10",

    "last_close":        2440.00,
    "ltp":               2440.00,   // Uses last_close as proxy
    "day_high":          2800.00,   // Falls back to week_52_high
    "day_low":           1950.00,   // Falls back to week_52_low
    "volume":            0,
    "percent_change":    0.0,

    "data_type":       "periodic_52week",
    "broadcast_time":  "2026-03-02T12:00:00Z",
    "last_updated":    "2026-03-02T11:58:00Z",
    "has_recent_data": true,     // true if DB was updated within last 24h
    "timestamp":       1700000000000
  },
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:852-877`

---

### 5.9 Market Status Broadcasts

Automatically pushed to **all** connected clients when market opens or closes.

```jsonc
// Market opened
{
  "type":    "market_opened",
  "status":  "info",
  "message": "Market is now open - data streaming started",
  "data": {
    "market_open":  true,
    "current_time": "09:15:00"
  },
  "timestamp": 1700000000000
}

// Market closed
{
  "type":    "market_closed",
  "status":  "info",
  "message": "Market is now closed - data streaming stopped",
  "data": {
    "market_open":  false,
    "current_time": "15:30:00"
  },
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:676-697`

---

### 5.10 Error

```jsonc
{
  "type":      "error",
  "status":    "error",
  "message":   "Unknown action: bad_action",
  "client_id": "YOUR_CLIENT_ID",
  "timestamp": 1700000000000
}
```

> Source: `enhanced_websocket.go:700-712`

---

## 6. Binary Frame Format (when `format=binary`)

Add `?format=binary` to the URL to receive binary WebSocket frames.
All values are **Little-Endian** byte order.

### Message Type Byte (first byte of every frame)

| Byte   | Message Type         |
|--------|----------------------|
| `0x01` | Market Data          |
| `0x02` | Welcome              |
| `0x03` | Subscription         |
| `0x04` | Ping                 |
| `0x05` | Pong                 |
| `0x06` | Error                |
| `0x07` | 52-Week Data         |
| `0x08` | Market Status        |
| `0x09` | Cached Data          |
| `0x0A` | Subscription Ack     |

> Source: `binary_encoding.go:14-25`

---

### Binary Market Data Frame Layout (`0x01`)

Total size: ~64–95 bytes depending on token/symbol length.

```
Offset  Size    Type      Field
──────  ──────  ────────  ──────────────────────────
0       1       uint8     Message Type (0x01)
1       8       int64     Timestamp (Unix ms)
9       1       uint8     Token length (N)
10      N       string    Token (ASCII)
10+N    1       uint8     Symbol length (M)
11+N    M       string    Symbol (ASCII)
11+N+M  1       uint8     Exchange (0=NSE, 1=BSE, 2=NFO, 3=MCX)
12+N+M  4       float32   LTP
16+N+M  4       float32   Open
20+N+M  4       float32   High
24+N+M  4       float32   Low
28+N+M  4       float32   Close
32+N+M  4       float32   PrevClose
36+N+M  8       int64     Volume
44+N+M  4       float32   PercentChange
48+N+M  4       float32   Week52High
52+N+M  4       float32   Week52Low
56+N+M  4       float32   DayHigh
60+N+M  4       float32   DayLow
64+N+M  1       uint8     IsNewWeek52High (0 or 1)
65+N+M  1       uint8     IsNewWeek52Low  (0 or 1)
```

> Source: `binary_encoding.go:36-110`

**Exchange byte encoding:**

| Byte | Exchange |
|------|----------|
| `0`  | NSE      |
| `1`  | BSE      |
| `2`  | NFO      |
| `3`  | MCX      |

---

### Binary Welcome Frame Layout (`0x02`)

```
Offset  Size    Type      Field
──────  ──────  ────────  ──────────
0       1       uint8     Message Type (0x02)
1       8       int64     Timestamp (Unix ms)
9       1       uint8     ClientID length
10      var     string    ClientID
var     1       uint8     IsMarketOpen (0 or 1)
```

> Source: `binary_encoding.go:197-215`

---

## 7. Reconnection & Session Persistence

- If a client reconnects with the same `client_id`, the server **restores** all previous subscriptions automatically.
- The old connection is closed before the new one is accepted.
- No re-subscribe messages are needed after a reconnect.

> Source: `enhanced_websocket.go:282-311`

---

## 8. Quick-Start Examples

### JavaScript (JSON mode)

```javascript
const ws = new WebSocket(
  'wss://localhost/enhanced-stream?client_id=my-app-001'
);

ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);

  if (msg.type === 'welcome') {
    // Subscribe to RELIANCE and INFY by token
    ws.send(JSON.stringify({
      type: 'request',
      action: 'subscribe',
      stocks: ['11536', '476']     // numeric token IDs
    }));
  }

  if (msg.type === 'market_data') {
    const { symbol, data } = msg;
    console.log(`${symbol}: ₹${data.ltp} (${data.percent_change.toFixed(2)}%)`);
  }
};

// Keep-alive every 30 seconds
setInterval(() => {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'request', action: 'ping' }));
  }
}, 30_000);
```

---

### Python (JSON mode)

```python
import websockets
import asyncio
import json

URL = "wss://localhost/enhanced-stream?client_id=py-client-001"

async def main():
    async with websockets.connect(URL) as ws:
        async for raw in ws:
            msg = json.loads(raw)

            if msg["type"] == "welcome":
                await ws.send(json.dumps({
                    "type": "request",
                    "action": "subscribe",
                    "stocks": ["11536", "INFY"]
                }))

            elif msg["type"] == "market_data":
                d = msg["data"]
                print(f"{d['symbol']} | LTP: {d['ltp']} | Change: {d['percent_change']:.2f}%")

asyncio.run(main())
```

---

### Python – Binary mode decoder

```python
import websockets
import asyncio
import struct

URL = "wss://localhost/enhanced-stream?client_id=py-bin-001&format=binary"

MSG_MARKET_DATA = 0x01
EXCHANGES = {0: "NSE", 1: "BSE", 2: "NFO", 3: "MCX"}

def decode_market_data(data: bytes) -> dict:
    offset = 0
    msg_type = data[offset]; offset += 1
    assert msg_type == MSG_MARKET_DATA

    timestamp = struct.unpack_from('<q', data, offset)[0]; offset += 8

    token_len = data[offset]; offset += 1
    token = data[offset:offset+token_len].decode(); offset += token_len

    sym_len = data[offset]; offset += 1
    symbol = data[offset:offset+sym_len].decode(); offset += sym_len

    exchange = EXCHANGES.get(data[offset], "NSE"); offset += 1

    ltp, open_, high, low, close, prev_close = struct.unpack_from('<6f', data, offset); offset += 24
    volume = struct.unpack_from('<q', data, offset)[0]; offset += 8
    percent_change = struct.unpack_from('<f', data, offset)[0]; offset += 4
    w52h, w52l = struct.unpack_from('<2f', data, offset); offset += 8
    day_high, day_low = struct.unpack_from('<2f', data, offset); offset += 8
    is_new_52h = bool(data[offset]); offset += 1
    is_new_52l = bool(data[offset]); offset += 1

    return {
        "timestamp": timestamp, "token": token, "symbol": symbol,
        "exchange": exchange, "ltp": ltp, "open": open_, "high": high,
        "low": low, "close": close, "prev_close": prev_close,
        "volume": volume, "percent_change": percent_change,
        "week_52_high": w52h, "week_52_low": w52l,
        "day_high": day_high, "day_low": day_low,
        "is_new_week_52_high": is_new_52h, "is_new_week_52_low": is_new_52l
    }

async def main():
    async with websockets.connect(URL) as ws:
        async for msg in ws:
            if isinstance(msg, bytes) and len(msg) > 0 and msg[0] == MSG_MARKET_DATA:
                tick = decode_market_data(msg)
                print(tick)

asyncio.run(main())
```

---

## 9. Data Source Reference

| Field in payload         | Source in `b2c_bridge.py`         |
|--------------------------|-----------------------------------|
| `ltp`                    | `stock_data['LTP']`               |
| `high`                   | `stock_data['HighPrice']`         |
| `low`                    | `stock_data['LowPrice']`          |
| `open`                   | `stock_data['OpenPrice']`         |
| `close` / `prev_close`   | `stock_data['ClosePrice']`        |
| `volume`                 | `stock_data['Volume']`            |
| `week_52_high`           | `stock_data['LifeTimeHigh']`      |
| `week_52_low`            | `stock_data['LifeTimeLow']`       |
| `timestamp`              | `stock_data['LUT']` * 1000 (ms)   |
| `percent_change`         | Calculated server-side: `((ltp − prev_close) / prev_close) × 100` |
| `percent_value`          | Calculated server-side: `ltp − prev_close` |

`prev_close` is locked after **15:15 IST** – the server reads the frozen value from Redis after that time.

> Source: `b2c_bridge.py:327-388`, `main.go:1231-1263`
