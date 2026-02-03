# Cash 52-Week High (CASH_52W_HIGH) — End-to-End Integration Doc

This document explains the **full Cash52W flow**, including:

- **Domain & architecture** (how services connect)
- **Public APIs** (REST + WebSocket via API Gateway)
- **Kafka topics + message shapes**
- **What frontend/team needs to send and what they will receive**

> Base Domain (Production)
>
> `https://stockkaskalgolive.indiratrade.com`

---

## 0) Domain/Service Map (High Level)

### Services involved

1. **API Gateway** (`/api/v1/...`, `/ws/...`)
   - Exposes REST + WebSocket to frontend
2. **user-config-service**
   - Stores managed Cash52W config in DB table `cash52w_configs`
   - Emits config events to Kafka topic `user-configs.cash52w`
3. **data-ingestion**
   - Watches Redis market snapshots and publishes 52W breakout events to Kafka `market.data.52w_breakouts`
4. **rules-engine**
   - Consumes `market.data.52w_breakouts`
   - Consumes `user-configs.cash52w` (to know enabled users + trading_mode)
   - Publishes order intents to Kafka topic `trade-signals`
   - For LIVE mode also publishes to RabbitMQ for trade-execution
5. **paper-execution**
   - Consumes `trade-signals`
   - Filters `trading_mode == PAPER`
   - Publishes simulated fills to `paper-executions.52w`
   - Publishes PnL snapshots/portfolio summaries to `paper-pnl.52w`

### User-specific LIVE vs PAPER behaviour

- If a user configures Cash52W with `trading_mode: PAPER`, then:
  - rules-engine marks every order for that user as `trading_mode=PAPER`
  - paper-execution consumes those signals
  - **NO real order** is sent to trade-execution

- If a user configures `trading_mode: LIVE`, then:
  - rules-engine marks signals as `LIVE`
  - paper-execution ignores them
  - order is published to RabbitMQ for real trade-execution

---

## 1) Configure / Enable Cash52W (REST)

### Endpoint

`POST /api/v1/strategies/cash52w/configure`

### Purpose
Enable/disable the managed strategy for a user and set capital + mode.

### Request JSON

| Field | Type | Required | Notes |
|---|---:|---:|---|
| `user_id` | string | ✅ | Example: `ISPL19027` |
| `enabled` | boolean | ✅ | `true` enables, `false` disables |
| `capital_per_stock` | number | ❌ | Default ~20000 if <= 0 |
| `trading_mode` | string | ❌ | `"PAPER"` or `"LIVE"` (default LIVE). Also accepts `tradingMode` |

### Example (Enable PAPER)

```bash
curl -X POST "https://stockkaskalgolive.indiratrade.com/api/v1/strategies/cash52w/configure" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "ISPL19027",
    "enabled": true,
    "capital_per_stock": 20000,
    "trading_mode": "PAPER"
  }'
```

### Copy/Paste Safe (single-line)

```bash
curl -X POST "https://stockkaskalgolive.indiratrade.com/api/v1/strategies/cash52w/configure" -H "Content-Type: application/json" -d '{"user_id":"ISPL19027","enabled":true,"capital_per_stock":20000,"trading_mode":"PAPER"}'
```

### Example response

```json
{
  "success": true,
  "user_id": "ISPL19027",
  "enabled": true,
  "capital_per_stock": 20000,
  "trading_mode": "PAPER"
}
```

### Notes

- This API also publishes a **compact config event** to Kafka topic `user-configs.cash52w`.
- It stores the config in PostgreSQL table `cash52w_configs`.

---

## 2) Disable Cash52W (REST)

### Endpoint

`POST /api/v1/strategies/cash52w/configure`

### Example (Disable)

```bash
curl -X POST "https://stockkaskalgolive.indiratrade.com/api/v1/strategies/cash52w/configure" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "ISPL19027",
    "enabled": false
  }'
```

### Copy/Paste Safe (single-line)

```bash
curl -X POST "https://stockkaskalgolive.indiratrade.com/api/v1/strategies/cash52w/configure" -H "Content-Type: application/json" -d '{"user_id":"ISPL19027","enabled":false}'
```

### Common mistake (causes shell issues / sometimes misleading 502 output)

Do **NOT** paste duplicated `curl` on the same line, like:

```text
curl -X POST "..." \        curl -X POST "..." \
```

If you see **502 Bad Gateway**, it is usually an nginx upstream issue (gateway temporarily down) or a malformed shell command. Re-run the **single-line** command above.

### Behaviour

- Deletes the row from `cash52w_configs` for the user
- Publishes a Kafka event with `event_type: DELETE` to `user-configs.cash52w`

---

## 3) Kafka: user-configs.cash52w (Config Events)

### Topic

`user-configs.cash52w`

### Producer

`user-config-service`

### Consumer

`rules-engine`

### Message schema

```json
{
  "event_type": "CREATE" | "UPDATE" | "DELETE",
  "user_id": "ISPL19027",
  "enabled": true,
  "capital_per_stock": 20000,
  "trading_mode": "PAPER",
  "timestamp": 1769668523
}
```

---

## 4) Kafka: market.data.52w_breakouts (52W Breakout Events)

### Topic

`market.data.52w_breakouts`

### Producer

`data-ingestion` (Redis 52W watcher)

### Consumer

`rules-engine` (Cash52W engine)

### Message schema (example)

```json
{
  "symbol": "ONGC",
  "token": "2475",
  "exchange": "NSE",
  "ltp": 274.01,
  "week_52_high": 275.73,
  "week_52_low": 205,
  "week_52_high_date": "2026-01-29",
  "timestamp": 1769668532421,
  "last_updated": "2026-01-29T12:05:33.154580965+05:30"
}
```

---

## 5) Kafka: trade-signals (Order Intents)

### Topic

`trade-signals`

### Producer

`rules-engine`

### Consumers

- `paper-execution` (**consumes only** `trading_mode=PAPER`)
- (LIVE execution path uses RabbitMQ for real order execution)

### Message schema (subset used by paper-execution)

```json
{
  "order_id": "...",
  "user_id": "ISPL19027",
  "strategy_id": "CASH_52W_HIGH",
  "strategy_name": "Cash 52-Week High",
  "stock_code": 2475,
  "token": 2475,
  "symbol": "ONGC",
  "exchange": "NSE",
  "order_type": "MARKET",
  "order_side": "BUY",
  "quantity": 72,
  "price": 274.01,
  "stop_loss": 246.609,
  "take_profit": 328.812,
  "timestamp": "2026-01-29T06:59:02.123Z",
  "trading_mode": "PAPER"
}
```

---

## 6) Kafka: paper-executions.52w (Paper Fills)

### Topic

`paper-executions.52w`

### Producer

`paper-execution`

### Message schema (example)

```json
{
  "event_id": "208b6c86-b5a0-4b0c-86e4-067313f12301",
  "strategy_id": "CASH_52W_HIGH",
  "user_id": "ISPL19027",
  "token": 30274,
  "symbol": "SILVERCASE",
  "exchange": "NSE",
  "order_side": "BUY",
  "quantity": 530,
  "price": 37.7,
  "leg": "ENTRY",
  "reason": "ENTRY",
  "buy_order_id": "b79eed51-8062-4917-acd1-884918219fb1",
  "pnl": 0,
  "created_at": "2026-01-29T06:59:05.627624736Z"
}
```

---

## 7) Kafka: paper-pnl.52w (Paper PnL stream)

### Topic

`paper-pnl.52w`

### Producer

`paper-execution`

### Message types (two shapes)

#### A) Lightweight snapshot

```json
{
  "user_id": "ISPL19027",
  "strategy_id": "CASH_52W_HIGH",
  "closed_pnl": 0,
  "open_positions": 25,
  "timestamp": "2026-01-29T07:21:02.581100462Z"
}
```

#### B) Full portfolio summary

```json
{
  "user_id": "ISPL19027",
  "strategy_id": "CASH_52W_HIGH",
  "open_positions": [
    {
      "user_id": "ISPL19027",
      "strategy_id": "CASH_52W_HIGH",
      "token": 2475,
      "symbol": "ONGC",
      "exchange": "NSE",
      "quantity": 72,
      "entry_price": 274.45,
      "current_price": 273.87,
      "unrealized_pnl": -41.76,
      "pnl_percent": -0.21,
      "timestamp": "2026-01-29T07:21:01.568792443Z"
    }
  ],
  "open_positions_count": 25,
  "total_market_value": 488512.11,
  "total_unrealized_pnl": -236.64,
  "total_closed_pnl": 0,
  "portfolio_value": 488512.11,
  "avg_per_stock": 19540.4844,
  "available_capital": 19540.4844,
  "timestamp": "2026-01-29T07:21:01.576073202Z"
}
```

---

## 8) Frontend Realtime PnL (WebSocket via Gateway)

### Endpoint

`GET /ws/pnl?user_id=ISPL19027`

### WSS (Production)

Use **wss** behind the domain:

```text
wss://stockkaskalgolive.indiratrade.com/ws/pnl?user_id=ISPL19027
```

### WS (Local)

```text
ws://localhost:8080/ws/pnl?user_id=ISPL19027
```

### Source

Kafka topic: **`paper-pnl.52w`**

### WebSocket message envelope

All Kafka payloads are forwarded as-is under `data`:

```json
{
  "type": "pnl",
  "user_id": "ISPL19027",
  "source": "kafka",
  "topic": "paper-pnl.52w",
  "data": { "...": "original kafka json" }
}
```

### Frontend parsing

- If `data.open_positions` is an array → render full portfolio summary.
- Else use the lightweight snapshot fields.

---

## 9) Operational Notes / Common Issues

### Why portfolio.realtime.52w is empty
That topic is produced by rules-engine realtime portfolio loop. In your
environment you already have `paper-pnl.52w` reliably populated by
paper-execution, so frontend should use `paper-pnl.52w`.

### Why user may get < 25 positions
25 is a cap. Positions are opened only for eligible same-day 52W breakouts.

### Auto-close all PAPER positions when user disables strategy

When you disable Cash52W for a user via:

```bash
curl -X POST "https://stockkaskalgolive.indiratrade.com/api/v1/strategies/cash52w/configure" -H "Content-Type: application/json" -d '{"user_id":"ISPL19027","enabled":false}'
```

`paper-execution` now listens to Kafka topic `user-configs.cash52w`.
On `event_type=DELETE` (or `enabled=false`) it will:

- Force-close **all open PAPER positions** for that user
- Emit SELL events to `paper-executions.52w` with:
  - `leg = "FORCE_EXIT"`
  - `reason = "STRATEGY_DISABLED"`
- Publish an updated portfolio snapshot/summary to `paper-pnl.52w`

---

## 10) Environment Variables (Gateway)

| Var | Default | Purpose |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Kafka brokers list |
| `KAFKA_TOPIC_PAPER_PNL` | `paper-pnl.52w` | Topic used by `/ws/pnl` |
