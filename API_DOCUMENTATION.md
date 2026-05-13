# Manthan Trading Platform — API Documentation

Frontend integration reference for the Manthan production API.

## Base URL

```
https://manthan.stockk.trade
```

All endpoints are reachable directly over HTTPS. nginx routes:

- `/api/v1/*`, `/livez`, `/readyz`, `/health`, `/ws/matches[/all]` → api-gateway
- `/ws/live-orders`, `/ws/paper-trades` → trade-execution
- everything else → Next.js frontend

## Authentication

After SSO login, the frontend obtains a JWT, `appId`, `userId`, and `authCode` from the Indira SSO flow. Every API call (except `/livez`, `/readyz`, `/health`) requires these four request headers:

| Header | Description | Example |
|---|---|---|
| `Authorization` | `Bearer <jwt>` from SSO login | `Bearer eyJhbGc...` |
| `userId` | Indira client ID | `ND03920` |
| `appId` | App identifier from SSO | `22024064ab9e2cd3857c0ffb2502177b1778502183834` |
| `source` | Platform source | `WEB`, `AND`, `IOS` |

**Missing any header → `401 Unauthorized`. Mismatch between header `userId` and body `user_id` → `403 Forbidden` (IDOR protection).**

**Auth lives in headers only — never duplicated in the body.** The body of any request (strategy create, force-exit, etc.) carries only domain data. The 4 auth headers identify the caller; the gateway uses them for the IDOR check and forwards them to internal services as the `IndiraAuthContext`.

**Two places auth is held:**

| Where | Set by | Used for |
|---|---|---|
| Request headers (every API call) | Frontend on each request | Authenticating this specific HTTP call + IDOR check |
| `user_credentials` DB row | Frontend via `POST /api/v1/auth/credentials` (once per SSO login + JWT refresh) | Server-side flows when the user is offline: rebalancer cron, protective replayer at 15:35 IST, trailing-SL workers, fill consumer |

The JWT expires every 24 hours. On expiry (`401 AU004 Session expired` from any endpoint), prompt the user to re-login and POST the fresh JWT to `/api/v1/auth/credentials`.

## Error response shape

All errors are returned as:

```json
{
  "error": "human-readable message"
}
```

with the appropriate 4xx/5xx HTTP status. Successful responses are documented per-endpoint below.

---

# Health probes (no auth required)

These are mounted at root, not under `/api/v1`. Frontends can use them for a service-up indicator.

### `GET /livez`
Process-alive check. Always returns `200` if the gateway is up.

```bash
curl https://manthan.stockk.trade/livez
```
Response: `{"status":"ok"}`

### `GET /readyz`
Read-only probe of every dependency (DB, Redis, Kafka, user-config, rules-engine, trade-execution).

```bash
curl https://manthan.stockk.trade/readyz
```
Response: `200` when healthy:
```json
{"status":"ok","checks":{"trading_db":"ok","trading_execution":"ok","redis":"ok","kafka":"ok","user_config":"ok","rules_engine":"ok","trade_execution":"ok"}}
```
Returns `503` if any check fails, with the failed checks listed.

### `GET /health`
Deep probe: `/readyz` plus a DB write check (UPSERT on `health_probes` table). Use sparingly.

---

# Authentication endpoint

### `POST /api/v1/auth/credentials`
Persists the broker JWT to the backend so server-side flows (rebalancer cron, protective replayer, trail-SL workers) can use it without the user being online. **Call this on every successful SSO login AND every JWT refresh.**

**Headers**: standard auth headers (the `Authorization` header carries the same JWT being persisted, or the session JWT — the body's `bearer_token` is what's actually stored).

**Body**:
```json
{
  "user_id": "ND03920",
  "bearer_token": "eyJhbGc...",
  "app_id": "22024064ab9e2cd3857c0ffb2502177b1778502183834",
  "source": "WEB"
}
```

| Field | Required | Description |
|---|---|---|
| `user_id` | optional | Defaults to header `userId`. Must match header `userId` if provided. |
| `bearer_token` | **required** | The broker JWT to persist. |
| `app_id` | optional | Defaults to header `appId`. |
| `source` | optional | Defaults to header `source`. |

**Response `200`**:
```json
{"success": true, "user_id": "ND03920"}
```

---

# Strategy CRUD

### `POST /api/v1/strategies`
Create a new strategy. For Manthan strategies, use `strategy_type: "MANTHAN"`. The strategy starts inactive unless `activate_immediately: true` is set.

**Body** (Manthan ATH-breakout example) — auth is in the request headers (see [Authentication](#authentication) above), never duplicated in the body:
```json
{
  "user_id": "ND03920",
  "strategy_name": "Manthan Live (Prod)",
  "description": "Production Manthan — ATH breakout, 5L capital",
  "strategy_type": "MANTHAN",
  "trading_mode": "LIVE",
  "activate_immediately": true,
  "trade_config": {
    "order_type": "LIMIT",
    "product_type": "DELIVERY",
    "validity": "DAY",
    "quantity": 1,
    "exchange": "NSE",
    "order_side": "BUY",
    "stop_loss_type": "TRAILING",
    "stop_loss_pct": 20,
    "trailing_sl_pct": 2,
    "position_sizing_mode": "EMA_ALLOCATION",
    "total_capital": 500000,
    "max_positions": 25
  },
  "risk_limits": {
    "enable_risk_checks": false,
    "enable_auto_square_off": false
  }
}
```

| Field | Type | Description |
|---|---|---|
| `strategy_name` | string | Required. Display name. |
| `strategy_type` | string | `"MANTHAN"` for ATH-breakout, `"52W_BREAKOUT"` for 52-week, `"NEWS"` for news-driven. |
| `trading_mode` | string | `"LIVE"` (real orders) or `"PAPER"` (simulated). |
| `activate_immediately` | bool | If `true`, strategy starts trading immediately. |
| `trade_config.total_capital` | number | Total ₹ to deploy across all positions. |
| `trade_config.max_positions` | int | Max concurrent positions (per-call sizing = `total_capital / max_positions`). |
| `trade_config.stop_loss_pct` | number | 20 = 20% below entry. |
| `trade_config.trailing_sl_pct` | number | 2 = 2% trail step. |
| `trade_config.position_sizing_mode` | string | `"EMA_ALLOCATION"` for Manthan (per-call × index EMA%). |

**Response `201`**:
```json
{
  "strategy_id": "5ff73910-d6b2-4547-8325-3e8026b32218",
  "strategy_name": "Manthan Live (Prod)",
  "strategy_type": "MANTHAN",
  "success": true,
  "trading_mode": "LIVE"
}
```

### `GET /api/v1/strategies/{strategy_id}`
Fetch a single strategy by ID.

### `PUT /api/v1/strategies/{strategy_id}`
Update strategy. Body shape mirrors `CreateStrategyRequest` — all fields optional, only provided fields are updated. Include `version` for optimistic concurrency.

### `DELETE /api/v1/strategies/{strategy_id}`
Soft-delete a strategy. Active positions are NOT exited — they're orphaned. Use `/live-orders/force-exit-strategy` first if you want to exit.

### `POST /api/v1/strategies/{strategy_id}/activate`
Enables a strategy. Trading resumes from the next signal.
Body: `{"user_id": "ND03920"}`

### `POST /api/v1/strategies/{strategy_id}/deactivate`
Disables a strategy. Active positions remain but no new entries fire.
Body: `{"user_id": "ND03920"}`

### `GET /api/v1/users/{user_id}/strategies`
List all strategies for a user.

**Response `200`**:
```json
{
  "strategies": [
    {
      "strategy_id": "fb7831f5-2c0e-4f04-ae5c-3b2f7ecd4bc7",
      "strategy_name": "Manthan Live (Prod)",
      "strategy_type": "MANTHAN",
      "trading_mode": "LIVE",
      "is_active": true,
      "trade_config": { ... },
      "created_at": "2026-05-12T06:54:33Z"
    }
  ]
}
```

---

# Manthan overview (single endpoint for the strategy dashboard)

### `GET /api/v1/manthan/overview?user_id=X&strategy_id=Y`

One aggregated payload for the Manthan dashboard — covers active positions, today's eligible signals, sector/MCap/index allocation buckets, cooldowns, EMA weights, and the order activity log.

`strategy_id` is optional; when omitted, positions across all Manthan strategies for the user are aggregated.

**Response `200`**:
```json
{
  "success": true,
  "user_id": "ND03920",
  "strategy_id": "fb7831f5-...",
  "positions": [
    {
      "symbol": "ARVIND",
      "isin": "INE034A01011",
      "industry": "Garments & Apparels",
      "mcap_bucket": "SMALL",
      "index_name": "NTYSLCP250",
      "entry_price": 449.00,
      "quantity": 13,
      "invested_amt": 5837.00,
      "ema_alloc_pct": 0.25,
      "high_since_entry": 449.00,
      "current_sl": 359.20,
      "last_trail_level": 449.00,
      "status": "ACTIVE",
      "entry_time": "2026-05-12T07:25:27Z",
      "days_held": 0,
      "strategy_id": "fb7831f5-...",
      "live_ltp": 451.30
    }
  ],
  "sector_allocation": [
    { "label": "Specialty Chemicals", "invested": 3278.10, "percent": 7.1, "cap_percent": 25.0, "over_cap": false }
  ],
  "mcap_allocation": [
    { "label": "SMALL", "invested": 46127.60, "percent": 100.0, "cap_percent": 50.0, "over_cap": true }
  ],
  "index_allocation": [
    { "label": "NTYSLCP250", "invested": 46127.60, "percent": 100.0, "cap_percent": 0, "over_cap": false }
  ],
  "ema_weights": {
    "NIFTY50": 0.25,
    "NIFTYNXT50": 0.25,
    "NTYSLCP250": 0.25
  },
  "today_signals": [
    {
      "symbol": "POLYCAB",
      "isin": "INE455K01017",
      "industry": "Cables - Electricals",
      "mcap_bucket": "LARGE",
      "index_name": "NIFTY50",
      "latest_price": 8194.50,
      "ath_close": 8610.50,
      "market_cap": 123456.78,
      "pe": 35.6,
      "fscore": 7.0,
      "taken_state": "eligible",
      "live_ltp": 8205.10
    }
  ],
  "cooldowns": [
    { "symbol": "KINGFA", "eligible_from": "2026-05-15T00:00:00Z", "reason": "user_override" }
  ],
  "orders": [
    {
      "id": 15,
      "signal_id": "ef5dff69-...",
      "strategy_id": "fb7831f5-...",
      "symbol": "ARVIND",
      "exchange": "NSE",
      "order_type": "LIMIT_BUY",
      "order_side": "BUY",
      "qty": 13,
      "filled_qty": 13,
      "limit_price": 449.00,
      "trigger_price": 0,
      "avg_fill_price": 449.00,
      "broker_order_id": "NZWKE0003E<5",
      "broker_status": "Executed",
      "status": "FILLED",
      "last_error": "",
      "retry_count": 0,
      "latest_event": "WSS fill confirmed",
      "created_at": "2026-05-12T07:25:16Z",
      "updated_at": "2026-05-12T07:25:27Z"
    }
  ],
  "totals": {
    "invested": 46127.60,
    "open_positions": 8,
    "eligible_today": 9,
    "in_cooldown": 0
  },
  "generated_at": "2026-05-12T08:30:00Z"
}
```

**Key fields for the dashboard:**

| Field | Use case |
|---|---|
| `positions[].current_sl` | Show the trailing SL trigger on each position card. |
| `positions[].live_ltp` | Live LTP from Indira market feed (0 if feed missing). Drives unrealised PnL. |
| `today_signals[].taken_state` | `"eligible"` (can enter), `"taken"` (already held), `"cooldown"` (blocked). |
| `sector_allocation[].over_cap` | Boolean — render red if the bucket is over its 25% (sector) / 50% (MCap) cap. |
| `ema_weights` | Per-index EMA allocation %. Drives the "EMA Allocation" panel. |
| `orders[].status` | `PENDING` / `PLACED` / `FILLED` / `REJECTED` / `CANCELLED` / `SL_PLACED`. |

---

# Live order endpoints (real broker)

### `GET /api/v1/live-orders?user_id=X`
List all live orders for a user.

### `GET /api/v1/live-orders/closed-orders?user_id=X`
Closed (filled/cancelled/rejected) live orders.

### `GET /api/v1/live-orders/indira-positions?user_id=X`
Real broker positions fetched from Indira's portfolio-services API. Live qty + avg price + LTP + day's PnL.

### `POST /api/v1/live-orders/force-exit-all`
Emergency MARKET SELL of all live positions for the user. Use with care.
Body: `{"user_id":"ND03920"}`

### `POST /api/v1/live-orders/force-exit-strategy`
Force-exit only positions belonging to one strategy.
Body: `{"user_id":"ND03920","strategy_id":"fb7831f5-..."}`

### `POST /api/v1/live-orders/subscribe-broker-ws`
Establishes the shared WebSocket subscription to Indira's order-status feed for this user. The gateway holds one connection per user — frontend doesn't talk to Indira's WS directly. Idempotent.

### `GET /api/v1/live-orders/price-watches?user_id=X`
List active price-watch entries (pre-trade alerts).

### `POST /api/v1/live-orders/cancel-price-watch`
Cancel a specific price watch. Body: `{"user_id":"X","watch_id":"..."}`

---

# Paper trading endpoints

Same shape as live-order endpoints, but operate on simulated paper trades. No broker calls, no real money.

| Endpoint | Method | Description |
|---|---|---|
| `/api/v1/paper-trades/positions` | GET | Paper positions for `user_id`. |
| `/api/v1/paper-trades/closed-orders` | GET | Paper history. |
| `/api/v1/paper-trades/force-exit-all` | POST | Close all paper positions. |
| `/api/v1/paper-trades/force-exit-strategy` | POST | Close paper positions for one strategy. |
| `/api/v1/paper-trades/ws-info` | GET | Returns the WS endpoint to subscribe to for live paper-trade updates. |

---

# Dashboard

### `GET /api/v1/dashboard-stats?user_id=X&mode=live`
Aggregate stats panel — open positions, today's PnL, total invested, win rate, etc.

`mode` is `live` or `paper`.

---

# WebSockets (real-time push)

The gateway exposes three WS endpoints. All require the same auth headers as REST endpoints, except passed via query params (browsers can't set headers on `WebSocket` upgrade).

### `wss://manthan.stockk.trade/ws/matches?user_id=X`
News-strategy match feed for a single user. Receives a JSON message per news match (with stocks ranked by impact).

### `wss://manthan.stockk.trade/ws/matches/all`
All users' news matches (admin/observability view).

### `wss://manthan.stockk.trade/ws/live-orders?user_id=X&token=<jwt>&app_id=...&source=WEB`
Real-time order status updates. On every broker WS event (PENDING → OPEN → EXECUTED / REJECTED / CANCELLED) the gateway pushes the updated order row to the connected clients. Also pushes a fresh positions snapshot on fill so the frontend doesn't need to poll.

Message shape:
```json
{
  "type": "order_update",
  "order": { /* same shape as orders[] in /manthan/overview */ }
}
```
or:
```json
{
  "type": "positions_snapshot",
  "positions": [ /* same shape as positions[] */ ]
}
```
or:
```json
{
  "type": "token_expired"
}
```
The last one signals the JWT has expired — the frontend should prompt re-login and call `/api/v1/auth/credentials` with the fresh token.

### `wss://manthan.stockk.trade/ws/paper-trades?user_id=X&token=<jwt>...`
Same as `/ws/live-orders` but for paper-trade fills (simulated).

---

# Sample integration flow (frontend bootstrap)

```
1. User clicks "Login" → opens Indira SSO popup → user authenticates
   → SSO returns { clientId, appId, authCode, jwt }

2. Frontend POSTs to /api/v1/auth/credentials with the jwt in body so the
   backend has it for offline flows (rebalancer cron, replayer, etc.).

3. Frontend calls /api/v1/users/{user_id}/strategies to list user's strategies.
   - If empty: show "Create Strategy" CTA.
   - If exists: load /api/v1/manthan/overview?user_id=X&strategy_id=Y for the dashboard.

4. Frontend opens wss://manthan.stockk.trade/ws/live-orders?user_id=X&token=...
   for real-time order status. Listen for token_expired and re-prompt SSO.

5. Frontend polls /api/v1/manthan/overview every 5-10s for the dashboard
   (positions, today's signals, allocation breakdown) — OR refreshes only
   when /ws/live-orders pushes an order_update event.

6. On JWT expiry (any 401 with AU004 body, or token_expired WS message),
   prompt SSO re-login → POST /api/v1/auth/credentials → resume.
```

---

# Common error codes

| HTTP | Body shape | Meaning |
|---|---|---|
| `400` | `{"error":"..."}` | Bad request — malformed body, missing required field, validation failure. |
| `401` | `{"error":"Missing authentication headers..."}` | Missing `Authorization`/`userId`/`appId`/`source` headers, OR JWT is expired (Indira returned AU004). |
| `403` | `{"error":"User ID mismatch..."}` | IDOR protection — body's `user_id` ≠ header `userId`. |
| `404` | depends | Resource not found (e.g. unknown strategy_id). |
| `500` | `{"error":"Failed to ..."}` | Backend error — gRPC call to user-config / rules-engine / trade-execution failed. Retry with backoff. |
| `503` | from `/readyz` | One or more dependencies down (DB/Redis/Kafka/gRPC). Backend is degraded. |

---

# Known constraints (production)

1. **JWT expiry is 24h.** The frontend MUST re-POST to `/auth/credentials` on every fresh login or token refresh. Backend-side flows (replayer, rebalancer cron) read the stored token from DB.
2. **Manthan strategies enter at market 9:15-15:30 IST only.** Outside market hours, signals are queued but not placed.
3. **One Manthan strategy per user is recommended** — multiple Manthan strategies on the same user share the broker's position book and can fight each other for capital.
4. **`force-exit-all` is irreversible** — it places MARKET SELL on every open position. Add a confirmation dialog.
5. **Top-up + SL behaviour**: when a Manthan top-up fills, the SL on the broker is extended to cover the combined qty (fix shipped 2026-05-12). The trailing SL trigger never lowers — it's ratcheted to the higher of (existing trigger, top-up × 0.80).
