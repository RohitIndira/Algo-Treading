# Trade Execution Database - ER Diagram V2 (Revised)

> **Purpose:** Replace the current 40+ column `orders` god-table with a normalized, event-sourced architecture that captures every order event from signal to execution.
>
> **Broker:** Indira Securities via Codifi REST APIs + WebSocket
>
> **Database:** PostgreSQL (`trading_execution`)
>
> **Revision Notes (from review):**
> - Positions use `position_fills` as source of truth (no `entry_order_id` / `exit_order_id`)
> - Idempotency: `UNIQUE(broker_order_id, exchange_trade_no)` on fills, `dedup_key` on status history
> - Partitioning: `order_status_history`, `fills`, `broker_requests`, `signal_metrics` partitioned monthly
> - Added `signal_metrics` table for end-to-end latency tracking across all services
> - GORM ORM used for all queries (no raw SQL)

---

## Current State vs Proposed

| Current | Problem | Proposed |
|---------|---------|----------|
| 1 `orders` table (40+ cols) | God table, impossible to maintain | 13 focused tables |
| `filled_qty` / `filled_price` overwritten | Partial fill history lost | `fills` table - one row per fill |
| Only latest `status` stored | Can't answer "why cancelled?" | `order_status_history` - append-only ledger |
| `PENDING` never persisted to DB | Crash = order stuck at RECEIVED | Every transition persisted |
| `bearer_token` on every order row | 180 rows x same token | `broker_accounts` table - one row per user |
| `is_paper_trade` + `paper_pnl` + `live_pnl` | Conditional logic everywhere | `positions` table with `trading_mode` |
| `CANCELLED` = 5 different meanings | Can't filter by actual reason | `order_status_history.source` + `positions.exit_reason` |
| `indira_response` overwritten on retry | Lost retry history | `broker_requests` - one row per API call |
| OCO via 3 columns on orders | No group-level status | `order_groups` + `order_group_legs` |
| `execution_events` is unstructured JSONB | Not queryable for analytics | Structured `fills` + `order_status_history` |

---

## Complete ER Diagram

```
================================================================================
                         DATABASE: trading_execution
================================================================================


 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                                                                             │
 │                          REFERENCE / MASTER DATA                            │
 │                                                                             │
 └─────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────────────┐       ┌────────────────────────────────────┐
 │         instruments           │       │         broker_accounts            │
 ├───────────────────────────────┤       ├────────────────────────────────────┤
 │ PK  instrument_id    SERIAL   │       │ PK  account_id       SERIAL       │
 │ UQ  stock_code       BIGINT   │       │     user_id          VARCHAR(50)  │
 │     symbol           VARCHAR  │       │     broker_name      VARCHAR(20)  │
 │     exchange         VARCHAR  │       │     broker_user_id   VARCHAR(100) │
 │     isin             VARCHAR  │       │     app_id           VARCHAR(100) │
 │     company_name     VARCHAR  │       │     source           VARCHAR(20)  │
 │     instrument_type  VARCHAR  │       │     bearer_token     TEXT (enc)   │
 │       (STK/OPT/FUT/IDX)      │       │     is_active        BOOLEAN      │
 │     exchange_token   VARCHAR  │       │     token_updated_at TIMESTAMPTZ  │
 │       (excToken for Codifi)   │       │     created_at       TIMESTAMPTZ  │
 │     lot_size         INT      │       │     updated_at       TIMESTAMPTZ  │
 │     tick_size        DECIMAL  │       │                                    │
 │     series           VARCHAR  │       │ UQ  (user_id, broker_name)        │
 │       (EQ/BE etc.)            │       └───────────────┬────────────────────┘
 │     codifi_symbol    VARCHAR  │                       │
 │       (STK_TCS_EQ_NSE_11536) │                       │
 │     is_active        BOOLEAN  │                       │
 │     created_at       TIMESTAMP│                       │
 │     updated_at       TIMESTAMP│                       │
 └───────────────┬───────────────┘                       │
                 │                                       │
                 │                                       │
 ┌───────────────┴───────────────────────────────────────┴────────────────────┐
 │                                                                             │
 │                             CORE ORDER LAYER                                │
 │                                                                             │
 └─────────────────────────────────────────────────────────────────────────────┘


 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                              orders  (~25 columns)                          │
 ├─────────────────────────────────────────────────────────────────────────────┤
 │ PK  order_id            UUID           DEFAULT gen_random_uuid()            │
 │ FK  instrument_id       INT            → instruments                        │
 │ FK  account_id          INT            → broker_accounts (nullable)         │
 │                                                                             │
 │ ── Identity ──────────────────────────────────────────────────────────────── │
 │     user_id             VARCHAR(50)    NOT NULL                             │
 │     strategy_id         VARCHAR(50)    NOT NULL                             │
 │     strategy_name       VARCHAR(255)                                        │
 │     event_id            UUID           NOT NULL (news event that triggered)  │
 │ UQ  signal_id           UUID           nullable (Kafka dedup)               │
 │                                                                             │
 │ ── Order Specification ───────────────────────────────────────────────────── │
 │     order_type          VARCHAR(10)    MARKET / LIMIT / STOP_LOSS / SL-M    │
 │     order_side          VARCHAR(10)    BUY / SELL                           │
 │     quantity            INT            NOT NULL                             │
 │     price               DECIMAL(15,2)  nullable for MARKET                  │
 │     validity            VARCHAR(10)    DAY / IOC                            │
 │     product_type        VARCHAR(20)    INTRADAY / DELIVERY / CASH / BRACKET │
 │     trading_mode        VARCHAR(10)    PAPER / LIVE                         │
 │                                                                             │
 │ ── Current State (denormalized for fast reads) ───────────────────────────── │
 │     status              VARCHAR(20)    latest status                        │
 │     filled_qty          INT            DEFAULT 0  (sum from fills)          │
 │     avg_fill_price      DECIMAL(15,2)  (computed from fills)                │
 │                                                                             │
 │ ── Risk ──────────────────────────────────────────────────────────────────── │
 │     risk_approved       BOOLEAN        DEFAULT false                        │
 │     risk_score          DECIMAL(5,2)                                        │
 │                                                                             │
 │ ── Timestamps ────────────────────────────────────────────────────────────── │
 │     created_at          TIMESTAMPTZ    DEFAULT NOW()                        │
 │     updated_at          TIMESTAMPTZ    DEFAULT NOW() (trigger)              │
 │     submitted_at        TIMESTAMPTZ    nullable                             │
 │     executed_at         TIMESTAMPTZ    nullable                             │
 │                                                                             │
 │ ── Error ─────────────────────────────────────────────────────────────────── │
 │     error_message       TEXT                                                │
 │     retry_count         INT            DEFAULT 0                            │
 │                                                                             │
 │ CONSTRAINTS                                                                 │
 │   CHECK (quantity > 0)                                                      │
 │   CHECK (price IS NULL OR price > 0)                                        │
 │                                                                             │
 │ INDEXES                                                                     │
 │   idx_orders_user_id        (user_id, created_at DESC)                      │
 │   idx_orders_status         (status, created_at DESC)                       │
 │   idx_orders_strategy       (strategy_id, status)                           │
 │   idx_orders_event          (event_id)                                      │
 │   idx_orders_signal_unique  UNIQUE (signal_id) WHERE signal_id IS NOT NULL  │
 │   idx_orders_instrument     (instrument_id, created_at DESC)                │
 └───┬──────────┬──────────┬──────────┬──────────┬──────────┬─────────────────┘
     │          │          │          │          │          │
     │          │          │          │          │          │
     ▼ 1:N      ▼ 1:N      ▼ 1:1      ▼ 1:N      ▼ 1:N      ▼ N:M
     │          │          │          │          │          │
     │          │          │          │          │          │


 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                                                                               │
 │                          EVENT SOURCING LAYER                                  │
 │         (Append-only tables - never update, only insert)                       │
 │                                                                               │
 └───────────────────────────────────────────────────────────────────────────────┘


 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                  order_status_history  (APPEND-ONLY LEDGER)                  │
 ├──────────────────────────────────────────────────────────────────────────────┤
 │ PK  id                  BIGSERIAL                                            │
 │ FK  order_id            UUID          → orders ON DELETE CASCADE              │
 │                                                                              │
 │     from_status         VARCHAR(20)   NULL on first entry (RECEIVED)          │
 │     to_status           VARCHAR(20)   NOT NULL                               │
 │     source              VARCHAR(20)   NOT NULL                               │
 │                                       SYSTEM     = internal logic             │
 │                                       BROKER     = Codifi WS update           │
 │                                       USER       = manual cancel/modify       │
 │                                       SCHEDULER  = price monitor / square-off │
 │                                       STRATEGY   = strategy deactivated       │
 │     reason              TEXT          human-readable explanation              │
 │     broker_raw_data     JSONB         full WS payload (when source=BROKER)    │
 │     actor               VARCHAR(50)   who triggered (user_id / system name)   │
 │     created_at          TIMESTAMPTZ   DEFAULT NOW()                          │
 │                                                                              │
 │ INDEXES                                                                      │
 │   idx_osh_order_time    (order_id, created_at)                               │
 │   idx_osh_status_time   (to_status, created_at DESC)                         │
 │   idx_osh_source        (source, created_at DESC)                            │
 └──────────────────────────────────────────────────────────────────────────────┘

  EXAMPLES:
  ┌──────────────────────────────────────────────────────────────────────────┐
  │ order_id  │ from    │ to        │ source    │ reason                    │
  ├───────────┼─────────┼───────────┼───────────┼───────────────────────────┤
  │ abc-123   │ NULL    │ RECEIVED  │ SYSTEM    │ Signal consumed from Kafka│
  │ abc-123   │ RECEIVED│ PENDING   │ SYSTEM    │ Risk approved, processing │
  │ abc-123   │ PENDING │ SUBMITTED │ SYSTEM    │ Placed via Codifi API     │
  │ abc-123   │ SUBMITTED│ EXECUTED │ BROKER    │ {full WS JSON payload}    │
  ├───────────┼─────────┼───────────┼───────────┼───────────────────────────┤
  │ def-456   │ PENDING │ CANCELLED │ SCHEDULER │ LTP 105.50 > max 103.00  │
  │ ghi-789   │ FILLED  │ CANCELLED │ STRATEGY  │ Strategy deactivated      │
  │ jkl-012   │ SUBMITTED│CANCELLED │ USER      │ Manual cancel via UI      │
  │ mno-345   │ SUBMITTED│CANCELLED │ BROKER    │ Reason: "Insufficient Qty"│
  └──────────────────────────────────────────────────────────────────────────┘


 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                          fills  (ONE ROW PER PARTIAL FILL)                   │
 ├──────────────────────────────────────────────────────────────────────────────┤
 │ PK  fill_id             UUID          DEFAULT gen_random_uuid()              │
 │ FK  order_id            UUID          → orders ON DELETE CASCADE              │
 │                                                                              │
 │     fill_qty            INT           NOT NULL                               │
 │     fill_price          DECIMAL(15,2) NOT NULL  (after DecimalLocator adj)    │
 │     exchange_trade_no   VARCHAR(50)   exchange trade ID (for dedup)           │
 │     exchange_order_no   VARCHAR(50)   exchange order number (OrderNumber)     │
 │     broker_order_id     VARCHAR(50)   Codifi order ID (UniqueCode)           │
 │     filled_at           TIMESTAMPTZ   NOT NULL                               │
 │                                                                              │
 │ CONSTRAINTS                                                                  │
 │   UQ (exchange_trade_no) WHERE exchange_trade_no IS NOT NULL                 │
 │   CHECK (fill_qty > 0)                                                       │
 │   CHECK (fill_price > 0)                                                     │
 │                                                                              │
 │ INDEXES                                                                      │
 │   idx_fills_order       (order_id, filled_at)                                │
 │   idx_fills_broker_ord  (broker_order_id)                                    │
 └──────────────────────────────────────────────────────────────────────────────┘

  QUERY: avg fill price for an order
    SELECT SUM(fill_qty * fill_price) / SUM(fill_qty) FROM fills WHERE order_id = ?

  QUERY: total filled
    SELECT SUM(fill_qty) FROM fills WHERE order_id = ?


 ┌──────────────────────────────────────────────────────────────────────────────┐
 │               broker_requests  (EVERY CODIFI API CALL LOGGED)               │
 ├──────────────────────────────────────────────────────────────────────────────┤
 │ PK  id                  BIGSERIAL                                            │
 │ FK  order_id            UUID          → orders ON DELETE CASCADE              │
 │                                                                              │
 │     action              VARCHAR(10)   PLACE / MODIFY / CANCEL                │
 │     broker_name         VARCHAR(20)   INDIRA (Codifi API)                    │
 │     attempt             INT           retry number (0, 1, 2...)              │
 │                                                                              │
 │ ── Codifi Request ──────────────────────────────────────────────────────────│
 │     request_url         VARCHAR(255)  /order-services/api/order/v1/place-ord │
 │     request_body        JSONB         {symbol, excToken, ordAction, ...}     │
 │                                                                              │
 │ ── Codifi Response ─────────────────────────────────────────────────────────│
 │     http_status         INT           200, 401, 500, etc.                    │
 │     response_body       JSONB         {orderId, infoID, infoMsg, ...}       │
 │     broker_order_id     VARCHAR(50)   orderId from response                  │
 │                                                                              │
 │ ── Diagnostics ─────────────────────────────────────────────────────────────│
 │     latency_ms          INT           round-trip time                        │
 │     error_type          VARCHAR(20)   TIMEOUT / AUTH / BUSINESS / NETWORK    │
 │     error_message       TEXT          if failed                              │
 │     created_at          TIMESTAMPTZ   DEFAULT NOW()                          │
 │                                                                              │
 │ INDEXES                                                                      │
 │   idx_br_order          (order_id, created_at)                               │
 │   idx_br_error          (error_type, created_at DESC)                        │
 │   idx_br_broker_ord     (broker_order_id) WHERE broker_order_id IS NOT NULL  │
 └──────────────────────────────────────────────────────────────────────────────┘

  EXAMPLES:
  ┌──────────────────────────────────────────────────────────────────────────┐
  │ attempt │ action │ http │ broker_order_id │ error_type │ latency_ms    │
  ├─────────┼────────┼──────┼─────────────────┼────────────┼───────────────┤
  │ 0       │ PLACE  │ 0    │ NULL            │ TIMEOUT    │ 15000         │
  │ 1       │ PLACE  │ 401  │ NULL            │ AUTH       │ 120           │
  │ 2       │ PLACE  │ 200  │ NZVND00001J2    │ NULL       │ 350           │
  └──────────────────────────────────────────────────────────────────────────┘



 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                                                                               │
 │                          STOP LOSS / OCO LAYER                                 │
 │                                                                               │
 └───────────────────────────────────────────────────────────────────────────────┘


 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                     stop_loss_config  (1:1 with order)                       │
 ├──────────────────────────────────────────────────────────────────────────────┤
 │ PK  id                  SERIAL                                               │
 │ FK  order_id            UUID          → orders (UNIQUE, 1:1)                 │
 │                                                                              │
 │     sl_type             VARCHAR(10)   FIXED / TRAILING                       │
 │     stop_loss_price     DECIMAL(15,2) calculated SL price                     │
 │     take_profit_price   DECIMAL(15,2)                                        │
 │     stop_loss_pct       DECIMAL(10,4) original SL % from strategy            │
 │     take_profit_pct     DECIMAL(10,4) original TP % from strategy            │
 │     trailing_pct        DECIMAL(10,4) trailing SL %                          │
 │     max_monitor_price   DECIMAL(15,2) ceiling for price monitor              │
 │     highest_price       DECIMAL(15,2) tracked by price monitor (trailing)    │
 │     is_active           BOOLEAN       DEFAULT true                           │
 │     triggered_at        TIMESTAMPTZ   NULL until SL/TP triggers              │
 │     trigger_reason      VARCHAR(20)   SL_HIT / TP_HIT / PRICE_EXCEEDED      │
 │     updated_at          TIMESTAMPTZ                                          │
 │                                                                              │
 │ INDEXES                                                                      │
 │   idx_slc_active        (is_active, sl_type) WHERE is_active = true          │
 └──────────────────────────────────────────────────────────────────────────────┘


 ┌───────────────────────────┐       ┌──────────────────────────────────────┐
 │      order_groups         │       │        order_group_legs              │
 ├───────────────────────────┤       ├──────────────────────────────────────┤
 │ PK  group_id     UUID     │  1:N  │ PK  id              SERIAL          │
 │     group_type   VARCHAR  │──────▶│ FK  group_id        UUID → groups   │
 │       OCO / BRACKET /     │       │ FK  order_id        UUID → orders   │
 │       SQUARE_OFF          │       │     leg_role        VARCHAR(20)     │
 │     user_id      VARCHAR  │       │       ENTRY / SL_LEG / TP_LEG      │
 │     status       VARCHAR  │       │     sequence        INT             │
 │       ACTIVE / COMPLETED /│       │     created_at      TIMESTAMPTZ    │
 │       CANCELLED           │       │                                      │
 │     created_at TIMESTAMPTZ│       │ UQ  (group_id, order_id)            │
 │     updated_at TIMESTAMPTZ│       │                                      │
 │                           │       │ INDEXES                              │
 │ INDEXES                   │       │   idx_ogl_order  (order_id)          │
 │   idx_og_status (status)  │       └──────────────────────────────────────┘
 │   idx_og_user   (user_id) │
 └───────────────────────────┘



 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                                                                               │
 │                          POSITION TRACKING LAYER                               │
 │                                                                               │
 └───────────────────────────────────────────────────────────────────────────────┘


 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                              positions                                       │
 ├──────────────────────────────────────────────────────────────────────────────┤
 │ PK  position_id         UUID          DEFAULT gen_random_uuid()              │
 │ FK  instrument_id       INT           → instruments                          │
 │                                                                              │
 │     user_id             VARCHAR(50)   NOT NULL                               │
 │     strategy_id         VARCHAR(50)                                          │
 │     strategy_name       VARCHAR(255)                                         │
 │     trading_mode        VARCHAR(10)   PAPER / LIVE                           │
 │     side                VARCHAR(10)   LONG / SHORT                           │
 │                                                                              │
 │ ── Entry ─────────────────────────────────────────────────────────────────── │
 │     entry_order_id      UUID          → orders (the order that opened this)  │
 │     open_qty            INT           remaining open quantity                │
 │     avg_entry_price     DECIMAL(15,2) weighted avg from entry fills          │
 │                                                                              │
 │ ── Exit ──────────────────────────────────────────────────────────────────── │
 │     exit_order_id       UUID          → orders (nullable, set on close)      │
 │     avg_exit_price      DECIMAL(15,2) weighted avg from exit fills           │
 │     exit_reason         VARCHAR(20)   nullable                               │
 │                           SL_HIT / TP_HIT / MANUAL / SQUARE_OFF /           │
 │                           STRATEGY_DELETED / PRICE_EXCEEDED /                │
 │                           BROKER_CANCELLED                                   │
 │                                                                              │
 │ ── P&L ───────────────────────────────────────────────────────────────────── │
 │     realized_pnl        DECIMAL(15,2) final P&L when closed                 │
 │     total_charges       DECIMAL(10,2) sum of all fill charges               │
 │     net_pnl             DECIMAL(15,2) realized_pnl - total_charges          │
 │                                                                              │
 │ ── Status ────────────────────────────────────────────────────────────────── │
 │     status              VARCHAR(10)   OPEN / CLOSED                         │
 │     opened_at           TIMESTAMPTZ                                         │
 │     closed_at           TIMESTAMPTZ   nullable                              │
 │     updated_at          TIMESTAMPTZ                                         │
 │                                                                              │
 │ INDEXES                                                                      │
 │   idx_pos_user_status   (user_id, status, trading_mode)                      │
 │   idx_pos_strategy      (strategy_id, status)                                │
 │   idx_pos_instrument    (instrument_id, status)                              │
 │   idx_pos_open          (status, trading_mode) WHERE status = 'OPEN'         │
 │   idx_pos_closed_date   (user_id, closed_at DESC) WHERE status = 'CLOSED'   │
 └──────────────┬───────────────────────────────────────────────────────────────┘
                │ 1:N
                ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │              position_fills  (links fills ↔ positions)                       │
 ├──────────────────────────────────────────────────────────────────────────────┤
 │ PK  id                  SERIAL                                               │
 │ FK  position_id         UUID          → positions                            │
 │ FK  fill_id             UUID          → fills                                │
 │     direction           VARCHAR(5)    ENTRY / EXIT                           │
 │     created_at          TIMESTAMPTZ                                          │
 │                                                                              │
 │ UQ  (position_id, fill_id)                                                   │
 └──────────────────────────────────────────────────────────────────────────────┘



 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                                                                               │
 │                          ANALYTICS / DASHBOARD LAYER                           │
 │                                                                               │
 └───────────────────────────────────────────────────────────────────────────────┘


 ┌──────────────────────────────────────────────────────────────────────────────┐
 │                         daily_pnl_summary                                    │
 ├──────────────────────────────────────────────────────────────────────────────┤
 │ PK  id                  SERIAL                                               │
 │     user_id             VARCHAR(50)   NOT NULL                               │
 │     strategy_id         VARCHAR(50)   nullable (NULL = all strategies)       │
 │     trading_mode        VARCHAR(10)   PAPER / LIVE                           │
 │     trade_date          DATE          NOT NULL                               │
 │                                                                              │
 │ ── Stats ─────────────────────────────────────────────────────────────────── │
 │     total_trades        INT                                                  │
 │     winning_trades      INT                                                  │
 │     losing_trades       INT                                                  │
 │     total_invested      DECIMAL(15,2) sum of entry_price * qty               │
 │     gross_pnl           DECIMAL(15,2)                                        │
 │     total_charges       DECIMAL(10,2)                                        │
 │     net_pnl             DECIMAL(15,2) gross_pnl - charges                    │
 │     max_drawdown        DECIMAL(15,2) worst peak-to-trough intraday          │
 │     win_rate            DECIMAL(5,2)  (winners / total) * 100                │
 │                                                                              │
 │ UQ  (user_id, strategy_id, trade_date, trading_mode)                         │
 │                                                                              │
 │ INDEXES                                                                      │
 │   idx_dpnl_user_date    (user_id, trade_date DESC)                           │
 │   idx_dpnl_strategy     (strategy_id, trade_date DESC)                       │
 └──────────────────────────────────────────────────────────────────────────────┘
```

---

## Relationship Diagram (Compact)

```
                    instruments ◄──────── orders ────────► broker_accounts
                         │                  │
                         │          ┌───────┼───────────────────┐
                         │          │       │                   │
                         │          ▼       ▼                   ▼
                         │    order_status  fills         broker_requests
                         │    _history                    (Codifi API log)
                         │          │       │
                         │          │       │
                         │          │   position_fills
                         │          │       │
                         ▼          │       ▼
                    positions ◄─────┘   order_groups
                         │              │
                         ▼              ▼
                    daily_pnl      order_group_legs
                    _summary

   orders ←──── stop_loss_config  (1:1)
```

---

## Table Relationships

| Parent | Child | Relationship | FK Column | On Delete |
|--------|-------|-------------|-----------|-----------|
| instruments | orders | 1:N | instrument_id | RESTRICT |
| broker_accounts | orders | 1:N | account_id | SET NULL |
| orders | order_status_history | 1:N | order_id | CASCADE |
| orders | fills | 1:N | order_id | CASCADE |
| orders | broker_requests | 1:N | order_id | CASCADE |
| orders | stop_loss_config | 1:1 | order_id (UQ) | CASCADE |
| order_groups | order_group_legs | 1:N | group_id | CASCADE |
| orders | order_group_legs | 1:N | order_id | CASCADE |
| instruments | positions | 1:N | instrument_id | RESTRICT |
| orders | positions | 1:1 | entry_order_id | RESTRICT |
| positions | position_fills | 1:N | position_id | CASCADE |
| fills | position_fills | 1:N | fill_id | CASCADE |

---

## Codifi/Indira API Data Mapping

### Where Each Codifi Field is Stored

| Codifi API Field | Current Column | New Table.Column |
|---|---|---|
| `orderId` / `ordId` (Place Order response) | `orders.indira_order_id` | `broker_requests.broker_order_id` |
| `infoID` / `infoMsg` (business error) | `orders.indira_response` (overwritten) | `broker_requests.response_body` (JSONB) |
| Full Place Order request body | Not stored | `broker_requests.request_body` (JSONB) |
| `WSOrderStatus.UniqueCode` | `orders.indira_order_id` | `broker_requests.broker_order_id` |
| `WSOrderStatus.OrderNumber` | `orders.exchange_order_number` | `fills.exchange_order_no` |
| `WSOrderStatus.OrderStatus` | `orders.broker_status` | `order_status_history.to_status` + `.broker_raw_data` |
| `WSOrderStatus.TradedQTY` | `orders.filled_quantity` (overwritten!) | `fills.fill_qty` (individual row) |
| `WSOrderStatus.TradedPrice` | `orders.filled_price` (overwritten!) | `fills.fill_price` (individual row) |
| `WSOrderStatus.Reason` | `orders.rejection_reason` | `order_status_history.reason` |
| `WSOrderStatus.TriggerPrice` | `orders.stop_loss` | `stop_loss_config.stop_loss_price` |
| `WSOrderStatus.DecimalLocator` | Not stored (applied in-memory) | `fills.fill_price` (already adjusted) |
| Full WS JSON payload | `orders.broker_ws_data` (overwritten!) | `order_status_history.broker_raw_data` (per event) |
| `WSOrderStatus.OrderEntryTime` | Not stored | `order_status_history.broker_raw_data` |
| `WSOrderStatus.Product` | Not stored | `broker_requests.request_body` (on place) |
| `bearer_token` (from frontend) | `orders.bearer_token` (per row!) | `broker_accounts.bearer_token` (per user) |
| `appId` | `orders.app_id` (per row!) | `broker_accounts.app_id` (per user) |
| `source` | `orders.source` (per row!) | `broker_accounts.source` (per user) |
| Codifi symbol format | Not stored | `instruments.codifi_symbol` |
| Exchange token (excToken) | Not stored | `instruments.exchange_token` |

### Codifi WebSocket Status → order_status_history Mapping

| Codifi WS `OrderStatus` | `to_status` | `source` |
|---|---|---|
| `PENDING` | `PENDING` | `BROKER` |
| `EXECUTED` | `FILLED` | `BROKER` |
| `TRADED` | `FILLED` | `BROKER` |
| `PARTIALLY TRADED` | `PARTIALLY_FILLED` | `BROKER` |
| `PARTIALLY EXECUTED` | `PARTIALLY_FILLED` | `BROKER` |
| `REJECTED` | `REJECTED` | `BROKER` |
| `A.REJECTED` | `REJECTED` | `BROKER` |
| `CANCELLED` | `CANCELLED` | `BROKER` |
| `EXPIRED` | `CANCELLED` | `BROKER` |

---

## Order Lifecycle — Complete Event Trail

```
Step  Status Change              Source     Stored In                    Codifi Interaction
────  ─────────────────────────  ────────  ───────────────────────────  ────────────────────────
 1    NULL → RECEIVED            SYSTEM    order_status_history         Kafka signal consumed
 2    RECEIVED → PENDING         SYSTEM    order_status_history         Risk check passed
 3    —                          SYSTEM    broker_requests (attempt=0)  POST /place-order →
                                                                        timeout (15s)
 4    —                          SYSTEM    broker_requests (attempt=1)  POST /place-order →
                                                                        401 (token stale)
 5    —                          SYSTEM    broker_accounts              Token refreshed from DB
 6    —                          SYSTEM    broker_requests (attempt=2)  POST /place-order →
                                                                        200 {orderId: "NZV..."}
 7    PENDING → SUBMITTED        SYSTEM    order_status_history         Order placed at broker
 8    SUBMITTED → PART_FILLED    BROKER    order_status_history +       WS: TradedQTY=50,
                                           fills (50 @ 100.25)          TradedPrice=100.25
 9    PART_FILLED → PART_FILLED  BROKER    order_status_history +       WS: TradedQTY=80 (delta=30),
                                           fills (30 @ 100.50)          TradedPrice=100.50
10    PART_FILLED → FILLED       BROKER    order_status_history +       WS: TradedQTY=100 (delta=20),
                                           fills (20 @ 100.75)          TradedPrice=100.75
11    —                          SYSTEM    positions (OPEN)             Position opened
12    —                          SYSTEM    position_fills (3 × ENTRY)   Link fills to position

── Later (SL triggered) ──
13    —                          SYSTEM    broker_requests (PLACE)      POST /place-order (sell)
14    —                          BROKER    fills (100 @ 98.00)          WS: EXECUTED sell
15    —                          SYSTEM    position_fills (1 × EXIT)   Link exit fill
16    —                          SYSTEM    positions (CLOSED)           Position closed, PnL = -237.50
```

---

## Cross-Database Context (Read-Only References)

The `trading_execution` database references entities from `trading_db` via IDs (not FK — separate databases):

```
  DATABASE: trading_db (user-config service)        DATABASE: trading_execution
  ┌─────────────────────────┐                       ┌─────────────────────────┐
  │ strategies              │   strategy_id          │ orders                  │
  │   strategy_id (PK) ─────│──── (VARCHAR, no FK) ──│   strategy_id           │
  │   user_id               │                       │   strategy_name (cached)│
  │   strategy_name         │                       └─────────────────────────┘
  │   active                │
  │   trading_mode          │                       ┌─────────────────────────┐
  │   conditions            │   strategy_id          │ positions               │
  │   trade_config          │──── (VARCHAR, no FK) ──│   strategy_id           │
  │   risk_limits           │                       │   strategy_name (cached)│
  └─────────────────────────┘                       └─────────────────────────┘

  ┌─────────────────────────┐                       ┌─────────────────────────┐
  │ trade_signals           │   signal_id / order_id │ orders                  │
  │   signal_id (PK) ───────│──── (UUID, no FK) ─────│   signal_id             │
  │   order_id (UQ)         │                       │   event_id              │
  │   event_id              │                       └─────────────────────────┘
  │   match_score           │
  │   impact_score          │
  └─────────────────────────┘

  Kafka Topics (event bus):
  ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────────┐
  │ trade-signals    │   │ user-config-events│   │ order-updates        │
  │ (rules → exec)   │   │ (config → exec)   │   │ (exec → frontend)    │
  └──────────────────┘   └──────────────────┘   └──────────────────────┘
```

---

## Migration Strategy (Current → New)

### Phase 1: Add New Tables (Non-Breaking)
Create `instruments`, `broker_accounts`, `order_status_history`, `fills`, `broker_requests`, `stop_loss_config`, `order_groups`, `order_group_legs`, `positions`, `position_fills`, `daily_pnl_summary` alongside the existing `orders` table.

### Phase 2: Dual-Write
Update Go code to write to both old and new tables simultaneously. Old `orders` table continues to work as-is. New tables start accumulating data.

### Phase 3: Backfill
Migrate existing `execution_events` JSONB data into structured `order_status_history` and `fills` tables.

### Phase 4: Read from New
Switch read queries to use new tables. Dashboard, positions, P&L all read from `positions` + `fills`.

### Phase 5: Drop Old Columns
Remove redundant columns from `orders` table (`indira_response`, `broker_ws_data`, `paper_pnl`, `live_pnl`, `bearer_token`, etc.).

---

## Quick Reference: Table Count & Purpose

| # | Table | Rows Growth | Purpose |
|---|-------|-------------|---------|
| 1 | `instruments` | Slow (master data) | Stock/instrument master with Codifi symbol format |
| 2 | `broker_accounts` | Slow (1 per user) | Encrypted Codifi auth (replaces per-order tokens) |
| 3 | `orders` | Medium (~orders/day) | Lean order record (~25 cols vs 40+) |
| 4 | `order_status_history` | Fast (N per order) | Every status change with reason + raw WS data |
| 5 | `fills` | Medium (1-3 per order) | Individual partial fills with exact prices |
| 6 | `broker_requests` | Medium (1-3 per order) | Every Codifi API call with request/response |
| 7 | `stop_loss_config` | Medium (1:1 orders) | SL/TP/trailing config separated from order |
| 8 | `order_groups` | Low | OCO/Bracket group-level tracking |
| 9 | `order_group_legs` | Low (2-3 per group) | Maps orders to group legs |
| 10 | `positions` | Medium | Aggregated position with entry/exit/PnL |
| 11 | `position_fills` | Medium | Links fills to positions (entry vs exit) |
| 12 | `daily_pnl_summary` | Low (1 per user/day) | Precomputed dashboard stats |

**Total: 12 tables** (vs current 3 tables with 1 god table)
