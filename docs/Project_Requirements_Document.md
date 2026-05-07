# StockAsk Algo Trading - Project Requirements Document (BRD)

**Version:** 1.0
**Date:** 2026-03-19
**Project:** AlgoNewsWithCodifi - News-Based Algorithmic Trading Platform
**Status:** Active Development

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [User Roles & Access](#2-user-roles--access)
3. [Module 1: Authentication & Session Management](#3-module-1-authentication--session-management)
4. [Module 2: Dashboard](#4-module-2-dashboard)
5. [Module 3: Strategy Management](#5-module-3-strategy-management)
6. [Module 4: Paper Trading](#6-module-4-paper-trading)
7. [Module 5: Live Trading](#7-module-5-live-trading)
8. [Module 6: Order Management](#8-module-6-order-management)
9. [Module 7: OCO Orders (One-Cancels-the-Other)](#9-module-7-oco-orders-one-cancels-the-other)
10. [Module 8: Risk Management](#10-module-8-risk-management)
11. [Module 9: Portfolio & Positions](#11-module-9-portfolio--positions)
12. [Module 10: Price Watch / Watchlist](#12-module-10-price-watch--watchlist)
13. [Module 11: Real-Time Market Data](#13-module-11-real-time-market-data)
14. [Module 12: Notifications & Alerts](#14-module-12-notifications--alerts)
15. [Module 13: Broker Integration (Indira Securities)](#15-module-13-broker-integration-indira-securities)
16. [Non-Functional Requirements](#16-non-functional-requirements)
17. [Glossary](#17-glossary)

---

## 1. Project Overview

### 1.1 What is this application?

A **news-based algorithmic trading platform** for the Indian stock market (NSE & BSE). Users create trading strategies with conditions based on news sentiment, impact scores, and market data. When real-time news matches a user's strategy conditions, the system **automatically places trades** on behalf of the user.

### 1.2 Core Value Proposition

| # | Feature | Description |
|---|---------|-------------|
| 1 | Automated Trading | User defines rules once; system auto-executes when conditions match |
| 2 | News-Driven | Real-time news sentiment & impact score trigger trades |
| 3 | Risk-Managed | Pre-trade risk checks, circuit breakers, daily loss limits |
| 4 | Paper Trading | Test strategies risk-free with virtual capital before going live |
| 5 | Live Trading | Real money execution via Indira Securities broker (Odin API) |
| 6 | Real-Time Data | Live market prices, P&L updates, order status via WebSocket |
| 7 | Multi-User | Supports 10,000+ concurrent users with isolated strategies |

### 1.3 Target Users

- Retail traders in the Indian stock market (NSE/BSE)
- Users who want to automate trading based on news events
- Beginners (paper trading) and experienced traders (live trading)

### 1.4 Supported Platforms

| Platform | Type |
|----------|------|
| Web | Primary (responsive browser-based UI) |
| iOS | Mobile app |
| Android | Mobile app |

### 1.5 Market Hours (Indian Market)

| Event | Time (IST) |
|-------|------------|
| Pre-open Session | 09:00 - 09:15 |
| Market Open | 09:15 |
| Market Close | 15:30 |
| Auto Square-Off Default | 15:15 |

---

## 2. User Roles & Access

### 2.1 End User / Trader

| Permission | Description |
|-----------|-------------|
| Create strategies | Define trading conditions, order config, risk limits |
| Manage own strategies | Edit, activate, deactivate, delete own strategies |
| View own orders | See order history, status, P&L |
| View own positions | See current holdings, unrealized P&L |
| View own risk metrics | See daily loss, trade count, portfolio exposure |
| Paper trading | Trade with virtual capital (no real money) |
| Live trading | Trade with real money via broker (after broker login) |
| Cannot access other users' data | Strict user isolation |

### 2.2 System Administrator

| Permission | Description |
|-----------|-------------|
| Manage all users | View and manage all user accounts |
| Monitor system health | View system metrics, logs, performance |
| Reset daily counters | Reset risk counters if needed |
| Audit trading activities | View all trading activity across users |

---

## 3. Module 1: Authentication & Session Management

### 3.1 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| AUTH-01 | Password Login | High | User must be able to login with User ID + Password |
| AUTH-02 | MPIN Login | Medium | User must be able to login with User ID + MPIN (mobile PIN) |
| AUTH-03 | Token-Based Login | Medium | User must be able to login via existing auth token |
| AUTH-04 | Third-Party Token | Low | User must be able to login via TP_TOKEN |
| AUTH-05 | OTP Verification | High | System must support OTP-based multi-factor authentication |
| AUTH-06 | TOTP Support | Medium | System must support time-based OTP (authenticator app) |
| AUTH-07 | Fingerprint Auth | Low | System must support biometric (fingerprint) login |
| AUTH-08 | Session Token | High | On successful login, system must generate a JWT session token with 24-hour expiry |
| AUTH-09 | Session Expiry | High | System must auto-logout user after token expires (24 hours) |
| AUTH-10 | Multiple Sessions | Medium | User must be able to have concurrent sessions from different devices |
| AUTH-11 | Logout | High | User must be able to logout, which terminates the session |
| AUTH-12 | Session Expiry Warning | Low | System should show warning before session expires |

### 3.2 UI Elements

| Element | Type | Required |
|---------|------|----------|
| User ID | Text input | Yes |
| Password | Password input | Yes |
| MPIN | Number input | Optional |
| Login Button | Button | Yes |
| OTP Input | Number input (6 digits) | Conditional (if MFA enabled) |
| Resend OTP | Link/Button | Conditional |
| Logout Button | Button (in navigation) | Yes |

### 3.3 Request Headers (sent with every API call after login)

| Header | Value | Description |
|--------|-------|-------------|
| Authorization | Bearer {token} | JWT token from login |
| userId | User's ID | User identification |
| appId | AlgoTradingApp | Application identifier |
| source | Web / iOS / Android | Platform source |

---

## 4. Module 2: Dashboard

### 4.1 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| DASH-01 | Trading Mode Toggle | High | User must be able to switch between Paper and Live mode. All displayed data must change accordingly. |
| DASH-02 | Total Orders Count | High | Display total number of orders placed in selected mode |
| DASH-03 | Filled Orders Count | High | Display number of successfully filled orders |
| DASH-04 | Pending Orders Count | High | Display number of orders awaiting execution |
| DASH-05 | Rejected Orders Count | High | Display number of rejected orders |
| DASH-06 | Cancelled Orders Count | High | Display number of cancelled orders |
| DASH-07 | Fill Rate % | Medium | Display: (Filled Orders / Total Orders) x 100 |
| DASH-08 | Rejection Rate % | Medium | Display: (Rejected Orders / Total Orders) x 100 |
| DASH-09 | Total Traded Value | High | Display sum of all trade values in INR (Rs.) |
| DASH-10 | Avg Order Value | Medium | Display average rupee value per order |
| DASH-11 | Avg Execution Time | Medium | Display average order execution time in milliseconds |
| DASH-12 | P95 Execution Time | Low | Display 95th percentile execution time in milliseconds |
| DASH-13 | Auto-Refresh | Medium | Dashboard data should update automatically when new trades are placed |

### 4.2 UI Elements

| Element | Type | Details |
|---------|------|---------|
| Paper / Live Toggle | Toggle switch | Switches data between paper and live trading |
| Mode Badge | Visual indicator | Shows "PAPER" or "LIVE" clearly |
| Stat Cards | Card widgets (11 cards) | Each stat displayed in its own card with label + value |

---

## 5. Module 3: Strategy Management

### 5.1 Strategy Definition

A **strategy** is a set of rules that defines:
- **WHEN** to trade (conditions based on news, sentiment, price, etc.)
- **HOW** to trade (order type, quantity, stop loss, etc.)
- **HOW MUCH RISK** to take (daily limits, position sizing, etc.)

### 5.2 Strategy Data Structure

```
Strategy {
  strategy_id       : Auto-generated UUID
  user_id           : Owner's user ID
  strategy_name     : User-defined name (required)
  description       : Optional text description
  active            : true/false (is strategy currently running?)
  trading_mode      : PAPER or LIVE
  conditions        : Strategy Conditions (see 5.4)
  trade_config      : Trade Configuration (see 5.5)
  risk_limits       : Risk Limits (see 5.6)
  version           : Auto-incremented (for conflict detection)
  created_at        : Timestamp
  updated_at        : Timestamp
}
```

### 5.3 Strategy List Screen Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| STRAT-01 | List All Strategies | High | Display all user strategies in a table with name, status, mode, dates |
| STRAT-02 | Create Strategy | High | Button to open strategy creation form |
| STRAT-03 | Edit Strategy | High | Edit button per row to modify any strategy |
| STRAT-04 | Delete Strategy | High | Delete button per row with confirmation popup |
| STRAT-05 | Activate Strategy | High | Activate button to start strategy matching |
| STRAT-06 | Deactivate Strategy | High | Deactivate button to stop strategy matching |
| STRAT-07 | Search by Name | Medium | Text search to filter strategies by name |
| STRAT-08 | Filter by Status | Medium | Filter dropdown: All / Active / Inactive |
| STRAT-09 | Pagination | Medium | Paginate when >10 strategies (configurable page size) |
| STRAT-10 | Sort | Low | Sort by name, created date, status |
| STRAT-11 | Empty State | Low | Show "No strategies found" with create button when list is empty |

### 5.4 Strategy Conditions (WHEN to trade)

These define **what news/market events** trigger the strategy.

| Req# | Condition | Field Type | Values / Range | Details |
|------|-----------|-----------|----------------|---------|
| COND-01 | Match All News | Toggle (on/off) | true / false | When ON: strategy triggers on ANY news event. Other conditions disabled. |
| COND-02 | Impact Score Range | Min-Max number inputs | 0 to 100 | News impact score filter. Higher = more impactful news. |
| COND-03 | Sentiment Filter | Multi-select checkboxes | Positive, Neutral, Negative | Select which news sentiments trigger the strategy |
| COND-04 | News Categories | Multi-select dropdown | Earnings, Mergers, Regulatory, Financial Results, Dividends, Acquisitions, etc. | Select which news categories trigger the strategy |
| COND-05 | Stock Codes | Multi-select with autocomplete | 5000+ Indian stock codes (NSE/BSE) | Target specific stocks. Empty = all stocks. |
| COND-06 | Price Range | Min-Max number inputs | Rs. 0 to Rs. 999999 | Filter by last traded price of stock |
| COND-07 | Market Cap Type | Multi-select checkboxes | Small Cap, Mid Cap, Large Cap | Filter by company size |
| COND-08 | Market Cap Range | Min-Max number inputs | In crores (Rs.) | Filter by exact market capitalization range |
| COND-09 | Volume Threshold | Number input | 0+ (shares/day) | Minimum daily trading volume required |
| COND-10 | % Change Range | Min-Max number inputs | -100% to +100% | Filter by daily price change percentage |
| COND-11 | Exchange | Multi-select checkboxes | NSE, BSE | Filter by stock exchange |

**Validation Rules:**
- Min values must be <= Max values (impact score, price, market cap, % change)
- At least one condition should be set (unless Match All News is ON)
- Stock codes must be valid NSE/BSE codes

### 5.5 Trade Configuration (HOW to trade)

These define **what type of order** the system places when conditions match.

| Req# | Field | Field Type | Values | Details |
|------|-------|-----------|--------|---------|
| TRADE-01 | Order Type | Radio buttons | Market, Limit, Stop Loss, Stop Loss Market | **Market**: execute at current price. **Limit**: execute at specified price or better. **Stop Loss**: trigger at stop price. **SL Market**: stop loss with market execution. |
| TRADE-02 | Product Type | Radio/Dropdown | Intraday (MIS), Delivery (CNC), Bracket Orders (BO), Cash | **Intraday**: auto squared-off same day. **Delivery**: held until manually sold. **Bracket**: entry with linked SL + target. |
| TRADE-03 | Order Side | Radio buttons | BUY, SELL | Direction of the trade |
| TRADE-04 | Quantity | Number input | 1+ (positive integer) | Number of shares per order. Must be > 0. No decimals. |
| TRADE-05 | Exchange | Dropdown | NSE, BSE | Which exchange to place the order on |
| TRADE-06 | Validity | Dropdown/Radio | DAY, IOC | **DAY**: valid until market close. **IOC**: fill immediately or cancel unfilled portion. |
| TRADE-07 | Limit Price | Number input | > 0 (decimal allowed) | Only visible/required when Order Type = Limit |
| TRADE-08 | Stop Loss % | Number input | 0.1% to 100% | Percentage below entry price to place stop loss |
| TRADE-09 | Stop Loss Type | Radio buttons | Fixed, Trailing | **Fixed**: stays at set price. **Trailing**: moves up with profit. |
| TRADE-10 | Trailing SL % | Number input | 0.1% to 100% | Only visible when Stop Loss Type = Trailing |
| TRADE-11 | Take Profit % | Number input | 0.1% to 1000% | Percentage above entry price to take profit |
| TRADE-12 | Bracket Order SL | Number input | > 0 | Only visible when Product Type = Bracket Orders |
| TRADE-13 | Bracket Order Target | Number input | > 0 | Only visible when Product Type = Bracket Orders |
| TRADE-14 | After Market Order (AMO) | Toggle (on/off) | true / false | Queue order for next market open |
| TRADE-15 | Disclosed Quantity | Number input | 0 to Quantity | Partial visibility of order size in market. 0 = fully disclosed. |
| TRADE-16 | Instrument | Dropdown | STK (Stock), OPT (Option), FUT (Future), IDX (Index) | Type of financial instrument |
| TRADE-17 | Lot Size | Number input | 1+ | For options/futures |

**Conditional Visibility Rules:**
- Limit Price field: visible only when Order Type = Limit
- Trailing SL %: visible only when Stop Loss Type = Trailing
- Bracket Order SL & Target: visible only when Product Type = Bracket Orders
- Disclosed Qty must be <= Quantity

### 5.6 Risk Limits (HOW MUCH RISK)

Per-strategy risk controls.

| Req# | Field | Field Type | Values | Details |
|------|-------|-----------|--------|---------|
| RISK-01 | Max Daily Trades | Number input | 1+ | Maximum number of trades this strategy can place per day |
| RISK-02 | Max Loss Per Day | Number input (Rs.) | > 0 | Strategy stops trading if cumulative daily loss reaches this limit |
| RISK-03 | Max Per-Trade Risk | Number input (Rs.) | > 0 | Maximum risk (potential loss) allowed per individual trade |
| RISK-04 | Max Portfolio Exposure % | Number input | 1% to 100% | Maximum percentage of total capital this strategy can use |
| RISK-05 | Position Sizing Strategy | Dropdown/Radio | Fixed, Percentage, Risk-Based | **Fixed**: same quantity every trade. **Percentage**: % of capital. **Risk-Based**: based on risk per trade. |
| RISK-06 | Enable Risk Checks | Toggle (on/off) | true / false | Enable/disable all risk validations for this strategy. Warning shown when disabled. |
| RISK-07 | Enable Auto Square-Off | Toggle (on/off) | true / false | Auto-close all positions at specified time |
| RISK-08 | Auto Square-Off Time | Time picker | HH:MM format | Default: 15:15 IST. Visible only when auto square-off is enabled. |

### 5.7 Strategy Lifecycle

```
User Creates Strategy
        |
        v
   [INACTIVE] -----> User clicks "Activate" -----> [ACTIVE]
        ^                                              |
        |                                              |
        +------ User clicks "Deactivate" <-------------+
        |
        v
   User clicks "Delete" -----> [DELETED]

ACTIVE: Rules Engine monitors news and triggers trades when conditions match
INACTIVE: Strategy exists but does not trigger any trades
```

### 5.8 Save & Validation Requirements

| Req# | Requirement | Priority |
|------|------------|----------|
| STRAT-SAVE-01 | Save button saves strategy and returns to list | High |
| STRAT-SAVE-02 | Save & Activate saves and immediately activates | Medium |
| STRAT-SAVE-03 | Cancel discards changes with confirmation dialog | High |
| STRAT-SAVE-04 | All required fields must be validated before save | High |
| STRAT-SAVE-05 | Edit form must pre-fill all existing values | High |
| STRAT-SAVE-06 | Version conflict detection (optimistic locking) | Medium |

---

## 6. Module 4: Paper Trading

### 6.1 Description

Paper trading is a **simulated trading environment** where users can test strategies with **virtual capital** (no real money at risk). All features work identically to live trading except no actual broker orders are placed.

### 6.2 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| PAPER-01 | Virtual Capital | High | Each user gets a virtual capital balance for paper trading |
| PAPER-02 | View Open Positions | High | Display all currently open paper trade positions in a table |
| PAPER-03 | View Closed Orders | High | Display history of all completed/closed paper orders |
| PAPER-04 | Force Exit All | High | Button to close all open positions at once. Requires confirmation. |
| PAPER-05 | Real-Time Prices | High | Position prices and P&L must update in real-time with live market data |
| PAPER-06 | Simulated Execution | High | Market orders fill instantly at LTP. Limit orders fill when price condition met. |
| PAPER-07 | Feature Parity | High | All order types, risk checks, and features must work same as live trading |
| PAPER-08 | No Real Money | Critical | Paper trades must NEVER call the broker API or affect real money |
| PAPER-09 | Separate Data | High | Paper and Live data must be completely separate |

### 6.3 Open Positions Table Columns

| Column | Description | Real-Time? |
|--------|-------------|-----------|
| Stock Symbol & Code | Stock identifier | No |
| Quantity | Number of shares held | No |
| Avg Entry Price | Weighted average purchase price | No |
| Current Market Price | Live market price | Yes (updates every few seconds) |
| Unrealized P&L (Rs.) | (Current Price - Avg Entry) x Quantity | Yes |
| Unrealized P&L (%) | ((Current - Avg) / Avg) x 100 | Yes |
| Investment Value | Avg Entry Price x Quantity | No |
| Current Value | Current Price x Quantity | Yes |

### 6.4 P&L Color Rules

| Condition | Color |
|-----------|-------|
| P&L > 0 (Profit) | Green |
| P&L < 0 (Loss) | Red |
| P&L = 0 | Neutral/Gray |

---

## 7. Module 5: Live Trading

### 7.1 Description

Live trading places **real orders** with real money through the Indira Securities broker (Odin API). All trades are executed on the actual NSE/BSE exchanges.

### 7.2 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| LIVE-01 | View Live Orders | High | Display all current live trading orders with status |
| LIVE-02 | View Closed Orders | High | Display history of all completed/cancelled live orders |
| LIVE-03 | Force Exit All | High | Close all live positions at once. Requires confirmation. |
| LIVE-04 | View Broker Positions | High | Fetch and display positions directly from Indira Securities account |
| LIVE-05 | Subscribe Broker WS | Medium | Connect to broker's WebSocket for real-time order/position updates |
| LIVE-06 | Cancel Order | High | Cancel any pending/submitted order |
| LIVE-07 | Modify Order | High | Modify quantity, price, validity, disclosed qty of pending orders |
| LIVE-08 | Order Details | High | Click any order to see full details including broker response |
| LIVE-09 | Real-Time Status | High | Order status transitions visible in real-time via WebSocket |
| LIVE-10 | Account Balance | Medium | Display real account balance and available margin |
| LIVE-11 | Broker Credential Setup | High | User must configure broker credentials before live trading |

### 7.3 Live Orders Table Columns

| Column | Description |
|--------|-------------|
| Order ID | Unique order identifier |
| Stock Symbol | Stock name/code |
| Order Type | BUY / SELL |
| Quantity | Number of shares |
| Price | Order price |
| Status | RECEIVED / VALIDATED / PENDING / SUBMITTED / FILLED / PARTIAL_FILLED / REJECTED / CANCELLED / FAILED |
| Filled Qty | Number of shares actually executed |
| Filled Price | Actual execution price |
| Commission | Broker charges |
| P&L | Profit/Loss (if filled) |
| Created At | Order creation timestamp |
| Submitted At | Broker submission timestamp |
| Executed At | Execution timestamp |

### 7.4 Order Status Color Coding

| Status | Color | Meaning |
|--------|-------|---------|
| FILLED | Green | Successfully executed |
| PENDING / SUBMITTED | Yellow | Waiting for execution |
| REJECTED / FAILED | Red | Order was rejected or failed |
| CANCELLED | Gray | User cancelled the order |
| PARTIAL_FILLED | Blue/Yellow | Partially executed |

### 7.5 Order Modification (Modal)

When user clicks "Modify" on a pending order, a modal opens with:

| Field | Editable? | Details |
|-------|-----------|---------|
| Quantity | Yes | Must be > 0 |
| Price | Yes | For Limit orders |
| Order Type | Yes | Can change order type |
| Validity | Yes | DAY or IOC |
| Trigger Price | Yes | For Stop Loss orders |
| Disclosed Qty | Yes | Must be <= Quantity |
| Order Side | No | Cannot change (must cancel and re-create) |
| Stock Code | No | Cannot change (must cancel and re-create) |

---

## 8. Module 6: Order Management

### 8.1 Description

Central screen to view, search, and filter **all orders** (both paper and live) with statistics.

### 8.2 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| ORD-01 | View All Orders | High | Display all orders in a filterable, sortable table |
| ORD-02 | Filter by Status | High | Multi-select: Received, Validated, Pending, Submitted, Filled, Partial Filled, Rejected, Cancelled, Failed |
| ORD-03 | Filter by Exchange | Medium | Dropdown: NSE / BSE |
| ORD-04 | Filter by Date Range | High | Date picker: From Date - To Date |
| ORD-05 | Filter by Stock | Medium | Text input with autocomplete |
| ORD-06 | Filter by Strategy | Medium | Dropdown of user's strategies |
| ORD-07 | Filter by Side | Low | Dropdown: BUY / SELL |
| ORD-08 | Clear All Filters | Medium | Button to reset all filters at once |
| ORD-09 | Combined Filters | Medium | All filters must work together (AND logic) |
| ORD-10 | Order Statistics | Medium | Summary section showing fill rate, rejection rate, total value, avg execution time |
| ORD-11 | Order Details | High | Click order row to see full details in modal/panel |
| ORD-12 | Pagination | Medium | For large order lists |

### 8.3 Order Lifecycle (All Possible Statuses)

```
RECEIVED ──> VALIDATED ──> PENDING ──> SUBMITTED ──> FILLED
   |              |            |            |
   |              |            |            └──> PARTIAL_FILLED
   |              |            |
   └──> REJECTED  └──> REJECTED  └──> CANCELLED
                                       |
                                       └──> FAILED

Terminal States: FILLED, PARTIAL_FILLED, CANCELLED, REJECTED, FAILED
```

| Status | Description | Can Cancel? | Can Modify? |
|--------|-------------|-------------|-------------|
| RECEIVED | Order received by system | No | No |
| VALIDATED | Passed risk checks | No | No |
| PENDING | Awaiting broker execution | Yes | Yes |
| SUBMITTED | Sent to broker | Yes | Yes |
| FILLED | Completely executed | No | No |
| PARTIAL_FILLED | Partially executed | Yes | Yes |
| REJECTED | Rejected by system/broker | No | No |
| CANCELLED | User/system cancelled | No | No |
| FAILED | Execution error | No | No |

---

## 9. Module 7: OCO Orders (One-Cancels-the-Other)

### 9.1 Description

OCO is an **advanced order type** that links three orders together:
1. **Entry Order** - Places the initial position
2. **Stop Loss Leg** - Auto-placed after entry fills (cuts losses)
3. **Take Profit Leg** - Auto-placed after entry fills (locks profits)

**Key behavior:** When either the SL or TP leg fills, the other is **automatically cancelled**.

### 9.2 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| OCO-01 | Create OCO Group | High | When product type is Bracket, system creates linked entry + SL + TP orders |
| OCO-02 | Auto-Place Legs | High | After entry order fills, system automatically places SL and TP legs |
| OCO-03 | Auto-Cancel | High | When SL fills → cancel TP. When TP fills → cancel SL. |
| OCO-04 | Trailing Stop Loss | Medium | SL price automatically moves in profitable direction as stock price moves |
| OCO-05 | Display OCO Group | Medium | UI should show all 3 orders as a linked group |
| OCO-06 | OCO Status | Medium | Show OCO lifecycle: Pending Entry → Placing Legs → Active → Completed |
| OCO-07 | Manual Cancel | Medium | User can cancel entire OCO group (all 3 orders) |

### 9.3 OCO Lifecycle

```
1. User creates strategy with Bracket Order product type
2. News matches strategy → Entry order placed
3. Entry order fills at Rs. 2450

4. System auto-calculates:
   - Stop Loss = 2450 x (1 - 2%) = Rs. 2401
   - Take Profit = 2450 x (1 + 5%) = Rs. 2572.50

5. System places SL and TP orders with broker

6. SCENARIO A: Stock drops to 2401
   → SL order fills
   → TP order auto-cancelled
   → Loss = Rs. 49 per share

7. SCENARIO B: Stock rises to 2572.50
   → TP order fills
   → SL order auto-cancelled
   → Profit = Rs. 122.50 per share
```

### 9.4 Trailing Stop Loss Behavior

```
Entry: BUY at Rs. 2450
Initial SL: Rs. 2401 (2% below entry)
Trailing SL %: 2%

Stock rises to Rs. 2500:
  → New SL = 2500 x 0.98 = Rs. 2450 (locked in breakeven)

Stock rises to Rs. 2600:
  → New SL = 2600 x 0.98 = Rs. 2548 (locked in 4% profit)

Stock drops to Rs. 2560:
  → SL stays at Rs. 2548 (never moves down, only up)

Stock drops to Rs. 2548:
  → SL triggers → Position closed at Rs. 2548
  → Profit = Rs. 98 per share
```

**Rules:**
- For BUY: SL only moves UP (new SL must be > current SL)
- For SELL: SL only moves DOWN (new SL must be < current SL)
- SL is recalculated on every price update

### 9.5 OCO Database Fields

| Field | Description |
|-------|-------------|
| oco_group_id | UUID linking all 3 orders |
| oco_role | ENTRY / SL_LEG / TP_LEG |
| parent_order_id | SL and TP legs reference entry order |

---

## 10. Module 8: Risk Management

### 10.1 Description

Risk management enforces trading discipline by performing **8 pre-trade checks** before every order and providing a monitoring dashboard.

### 10.2 Pre-Trade Risk Checks (8 Checks)

Every order must pass ALL enabled checks before being sent to the broker.

| Check# | Check Name | Logic | Action on Fail |
|--------|-----------|-------|----------------|
| 1 | Daily Trade Limit | daily_trade_count >= max_daily_trades | Reject order |
| 2 | Daily Loss Limit | daily_loss >= max_loss_per_day | Reject order, halt trading |
| 3 | Position Size Limit | (quantity x price) > max_position_size | Reject order |
| 4 | Per-Trade Risk | (quantity x stop_loss_amount) > max_per_trade_risk | Reject order |
| 5 | Portfolio Exposure | (total_positions / portfolio_value) > max_exposure_% | Reject order |
| 6 | Concentration Risk | (single_stock_value / portfolio_value) > max_concentration_% | Reject order |
| 7 | Circuit Breaker | (daily_loss / portfolio_value) >= circuit_breaker_% | HALT all trading |
| 8 | Duplicate Order | Same fingerprint (stock + side + qty + strategy) within 5 min | Reject order |

### 10.3 Risk Profiles

| Profile | Multiplier | Use Case |
|---------|-----------|----------|
| Conservative | 0.5x base limits | Cautious traders, small accounts |
| Moderate | 1.0x base limits | Standard (default) |
| Aggressive | 1.5x base limits | Experienced traders, larger accounts |

### 10.4 Risk Dashboard Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| RISK-UI-01 | Daily Loss Progress Bar | High | Show: "Rs. X / Rs. Y limit" with visual progress bar |
| RISK-UI-02 | Trade Count Display | High | Show: "X / Y trades today" with visual bar |
| RISK-UI-03 | Portfolio Exposure Gauge | High | Show: "X% / Y% max exposure" |
| RISK-UI-04 | Risk Status Indicator | High | Green (safe), Yellow (>80% of limit), Red (breached) |
| RISK-UI-05 | Violations Log | Medium | Table of past risk violations with: Type, Time, Description, Action Taken |
| RISK-UI-06 | Circuit Breaker Status | High | Show "ACTIVE" (green) or "TRIGGERED" (red, all trading halted) |
| RISK-UI-07 | Daily Reset | Medium | All daily counters reset at market close (15:30 IST) |

### 10.5 Risk Violation Types

| Violation | Displayed Message |
|-----------|------------------|
| Daily trade limit exceeded | "Daily trade limit of X reached. No more orders today." |
| Daily loss limit exceeded | "Daily loss limit of Rs. X reached. Trading halted." |
| Position size exceeded | "Order value exceeds maximum position size." |
| Per-trade risk exceeded | "Trade risk exceeds per-trade limit of Rs. X." |
| Duplicate order | "Duplicate order detected. Same order placed X seconds ago." |
| Insufficient margin | "Insufficient margin for this order." |
| Circuit breaker triggered | "Circuit breaker triggered. All trading halted." |
| Concentration limit exceeded | "Position in this stock exceeds concentration limit." |

---

## 11. Module 9: Portfolio & Positions

### 11.1 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| PORT-01 | Portfolio Summary Cards | High | Display key metrics: Daily P&L, Open Positions, Total Unrealized P&L, Exposure %, Drawdown |
| PORT-02 | Holdings Table | High | List all open positions with real-time data |
| PORT-03 | Real-Time P&L | High | P&L updates in real-time as market prices change |
| PORT-04 | Sort Positions | Medium | Sort by stock name, P&L, quantity |
| PORT-05 | P&L Calculations | High | Correct formulas for unrealized P&L, investment value, current value |

### 11.2 Portfolio Summary Cards

| Card | Formula | Real-Time? |
|------|---------|-----------|
| Daily P&L | Sum of all realized + unrealized P&L for today | Yes |
| Open Positions Count | Count of positions with quantity > 0 | Yes |
| Total Unrealized P&L | Sum of (Current Price - Avg Entry) x Qty for all positions | Yes |
| Portfolio Exposure % | (Total Position Value / Total Capital) x 100 | Yes |
| Current Drawdown % | ((Peak Value - Current Value) / Peak Value) x 100 | Yes |
| Max Drawdown % | Maximum historical drawdown | No (historical) |
| Sharpe Ratio | Risk-adjusted return metric | No (calculated periodically) |

### 11.3 Holdings Table Columns

| Column | Formula | Real-Time? |
|--------|---------|-----------|
| Stock Symbol & Code | From order data | No |
| Quantity | Total shares held | No |
| Avg Entry Price | Weighted average of all buy prices | No |
| Current Market Price | Live LTP from market data | Yes |
| Unrealized P&L (Rs.) | (Current Price - Avg Entry) x Quantity | Yes |
| Unrealized P&L (%) | ((Current - Avg) / Avg) x 100 | Yes |
| Investment Value | Avg Entry Price x Quantity | No |
| Current Value | Current Price x Quantity | Yes |

---

## 12. Module 10: Price Watch / Watchlist

### 12.1 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| WATCH-01 | View All Watches | High | Display all active price watches in a table |
| WATCH-02 | Cancel Watch | High | Button to remove a price watch |
| WATCH-03 | Real-Time Prices | High | Watched stock prices update in real-time |
| WATCH-04 | Price Direction | Medium | Up arrow (green) for increase, Down arrow (red) for decrease |
| WATCH-05 | Empty State | Low | "No price watches" message when list is empty |

### 12.2 Watchlist Table Columns

| Column | Description |
|--------|-------------|
| Stock Symbol & Code | Stock identifier |
| Current Price | Live market price (real-time) |
| Target Price | User-set target price |
| Status | Above/Below target |
| Created Time | When watch was created |

---

## 13. Module 11: Real-Time Market Data

### 13.1 Description

The application receives **real-time stock market data** via WebSocket connection during market hours (09:15 - 15:30 IST).

### 13.2 Data Fields Available

| Field | Description | Example |
|-------|-------------|---------|
| Last Traded Price (LTP) | Current stock price | Rs. 2450.75 |
| Open | Day's opening price | Rs. 2420.00 |
| High | Day's highest price | Rs. 2455.00 |
| Low | Day's lowest price | Rs. 2420.50 |
| Close | Last closing price | Rs. 2450.75 |
| Previous Close | Yesterday's close | Rs. 2415.50 |
| Volume | Total shares traded today | 1,250,000 |
| % Change | Change from previous close | +1.46% |
| 52-Week High | Highest price in 52 weeks | Rs. 2650.00 |
| 52-Week Low | Lowest price in 52 weeks | Rs. 1900.00 |
| Day High | Today's high | Rs. 2455.00 |
| Day Low | Today's low | Rs. 2420.50 |
| New 52W High Flag | Is this a new 52-week high? | true/false |
| New 52W Low Flag | Is this a new 52-week low? | true/false |

### 13.3 Real-Time Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| RT-01 | WebSocket Connection | High | App must maintain persistent WebSocket connection for live data |
| RT-02 | Auto-Reconnect | High | If connection drops, auto-reconnect and resume data |
| RT-03 | Connection Indicator | Medium | UI must show "Connected" (green) or "Disconnected" (red) |
| RT-04 | Market Status | Medium | Show "Market Open" or "Market Closed" based on hours |
| RT-05 | Price Updates | High | Positions, P&L, watchlist must update every few seconds |
| RT-06 | Order Status Updates | High | Order status changes must appear in real-time |
| RT-07 | Strategy Match Notifications | Medium | When a strategy matches news, notification appears in real-time |
| RT-08 | Binary Format Support | Low | Support binary WebSocket messages for ~80% bandwidth savings |

---

## 14. Module 12: Notifications & Alerts

### 14.1 Toast Notifications

| Trigger Event | Toast Type | Message |
|--------------|------------|---------|
| Strategy created | Success (Green) | "Strategy created successfully" |
| Strategy updated | Success (Green) | "Strategy updated successfully" |
| Strategy deleted | Success (Green) | "Strategy deleted" |
| Strategy activated | Success (Green) | "Strategy activated" |
| Strategy deactivated | Warning (Yellow) | "Strategy deactivated" |
| Order placed | Success (Green) | "Order placed successfully" |
| Order filled | Success (Green) | "Order filled - {Stock} {Qty} @ Rs. {Price}" |
| Order rejected | Error (Red) | "Order rejected - {Reason}" |
| Order cancelled | Warning (Yellow) | "Order cancelled" |
| Force exit all | Success (Green) | "All positions closed" |
| Risk violation | Error (Red) | "Risk limit breached - {Type}" |
| Circuit breaker | Error (Red) | "Circuit breaker triggered. Trading halted." |
| Session expiring | Warning (Yellow) | "Session expiring soon" |
| Connection lost | Error (Red) | "Connection lost. Reconnecting..." |
| Connection restored | Success (Green) | "Connection restored" |

### 14.2 Toast Behavior

| Req# | Requirement | Priority |
|------|------------|----------|
| NOTIF-01 | Auto-dismiss after 3-5 seconds | Medium |
| NOTIF-02 | Manual close (X button) on each toast | Medium |
| NOTIF-03 | Stack multiple toasts (don't overlap) | Low |
| NOTIF-04 | Error toasts stay longer (5-10 seconds) | Low |

### 14.3 Confirmation Dialogs (Required Before Destructive Actions)

| Action | Confirmation Message |
|--------|---------------------|
| Delete Strategy | "Are you sure you want to delete this strategy?" |
| Deactivate Strategy | "Deactivate this strategy? It will stop trading." |
| Force Exit All Positions | "Close ALL open positions? This cannot be undone." |
| Cancel Order | "Cancel this order?" |
| Discard Form Changes | "You have unsaved changes. Discard?" |

---

## 15. Module 13: Broker Integration (Indira Securities)

### 15.1 Description

The application integrates with **Indira Securities** via their **Odin API** for live order execution on NSE and BSE.

### 15.2 Functional Requirements

| Req# | Requirement | Priority | Details |
|------|------------|----------|---------|
| BROKER-01 | Broker Login | High | User must authenticate with Indira Securities credentials |
| BROKER-02 | Credential Storage | High | Broker credentials stored encrypted (AES-256) in database |
| BROKER-03 | Order Placement | High | Submit orders to Odin API (Market, Limit, SL, SL-M) |
| BROKER-04 | Order Modification | High | Modify pending orders via Odin API |
| BROKER-05 | Order Cancellation | High | Cancel pending orders via Odin API |
| BROKER-06 | Broker Positions Sync | Medium | Fetch real positions from broker account |
| BROKER-07 | Broker WebSocket | Medium | Real-time order/position updates from broker |
| BROKER-08 | Auto-Reconnect | Medium | Reconnect to broker WebSocket on disconnect |
| BROKER-09 | Order Notify Webhook | Medium | Receive broker push notifications for order status changes |
| BROKER-10 | Retry Logic | High | 3 retries with exponential backoff (1s, 2s, 4s) for failed API calls |
| BROKER-11 | 30s Timeout | High | API calls timeout after 30 seconds |

### 15.3 Supported Broker Order Parameters

| Parameter | Values |
|-----------|--------|
| Exchange | NSE, BSE |
| Order Action | BUY, SELL |
| Order Type | Market, Limit, SL, SL-M |
| Product Type | INTRADAY, DELIVERY, CASH, MTF |
| Instrument | STK (Stock), OPT (Option), FUT (Future), IDX (Index) |
| Validity | DAY, IOC |
| AMO | true / false |

---

## 16. Non-Functional Requirements

### 16.1 Performance

| Metric | Target |
|--------|--------|
| Strategy Matching Latency (p95) | < 100ms |
| Order Execution Latency (p95) | < 500ms |
| API Response Time (p95) | < 200ms |
| WebSocket Price Update Latency | < 100ms |
| Cache Hit Rate | > 90% |
| Events Processed Per Hour | 1000+ |

### 16.2 Scalability

| Component | Capacity |
|-----------|----------|
| Concurrent Users | 10,000+ |
| Active Strategies | 50,000+ |
| Orders Per Second (Peak) | 100+ |

### 16.3 Availability

| Metric | Target |
|--------|--------|
| System Uptime | 99.9% (< 43 min downtime/month) |
| Recovery Time (RTO) | < 1 hour |
| Data Loss Tolerance (RPO) | < 5 minutes |

### 16.4 Security

| Requirement | Implementation |
|-------------|---------------|
| Authentication | JWT tokens (24-hour expiry) |
| Authorization | Role-Based Access Control (RBAC) |
| Communication | TLS 1.3 for all service communication |
| Credential Storage | AES-256 encryption |
| Audit Logging | All strategy/order changes logged |
| Rate Limiting | Redis-based, per-user throttling |
| Input Validation | All user inputs validated server-side |
| XSS Prevention | All user-entered text escaped before rendering |
| SQL Injection Prevention | Parameterized queries only |

### 16.5 UI / UX

| Requirement | Details |
|-------------|---------|
| Responsive Design | Works on desktop, tablet, and mobile browsers |
| Loading States | Spinner/skeleton shown during data loading |
| Error States | User-friendly error messages with retry option |
| Confirmation Dialogs | Required for all destructive actions |
| Double-Click Prevention | Save/submit buttons disabled after first click |
| Unsaved Changes Warning | Warn before leaving form with unsaved data |

---

## 17. Glossary

| Term | Full Form | Description |
|------|-----------|-------------|
| NSE | National Stock Exchange | India's primary stock exchange |
| BSE | Bombay Stock Exchange | India's oldest stock exchange |
| LTP | Last Traded Price | Most recent price at which a stock traded |
| OHLC | Open, High, Low, Close | Four key price points for a trading day |
| MIS | Margin Intraday Square-off | Intraday trading product (squared-off same day) |
| CNC | Cash and Carry | Delivery-based trading (held until sold) |
| BO | Bracket Order | Order with linked stop loss and target |
| OCO | One-Cancels-the-Other | Linked orders where one fills and others cancel |
| SL | Stop Loss | Order to limit losses by selling at preset price |
| TP | Take Profit | Order to lock profits by selling at preset price |
| AMO | After Market Order | Order placed outside market hours for next opening |
| IOC | Immediate or Cancel | Fill immediately or cancel unfilled portion |
| P&L | Profit and Loss | Financial gain or loss from trading |
| IST | Indian Standard Time | UTC+5:30 timezone |
| JWT | JSON Web Token | Authentication token format |
| RBAC | Role-Based Access Control | Permission system based on user roles |
| MPIN | Mobile PIN | Numeric PIN for mobile login |
| OTP | One-Time Password | Single-use password for verification |
| TOTP | Time-Based OTP | Time-limited OTP from authenticator app |
| Odin API | - | Indira Securities' trading API platform |

---

*Document generated on 2026-03-19*
*For project: AlgoNewsWithCodifi - News-Based Algorithmic Trading Platform*
