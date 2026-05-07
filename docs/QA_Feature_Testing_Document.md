# StockAsk Algo Trading - Manual UI Testing Document

**Version:** 2.0
**Date:** 2026-03-19
**For:** Manual QA Testers
**Purpose:** Step-by-step guide for testing all UI screens, forms, buttons, and user interactions

---

## Table of Contents

1. [Login Screen](#1-login-screen)
2. [Navigation & Layout](#2-navigation--layout)
3. [Dashboard Screen](#3-dashboard-screen)
4. [Strategy List Screen](#4-strategy-list-screen)
5. [Create / Edit Strategy Screen](#5-create--edit-strategy-screen)
6. [Paper Trading Screen](#6-paper-trading-screen)
7. [Live Trading Screen](#7-live-trading-screen)
8. [Order Management Screen](#8-order-management-screen)
9. [Portfolio & Positions Screen](#9-portfolio--positions-screen)
10. [Risk Management Screen](#10-risk-management-screen)
11. [Price Watch / Watchlist Screen](#11-price-watch--watchlist-screen)
12. [Real-Time Updates (Live Data)](#12-real-time-updates-live-data)
13. [Notifications & Alerts](#13-notifications--alerts)
14. [Negative & Edge Case Testing](#14-negative--edge-case-testing)

---

## 1. Login Screen

### What you see on screen
- User ID input field
- Password input field
- MPIN input field (optional)
- Login button
- OTP / TOTP verification input (appears after initial login if MFA enabled)

### Test Cases

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 1.1 | Valid Login | 1. Enter valid User ID and Password 2. Click "Login" | User is redirected to Dashboard. Session is active. |
| 1.2 | Invalid Password | 1. Enter valid User ID 2. Enter wrong Password 3. Click "Login" | Error message shown: "Invalid credentials" or similar. User stays on login screen. |
| 1.3 | Empty Fields | 1. Leave User ID and Password blank 2. Click "Login" | Validation error shown for required fields. Login button should not proceed. |
| 1.4 | MPIN Login | 1. Enter User ID 2. Enter MPIN instead of password 3. Click "Login" | Login succeeds if MPIN is correct. Error if incorrect. |
| 1.5 | OTP Verification | 1. Login with valid credentials 2. OTP screen appears 3. Enter correct OTP | Login completes. Dashboard loads. |
| 1.6 | Wrong OTP | 1. Login with valid credentials 2. Enter wrong OTP | Error message: "Invalid OTP". User stays on OTP screen. |
| 1.7 | Expired OTP | 1. Login 2. Wait for OTP to expire 3. Enter expired OTP | Error shown. Option to resend OTP. |
| 1.8 | Session Expiry | 1. Login successfully 2. Leave app idle for 24+ hours 3. Try any action | User is redirected to login screen. Session expired message shown. |
| 1.9 | Logout | 1. Click Logout button | User is redirected to login screen. Session is terminated. |

---

## 2. Navigation & Layout

### What you see on screen
- Sidebar / Top navigation bar with menu items
- Navigation links to all major screens

### Menu Items to Verify

| TC# | Menu Item | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 2.1 | Dashboard | Click "Dashboard" in menu | Dashboard screen loads with stats cards |
| 2.2 | Strategies | Click "Strategies" in menu | Strategy list screen loads |
| 2.3 | Paper Trading | Click "Paper Trading" in menu | Paper trading screen loads with positions table |
| 2.4 | Live Trading | Click "Live Trading" in menu | Live trading screen loads with orders table |
| 2.5 | Orders | Click "Orders" in menu | Order history/management screen loads |
| 2.6 | Positions / Portfolio | Click "Positions" in menu | Portfolio screen loads with holdings |
| 2.7 | Risk Management | Click "Risk" in menu | Risk dashboard loads with metrics |
| 2.8 | Price Watches | Click "Price Watches" in menu | Watchlist screen loads |
| 2.9 | Logout | Click "Logout" button | User is logged out, login screen appears |

---

## 3. Dashboard Screen

### What you see on screen
- **Paper / Live toggle switch** at the top
- **Statistics cards/widgets** showing trading summary
- Data refreshes based on selected mode

### Test Cases

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 3.1 | Page Load | Navigate to Dashboard | All stat cards load with numbers. No errors. |
| 3.2 | Paper Mode | Click/toggle to "Paper" mode | All stats show paper trading data only. Badge shows "PAPER". |
| 3.3 | Live Mode | Click/toggle to "Live" mode | All stats show live trading data only. Badge shows "LIVE". |
| 3.4 | Total Orders Card | View "Total Orders" card | Shows correct count of all orders placed |
| 3.5 | Filled Orders Card | View "Filled Orders" card | Shows count of successfully executed orders |
| 3.6 | Pending Orders Card | View "Pending Orders" card | Shows count of orders still awaiting execution |
| 3.7 | Rejected Orders Card | View "Rejected Orders" card | Shows count of orders rejected by system/broker |
| 3.8 | Cancelled Orders Card | View "Cancelled Orders" card | Shows count of user-cancelled orders |
| 3.9 | Fill Rate % | View "Fill Rate" display | Shows correct percentage: (filled / total) x 100 |
| 3.10 | Rejection Rate % | View "Rejection Rate" display | Shows correct percentage: (rejected / total) x 100 |
| 3.11 | Total Traded Value | View "Total Traded Value" | Shows sum of all trade values in rupees |
| 3.12 | Avg Order Value | View "Avg Order Value" | Shows average rupee value per order |
| 3.13 | Avg Execution Time | View "Avg Execution Time" | Shows average time in milliseconds |
| 3.14 | P95 Execution Time | View "P95 Execution Time" | Shows 95th percentile execution latency |
| 3.15 | Data Refresh | Place a new trade, come back to Dashboard | Stats should update to reflect the new trade |
| 3.16 | Mode Switch Data | 1. View stats in Paper mode 2. Switch to Live mode | Numbers change to reflect live trading data (should be different from paper) |

---

## 4. Strategy List Screen

### What you see on screen
- Table/grid listing all strategies
- Search/filter controls
- "Create Strategy" button
- Action buttons per row (Edit, Activate, Deactivate, Delete)
- Pagination controls

### Table Columns to Verify
- Strategy Name
- Description
- Status (Active / Inactive)
- Trading Mode (PAPER / LIVE)
- Created Date
- Last Updated Date

### Test Cases

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 4.1 | Page Load | Navigate to Strategies screen | List of all strategies displayed in table |
| 4.2 | Search by Name | Type strategy name in search box | Table filters to show only matching strategies |
| 4.3 | Filter Active Only | Select "Active" filter | Only active strategies shown |
| 4.4 | Pagination | If >10 strategies, click page 2 | Next set of strategies loaded. Page indicator updates. |
| 4.5 | Create Button | Click "Create Strategy" button | Strategy creation form opens |
| 4.6 | Edit Button | Click "Edit" on a strategy row | Strategy edit form opens with pre-filled data |
| 4.7 | Activate Strategy | Click "Activate" on an inactive strategy | Status changes to "Active". Success notification shown. |
| 4.8 | Deactivate Strategy | Click "Deactivate" on an active strategy | Confirmation popup appears. On confirm, status changes to "Inactive". |
| 4.9 | Delete Strategy | Click "Delete" on a strategy | Confirmation popup: "Are you sure?" On confirm, strategy removed from list. |
| 4.10 | Delete Active Strategy | Try to delete an active strategy | Should either block deletion or ask to deactivate first |
| 4.11 | Empty State | Delete all strategies | "No strategies found" message displayed with "Create" button |
| 4.12 | Sort by Name | Click "Name" column header | Strategies sorted alphabetically |
| 4.13 | Sort by Date | Click "Created Date" column header | Strategies sorted by date |

---

## 5. Create / Edit Strategy Screen

### What you see on screen
This is a multi-section form with the following areas:
- **Basic Info** section
- **Strategy Conditions** section (news filters)
- **Trade Configuration** section (order settings)
- **Risk Management** section (limits)
- Save / Cancel buttons

### 5A. Basic Information Section

| TC# | Field / Element | Type | Steps | Expected Result |
|-----|----------------|------|-------|-----------------|
| 5A.1 | Strategy Name | Text input | Enter name (e.g., "My Algo Strategy") | Name accepted. Required field validation if empty. |
| 5A.2 | Description | Text area | Enter description | Optional field. Text saved correctly. |
| 5A.3 | Trading Mode | Dropdown | Select "PAPER" or "LIVE" | Mode saved. Determines where orders go. |
| 5A.4 | Name Validation | Text input | Leave name blank, click Save | Error: "Strategy name is required" |
| 5A.5 | Long Name | Text input | Enter very long name (200+ chars) | Should truncate or show max length error |

### 5B. Strategy Conditions Section (News Filters)

| TC# | Field / Element | Type | Steps | Expected Result |
|-----|----------------|------|-------|-----------------|
| 5B.1 | Match All News | Toggle/Checkbox | Enable "Match All News" toggle | Other condition fields become disabled/grayed out. Strategy will trigger on any news. |
| 5B.2 | Match All News OFF | Toggle/Checkbox | Disable "Match All News" | All condition fields become editable again. |
| 5B.3 | Impact Score Min | Slider/Number input | Set minimum impact score to 70 | Value shows "70". Only news with score >= 70 will match. |
| 5B.4 | Impact Score Max | Slider/Number input | Set maximum impact score to 100 | Value shows "100". |
| 5B.5 | Impact Score Range | Both inputs | Set min=80, max=50 (invalid) | Should show error: "Min cannot be greater than max" |
| 5B.6 | Sentiment - Positive | Checkbox | Check "Positive" | Only positive sentiment news will trigger |
| 5B.7 | Sentiment - Neutral | Checkbox | Check "Neutral" | Neutral sentiment news will trigger |
| 5B.8 | Sentiment - Negative | Checkbox | Check "Negative" | Negative sentiment news will trigger |
| 5B.9 | Multiple Sentiments | Checkboxes | Check "Positive" and "Negative" | Both sentiment types will trigger |
| 5B.10 | News Categories | Multi-select dropdown | Select "Earnings" and "Mergers" | Only those categories will trigger the strategy |
| 5B.11 | Stock Code | Text input / Autocomplete | Type "RELIANCE", select from dropdown | Strategy will only trigger for RELIANCE stock |
| 5B.12 | Multiple Stock Codes | Multi-select | Add "RELIANCE", "TCS", "INFY" | Strategy triggers for all three stocks |
| 5B.13 | Price Range - Min | Number input | Enter minimum price: 100 | Only stocks priced >= 100 will match |
| 5B.14 | Price Range - Max | Number input | Enter maximum price: 5000 | Only stocks priced <= 5000 will match |
| 5B.15 | Market Cap Type | Radio/Checkbox | Select "Large Cap" | Only large-cap stocks will match |
| 5B.16 | Market Cap Range | Number inputs | Enter min: 10000, max: 50000 (crores) | Filters by market cap range |
| 5B.17 | Volume Threshold | Number input | Enter: 100000 | Only stocks with volume >= 100000 will match |
| 5B.18 | % Change Range | Number inputs | Enter min: -5%, max: +10% | Filters by price change percentage |
| 5B.19 | Exchange - NSE | Checkbox | Select "NSE" | Only NSE-listed stocks match |
| 5B.20 | Exchange - BSE | Checkbox | Select "BSE" | Only BSE-listed stocks match |
| 5B.21 | Exchange - Both | Checkboxes | Select "NSE" and "BSE" | Stocks from both exchanges match |

### 5C. Trade Configuration Section

| TC# | Field / Element | Type | Steps | Expected Result |
|-----|----------------|------|-------|-----------------|
| 5C.1 | Order Type - Market | Radio button | Select "Market" | Order will execute at current market price |
| 5C.2 | Order Type - Limit | Radio button | Select "Limit" | Limit Price field appears/becomes enabled |
| 5C.3 | Order Type - Stop Loss | Radio button | Select "Stop Loss" | Stop loss price field appears |
| 5C.4 | Order Type - SL Market | Radio button | Select "Stop Loss Market" | Stop loss market fields appear |
| 5C.5 | Product - Intraday | Radio/Dropdown | Select "Intraday (MIS)" | Orders will be squared off same day |
| 5C.6 | Product - Delivery | Radio/Dropdown | Select "Delivery (CNC)" | Orders held until manually sold |
| 5C.7 | Product - Bracket | Radio/Dropdown | Select "Bracket Orders (BO)" | Bracket order fields (SL + Target) appear |
| 5C.8 | Order Side - Buy | Radio button | Select "BUY" | Strategy will place buy orders |
| 5C.9 | Order Side - Sell | Radio button | Select "SELL" | Strategy will place sell orders |
| 5C.10 | Quantity | Number input | Enter: 100 | Quantity set to 100 shares. Validate no negative/zero/decimal. |
| 5C.11 | Quantity - Invalid | Number input | Enter: -5 or 0 or "abc" | Validation error shown |
| 5C.12 | Exchange | Dropdown | Select "NSE" or "BSE" | Order will be placed on selected exchange |
| 5C.13 | Validity - DAY | Dropdown/Radio | Select "DAY" | Order valid until market close |
| 5C.14 | Validity - IOC | Dropdown/Radio | Select "IOC" | Order fills immediately or cancels |
| 5C.15 | Limit Price | Number input | Enter: 1500.50 (when Limit order type selected) | Price set. Appears only for Limit orders. |
| 5C.16 | Stop Loss % | Number input | Enter: 2.5 | Stop loss set at 2.5% below entry price |
| 5C.17 | Stop Loss Type - Fixed | Radio button | Select "Fixed" | Stop loss stays at fixed price level |
| 5C.18 | Stop Loss Type - Trailing | Radio button | Select "Trailing" | Stop loss moves up as price increases |
| 5C.19 | Take Profit % | Number input | Enter: 5.0 | Take profit set at 5% above entry price |
| 5C.20 | Bracket Order SL | Number input | Enter bracket stop loss value | Appears only when product type is "Bracket" |
| 5C.21 | Bracket Order Target | Number input | Enter bracket target value | Appears only when product type is "Bracket" |
| 5C.22 | AMO Toggle | Toggle/Checkbox | Enable "After Market Order" | Order queued for next market open |
| 5C.23 | Disclosed Quantity | Number input | Enter: 25 (when total qty = 100) | Only 25 shares visible to market at a time |

### 5D. Risk Management Section

| TC# | Field / Element | Type | Steps | Expected Result |
|-----|----------------|------|-------|-----------------|
| 5D.1 | Max Daily Trades | Number input | Enter: 10 | Strategy stops after 10 trades per day |
| 5D.2 | Max Loss Per Day | Number input (Rs.) | Enter: 5000 | Strategy stops if daily loss reaches Rs. 5000 |
| 5D.3 | Max Per-Trade Risk | Number input (Rs.) | Enter: 1000 | Individual trades limited to Rs. 1000 risk |
| 5D.4 | Max Portfolio Exposure % | Number input | Enter: 25 | Maximum 25% of capital in this strategy |
| 5D.5 | Position Sizing - Fixed | Dropdown/Radio | Select "Fixed" | Uses fixed quantity for each trade |
| 5D.6 | Position Sizing - Percentage | Dropdown/Radio | Select "Percentage" | Calculates quantity based on % of capital |
| 5D.7 | Position Sizing - Risk-Based | Dropdown/Radio | Select "Risk-Based" | Calculates quantity based on risk per trade |
| 5D.8 | Enable Risk Checks | Toggle | Enable "Risk Checks" toggle | All risk validations active for this strategy |
| 5D.9 | Disable Risk Checks | Toggle | Disable "Risk Checks" toggle | Risk checks bypassed (warning should appear) |
| 5D.10 | Auto Square-Off | Toggle | Enable "Auto Square-Off" | Time input field appears |
| 5D.11 | Auto Square-Off Time | Time picker | Enter: 15:15 | Positions auto-closed at 3:15 PM |

### 5E. Save & Validation

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 5E.1 | Save Strategy | Fill all required fields, click "Save" | Strategy saved. Redirect to strategy list. Success notification. |
| 5E.2 | Save & Activate | Fill fields, click "Save & Activate" | Strategy saved AND activated immediately |
| 5E.3 | Cancel | Fill some fields, click "Cancel" | Confirmation: "Discard changes?" On confirm, go back to list. Data not saved. |
| 5E.4 | Required Fields Empty | Leave required fields blank, click Save | Red validation errors shown on all required fields |
| 5E.5 | Edit Existing | Open existing strategy, change name, Save | Name updated. Other fields unchanged. |
| 5E.6 | Edit Pre-fill | Open existing strategy for edit | All previously saved values pre-filled in the form |

---

## 6. Paper Trading Screen

### What you see on screen
- "PAPER TRADING" badge/indicator
- Virtual Capital balance display
- **Open Positions** table
- **Closed Orders** table/tab
- "Force Exit All" button
- Refresh button

### Open Positions Table Columns
- Stock Symbol & Code
- Quantity
- Avg Entry Price
- Current Market Price (live)
- Unrealized P&L (amount and %)
- Investment Value
- Current Value

### Test Cases

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 6.1 | Page Load | Navigate to Paper Trading | Open positions table loads. Virtual capital shown. |
| 6.2 | View Open Positions | Check positions table | All open paper positions displayed with correct data |
| 6.3 | P&L Color Coding | View P&L column | Green for profit, Red for loss |
| 6.4 | Live Price Update | Watch "Current Price" column during market hours | Prices update in real-time without page refresh |
| 6.5 | P&L Live Update | Watch "Unrealized P&L" column | P&L recalculates as price changes |
| 6.6 | View Closed Orders | Click "Closed Orders" tab | Historical closed paper orders displayed |
| 6.7 | Closed Orders Data | Check closed orders table | Shows: Stock, Type, Qty, Entry Price, Exit Price, Realized P&L, Date |
| 6.8 | Force Exit All | Click "Force Exit All" button | Confirmation popup appears: "Close all positions?" |
| 6.9 | Confirm Force Exit | Click "Confirm" on force exit popup | All open positions closed. Positions table becomes empty. Success notification. |
| 6.10 | Cancel Force Exit | Click "Cancel" on force exit popup | No positions closed. Table unchanged. |
| 6.11 | Refresh | Click Refresh button | Data reloads from server |
| 6.12 | No Positions | View screen when no open positions | "No open positions" empty state message |
| 6.13 | Virtual Capital | Check capital display | Shows remaining virtual capital after trades |

---

## 7. Live Trading Screen

### What you see on screen
- "LIVE TRADING" badge/indicator
- Account balance / Margin display
- **Live Orders** table
- **Closed Orders** tab
- **Broker Positions** tab
- "Force Exit All" button
- "Subscribe to Broker Updates" button

### Live Orders Table Columns
- Order ID
- Stock Symbol
- Order Type (BUY/SELL)
- Quantity
- Price
- Status (color-coded)
- Filled Qty
- Filled Price
- Commission
- P&L
- Timestamps

### Test Cases

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 7.1 | Page Load | Navigate to Live Trading | Live orders table loads with current orders |
| 7.2 | View Live Orders | Check orders table | All live orders displayed with correct status colors |
| 7.3 | Order Status Colors | View status column | Green=Filled, Yellow=Pending, Red=Rejected, Gray=Cancelled |
| 7.4 | View Closed Orders | Click "Closed Orders" tab | Historical completed/cancelled orders shown |
| 7.5 | View Broker Positions | Click "Broker Positions" tab | Positions synced from Indira Securities broker displayed |
| 7.6 | Cancel Order | Click "Cancel" on a Pending order row | Confirmation popup. On confirm, order cancelled. Status changes to "Cancelled". |
| 7.7 | Modify Order | Click "Modify" on a Pending order row | Modify modal opens with fields: Quantity, Price, Order Type, Validity, Disclosed Qty |
| 7.8 | Save Modified Order | Change quantity in modify modal, click Save | Order updated. New quantity reflected. Success notification. |
| 7.9 | Force Exit All | Click "Force Exit All" | Confirmation popup. On confirm, all positions closed. |
| 7.10 | Subscribe Broker WS | Click "Subscribe to Broker Updates" | Real-time data starts flowing from broker. Connection status shows "Connected". |
| 7.11 | Real-Time Order Updates | Place an order, watch status column | Status transitions visible in real-time: Received -> Validated -> Submitted -> Filled |
| 7.12 | Order Details | Click on an order row | Order details modal/panel opens showing all fields including broker response |
| 7.13 | Rejected Order | View a rejected order's details | Shows rejection reason and error message |

---

## 8. Order Management Screen

### What you see on screen
- **Filter controls** at top
- **Orders table** showing all orders
- **Order statistics** summary section

### Filter Controls

| TC# | Filter | Type | Steps | Expected Result |
|-----|--------|------|-------|-----------------|
| 8.1 | Status Filter | Multi-select dropdown | Select "Filled" and "Pending" | Table shows only Filled and Pending orders |
| 8.2 | Exchange Filter | Dropdown | Select "NSE" | Only NSE orders shown |
| 8.3 | Date Range | Date picker | Select From: 01/03/2026, To: 19/03/2026 | Only orders within date range shown |
| 8.4 | Stock Filter | Text/Autocomplete | Type "RELIANCE" | Only RELIANCE orders shown |
| 8.5 | Strategy Filter | Dropdown | Select a strategy name | Only orders from that strategy shown |
| 8.6 | Side Filter | Dropdown | Select "BUY" | Only buy orders shown |
| 8.7 | Clear Filters | Click "Clear" / "Reset" button | All filters removed. All orders displayed. |
| 8.8 | Combined Filters | Set Status=Filled + Exchange=NSE + Stock=TCS | Only TCS filled orders on NSE shown |

### Order Statistics Section

| TC# | Stat | Steps | Expected Result |
|-----|------|-------|-----------------|
| 8.9 | Total Orders | View stat | Correct count of all orders (matches table count) |
| 8.10 | Fill Rate | View stat | Correct percentage displayed |
| 8.11 | Rejection Rate | View stat | Correct percentage displayed |
| 8.12 | Total Traded Value | View stat | Correct sum in rupees |
| 8.13 | Avg Execution Time | View stat | Correct average in milliseconds |

---

## 9. Portfolio & Positions Screen

### What you see on screen
- **Portfolio summary cards** at top
- **Holdings/Positions table** below
- Sorting options

### Portfolio Summary Cards

| TC# | Card | Steps | Expected Result |
|-----|------|-------|-----------------|
| 9.1 | Daily P&L | View card | Shows today's profit or loss (green/red color) |
| 9.2 | Open Positions Count | View card | Shows number of currently open positions |
| 9.3 | Total Unrealized P&L | View card | Sum of P&L across all open positions |
| 9.4 | Portfolio Exposure % | View card | Shows total exposure as percentage of capital |
| 9.5 | Current Drawdown % | View card | Shows current loss from portfolio peak |
| 9.6 | Max Drawdown % | View card | Shows largest historical drawdown |

### Holdings Table

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 9.7 | View Holdings | Check table | All open positions listed with correct data |
| 9.8 | Stock Info | Check first column | Stock symbol, code, exchange displayed correctly |
| 9.9 | Quantity | Check quantity column | Matches actual bought quantity |
| 9.10 | Avg Entry Price | Check average price | Correctly calculated for multiple buys at different prices |
| 9.11 | Current Price | Check current price column | Updates in real-time with market data |
| 9.12 | Unrealized P&L | Check P&L column | Correct: (Current Price - Avg Entry) x Quantity |
| 9.13 | P&L Percentage | Check P&L % | Correct: ((Current - Avg) / Avg) x 100 |
| 9.14 | Investment Value | Check investment column | Correct: Avg Entry Price x Quantity |
| 9.15 | Current Value | Check current value column | Correct: Current Price x Quantity |
| 9.16 | Sort by P&L | Click P&L column header | Positions sorted by profit/loss |
| 9.17 | Sort by Stock | Click Stock column header | Positions sorted alphabetically |

---

## 10. Risk Management Screen

### What you see on screen
- **Risk metric gauges/progress bars** showing usage vs limits
- **Risk violations log** table
- **Circuit breaker status** indicator

### Risk Metrics Display

| TC# | Metric | Steps | Expected Result |
|-----|--------|-------|-----------------|
| 10.1 | Daily Loss Tracker | View progress bar | Shows: "Rs. X / Rs. Y limit" with visual bar |
| 10.2 | Trade Count | View counter | Shows: "X / Y trades today" with visual bar |
| 10.3 | Portfolio Exposure | View gauge | Shows: "X% / Y% max exposure" |
| 10.4 | Risk Status - Green | When all metrics within limits | Green indicator: "All Clear" |
| 10.5 | Risk Status - Yellow | When approaching a limit (>80%) | Yellow indicator: "Warning" |
| 10.6 | Risk Status - Red | When a limit is breached | Red indicator: "Critical / Breached" |

### Risk Violations Log

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 10.7 | View Violations | Check violations table | Lists all past risk violations with timestamps |
| 10.8 | Violation Details | Check each row | Shows: Violation Type, Time, Description, Action Taken |
| 10.9 | Daily Trade Limit Hit | Exceed max daily trades in a strategy | Violation logged: "Daily trade limit exceeded". New orders blocked. |
| 10.10 | Daily Loss Limit Hit | Accumulate losses to max | Violation logged: "Daily loss limit exceeded". Trading halted. |
| 10.11 | Circuit Breaker | Max loss threshold breached | Circuit breaker shows "TRIGGERED" (red). All trading halted. Alert shown. |
| 10.12 | Next Day Reset | Check after market close / next day | Counters reset to 0. Circuit breaker resets. Trading resumes. |

---

## 11. Price Watch / Watchlist Screen

### What you see on screen
- **Price watches table** listing monitored stocks/orders
- "Cancel Watch" button per row
- Real-time price updates

### Test Cases

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 11.1 | View Watches | Navigate to Price Watches | All active watches listed in table |
| 11.2 | Watch Data | Check table columns | Shows: Stock, Current Price, Target Price, Status, Created Time |
| 11.3 | Live Price | Watch price column during market hours | Prices update in real-time |
| 11.4 | Cancel Watch | Click "Cancel" on a watch row | Confirmation popup. On confirm, watch removed from list. |
| 11.5 | No Watches | View when no watches exist | "No price watches" empty state |
| 11.6 | Price Indicator | View price changes | Up arrow (green) for price increase, Down arrow (red) for decrease |

---

## 12. Real-Time Updates (Live Data)

### What to verify on screen
These tests check that data updates **without page refresh** during market hours.

| TC# | Feature | Where to Check | Expected Result |
|-----|---------|---------------|-----------------|
| 12.1 | Price Updates | Paper/Live Trading positions table | "Current Price" column updates every few seconds |
| 12.2 | P&L Updates | Paper/Live Trading positions table | "Unrealized P&L" recalculates as price changes |
| 12.3 | Order Status | Live Trading orders table | Status transitions visible in real-time (e.g., Pending -> Filled) |
| 12.4 | Dashboard Stats | Dashboard screen | Stats update after new trades without refresh |
| 12.5 | Strategy Match | When news triggers a strategy | Match notification appears in real-time |
| 12.6 | Connection Lost | Disconnect internet briefly, reconnect | "Connection lost" warning shown. Auto-reconnects. Data resumes. |
| 12.7 | Connection Indicator | Check connection status icon | Shows "Connected" (green) or "Disconnected" (red) |
| 12.8 | Market Status | During market open/close | Status indicator shows "Market Open" or "Market Closed" |

---

## 13. Notifications & Alerts

### What to verify on screen
Toast notifications / popup alerts that appear during user actions.

| TC# | Action | Expected Notification |
|-----|--------|-----------------------|
| 13.1 | Strategy Created | Green success toast: "Strategy created successfully" |
| 13.2 | Strategy Updated | Green success toast: "Strategy updated successfully" |
| 13.3 | Strategy Deleted | Green success toast: "Strategy deleted" |
| 13.4 | Strategy Activated | Green toast: "Strategy activated" |
| 13.5 | Strategy Deactivated | Yellow toast: "Strategy deactivated" |
| 13.6 | Order Placed | Green toast: "Order placed successfully" |
| 13.7 | Order Filled | Green toast: "Order filled - [Stock] [Qty] @ [Price]" |
| 13.8 | Order Rejected | Red toast: "Order rejected - [Reason]" |
| 13.9 | Order Cancelled | Yellow toast: "Order cancelled" |
| 13.10 | Force Exit All | Green toast: "All positions closed" |
| 13.11 | Risk Violation | Red alert: "Risk limit breached - [Type]" |
| 13.12 | Circuit Breaker | Red alert: "Circuit breaker triggered. Trading halted." |
| 13.13 | Session Expiry Warning | Yellow toast: "Session expiring soon" (before 24hr timeout) |
| 13.14 | Connection Lost | Red toast: "Connection lost. Reconnecting..." |
| 13.15 | Connection Restored | Green toast: "Connection restored" |
| 13.16 | Validation Error | Red inline error on the field: "This field is required" / "Invalid value" |
| 13.17 | Toast Auto-Dismiss | Wait 3-5 seconds after any toast | Toast disappears automatically |
| 13.18 | Toast Close Button | Click "X" on a toast notification | Toast dismisses immediately |

---

## 14. Negative & Edge Case Testing

### Form Validation

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 14.1 | Empty Strategy Name | Leave name blank on strategy form, click Save | Error: "Strategy name is required" |
| 14.2 | Negative Quantity | Enter quantity: -10 | Error: "Quantity must be positive" |
| 14.3 | Zero Quantity | Enter quantity: 0 | Error: "Quantity must be greater than 0" |
| 14.4 | Decimal Quantity | Enter quantity: 10.5 | Error or auto-round to 10 |
| 14.5 | Text in Number Field | Enter "abc" in quantity/price fields | Field rejects non-numeric input or shows error |
| 14.6 | Stop Loss > 100% | Enter stop loss: 150% | Error: "Stop loss must be between 0-100%" |
| 14.7 | Take Profit = 0 | Enter take profit: 0% | Error: "Take profit must be greater than 0" |
| 14.8 | Min > Max Price | Set min price 500, max price 100 | Error: "Min price cannot be greater than max" |
| 14.9 | Very Large Number | Enter quantity: 999999999 | Should show error or cap at maximum |
| 14.10 | Special Characters | Enter strategy name: "<script>alert(1)</script>" | Characters escaped. No script execution. |

### UI Behavior

| TC# | Test Case | Steps | Expected Result |
|-----|-----------|-------|-----------------|
| 14.11 | Double Click Submit | Rapidly click "Save" twice | Only one request sent. No duplicate strategy created. |
| 14.12 | Back Button | Fill form half-way, click browser Back | Warning: "Unsaved changes. Leave page?" |
| 14.13 | Refresh During Form | Fill form, press F5/refresh | Warning: "Unsaved changes will be lost" |
| 14.14 | Slow Network | Throttle network to slow 3G, perform actions | Loading spinners shown. No timeouts for reasonable actions. |
| 14.15 | Multiple Tabs | Open app in 2 tabs, perform actions in both | Both tabs reflect latest data. No conflicts. |
| 14.16 | Long Table Scroll | 100+ orders in table | Smooth scrolling. Pagination works. No UI freeze. |
| 14.17 | Mobile / Responsive | Resize browser to small screen | UI adjusts responsively. All features accessible. |
| 14.18 | Confirmation Dialogs | Try Force Exit All, Delete Strategy | Confirmation popup appears before destructive actions |
| 14.19 | Loading States | Click any button that fetches data | Loading spinner/skeleton shown while data loads |
| 14.20 | Error State | Disconnect server, try to load data | Friendly error message: "Failed to load data. Try again." with Retry button. |

---

## Quick Reference: Color Coding on UI

| Color | Meaning |
|-------|---------|
| Green | Profit, Success, Active, Filled, Connected |
| Red | Loss, Error, Rejected, Failed, Disconnected, Risk Breach |
| Yellow/Orange | Warning, Pending, Cancelled, Deactivated |
| Gray | Inactive, Disabled, Cancelled |
| Blue | Informational, Neutral |

---

## Quick Reference: Confirmation Popups Required

| Action | Should Show Confirmation? |
|--------|--------------------------|
| Delete Strategy | Yes |
| Deactivate Strategy | Yes |
| Force Exit All Positions | Yes |
| Cancel Order | Yes |
| Modify Order | Yes (Save button in modal) |
| Logout | Optional |

---

*Document generated on 2026-03-19 for Manual QA Testing*
*Total Test Cases: 150+*
