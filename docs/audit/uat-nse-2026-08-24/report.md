# UAT Test Report — NSE, Auditor's Checklist (Mock Market Session 2026 v1.0)

**Date:** 2026-08-24, 16:08–16:22 IST
**Environment:** UAT — base URL `https://trade.indiratrade.com` (production API surface, UAT exchange simulator)
**Account:** ND03920 (`loginSource: APP`, source `AND`)
**Evidence files:** `uat-cases-ND03920-NSE.json` (per-case request/response capture), `uat-orderbook-ND03920-1619.json` / `uat-orderbook-ND03920-final.json` (broker order-book snapshots). Screenshots attached separately (SS-1 … SS-7).

---

## 1. Environment validation

| Check | Result | Evidence |
|---|---|---|
| UAT host serves the same middleware API | ✅ identical response envelopes (`AU004` on unauthenticated calls) across order-services, portfolio-services, payments, auth-services | SS-1 |
| Session isolation from production | ✅ a live-valid ND03920 JWT is rejected on UAT (`AU004`); UAT requires its own login | probe log 16:05 IST |
| UAT session established | ✅ order-book 200 with UAT JWT (`userId ND03920`, `loginSource APP`, 24 h expiry) | SS-1 |
| UAT ledger | ✅ available margin ₹3,00,00,000 (test funds) | SS-2 |
| Order-series isolation | ✅ UAT orders carry their own series prefix `NZVAH…` (production/live-mock used `NZWKE…`/`NYMZX…`) | order book |
| Order-status WSS | ⚠️ `/order-notify/ws/createWsToken` is **not routed** on this host (returns the web app) — fill detection must use order-book polling |  |
| UAT sim trading hours | ✅ accepted and filled orders at 16:18 IST (after real NSE close) | A6 |

## 2. Checklist test cases and exchange verdicts

All orders placed for ND03920 on TCS (`STK_TCS_EQ_NSE_11536`, token 11536, tick 0.1); reference price 2284. Timestamps are IST from the broker order book.

| # | Case | Order id | Exchange order no. | Request | UAT verdict |
|---|---|---|---|---|---|
| A1 | New order request/response (resting limit) | NZVAH00001H8 | 1300000000047686 | BUY Limit 1 @ 2215.50 DAY DELIVERY | Requested → resting → **Cancelled** on A8 request (16:18:31) |
| A2 | **Price Check** — band breach high (+25 %) | NZVAH00002H8 | 1300000000047689 | BUY Limit 1 @ 2855.00 | **Rejected — "The order has been cancelled due to price freeze."** |
| A3 | **Price Check** — band breach low (−30 %) | NZVAH00003H8 | 1300000000047690 | BUY Limit 1 @ 1598.80 | **Rejected — price freeze** |
| A4 | **Quantity Check** — per-order qty limit | NZVAH00004H8 | — | BUY Limit 5,000,000 @ 2284.00 | **Rejected — "Quantity is more than Maximum Quantity (47035) allowed by the exchange"** |
| A5 | **Order Value Check** — ≈ ₹12 cr | NZVAH00005H8 | — | BUY Limit 52,539 @ 2284.00 | **Rejected — same 47,035 cap** (value limit expressed as qty) |
| A6 | Market order + **Trade Confirmation** | NZVAH00006H8 | 1300000000047693 | BUY Market 1 | **Executed 1 @ 2251.20**, trade book confirms `tradedPrice 2251.2, tradeTime 16:18:29` |
| A7 | SL order — tick validation | NZVAH00007H8 | — | SELL SL trig 1827.20 / lim 1818.05 | Rejected — `EG003 Price Not in multiple of PriceTick` (probe rounded to 0.05 on a 0.1-tick scrip; validation working) |
| A7′ | SL 20 % below fill — outside DPR | NZVAH00008H8 | 1300000000047761 | SELL SL GTC trig 1801.00 / lim 1795.60 | **Rejected — price freeze** (trigger below DPR floor 2214.4) — confirms the exchange behaviour our `SL_DEFERRED_BAND` policy guards against |
| A7″ | SL inside band — resting | NZVAH00009H8 | 1300000000047940 | SELL SL GTC trig 2225.00 / lim 2220.00 | **Pending (resting)** at 16:22:28 |
| A8 | **Cancel request/response** | (cancels A1) | — | cancel NZVAH00001H8 | "cancel accepted" → order book shows Cancelled |

## 3. Checklist item coverage

| Auditor item | Verdict | Cases |
|---|---|---|
| Price Check / dummy filters | ✅ enforced by UAT exchange (band high/low, tick) | A2, A3, A7, A7′ |
| Quantity Check | ✅ per-order max 47,035 for TCS | A4 |
| Order Value Check | ✅ (as qty cap) | A5 |
| Trade Price Protection | covered under price checks | A2/A3/A7′ |
| New Order Request/Response capture | ✅ full request + `ordId`/`ordStatus` per case in capture JSON | A1–A7″ |
| Trade Confirmation | ✅ trade book with exchange order no. + trade time | A6 |
| Cancel request/response | ✅ | A8 |
| SL place / reject / rest | ✅ all three outcomes exercised | A7, A7′, A7″ |

## 4. Observations / defects

1. **GTC → DAY silent downgrade reproduces on UAT**: A7″ was submitted `ordValidity: GTC`; the order book shows `DAY`. Matches the long-standing production observation (open issue with Indira).
2. **Order-status WSS not available on the UAT base URL** — any system-level UAT run must rely on order-book polling + reconciler (both exist in trade-execution).
3. The UAT simulator trades outside real market hours (fills at 16:18 IST), making it suitable for off-hours regression runs.
4. Strategy-pipeline (rules-engine → trade-execution) UAT execution requires a dedicated trade-execution instance with `INDIRA_BASE_URL=https://trade.indiratrade.com` — NOT executed today (would have repointed live accounts); broker-level API testing only.

## 5. Residual UAT state (for cleanup or continued testing)

- Holding: 1 TCS @ 2251.20 (from A6)
- Resting order: NZVAH00009H8 SELL SL trig 2225.00 (DAY — lapses at UAT session end)
