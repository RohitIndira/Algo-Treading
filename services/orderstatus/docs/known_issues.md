# orderstatus — known issues

## Deferred to a coordinated pkg/indira "broker auth robustness" PR

- **doRequest AU004 detection uses `HasPrefix(InfoID, "AU0")`.** Covers AU001/AU004
  (the live cases). Hardening it to the richer `CodifiInfoID.IsAuthError()` would catch
  any non-`AU0` auth code — but that edits shared `pkg/indira` (used by
  trade-execution / orderstatus / rebalancer / positions), so it ships as a separate
  coordinated PR, not bundled here.
- **WSS boot-frozen auth (4a).** `wss.Listener` captures each user's auth at boot and
  never refreshes it, so a mid-session JWT rotation silently kills the WSS reconnect until
  a service restart (REST polls still cover). The auth-refresh fix belongs with the
  IsAuthError hardening above.

## Fix 4 verification (2026-07-31): AU004 read-path is already correct — no fix

The AU004 "silent fill-miss" risk does **not** exist: `pkg/indira` `doRequest`
(client.go:196-218) parses the response body **before** the HTTP-status gate and returns
`ErrAuthExpired` for both the 200-with-`{"infoID":"AU004"}` body and the 401 body.
`GetOrderBook`/`GetTradeBook` propagate it via `%w`, so `errors.Is(err, ErrAuthExpired)`
fires in every poll path. This PR only added **log parity** — the orderbook `pollUser`
now logs AU004 distinctly, matching the tradebook path (grep `"auth expired"` catches
both REST paths).

## Verification log

### Phase 4 live check (2026-07-31)

- **AU004 read-path — LIVE CONFIRMED.** A real `GetTradeBook` to production
  (`livemiddleware.indiratrade.com`) with an inactive-session token returned **HTTP 401**
  `{"infoID":"AU004","infoMsg":"Session expired"}`; `pkg/indira` `doRequest` surfaced it
  as `ErrAuthExpired` and both REST paths logged "auth expired". Confirms Fix 4's 401
  branch against the real broker.
- **Token gotcha (resolved).** The `service:nextjs` login-response JWTs (no `loginSource`
  claim) have NO active broker session → AU004 on every strict endpoint (order-book +
  trade-book), regardless of `sso`/source. The API-valid token is the HS512
  `loginSource:SSO`, `role:ACTIVE` token (carries `vendorAppCode`). With that token,
  trade-book returns **HTTP 200** `{"infoID":"0","data":{"trades":[...]}}`.
- **`sso` header case does NOT matter.** Both `sso: True` (what the service sends) and
  `sso: true` return 200 with a valid token — so the capital-`True` in `doRequest` is
  fine, not a bug.
- **Live sweep path — VALIDATED.** With the valid token, the service code
  (`GetTradeBook` → `RunTradebookSweepOnce`) ran clean against production: `GetTradeBook
  OK (trades:0)`, sweep completed with 0 observed/inserted/failed. Confirms the live broker
  call + the nested `{"data":{"trades":[]}}` parse (GetTradeBook's fallback) + the sweep
  wiring end-to-end.
- **Still deferred to a day with actual S4450 fills:** real `REST_TRADEBOOK` rows in
  `broker_events`, the `order.events` wire check, positions `TradedPrice`, and a *direct*
  live `TradeTime` sample. S4450 had zero orders/trades on 2026-07-31, and the tradebook
  endpoint is day-scoped (filtering by a historical order-id still returns empty), so no
  real trade row was available to insert. The DB-layer behavior is covered by the
  integration test; TradeTime=IST by the `ordDate` sibling proof.

### TradeTime timezone — empirically verified IST (2026-07-30)

The tradebook cutoff guard parses `TradeBook.TradeTime` (`"2026-01-02 15:04:05"`,
no TZ suffix) as `Asia/Kolkata`. Verified against real data rather than assumed:

- The broker's sibling naive timestamp `OrderBook.ordDate` (same `/portfolio-services/`
  API family, same format) was compared to `broker_events.observed_at` (stamped IST by
  Postgres) across all 175 orderbook rows.
- **Decisive evidence:** the minimum delta is **3 seconds** (`ordDate=15:12:56`,
  `observed_at=15:12:59`). If `ordDate` were UTC, that value would be `20:42 IST` —
  *after* the observation, an impossible negative delta. A ≥3s positive delta at that
  magnitude is only possible if `ordDate` is IST.
- The large deltas (up to 6.4h) are boot catch-up: `08:50` AMO orders all observed at the
  `15:12` startup sweep — poll latency, not a 5.5h TZ offset (which would floor every
  delta at 19800s).

Residual: a *direct* `TradeTime` sample was not captured — the only local broker token
(S4450) is dated 2026-07-17 (stale → AU004) and no tradebook fixture exists. `TradeTime`
shares `ordDate`'s API family/format, so IST is well-founded. If a fresh JWT becomes
available, capture one live `TradeTime` row and confirm directly.

---


## OPEN: multi-fill manual-sell double-count (cross-source event_seq not unified)

**Severity:** Medium (data — over-counts a manual sell). Rare in MANTHAN-only mode today.

**Status:** Accepted trade-off for shipping the tradebook loop now. Full fix is the
follow-up cross-source `event_seq` unification PR.

### Scenario
`broker_events` dedups on `UNIQUE (broker_order_id, event_seq)`, but the three
observation paths derive `event_seq` differently and do **not** agree for the same
fill:

| Source | event_seq derivation |
|--------|----------------------|
| WSS | `MessageSequenceNumber` |
| REST orderbook | `exchOrdId * 1000 + cumulative tradedQty` |
| REST tradebook (this PR) | `ExchTrdId` (exchange trade id, per-trade) |

So one fill seen by both the orderbook poll **and** the tradebook sweep inserts **two**
rows (different `event_seq`) and publishes **two** `order.events`. Downstream this is
harmless for the Manthan flow — positions svc dedups BUY by `broker_order_id` and
Manthan SELL by lot state (ACTIVE→EXITED). But the **manual-sell FIFO** path dedups its
audit rows by `event_id = broker_order_id-event_seq`, so two events with different
`event_seq` are applied **twice** → the position is over-sold in `positions_db`.

### Blast radius
Manual sells (user sells via the broker mobile app, no WSS event) that are observed by
BOTH the orderbook poll and the tradebook sweep, AND fill in multiple trades. Single-fill
manual sells are unaffected in practice; MANTHAN-only deployments rarely do manual sells.

### Detection query (positions_db)
A manual sell should apply once per broker fill. More `MANUAL_SELL_APPLIED` audit rows
than the broker actually reported for a `(user, symbol, day)` indicates a double-apply:

```sql
SELECT user_id, symbol, DATE(observed_at) AS day, COUNT(*) AS applied_rows
FROM position_events
WHERE event_type = 'MANUAL_SELL_APPLIED'
GROUP BY user_id, symbol, DATE(observed_at)
HAVING COUNT(*) > /* expected number of manual-sell fills from the broker tradebook */ 1;
```

Cross-check the count against `order_status_db.broker_events` for the same
`broker_order_id` (how many FILLED rows exist across sources).

### Full fix (follow-up PR)
Unify WSS + orderbook onto a single order-level key `(exchange_order_id,
cumulative_filled_qty)`, add a downstream "later FILLED with a better price supersedes"
contract in positions svc so a price-correction event doesn't re-run FIFO, and migrate
(`observed_at >= migration_ts` cutoff so new-scheme seqs don't collide with historical
rows). Tracked separately.

### This-PR mitigation
None on the write side. The tradebook loop logs a **loud WARN** on any
`(broker_order_id, event_seq)` collision it hits (`observeTradeEvent` in
`internal/reconciler/reconciler.go`), so if an `ExchTrdId` ever coincides with an
existing WSS/orderbook seq we see it live rather than silently dropping the row.
