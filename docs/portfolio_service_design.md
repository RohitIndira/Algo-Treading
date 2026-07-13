# Portfolio Service — Design

Author: rohitt
Started: 2026-07-13

## 1. Purpose

Read-only, per-user portfolio aggregations for the UI:

- **Summary**: invested / current value / realized P&L / unrealized P&L / day change.
- **Active positions**: every ACTIVE lot with entry_price, quantity, unrealized P&L.
- **Closed positions**: every EXITED lot with realized_pnl, paginated by exit_time DESC.

No writes. No state machine. No Kafka producers. This service is a **query
side** on top of `positions_db.positions`, matching the CQRS split we
completed in the positions svc.

## 2. Why a separate service

- `positions_db` is owned by positions svc. api-gateway must NOT
  directly SELECT it — same reason we split `trading_db` away from
  api-gateway during the earlier CQRS work.
- Portfolio queries are heavy joins + aggregations. Keeping them off
  the positions-svc process protects the state-machine hot path from
  UI-driven query load.
- Independent scale. UI traffic bursts on market open + on push
  notifications; positions svc lifecycle traffic is broker-paced.

## 3. Architecture

```
        Browser / mobile
              │  HTTPS + JWT
              ▼
      api-gateway                       ← auth, LTP enrichment,
              │                           tunnel management
              │  gRPC (per-request)
              ▼
      portfolio svc                     ← pure positions_db aggregations
              │
              ▼
         positions_db  (read-only pool)
```

Deliberate split of concerns:

| Concern                | Who                                 |
|------------------------|-------------------------------------|
| JWT / auth             | api-gateway (existing middleware)   |
| Positions data         | portfolio svc → positions_db        |
| LTP / unrealized P&L   | api-gateway (owns Redis + tokens)   |
| Response shape / paging| api-gateway HTTP handlers           |

**Rationale — why LTP is api-gateway's job**: the LTP key is
`market:nse:{exchange_token}` and `positions_db` does not carry
`exchange_token` (it's not in the `order.events` envelope either).
api-gateway already resolves symbol→token via `stockk_trading.manthan_orders`
and already opens the Redis tunnel. Duplicating that plumbing in
portfolio svc would repeat the exact tunnel-management bug documented
in `reference_ltp_tunnel_silent_fail`. Instead — chunk **AG.LTP** hardens
the api-gateway LTP subsystem once, and both live-algos and portfolio
inherit the fix.

## 4. LTP: no silent failures

The current api-gateway LTP client has no runtime health check —
missing keys are indistinguishable from a half-closed tunnel. **AG.LTP**
adds:

1. A background `PING` probe every 5 s against the LTP redis.
2. `atomic.Bool ltpHealthy` — set on ping success, cleared on ping failure.
3. Every response that carries an `ltp` field also carries an
   `ltp_status` field: `HEALTHY | STALE | UNAVAILABLE`.
4. When `!ltpHealthy` OR a per-key MGet returns `nil`, the response
   sets `ltp = null`, `unrealized_pnl = null`, `ltp_status = UNAVAILABLE`.
   Never falls back to 0.

The UI can then show `—` instead of the misleading `₹0` from the
2026-07 incident.

## 5. gRPC surface — portfolio svc

Package: `services/portfolio/api/proto/portfolio.proto`.
Port: `:9005` (next after trade-execution=9004).

### 5.1 `GetPortfolioSummary(user_id) → Summary`

Aggregations over ALL of `positions` for the user:

- `total_invested` = SUM(invested_amount) WHERE status='ACTIVE'
- `total_realized_pnl` = SUM(realized_pnl) WHERE status='EXITED'
- `today_realized_pnl` = SUM(realized_pnl) WHERE exit_time >= today
- `active_lot_count` / `closed_lot_count`
- `manthan_invested` / `user_manual_invested` (split for the UI)

Unrealized P&L is NOT computed here — api-gateway does that after
LTP fetch.

### 5.2 `GetActivePositions(user_id) → []Position`

Every ACTIVE lot with columns needed for the UI card:

`position_id, origin, symbol, exchange, strategy_id, signal_id,
entry_time, entry_price, quantity, invested_amount,
current_sl, high_since_entry`.

Sorted `entry_time ASC` — matches FIFO display + the FIFO exit rule
in §7.2 of `positions_service_design.md`.

### 5.3 `GetClosedPositions(user_id, page, page_size) → []ClosedPosition`

Every EXITED lot, paginated:

`position_id, origin, symbol, entry_time, entry_price, quantity,
exit_time, exit_price, exit_reason, realized_pnl, invested_amount`.

Sorted `exit_time DESC`. Default page_size = 50, max 200.

### 5.4 `HealthCheck` — mirrors the `common.HealthCheckRequest` pattern.

## 6. HTTP surface — api-gateway proxy

Routes (protected sub-router — reuses `AuthMiddleware`):

- `GET /api/v1/users/me/portfolio/summary`
- `GET /api/v1/users/me/portfolio/positions`
- `GET /api/v1/users/me/portfolio/history?page=&page_size=`

Each handler:

1. `auth.UserIDFromContext(r.Context())` — mirror live-algos.
2. gRPC to portfolio svc.
3. For `/summary` and `/positions`: enrich with LTP + unrealized P&L
   via the hardened LTP client.
4. Wrap in the standard Indira envelope
   `{code, message, data, timestamp}`.

## 7. DB access model

Portfolio svc connects to `positions_db` with a **separate read-only
role** `positions_reader` (no INSERT / UPDATE grants). If positions
svc ever gets compromised in the query side, no writes leak. Migration
0002 in `services/portfolio/migrations/` creates that role.

Connection pool: 20 max open, 10 idle, 5 min lifetime — heavier than
positions svc's 10/5 since portfolio queries fan out from UI.

## 8. Chunk plan — 5 chunks

- **PF.A** — Skeleton: `services/portfolio/cmd/main.go` + design-doc
             ratification. Boots, connects positions_db read-only,
             exposes `/health`, gRPC server placeholder.
- **PF.B** — Store read layer: `internal/store/store.go` with three
             query methods matching §5.1-5.3. Real-DB tests.
- **PF.C** — gRPC handlers: proto + generated code + server
             implementations. Wire tests via `bufconn`.
- **AG.LTP** — api-gateway hardening (see §4). Independent of portfolio
             svc — benefits live-algos too. Ships fixed
             `reference_ltp_tunnel_silent_fail` bug.
- **PF.D** — api-gateway HTTP handlers + gRPC client
             (`internal/grpc_clients/portfolio_client.go`). Wires the
             LTP enrichment step. End-to-end tests.

## 9. What ships at end of each chunk

| Chunk | Ships | Value                                  |
|-------|-------|----------------------------------------|
| PF.A  | boots | Deploy target exists                   |
| PF.B  | data  | positions_db reads are unit-tested     |
| PF.C  | RPC   | Can call from grpcurl                  |
| AG.LTP| probe | live-algos stops lying when tunnel dies|
| PF.D  | UI    | Real UI has portfolio pages            |

## 10. Open questions

- **Q1**: Do we need a `strategy_id → strategy_name` join for display?
  For MVP the UI can render the UUID; joining to `stockk_trading.strategies`
  crosses DBs. Defer to PF.D+1 if UX demands.
- **Q2**: Redis cache for summary rows? Not for MVP —
  `positions_db` query is cheap (one user's rows). Add if p99 > 100ms.

## 11. Sign-off checklist before PF.A

- [x] Split of concerns confirmed with user (2026-07-13)
- [x] LTP source decided: Redis via api-gateway with active probe
- [x] Silent-fail hazard addressed by AG.LTP chunk
- [x] `positions_reader` role — approach agreed (deferred to PF.A migration)
