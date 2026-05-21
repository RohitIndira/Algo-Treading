# Implementation Plan — Per-User, Per-Socket Order WebSocket

**Status:** Proposed
**Date:** 2026-05-20
**Supersedes the shared-socket design described in [Walkthrough.md](./Walkthrough.md)**

---

## 1. Problem

`OrderStatusService` currently keeps **one shared `WSClient`** — a single TCP
connection to Indira (`wss://livemiddleware.indiratrade.com/order-notify/websocket`).
Extra users are added by sending another `WSConnectionRequest`
(`{"userId","orderToken"}`) on that same socket via `WSClient.Subscribe()`.

The Indira broker binds a connection to a single authenticated user, so
clients 2..N on the shared socket silently receive **no order-status updates**.
One socket cannot deliver order status for multiple client IDs.

## 2. Goal

Move to **one WebSocket connection per user**. Each user gets its own
connection, order token, heartbeat, reconnect loop and token lifecycle —
fully isolated from every other user.

The per-user building blocks already exist and are reused:
- `WSManager` — `pkg/indira/ws_manager.go` (connection registry keyed by `UserId`).
- `WSClient` — `pkg/indira/websocket.go` (already fully per-user capable:
  own auth, own heartbeat using `w.auth.UserId`, own reconnect/monitor).

This change is largely **removing the shared-socket layer**, not building new
infrastructure.

## 3. Target Architecture

```
Client A ─ StartSubscription(A) ─► WSClient A ─► socket A ─► Indira
Client B ─ StartSubscription(B) ─► WSClient B ─► socket B ─► Indira
Client C ─ StartSubscription(C) ─► WSClient C ─► socket C ─► Indira

  each WSClient: own token, own heartbeat (45s), own reconnect, own monitor
                          │
       per-user reader goroutine drains WSClient.Updates
                          │
                handleStatusUpdate()   (unchanged)
                per-order mutex stays GLOBAL (broker order IDs are unique)
```

Per user = 1 TCP connection + read/write/monitor goroutines + 1 reader
goroutine inside `statusservice`.

## 4. Changes by File

### Phase 1 — `services/trade-execution/internal/indira/client.go` (ExecutionClient)

- **Remove** `sharedWS`, `sharedWSMu`, `GetSharedWSClient()`.
- **Add** `GetUserWSClient(ctx, auth) (*WSClient, error)` → delegates to the
  existing `c.wsManager.GetOrCreateClient(ctx, auth)` (keyed by `UserId`,
  idempotent, auto-connects).
- **Add** `CloseUserWS(userID string)` → `c.wsManager.CloseClient(userID)`.
- **Add** `CloseAllWS()` → `c.wsManager.CloseAll()` (graceful shutdown).
- `SubscribeOrderStatus` — currently unused by statusservice; delete or leave.

### Phase 2 — `services/trade-execution/internal/statusservice/service.go` (main rewrite)

Replace single-connection state with a per-user registry:

- **Struct changes:**
  - Remove `wsClient`, `wsMu`, `processorRunning`.
  - Add `wsClients sync.Map` (`userID → *WSClient`).
  - Add `processors sync.Map` (`userID → context.CancelFunc`).
  - Keep `subscriberAuths` and `orderMutexes`. The per-order mutex stays
    **global** — broker order IDs are globally unique, so cross-user fills
    are still serialized correctly.

- **`StartSubscription(ctx, userID, auth)`** — idempotent:
  1. Store auth in `subscriberAuths`.
  2. If `wsClients` already has an active client for `userID` → return
     (just refresh auth).
  3. Else `client, err := execClient.GetUserWSClient(ctx, auth)`.
  4. Wire **per-user** callbacks on that client:
     - `OnReconnected` → metrics only. No `resubscribeAll` — on reconnect,
       `dialLocked()` already re-sends that user's own `WSConnectionRequest`.
     - `OnAuthRefresh` → `refreshAuthFromDB` (unchanged, already per-user).
     - `OnTokenExpired` → notify **only that user** (not all users).
       Correctness win: one user's expired token no longer affects others.
  5. Store in `wsClients`; spawn dedicated
     `go s.processUpdates(perUserCtx, userID, client)`.

- **`processUpdates(ctx, userID, client)`** — now per-user: reads
  `client.Updates`, dispatches each event to `go handleStatusUpdate(...)`.
  One processor per connection instead of one global.

- **`StopSubscription(userID)`** — now actually tears down: cancel the
  processor context, `execClient.CloseUserWS(userID)`, delete from
  `wsClients` / `subscriberAuths`.

- **`ResumeUserSubscription(userID, auth)`** — look up that user's client in
  `wsClients`, call `ResumeWithNewAuth`.

- **Delete** `resubscribeAll()` (shared-mode only).

- **`handleStatusUpdate` / `publishNotification` / `ReconcileOrderFromBrokerBook`**
  — unchanged. Routing already works off `wsStatus.UniqueCode` → DB →
  `order.UserID`.

### Phase 3 — `pkg/indira/websocket.go`

- `WSClient` needs **no behavior change** — already per-user.
- Optionally delete the now-unused shared-mode `Subscribe()` method to
  prevent future misuse.

### Phase 4 — `services/trade-execution/cmd/main.go`

- Wiring mostly unchanged — already calls `StartSubscription(ctx, userID, auth)`
  per user (lazy executor path + startup pre-warm loop both work naturally;
  each user now gets its own socket).
- **Add** `indiraClient.CloseAllWS()` to the shutdown sequence.

### Phase 5 — Metrics & idle cleanup (INCLUDED)

- Add gauge `BrokerWSActiveConnections` — inc on `StartSubscription`,
  dec on `StopSubscription`.
- Keep `BrokerWSReconnects` (now per-user).
- **Idle cleanup:** add a periodic sweep (every ~15 min) that calls
  `StopSubscription` for users with **no live exposure**. Reuse
  `orderRepo.GetUsersWithLiveExposure()` — close any user in `wsClients`
  not in that set. Without this, per-user sockets only accumulate during
  the trading day.

### Phase 6 — Verification

- `go build ./...` in `services/trade-execution`.
- Two-user manual test: place orders for user A and user B; confirm **both**
  receive updates on `/ws/live-orders` (the original bug).
- Kill one user's socket → only that user reconnects; the other is untouched.
- Expire one user's token → only that user receives `token_expired`.
- Leave a user idle past the sweep interval → confirm its socket is closed.

## 5. Trade-offs

| Aspect                  | Shared socket (current) | Per-user socket (planned) |
|-------------------------|-------------------------|---------------------------|
| Multi-client correctness| broken                  | fixed                     |
| Connections / goroutines| 1 conn, ~3 goroutines   | N conns, ~4N goroutines   |
| Token expiry blast radius| all users suspended    | only the affected user    |
| Reconnect               | re-subscribe everyone   | each user independent     |
| Resource cost           | minimal                 | bounded by idle cleanup   |

For a few hundred users the cost is acceptable; the Phase 5 idle-cleanup
sweep keeps connection count bounded.

## 6. Out of Scope

- No DB schema changes.
- No change to `handleStatusUpdate` business logic (VWAP fills, OCO/ML
  hooks, Kafka notifications, manual-exit detection).
- No change to the `/ws/live-orders` frontend protocol.
