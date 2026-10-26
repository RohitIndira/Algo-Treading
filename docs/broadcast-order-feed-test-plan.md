# Broadcast order feed — test plan (before implementation)

Date: 2026-10-26. Owner: trade desk / platform. Status: plan agreed before build.

## What is being built

A second listener in the **orderstatus** service that subscribes **once** to Indira's
dealer broadcast socket (`wss://livemiddleware.indiratrade.com/order-notify/websocket/broadcast`)
and routes every frame to the existing per-user pipeline by `UCC` (client code).
Frames are the same `ORD_NRML` shape the per-user listener already parses
(`services/orderstatus/internal/wss/listener.go` → `handleWSEvent`, routes by `UCC`,
dedupes by broker order id + sequence). New parts: token fetch
(`GET /order-notify/ws/createWsToken` with a live session), the
`{"userId","orderToken"}` handshake, a 50 s heartbeat, reconnect on the 5-minute
idle close, re-registration when the anchoring session expires, and an
"anchor session" chooser (dedicated dealer login preferred; any live client
session as fallback).

Why: today we see a client's orders only while that client has a session with us.
FIV99's nine positions left the account during such a blind period. With the
broadcast, a manual sell by any client is booked as MANUAL_EXIT at the real price
within a minute, our own fills are booked from the exchange's word even when the
client's session lapsed, and AMO re-ids are seen at conversion time.

The feed is **read-only**. Connecting and listening places, modifies or cancels
nothing, so every stage below can run against the real broker.

## Stage 1 — unit (no network)

| # | Case | Expect |
|---|------|--------|
| 1.1 | Handshake frame built from (userId, orderToken) | exact JSON text, no extra fields |
| 1.2 | Heartbeat frame `{"userId":"…","heartbeat":"h"}` every 50 s | timer fires at 50 s, stops on close |
| 1.3 | Frame routing by `UCC` | frame for S4450 reaches S4450's handler; unknown UCC counted, not written |
| 1.4 | Same frame arriving via per-user socket AND broadcast | one stored event, one published event (dedupe by broker order id + `MessageSequenceNumber`) |
| 1.5 | Out-of-order frames (sequence 5 before 4) | store keeps both, consumers apply by sequence as today |
| 1.6 | `TradedQTY`/`TradedPrice` per-frame vs cumulative | filled qty and price computed as the existing listener does (FIX 7 path) |
| 1.7 | AMO frames (`OrderStatus` "AMO SUBMITTED", `AMOOrderID` vs `UniqueCode`) | event carries both ids so the AMO-conversion matcher can adopt the new id |
| 1.8 | Idle close / server close / read error | reconnect with backoff 1 s → 2 s → 5 s → 10 s, capped; no busy loop |
| 1.9 | Token endpoint returns 401 | listener marks anchor session dead, tries the next candidate session, alerts once |
| 1.10 | Anchor chooser | prefers the configured dealer login; falls back to the newest live client session; never picks an expired one |
| 1.11 | Malformed frame | logged, counted, skipped; connection stays up |
| 1.12 | Manual sell frame for a client with no platform session | published to `order.events` exactly like a session user's frame |

## Stage 2 — integration (local stack, fake broker socket)

A small fake server speaks the protocol (token endpoint, handshake, heartbeat
check, broadcast frames, idle close after 5 min without heartbeat).

| # | Scenario | Expect |
|---|----------|--------|
| 2.1 | Full happy path | token → handshake `{"status":"Ok"}` → frames stored in `order_status_db.broker_events` with `source=WSS_BROADCAST` → `order.events` published |
| 2.2 | Manual sell of a Manthan lot by a client without a session | positions service books MANUAL_EXIT at the frame's traded price; trade-execution ledger projector writes the SELL row; rules-engine frees the slot (chain already exists) |
| 2.3 | Our own stop fill seen first on broadcast | trade-execution books SL_FILLED once; no duplicate when the REST poll later sees it |
| 2.4 | Partial fill then remainder | two frames → quantities add up; exit-price sweeper later corrects to the trade-book average as today |
| 2.5 | AMO placed at 16:00, converted at 08:50 with a new id | the frame at conversion lets the reconciler adopt the new id instead of matching the previous night's rejection |
| 2.6 | Fake server drops the socket every 5 min | reconnects; no frame loss beyond the gap; gap is back-filled by the existing REST order-book poll |
| 2.7 | Anchor session expires mid-day | token refetch fails → fallback session used → feed resumes; one OPS_ALERT, not a storm |
| 2.8 | Broadcast down for 10 minutes during market hours | `FEED_STALE` alert after 3 min without frames while any client has activity; REST poll continues as the safety net |
| 2.9 | Frames for clients we do not manage | ignored, counted in a metric, never create positions |
| 2.10 | Replay of the whole day's frames twice | idempotent: no duplicate events, no duplicate exits |

## Stage 3 — live broker, shadow mode (read-only, one full session)

Deployed to production **with writes disabled** (`BROADCAST_FEED_MODE=shadow`):
the listener connects, parses and **compares** against what the per-user sockets
and REST polls produced, and only logs/metrics.

Pass criteria, measured over one trading day 09:00–16:00 IST:

- connected ≥ 99% of the session; reconnects ≤ 12; no reconnect storm.
- every event seen by the per-user path is also seen on the broadcast (recall 100%); extra events (clients without sessions) listed for review.
- for our own orders, broadcast frame arrives before or within 2 s of the per-user frame.
- manual sells detected for at least one client without a session (if any happen) and reconciled by hand against the back office.
- zero frames with unknown `UCC` that belong to a managed client.
- token refresh after the 5-minute idle close works unattended for the full day.

## Stage 4 — enable (writes on), staged

1. Enable `order.events` publishing for **our own orders only** (frames whose broker id is in our ledger). One session. Verify fills/cancels/rejections are booked from the broadcast and the REST poll finds nothing left to fix.
2. Enable for **all frames of managed clients** (manual sells become MANUAL_EXIT at the real price). One session. Verify against the back office for any manual sell that day.
3. Keep the reconciler (book-vs-broker verdicts) running; its NOT_HELD closures should now be rare and each one must have a clear story.

## Operational checks shipped with the feature

- Metrics: `broadcast_connected`, `broadcast_last_frame_age_seconds`, `broadcast_frames_total{ucc}`, `broadcast_reconnects_total`, `broadcast_unknown_ucc_total`, `broadcast_anchor_session{user}`.
- Alerts (manthan.notifications, admin attention): feed stale > 3 min in market hours; anchor session lost with no fallback; token endpoint failing > 5 min.
- Rollback: `BROADCAST_FEED_MODE=off` and restart orderstatus; the per-user sockets and REST polls are untouched by this feature.

## Decisions needed before build

1. Anchor session: dedicated dealer/ops login (preferred) or S4450 with fallback.
2. Whether frames for clients that are not Manthan users should be stored at all (default: counted only).
3. Alert channel for feed-stale (admin attention only until the WhatsApp channel exists).

## Operations (shipped 2026-10-26)

Code: `services/orderstatus/internal/wss/broadcast.go` (listener, modes, shadow
compare, stale monitor), `broadcast_anchor.go` (anchor chooser),
`broadcast_metrics.go`, `internal/alert` (OPS_ALERT on `manthan.notifications`),
`internal/tradeexec` (LookupOrderMeta client for own_orders), migration
`services/orderstatus/migrations/004_broker_events_source_wss_broadcast.sql`.
Frames go through the same `Listener.ingest` as the per-user socket with
`source = WSS_BROADCAST`; dedup is the existing `UNIQUE (broker_order_id, event_seq)`.

### Modes — `BROADCAST_FEED_MODE`

| Mode | Broker | Store / Kafka | Use |
|------|--------|---------------|-----|
| `off` | not connected | nothing | rollback; service identical to before the feature |
| `shadow` (default) | connected, heartbeat, reconnect | **nothing written**; every managed-client frame is compared against what the per-user socket / REST poll stored | stage 3 |
| `own_orders` | connected | writes/publishes only frames whose `UniqueCode` **or** `AMOOrderID` is in `manthan_orders` (trade-execution `LookupOrderMeta`, positive cache for life, negative 60 s). No lookup reachable → frame skipped (fail closed), `broadcast_skipped_not_own_total` grows | stage 4.1 |
| `all` | connected | writes/publishes every managed-client frame; manual sells become MANUAL_EXIT downstream | stage 4.2 |

Frames for client codes orderstatus does not manage (no active strategy in
user-config) are only counted (`broadcast_unknown_ucc_total`,
`broadcast_frames_total{ucc="other"}`), in every mode.

### Environment

| Var | Default | Meaning |
|-----|---------|---------|
| `BROADCAST_FEED_MODE` | `shadow` | see above; invalid value = service refuses to start |
| `BROADCAST_ANCHOR_USER` | empty | dedicated dealer/ops login whose session fetches the broadcast token. Empty or unusable → newest live (largest JWT `iat`, not expired) session among managed users. A session the broker rejects (AU0xx / 401 / handshake not Ok) is retired until that user logs in again or 30 min pass |
| `TRADE_EXEC_GRPC_ADDR` | unset | trade-execution gRPC for own_orders. In `deployments/separate-namespace/ecosystem.config.js` it is `localhost:9004` |
| `INDIRA_BASE_URL` | livemiddleware | REST base; the socket URL is derived from it (`wss://…/order-notify/websocket/broadcast`) |

`ecosystem.config.js` forwards `BROADCAST_FEED_MODE` and
`BROADCAST_ANCHOR_USER` from `deployments/separate-namespace/.env` (its env
blocks are whitelists). `.env` lines for shadow:

```
BROADCAST_FEED_MODE=shadow
BROADCAST_ANCHOR_USER=
```

Protocol constants (not configurable): handshake reply within 10 s, heartbeat
every 50 s, reconnect when no bytes for 6 min (longer than the broker's
5-minute idle close), backoff 1 s → 2 s → 5 s → 10 s capped, a fresh token on
every reconnect, close frame on shutdown.

### Metrics — `GET :HTTP_PORT/metrics` (orderstatus is `:9006` on the box)

| Metric | Meaning |
|--------|---------|
| `broadcast_connected` | 1 while connected and handshake accepted |
| `broadcast_last_frame_age_seconds` | seconds since the last order frame (−1 before the first) |
| `broadcast_frames_total{ucc}` | frames per managed client; everyone else `other` |
| `broadcast_unknown_ucc_total` | frames for clients we do not manage |
| `broadcast_reconnects_total` | session ends → reconnect attempts |
| `broadcast_anchor_session{user}` | 1 for the user anchoring the live connection |
| `broadcast_anchor_switches_total` | anchors retired after a broker rejection |
| `broadcast_token_errors_total` | failed `createWsToken` calls |
| `broadcast_shadow_recall_total{seen_by_user_path="true"/"false"}` | shadow: did the per-user path store the same `(broker_order_id, event_seq)` within 5 s |
| `broadcast_shadow_lead_seconds` | shadow: per-user `observed_at` − broadcast arrival; positive = broadcast was earlier |
| `broadcast_written_total{result=inserted/deduped/dropped/error}` | own_orders / all: outcome of each hand-off to the store |
| `broadcast_skipped_not_own_total` | own_orders: frames not ours (or lookup failed) |
| `broadcast_malformed_frames_total`, `broadcast_control_frames_total` | non-order frames |

### Alerts

All go to `manthan.notifications` as `OPS_ALERT` (same shape as
trade-execution's, `kind` below, dedup 5 min) and to the log at Error with the
stable message `broadcast feed alert`:

- `BROADCAST_FEED_STALE` — no frame for 3 min between 09:15–15:30 IST on a
  trading day; raised once, re-armed when frames resume.
- `BROADCAST_ANCHOR_LOST` — the anchor session was rejected (once per
  session), or no session at all can anchor the feed (once until one can).
- `BROADCAST_TOKEN_FAILING` — `createWsToken` failing for more than 5 min.

Per-user sockets and REST polls are untouched by any of this; they keep
covering while the broadcast is down.

### Stage 3 — one shadow day, what to read

1. Apply migration 004 only if you intend to flip to a writing mode the same
   week (shadow needs nothing). `.env`: `BROADCAST_FEED_MODE=shadow`; restart
   orderstatus; confirm the log line `broadcast listener enabled mode=shadow`
   and `broadcast connected anchor_user=…`.
2. During the day: `curl -s localhost:9006/metrics | grep ^broadcast_`.
   Expect `broadcast_connected 1`, `broadcast_last_frame_age_seconds` small in
   market hours, `broadcast_reconnects_total` ≤ 12 by 16:00.
3. Recall: `broadcast_shadow_recall_total{seen_by_user_path="false"}` should
   be 0 for clients with a live session. Every miss is logged as
   `broadcast shadow: frame not seen by per-user path` with ucc, broker id,
   event_seq, status — that list is the review list (expected content: clients
   without a session, i.e. the FIV99 case the feature exists for).
   **If misses appear for clients WITH a session, check `event_seq`: a
   `MessageSequenceNumber` that differs between the two sockets for the same
   event would defeat the dedup and must be fixed before any writing mode.**
4. Latency: `broadcast_shadow_lead_seconds` histogram — the bulk should sit at
   ≥ 0 (broadcast first) and never below −2 s for our own orders.
5. Unattended token refresh: `broadcast_reconnects_total` grows slowly across
   the day with `broadcast_token_errors_total` flat and
   `broadcast_anchor_switches_total` 0 (unless the anchor really logged out).

### Flips

- shadow → own_orders: apply migration 004 (`psql … -f services/orderstatus/migrations/004_…sql`
  against order_status_db), set `BROADCAST_FEED_MODE=own_orders`, restart.
  Verify `broadcast_written_total{result="inserted"}` grows only for our ids,
  `broadcast_skipped_not_own_total` grows for everything else, and
  `SELECT source, count(*) FROM broker_events WHERE observed_at > now() - interval '1 day' GROUP BY 1`
  shows `WSS_BROADCAST` rows alongside `WSS` with no duplicate
  `(broker_order_id, event_seq)`. Downstream: trade-execution books fills once;
  the REST poll finds nothing left to fix.
- own_orders → all: `BROADCAST_FEED_MODE=all`, restart. Verify every manual
  sell that day against the back office (positions books MANUAL_EXIT at the
  frame's traded price).

### Rollback

`BROADCAST_FEED_MODE=off` in `.env`, `pm2 restart ecosystem.config.js --only orderstatus --update-env`.
Rows already written with `source = WSS_BROADCAST` are ordinary events and stay.
