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
