# Notifications WS — Frontend Integration Guide

Hand-off for the frontend team. Everything needed to wire the new
`/ws/notifications` channel so users see broker-session-expired,
manual-exit, and other backend-emitted notifications in real time —
and can complete the re-login flow.

The **backend is live on staging** at `wss://manthan.stockk.trade/ws/notifications`. Nothing to deploy on your side — just consume.

---

## 1. What this is

A per-user WebSocket stream of notifications produced by backend
services (rules-engine, trade-execution, replayer, etc.) on the Kafka
topic `manthan.notifications`. The gateway bridges Kafka → WS so you
get push delivery without polling.

**Concrete use cases shipping today:**
- Broker session expired (`AU004` detected by reconciler / safety
  monitor / entry handler / protective replayer / external-activity
  detector) → show banner + force re-login.
- JWT expiring within 8 hours (pre-warning) → soft warning toast.
- Manual exit detected (user closed a position via the broker app
  directly) → toast acknowledging the externally-driven change.

The same channel is meant for any future user-facing notification —
keep your handler generic and switch on `type`.

---

## 2. Architecture (read once, then forget)

```
  trade-execution / rules-engine          api-gateway              your browser
  ──────────────────────────────         ─────────────             ────────────
  AU004 detected (5 sites)        ──┐
  manual exit detected            ──┤
  JWT expiry warning              ──┤  Kafka topic            WS push
                                    └─▶ manthan.notifications ───▶ /ws/notifications
                                          │                          │
                                          ▼                          ▼
                                    Consumer reads               useNotifications
                                    + per-user Broadcaster        hook fans out to
                                    fan-out (non-blocking)        Banner + toasts
```

You only need to think about the right-hand column.

---

## 3. WebSocket endpoint spec

### URL
```
wss://manthan.stockk.trade/ws/notifications?user_id={userId}
```
| Param | Required | Notes |
|---|---|---|
| `user_id` | yes | The Indira `clientId` of the logged-in user, e.g. `ND03920`. |

> Auth note: matches the existing `/ws/matches` pattern (query-param
> based). If we add JWT-in-query later, this endpoint will follow
> the same convention — your hook should already accept a `token`
> argument so the upgrade is a one-line change.

### Lifecycle messages

Every message is a single JSON object. **Switch on the `type` field.**

#### `connected` (sent once, on open)
```json
{ "type": "connected", "message": "Connected to notifications feed", "user_id": "ND03920" }
```

#### `heartbeat` (every 30s)
```json
{ "type": "heartbeat", "timestamp": 1715760000 }
```
Use it as a liveness signal. If you don't see one for ~60s, treat the
connection as dead and reconnect.

#### Notification events
```json
{
  "type": "SESSION_EXPIRED",
  "severity": "error",
  "user_id": "ND03920",
  "strategy_id": "fb7831f5-...",
  "signal_id": "",
  "symbol": "",
  "title": "Broker session expired",
  "message": "Your broker session was invalidated. Open positions are unprotected until you re-login.",
  "action_hint": "RELOGIN",
  "timestamp": "2026-05-15T10:42:11.123Z"
}
```

Field reference:

| Field | Type | Meaning |
|---|---|---|
| `type` | string | Event class. Today: `SESSION_EXPIRED`, `JWT_EXPIRING`, plus rules-engine emits position/order events (e.g. `MANUAL_EXIT_DETECTED`). New types may appear without a release — your handler **must** tolerate unknown values (fall back to a generic toast). |
| `severity` | `info` \| `warning` \| `error` \| `critical` | Drives toast color and modal vs. inline. |
| `user_id` | string | Always present. The gateway only delivers to the matching user — you don't need to re-filter, but you can use it as a sanity check. |
| `strategy_id` | string | Optional. Present when the event ties to a specific strategy — link to that strategy's page. |
| `signal_id` | string | Optional. Origin signal/event id for log correlation. |
| `symbol` | string | Optional. Stock symbol if the event is symbol-specific. |
| `title` | string | One-line headline — show in toast / banner. |
| `message` | string | Longer description — show in modal / detail view. |
| `action_hint` | string | Optional UI hint, e.g. `"RELOGIN"`. Treat as advisory. |
| `timestamp` | string | ISO-8601, set by the producer (UTC). |

### Per-user delivery guarantee

- Gateway only writes a message to `/ws/notifications?user_id=X` if the
  notification's `user_id == X`. Other users' events never reach you.
- If multiple tabs open the same `/ws/notifications?user_id=X`, **all
  of them receive every message** (intentional — every tab should
  update its UI). Dedup on the frontend if you don't want duplicate
  toasts across tabs (recommend: `BroadcastChannel` API).
- Non-blocking on the gateway side: a slow client will drop messages
  (small per-subscriber buffer) but never back-pressure the bus. This
  is invisible in practice unless you intentionally pause the JS event
  loop for long.

---

## 4. The `useNotifications` hook

Drop this in `src/hooks/useNotifications.ts`. It handles connect, parse,
heartbeat-watchdog, exponential-backoff reconnect, and unmount cleanup.

```ts
// src/hooks/useNotifications.ts
import { useEffect, useRef, useState, useCallback } from "react";

export type NotificationSeverity = "info" | "warning" | "error" | "critical";

export interface Notification {
  type: string;            // e.g. "SESSION_EXPIRED", "JWT_EXPIRING", "MANUAL_EXIT_DETECTED", ...
  severity: NotificationSeverity;
  user_id: string;
  strategy_id?: string;
  signal_id?: string;
  symbol?: string;
  title: string;
  message: string;
  action_hint?: string;
  timestamp: string;       // ISO-8601
}

export type NotificationHandler = (n: Notification) => void;

export interface UseNotificationsOpts {
  userId: string;
  onNotification: NotificationHandler;
  /** Called when the WS goes live. Use for connection-status UI. */
  onOpen?: () => void;
  /** Called on every disconnect (auto-reconnect runs after). */
  onClose?: () => void;
}

/**
 * Subscribes to wss://.../ws/notifications?user_id=X and dispatches every
 * Notification message to `onNotification`. Heartbeat watchdog reconnects
 * if no message lands for ~60s (heartbeats are every 30s server-side).
 * Exponential backoff: 1s → 2s → 4s → 8s → 16s → 30s cap.
 *
 * Returns the WS connection state for optional UI badging.
 */
export function useNotifications(opts: UseNotificationsOpts) {
  const { userId, onNotification, onOpen, onClose } = opts;
  const [connected, setConnected] = useState(false);
  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const lastMsgAtRef = useRef<number>(Date.now());
  const watchdogRef = useRef<number | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const stoppedRef = useRef(false);

  // Stable handler reference so the connect effect doesn't re-run when
  // the parent re-renders.
  const onNotifRef = useRef(onNotification);
  onNotifRef.current = onNotification;
  const onOpenRef = useRef(onOpen);
  onOpenRef.current = onOpen;
  const onCloseRef = useRef(onClose);
  onCloseRef.current = onClose;

  const connect = useCallback(() => {
    if (stoppedRef.current || !userId) return;

    const url = `${
      window.location.protocol === "https:" ? "wss:" : "ws:"
    }//${window.location.host}/ws/notifications?user_id=${encodeURIComponent(
      userId
    )}`;

    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      retryRef.current = 0;
      lastMsgAtRef.current = Date.now();
      setConnected(true);
      onOpenRef.current?.();
    };

    ws.onmessage = (ev) => {
      lastMsgAtRef.current = Date.now();
      let parsed: any;
      try {
        parsed = JSON.parse(ev.data as string);
      } catch {
        return; // ignore non-JSON frames
      }
      // Ignore the server's housekeeping messages.
      if (parsed?.type === "connected" || parsed?.type === "heartbeat") return;
      // Anything else is a real notification.
      if (parsed && typeof parsed.type === "string" && typeof parsed.user_id === "string") {
        onNotifRef.current(parsed as Notification);
      }
    };

    ws.onerror = () => {
      // onclose will fire next; do the bookkeeping there.
    };

    ws.onclose = () => {
      setConnected(false);
      onCloseRef.current?.();
      if (stoppedRef.current) return;
      const backoff = Math.min(30_000, 1000 * Math.pow(2, retryRef.current));
      retryRef.current += 1;
      reconnectTimerRef.current = window.setTimeout(connect, backoff);
    };
  }, [userId]);

  // Heartbeat watchdog: force-close if no traffic for >60s.
  useEffect(() => {
    if (!userId) return;
    watchdogRef.current = window.setInterval(() => {
      if (Date.now() - lastMsgAtRef.current > 60_000 && wsRef.current) {
        try { wsRef.current.close(); } catch { /* noop */ }
      }
    }, 15_000);
    return () => {
      if (watchdogRef.current) window.clearInterval(watchdogRef.current);
    };
  }, [userId]);

  // Lifecycle.
  useEffect(() => {
    stoppedRef.current = false;
    connect();
    return () => {
      stoppedRef.current = true;
      if (reconnectTimerRef.current) window.clearTimeout(reconnectTimerRef.current);
      if (wsRef.current) {
        try { wsRef.current.close(); } catch { /* noop */ }
        wsRef.current = null;
      }
    };
  }, [connect]);

  return { connected };
}
```

Why these defaults:
- **No `useCallback` on `onNotification`** — we mirror it through a ref
  so callers don't have to memoize. This is the single biggest
  developer-experience trap with `useWebSocket`-style hooks.
- **Reconnect on close, not on every error** — `onerror` is fired
  before `onclose`; doing the reconnect in `onclose` avoids double-
  scheduling.
- **60s watchdog** — heartbeats are every 30s; a missed one is fine;
  two missed and we close + reconnect.
- **Stop flag** — set on unmount so a late `onclose` doesn't schedule
  another reconnect after the component is gone.

---

## 5. Wire it in (3 places)

### a) Global dispatcher — once, near the auth boundary

Put this anywhere a logged-in `userId` is in scope. The natural home
is the same level as `AlgoTradingContent` (which already owns
`BrokerSessionBanner` state).

```tsx
// src/components/algo-trading/AlgoTradingContent.tsx  (excerpt)
import toast from "react-hot-toast";
import { useNotifications, Notification } from "@/hooks/useNotifications";

export function AlgoTradingContent({ userId }: { userId: string }) {
  const [brokerSessionExpired, setBrokerSessionExpired] = useState(false);

  useNotifications({
    userId,
    onNotification: (n: Notification) => {
      // Existing banner state — keep its current contract.
      if (n.type === "SESSION_EXPIRED") {
        setBrokerSessionExpired(true);
        toast.error(n.title, { duration: Infinity, id: "broker-session-expired" });
        return;
      }
      // Generic toast for everything else, severity-styled.
      const opts = { duration: n.severity === "info" ? 4000 : 8000 };
      switch (n.severity) {
        case "info":     toast(n.title, opts); break;
        case "warning":  toast(n.title, { ...opts, icon: "⚠️" }); break;
        case "error":
        case "critical": toast.error(n.title, opts); break;
        default:         toast(n.title, opts);
      }
    },
  });

  // ... existing JSX, including the BrokerSessionBanner that already
  // reads `brokerSessionExpired`.
}
```

> **Important**: instantiate the hook **once** per logged-in session
> (e.g. in a top-level layout or a context provider). Multiple
> instances → multiple sockets per tab.

### b) `BrokerSessionBanner` — already exists

The component at `src/components/algo-trading/AlgoTradingContent.tsx`
already renders a banner when `brokerSessionExpired === true`. Keep
its current "Re-login" button; change *what it does* (next section).

### c) The "Re-login" button — wire SSO + `/api/v1/auth/credentials`

After the user re-logs via Indira SSO, you receive `{ jwt, appId,
userId, source }`. **The frontend MUST POST this to the backend** so
server-side flows (rebalancer cron, replayer, hft-engine) keep
running:

```ts
async function persistFreshBrokerCredentials(creds: {
  userId: string;
  appId: string;
  source: string;        // "WEB" | "AND" | "IOS"
  bearerToken: string;   // the fresh JWT
}) {
  const res = await fetch("/api/algo/api/v1/auth/credentials", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Bearer ${creds.bearerToken}`,
      "appId": creds.appId,
      "source": creds.source,
      "userId": creds.userId,
    },
    body: JSON.stringify({
      user_id: creds.userId,
      bearer_token: creds.bearerToken,
      app_id: creds.appId,
      source: creds.source,
    }),
  });
  if (!res.ok) {
    throw new Error(`auth/credentials failed: ${res.status}`);
  }
  // Banner can now clear itself.
  setBrokerSessionExpired(false);
  toast.dismiss("broker-session-expired");
  toast.success("Broker session restored");
}
```

> Note the path is `/api/algo/api/v1/auth/credentials` — the Next.js
> BFF proxy at `/api/algo/[...path]` forwards everything to the
> gateway. All other gateway endpoints follow the same convention.

---

## 6. Reconnect, dedup, multi-tab

- **Reconnect** — handled by the hook (exponential backoff, watchdog).
  Show a small "Reconnecting…" pill in the UI if `connected === false`
  for more than ~3 seconds; users notice silence.
- **Dedup across tabs** — every tab gets every event. If you don't
  want duplicate toasts:
  ```ts
  const channel = new BroadcastChannel("notifications");
  // In onNotification: only show toast if leader-elected by Math.min(performance.now())
  ```
  Or just live with N toasts for N tabs — it's a small annoyance and
  the banner state is per-tab anyway.
- **Replay on reconnect** — there is **none** today. The gateway only
  delivers events that arrive *while you're connected*. If the tab is
  closed and the user misses a `SESSION_EXPIRED`, they discover it on
  the next backend call that 401s. That's fine — the banner is a
  convenience layer, not a guarantee.

---

## 7. Testing

### Trigger a real `SESSION_EXPIRED` (staging)
1. Log in as `ND03920` on `https://manthan.stockk.trade`.
2. Have a backend ops engineer revoke the broker session, OR wait for
   the natural Indira expiry, OR ask backend to manually publish a
   test event:
   ```bash
   # On the staging server:
   echo '{"type":"SESSION_EXPIRED","severity":"error","user_id":"ND03920","title":"Test","message":"Test event","timestamp":"2026-05-15T10:00:00Z"}' \
     | docker exec -i <kafka-container> kafka-console-producer.sh \
       --broker-list localhost:9092 --topic manthan.notifications
   ```
3. The connected tab should immediately show the banner + toast.

### Quick smoke test from the browser console
```js
const ws = new WebSocket("wss://manthan.stockk.trade/ws/notifications?user_id=ND03920");
ws.onmessage = (e) => console.log("notif:", JSON.parse(e.data));
ws.onopen = () => console.log("connected");
ws.onclose = (e) => console.log("closed", e.code, e.reason);
```
You should see `{type:"connected",...}` then heartbeats every 30s.

### Common gotchas
- **Wrong `user_id`** → connection opens, but no events. Verify you're
  passing the Indira `clientId` (`ND03920`), not your internal numeric
  user id.
- **HTTP instead of HTTPS** in dev → use `ws://` instead of `wss://`.
  The hook above auto-picks based on `window.location.protocol`.
- **Multiple hook instances** → multiple sockets per tab. Use a context
  provider or instantiate once at the top.

---

## 8. Acceptance checklist

| | |
|---|---|
| [ ] `useNotifications` hook created at `src/hooks/useNotifications.ts` |
| [ ] Mounted once per logged-in session (no duplicate sockets per tab) |
| [ ] Switch handles `SESSION_EXPIRED` → existing `BrokerSessionBanner` state |
| [ ] Switch handles **unknown types** → generic toast (do NOT throw) |
| [ ] `react-hot-toast` styled by severity (info/warning/error/critical) |
| [ ] "Re-login" button → SSO flow → `POST /api/algo/api/v1/auth/credentials` |
| [ ] On 200 from auth/credentials: clear banner + dismiss the broker-session toast + show success |
| [ ] Reconnects on close with exponential backoff (already in hook) |
| [ ] Closes WS on logout / unmount |
| [ ] (Optional) Connection-status pill when `connected === false` for >3s |
| [ ] Smoke test against staging WS confirmed `connected` + heartbeat messages arrive |

---

## 9. Backend contracts you can rely on

These won't change without coordination:

| Contract | Value |
|---|---|
| WS URL | `wss://manthan.stockk.trade/ws/notifications?user_id={id}` |
| Auth model | Query param `user_id` (same as `/ws/matches`); per-user delivery enforced server-side |
| Welcome message | `{type:"connected", message, user_id}` once on open |
| Heartbeat | `{type:"heartbeat", timestamp}` every 30s |
| Notification shape | The `Notification` interface in §3 / §4 — stable, additive (we add fields, never remove) |
| Per-user routing | Server filters by `user_id`; you never see other users |
| Reliability | Best-effort while connected; no replay/queue (events you miss while offline stay missed) |
| Token refresh endpoint | `POST /api/v1/auth/credentials` — already documented in `API_DOCUMENTATION.md` |

Backend producer sites (so you know where new event types could come
from in future):
- `services/rules-engine/internal/manthan/notification_publisher.go` — rule-driven events (manual exits, etc.)
- `services/trade-execution/internal/manthan/jwt_expiry_notifier.go` — JWT/session events

If you need a new field on the `Notification` payload, ping backend —
the producer + gateway shape need to evolve together but it's a small
change (one Go struct + one TS interface).
