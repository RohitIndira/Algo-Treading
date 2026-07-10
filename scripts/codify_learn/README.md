# Codify WSS Learning Scripts

Two scripts, run in two terminals. Together they let you fire test orders on
Codify and capture every WebSocket event the broker sends, so we understand
the real event shapes before writing production code.

## Files

| File | Purpose |
|---|---|
| `00_wss_logger.py` | Silent listener. Connects to Codify's order-notify WSS, logs every event to `/tmp/wss_events_<timestamp>.jsonl`, prints one-line summaries to screen. Runs until Ctrl+C. |
| `01_wss_investigate.py` | Fires 8 test orders (1 share IDEA) so we see every event type. Pauses between each so you can watch the logger. |

## What we're learning

Every WSS event has fields like `OrderStatus`, `TradedPrice`, `TradedQTY`.
Codify's docs only show one example. We need to see the FULL menagerie —
what does a `REJECTED` event look like? A `MODIFIED` one? A partial fill?

Once we know the exact shapes, we can update our `trade-execution` code
to react to them correctly (e.g., compute realized_pnl from the real
`TradedPrice` when a SL SELL fills).

## How to run

### Terminal 1 (start FIRST — the logger)

```bash
export JWT="eyJhbGciOi...(your fresh S4450 JWT)"
python3 00_wss_logger.py
```

You should see:

```
Codify WSS Order-Notify Logger  |  user=S4450
Log file:  /tmp/wss_events_20260708_140000.jsonl
Connecting to wss://livemiddleware.indiratrade.com/order-notify/websocket
↓ #1 handshake reply: {'status': 'Ok'}
```

Now it waits. Leave it running.

### Terminal 2 (fire the tests)

```bash
export JWT="eyJhbGciOi...(SAME JWT)"
export APP_ID="7d880cc909a67e1cdb9a3a6e4bf6cfca1783492282293"
python3 01_wss_investigate.py
```

Between each test the script pauses and asks you to press ENTER when
you've observed the events. Watch Terminal 1 or Postman during those
pauses.

### JWT and APP_ID sources

- **JWT** — copy from Postman environment `Manthan-Codify Live` → `jwt`
- **APP_ID** — decode the JWT payload (base64) and read the `appId` claim.
  Or in Postman env → `appId`.
- Both expire — if you get 401, refresh by logging into Indira mobile app.

## Safety

- Tests 1-7 place orders that can't fill (price too far from market
  or nonsense params). **Zero rupees spent.**
- Test 8 (Market BUY + Market SELL) is real. Costs the bid-ask spread —
  usually ₹0.05 to ₹2. The script **explicitly asks** `yes` before running it.
- All orders use symbol IDEA (S4450 doesn't hold it) so nothing can
  accidentally trigger a real position exit.

## What to do with the log file after

The log at `/tmp/wss_events_YYYYMMDD_HHMMSS.jsonl` has one JSON per line:

```json
{"ts":"2026-07-08T14:30:15.123","event":{"OrderStatus":"OPEN","OrderNumber":"NZ...","Symbol":"IDEA",...}}
{"ts":"2026-07-08T14:30:45.789","event":{"OrderStatus":"CANCELLED",...}}
```

Send that file path back to me (or open it in the IDE). We'll write a
tiny parser that extracts every unique `OrderStatus` value and shows one
sample per status. That becomes the design reference for the
FillHandler code fix.

## Prerequisites

- Python 3.10+
- `pip install --user websockets`
- Fresh Codify JWT (obtained via mobile app SSO login)
- Manthan-Codify Live environment activated in Postman (optional but nice to watch alongside)
