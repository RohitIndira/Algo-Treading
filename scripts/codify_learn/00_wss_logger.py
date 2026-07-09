#!/usr/bin/env python3
"""
Codify Order-Notify WSS Logger
==============================

Silently connects to the Codify order-notify WebSocket for the user and:
  1. Logs every incoming event to /tmp/wss_events_<timestamp>.jsonl (one JSON per line)
  2. Prints a compact human-readable summary of each event to stdout
  3. Sends heartbeats every 45s so the connection stays alive

Runs until you press Ctrl+C. Reconnects automatically on disconnect.

USAGE:
  export JWT="eyJhbG..."
  export APP_ID="7d880cc..."      # not strictly needed for WSS but validated
  export USER_ID="S4450"          # optional, default S4450
  python3 00_wss_logger.py

  # In a separate terminal, run 01_wss_investigate.py to trigger events.
  # This logger captures them all.

OUTPUT FILE: /tmp/wss_events_YYYYMMDD_HHMMSS.jsonl
"""

import asyncio
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime

try:
    import websockets
except ImportError:
    print("ERROR: pip install websockets", file=sys.stderr)
    sys.exit(2)


JWT = os.environ.get("JWT", "").strip()
APP_ID = os.environ.get("APP_ID", "").strip()
USER_ID = os.environ.get("USER_ID", "S4450").strip()

BASE_URL = "https://livemiddleware.indiratrade.com"
WSS_URL = "wss://livemiddleware.indiratrade.com/order-notify/websocket"

LOG_FILE = f"/tmp/wss_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"

HEARTBEAT_INTERVAL = 45  # seconds — safely under 5-min server timeout
RECONNECT_DELAY = 5      # seconds — wait before reconnecting after drop


# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────
def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def get_wss_token() -> str:
    """REST call: /order-notify/ws/createWsToken → returns 64-char orderToken."""
    req = urllib.request.Request(
        f"{BASE_URL}/order-notify/ws/createWsToken", method="GET"
    )
    req.add_header("Authorization", f"Bearer {JWT}")
    req.add_header("Content-Type", "application/json")
    req.add_header("sso", "True")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Token API HTTP {e.code}: {body}")

    if data.get("status") != "Ok":
        raise RuntimeError(f"Token API failed: {data}")
    return data["result"][0]["orderToken"]


def summarize_event(event: dict) -> str:
    """Pull the fields we care about most into a one-line summary."""
    parts = []
    for k in ("OrderStatus", "Symbol", "OrderNumber", "Buy_Sell",
              "OrderType", "Product",
              "TradedQTY", "TradedPrice",
              "OrderPrice", "TriggerPrice", "PendingQty",
              "Reason"):
        if k in event and event[k] not in (None, "", "0"):
            parts.append(f"{k}={event[k]}")
    return "  ".join(parts) if parts else "(no notable fields)"


# ────────────────────────────────────────────────────────────
# Main WSS loop
# ────────────────────────────────────────────────────────────
async def listen_once(log_fh):
    """One full connect → listen → disconnect cycle. Returns on close."""
    log("Fetching fresh WSS token via REST…")
    token = get_wss_token()
    log(f"Token: {token[:16]}…{token[-8:]}")

    log(f"Connecting to {WSS_URL}")
    async with websockets.connect(WSS_URL, ping_interval=None, open_timeout=10) as ws:
        # Handshake
        handshake = {"userId": USER_ID, "orderToken": token}
        await ws.send(json.dumps(handshake))
        log("→ handshake sent")

        # Heartbeat co-routine
        async def heartbeat():
            while True:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                try:
                    await ws.send(json.dumps({"userId": USER_ID, "heartbeat": "h"}))
                    log("→ heartbeat")
                except Exception:
                    return

        hb_task = asyncio.create_task(heartbeat())

        # Recv loop
        event_count = 0
        try:
            async for raw in ws:
                event_count += 1
                # Try to parse as JSON
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    log(f"↓ #{event_count} (non-JSON): {raw[:200]}")
                    log_fh.write(json.dumps({
                        "ts": datetime.now().isoformat(),
                        "raw": raw if isinstance(raw, str) else raw.hex(),
                    }) + "\n")
                    log_fh.flush()
                    continue

                # Handshake reply looks like {"status":"Ok"} — 1 line
                if isinstance(event, dict) and set(event.keys()) == {"status"}:
                    log(f"↓ #{event_count} handshake reply: {event}")
                else:
                    log(f"↓ #{event_count}  {summarize_event(event)}")

                # Save FULL event to log file
                log_fh.write(json.dumps({
                    "ts": datetime.now().isoformat(),
                    "event": event,
                }) + "\n")
                log_fh.flush()
        finally:
            hb_task.cancel()


async def main():
    if not JWT:
        print("ERROR: set JWT env var first:", file=sys.stderr)
        print('  export JWT="eyJhbG..."', file=sys.stderr)
        sys.exit(2)

    print()
    print("═" * 60)
    print(f"Codify WSS Order-Notify Logger  |  user={USER_ID}")
    print("═" * 60)
    print(f"Log file:  {LOG_FILE}")
    print(f"Heartbeat: every {HEARTBEAT_INTERVAL}s")
    print(f"WSS URL:   {WSS_URL}")
    print()
    print("Ctrl+C to stop.")
    print()

    with open(LOG_FILE, "a") as f:
        # Write file header (parseable by tools)
        f.write(json.dumps({
            "ts": datetime.now().isoformat(),
            "meta": {
                "user_id": USER_ID,
                "wss_url": WSS_URL,
                "logger_version": "1.0",
            },
        }) + "\n")
        f.flush()

        while True:
            try:
                await listen_once(f)
                log("Connection closed by server — reconnecting…")
            except KeyboardInterrupt:
                raise
            except Exception as e:
                log(f"ERROR: {type(e).__name__}: {e}")

            log(f"Sleeping {RECONNECT_DELAY}s then reconnecting…")
            await asyncio.sleep(RECONNECT_DELAY)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print()
        print(f"[{ts()}] Stopped by user. Log written to: {LOG_FILE}")
