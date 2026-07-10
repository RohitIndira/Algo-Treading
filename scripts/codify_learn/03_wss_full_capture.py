#!/usr/bin/env python3
"""
03_wss_full_capture.py — FULLY AUTOMATED WSS state-machine capture

Single command. Connects to Codify's order-notify WebSocket AND fires
every safe test we can run, back-to-back, no manual pauses. Captures
COMPLETE WSS messages for later analysis.

Auto-detects market open/closed and picks the right test suite:
  • Market OPEN  → Phase A (OPEN, MODIFIED, CANCELLED, EXECUTED events)
  • Market CLOSED → Phase B (AMO SUBMITTED, MODIFIED, CANCELLED events)
  • Both      → Phase C (edge cases)

Output directory: /tmp/wss_capture_<TIMESTAMP>/
  ├─ events.jsonl        — every WSS event (one JSON per line)
  ├─ actions.jsonl       — every REST call: request, response, order_id
  └─ summary.json        — action → order_id → events count

USAGE:
  export JWT="eyJhbG..."
  export APP_ID="8609ea..."
  python3 03_wss_full_capture.py

  # Optional:
  export IDEA_LTP=13.85          # if not set, prompts once or defaults to 13.50
  export SKIP_REAL_MONEY=1       # skip A6 (market round-trip)
  export USER_ID=S4450

Runtime: ~3-4 min if market open, ~1 min if market closed.
Cost: ~₹1-3 real money (A6 round-trip) unless SKIP_REAL_MONEY=1.
"""

import asyncio
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, time as dtime
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

try:
    import websockets
except ImportError:
    print("ERROR: pip install websockets", file=sys.stderr)
    sys.exit(2)


# ────────────────────────────────────────────────────────────
# Config
# ────────────────────────────────────────────────────────────
JWT = os.environ.get("JWT", "").strip()
APP_ID = os.environ.get("APP_ID", "").strip()
USER_ID = os.environ.get("USER_ID", "S4450").strip()
IDEA_LTP_ENV = os.environ.get("IDEA_LTP", "").strip()
SKIP_REAL_MONEY = os.environ.get("SKIP_REAL_MONEY", "").strip() in ("1", "true", "yes")

BASE_URL = "https://livemiddleware.indiratrade.com"
WSS_URL = "wss://livemiddleware.indiratrade.com/order-notify/websocket"

SYMBOL = "STK_IDEA_EQ_NSE_14366"
EXC_TOKEN = "14366"
EXCHANGE = "NSE"

TS = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_DIR = Path(f"/tmp/wss_capture_{TS}")

# Timing
SLEEP_AFTER_ACTION = 8       # sec — wait for WSS events after each REST call
SLEEP_HANDSHAKE = 3          # sec — wait after handshake before firing actions
HEARTBEAT_INTERVAL = 45
DRAIN_TAIL = 10              # sec — wait after last action for late events


# ────────────────────────────────────────────────────────────
# Session state
# ────────────────────────────────────────────────────────────
class Session:
    ltp: float = 13.50   # sensible IDEA default
    last_order_id: str | None = None
    last_amo_id: str | None = None
    market_open: bool = False
    actions_log: list = []   # each entry: {ts, action, request, response, order_id}
    events_log: list = []    # each entry: {ts, event}
    files: dict = {}         # open file handles

S = Session()


# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────
def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def log(msg: str = "") -> None:
    print(f"[{now_str()}] {msg}", flush=True)


def is_market_open_ist() -> bool:
    """NSE cash market: Mon-Fri 09:15-15:30 IST."""
    if ZoneInfo is None:
        return False  # can't verify safely
    now = datetime.now(ZoneInfo("Asia/Kolkata"))
    if now.weekday() >= 5:
        return False
    t = now.time()
    return dtime(9, 15) <= t <= dtime(15, 30)


def write_action(action: str, request: dict, response: dict, order_id: str | None):
    entry = {
        "ts": datetime.now().isoformat(),
        "action": action,
        "request": request,
        "response": response,
        "order_id": order_id,
    }
    S.actions_log.append(entry)
    S.files["actions"].write(json.dumps(entry) + "\n")
    S.files["actions"].flush()


def write_event(event: dict):
    entry = {"ts": datetime.now().isoformat(), "event": event}
    S.events_log.append(entry)
    S.files["events"].write(json.dumps(entry) + "\n")
    S.files["events"].flush()


# ────────────────────────────────────────────────────────────
# REST helpers
# ────────────────────────────────────────────────────────────
def rest_call(path: str, body: dict | None, method: str = "POST") -> dict:
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {JWT}")
    req.add_header("userId", USER_ID)
    req.add_header("appId", APP_ID)
    req.add_header("source", "WEB")
    req.add_header("sso", "True")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        try:
            return json.loads(raw)
        except Exception:
            return {"_http_error": e.code, "_body": raw}
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {e}"}

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"_raw": raw}


def get_wss_token() -> str:
    req = urllib.request.Request(f"{BASE_URL}/order-notify/ws/createWsToken", method="GET")
    req.add_header("Authorization", f"Bearer {JWT}")
    req.add_header("Content-Type", "application/json")
    req.add_header("sso", "True")
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if data.get("status") != "Ok":
        raise RuntimeError(f"Token API failed: {data}")
    return data["result"][0]["orderToken"]


def extract_order_id(resp: dict):
    if not isinstance(resp, dict):
        return None
    d = resp.get("data")
    if isinstance(d, dict):
        return (d.get("nstOrdNo")
                or d.get("ordId")
                or d.get("orderNumber")
                or d.get("brokerOrderId"))
    return None


# ────────────────────────────────────────────────────────────
# Order operations
# ────────────────────────────────────────────────────────────
def op_place(**overrides) -> tuple[dict, dict, str | None]:
    body = {
        "symbol": SYMBOL, "excToken": EXC_TOKEN, "exc": EXCHANGE,
        "ordAction": "BUY", "ordValidity": "DAY", "ordType": "Limit",
        "prdType": "DELIVERY", "limitPrice": 5.00, "triggerPrice": 0.0,
        "qty": 1, "disQty": 0, "lotSize": 1, "instrument": "STK",
        "amo": False, "boStpLoss": None, "boTgtPrice": None,
    }
    body.update(overrides)
    resp = rest_call("/order-services/api/order/v1/place-order", body)
    return body, resp, extract_order_id(resp)


def op_modify(ord_id: str, **overrides) -> tuple[dict, dict]:
    body = {
        "ordId": ord_id, "symbol": SYMBOL, "ordAction": "BUY",
        "ordValidity": "DAY", "exchangeToken": EXC_TOKEN, "exc": EXCHANGE,
        "qty": 1, "limitPrice": 5.00, "triggerPrice": 0.0,
        "ordType": "Limit", "prdType": "DELIVERY", "instrument": "STK",
        "lotSize": 1, "disQty": 0, "offMktFlag": False, "tradedQty": 0,
        "boStpLoss": None, "boTgtPrice": None,
    }
    body.update(overrides)
    resp = rest_call("/order-services/api/order/v1/modify-order", body)
    return body, resp


def op_cancel(ord_id: str) -> tuple[dict, dict]:
    body = {"symbol": SYMBOL, "exc": EXCHANGE, "ordId": ord_id}
    resp = rest_call("/order-services/api/order/v1/cancel-order", body)
    return body, resp


# ────────────────────────────────────────────────────────────
# WSS listener
# ────────────────────────────────────────────────────────────
async def wss_listener(ready_event: asyncio.Event, stop_event: asyncio.Event):
    log("WSS: fetching token...")
    token = get_wss_token()
    log(f"WSS: token {token[:12]}…{token[-6:]}")

    async with websockets.connect(WSS_URL, ping_interval=None, open_timeout=10) as ws:
        # handshake
        handshake = {"userId": USER_ID, "orderToken": token}
        await ws.send(json.dumps(handshake))
        log("WSS: → handshake sent")

        # heartbeat
        async def heartbeat():
            while not stop_event.is_set():
                try:
                    await asyncio.sleep(HEARTBEAT_INTERVAL)
                    await ws.send(json.dumps({"userId": USER_ID, "heartbeat": "h"}))
                except Exception:
                    return

        hb_task = asyncio.create_task(heartbeat())

        try:
            while not stop_event.is_set():
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    log(f"WSS: recv error {type(e).__name__}: {e}")
                    return

                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    write_event({"_raw": raw if isinstance(raw, str) else raw.hex()})
                    continue

                # handshake reply
                if isinstance(event, dict) and set(event.keys()) == {"status"}:
                    log(f"WSS: ↓ handshake reply {event}")
                    ready_event.set()
                    write_event(event)
                    continue

                # normal event
                write_event(event)
                summary = summarize(event)
                log(f"WSS: ↓ {summary}")
        finally:
            hb_task.cancel()


def summarize(event: dict) -> str:
    parts = []
    for k in ("MessageType", "OrderStatus", "OMSOrderStatus",
              "UniqueCode", "OrderNumber", "Symbol", "Buy_Sell",
              "OrderType", "Product",
              "TradedQTY", "TradedPrice",
              "OrderPrice", "TriggerPrice", "PendingQty",
              "Reason"):
        if k in event and event[k] not in (None, "", "0"):
            parts.append(f"{k}={event[k]}")
    return "  ".join(parts) if parts else "(no notable fields)"


# ────────────────────────────────────────────────────────────
# Test actions
# ────────────────────────────────────────────────────────────
async def do_action(name: str, description: str, fn):
    log(f"─ ACTION {name}: {description}")
    try:
        result = fn()
    except Exception as e:
        log(f"  ✗ {name} failed: {type(e).__name__}: {e}")
        write_action(name, {}, {"_error": str(e)}, None)
        return None
    await asyncio.sleep(SLEEP_AFTER_ACTION)
    return result


def act_A1():
    """LIMIT BUY at LTP-0.05 → OPEN"""
    price = round(S.ltp - 0.05, 2)
    log(f"  place LIMIT BUY 1 IDEA @ ₹{price}")
    body, resp, oid = op_place(limitPrice=price)
    write_action("A1_limit_buy_open", body, resp, oid)
    if oid:
        S.last_order_id = oid
        log(f"  ✓ order_id={oid}")
    else:
        log(f"  ✗ no order_id, resp={resp}")
    return oid


def act_A2():
    """MODIFY last order → MODIFIED"""
    if not S.last_order_id:
        log("  skip: no last_order_id")
        return None
    price = round(S.ltp - 0.10, 2)
    log(f"  modify {S.last_order_id} → ₹{price}")
    body, resp = op_modify(S.last_order_id, limitPrice=price)
    write_action("A2_modify_open", body, resp, S.last_order_id)
    log(f"  resp={resp}")


def act_A3():
    """CANCEL last order → CANCELLED"""
    if not S.last_order_id:
        log("  skip: no last_order_id")
        return None
    log(f"  cancel {S.last_order_id}")
    body, resp = op_cancel(S.last_order_id)
    write_action("A3_cancel_open", body, resp, S.last_order_id)
    log(f"  resp={resp}")


def act_A4():
    """LIMIT SELL at LTP+5 → OPEN (sell side)"""
    price = round(S.ltp + 5.00, 2)
    log(f"  place LIMIT SELL 1 IDEA @ ₹{price}")
    body, resp, oid = op_place(ordAction="SELL", limitPrice=price)
    write_action("A4_limit_sell_open", body, resp, oid)
    if oid:
        S.last_order_id = oid
        log(f"  ✓ order_id={oid}")
    return oid


def act_A5_cancel_sell():
    """Cancel A4's SELL"""
    if not S.last_order_id:
        log("  skip: no last_order_id")
        return None
    log(f"  cancel {S.last_order_id}")
    body, resp = op_cancel(S.last_order_id)
    write_action("A5_cancel_sell", body, resp, S.last_order_id)
    log(f"  resp={resp}")


def act_A6_market_roundtrip():
    """MARKET BUY + MARKET SELL — real ₹"""
    log("  MARKET BUY 1 IDEA")
    body, resp, buy_id = op_place(ordType="Market", limitPrice=0.0, triggerPrice=0.0)
    write_action("A6_market_buy", body, resp, buy_id)
    if not buy_id:
        log(f"  ✗ BUY failed, resp={resp}")
        return None
    log(f"  ✓ buy_id={buy_id}, sleeping 5s for fill...")
    time.sleep(5)

    log("  MARKET SELL 1 IDEA")
    body, resp, sell_id = op_place(ordAction="SELL", ordType="Market", limitPrice=0.0, triggerPrice=0.0)
    write_action("A6_market_sell", body, resp, sell_id)
    if not sell_id:
        log(f"  ✗ SELL failed — YOU MAY OWN 1 IDEA. resp={resp}")
        return None
    log(f"  ✓ sell_id={sell_id}")


def act_B1_amo():
    price = round(S.ltp * 0.98, 2)
    log(f"  AMO LIMIT BUY 1 IDEA @ ₹{price}")
    body, resp, oid = op_place(amo=True, limitPrice=price)
    write_action("B1_amo_buy", body, resp, oid)
    if oid:
        S.last_amo_id = oid
        log(f"  ✓ amo_id={oid}")
    return oid


def act_B2_modify_amo():
    if not S.last_amo_id:
        log("  skip: no last_amo_id")
        return None
    log(f"  modify AMO {S.last_amo_id} → ₹4")
    body, resp = op_modify(S.last_amo_id, limitPrice=4.00)
    write_action("B2_amo_modify", body, resp, S.last_amo_id)
    log(f"  resp={resp}")


def act_B3_cancel_amo():
    if not S.last_amo_id:
        log("  skip: no last_amo_id")
        return None
    log(f"  cancel AMO {S.last_amo_id}")
    body, resp = op_cancel(S.last_amo_id)
    write_action("B3_amo_cancel", body, resp, S.last_amo_id)
    log(f"  resp={resp}")


def act_C1_admin_pending():
    """LIMIT BUY very far → ADMIN PENDING → A.REJECTED"""
    log("  place LIMIT BUY 1 IDEA @ ₹5 (very far from LTP)")
    body, resp, oid = op_place(limitPrice=5.00)
    write_action("C1_admin_pending", body, resp, oid)
    log(f"  order_id={oid}")


def act_C2_order_error():
    """SL-M junk trigger → ORDER ERROR (but broker may clamp!)"""
    log("  place SL-M BUY with junk trigger 0.01 (broker MAY clamp and morph to MARKET)")
    log("  ⚠ if broker morphs → 1 IDEA will be bought at market. Will cleanup after.")
    body, resp, oid = op_place(ordType="SL-M", triggerPrice=0.01, limitPrice=0.0)
    write_action("C2_order_error", body, resp, oid)
    log(f"  order_id={oid}")


def act_C3_sell_no_holding():
    log("  place LIMIT SELL 1 IDEA @ ₹1000 (S4450 doesn't hold IDEA)")
    body, resp, oid = op_place(ordAction="SELL", limitPrice=1000.00)
    write_action("C3_sell_no_holding", body, resp, oid)
    if oid:
        S.last_order_id = oid
    log(f"  order_id={oid}")


# ────────────────────────────────────────────────────────────
# Runner
# ────────────────────────────────────────────────────────────
async def run_test_suite():
    log("")
    log("═" * 62)
    log("TEST SUITE")
    log("═" * 62)

    if S.market_open:
        log("Market is OPEN → running Phase A + C")
        # A1 → A2 → A3 chain (OPEN → MODIFIED → CANCELLED)
        await do_action("A1", "LIMIT BUY at LTP-0.05  → expect OPEN", act_A1)
        await do_action("A2", "MODIFY last order → expect MODIFIED", act_A2)
        await do_action("A3", "CANCEL last order → expect CANCELLED", act_A3)

        # A4 → A5 chain (SELL OPEN → CANCELLED)
        await do_action("A4", "LIMIT SELL at LTP+5 → expect OPEN (sell)", act_A4)
        await do_action("A5", "CANCEL sell → expect CANCELLED", act_A5_cancel_sell)

        if SKIP_REAL_MONEY:
            log("─ ACTION A6: SKIPPED (SKIP_REAL_MONEY=1)")
        else:
            await do_action("A6", "MARKET BUY + SELL round-trip → expect EXECUTED (real ₹)",
                            act_A6_market_roundtrip)

        # Edge cases during market open
        await do_action("C1", "LIMIT far from market → expect ADMIN PENDING → A.REJECTED",
                        act_C1_admin_pending)
        await do_action("C3", "SELL with no holding → expect OPEN or REJECTED",
                        act_C3_sell_no_holding)
        # C2 (SL-M junk) can morph to real MARKET buy — skip in auto mode to avoid uncleaned position
        log("─ ACTION C2: SKIPPED in auto mode (may morph to real MARKET BUY)")

    else:
        log("Market is CLOSED → running Phase B + C")
        # AMO chain
        await do_action("B1", "AMO LIMIT BUY → expect AMO SUBMITTED", act_B1_amo)
        await do_action("B2", "MODIFY AMO → expect AMO MODIFIED", act_B2_modify_amo)
        await do_action("B3", "CANCEL AMO → expect AMO CANCELLED", act_B3_cancel_amo)

        # These also work when market closed (they get queued or rejected instantly)
        await do_action("C1", "LIMIT far from market → expect state event",
                        act_C1_admin_pending)


# ────────────────────────────────────────────────────────────
# Summary
# ────────────────────────────────────────────────────────────
def build_summary() -> dict:
    """After capture, correlate actions and events by UniqueCode."""
    events_by_uniquecode = {}
    unique_statuses = set()
    unique_omscodes = set()
    unique_msgtypes = set()

    for entry in S.events_log:
        ev = entry.get("event", {})
        if not isinstance(ev, dict):
            continue
        uc = str(ev.get("UniqueCode") or ev.get("uniqueCode") or "").strip()
        if uc:
            events_by_uniquecode.setdefault(uc, []).append(entry)
        st = str(ev.get("OrderStatus") or "").strip()
        if st:
            unique_statuses.add(st)
        oc = ev.get("OMSOrderStatus")
        if oc is not None and oc != "":
            unique_omscodes.add(str(oc))
        mt = str(ev.get("MessageType") or "").strip()
        if mt:
            unique_msgtypes.add(mt)

    per_action = []
    for a in S.actions_log:
        oid = a.get("order_id")
        events = events_by_uniquecode.get(str(oid), []) if oid else []
        per_action.append({
            "action": a["action"],
            "order_id": oid,
            "events_seen": len(events),
            "statuses_seen": [
                str(e["event"].get("OrderStatus") or "").strip()
                for e in events if isinstance(e.get("event"), dict)
            ],
        })

    return {
        "session": TS,
        "market_open": S.market_open,
        "totals": {
            "actions": len(S.actions_log),
            "events": len(S.events_log),
            "unique_OrderStatus_values": sorted(unique_statuses),
            "unique_OMSOrderStatus_codes": sorted(unique_omscodes),
            "unique_MessageType_values": sorted(unique_msgtypes),
        },
        "per_action": per_action,
    }


# ────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────
async def main():
    if not JWT or not APP_ID:
        print("ERROR: set env vars first:", file=sys.stderr)
        print('  export JWT="eyJhbG..."', file=sys.stderr)
        print('  export APP_ID="8609ea..."', file=sys.stderr)
        sys.exit(2)

    # LTP
    if IDEA_LTP_ENV:
        try:
            S.ltp = float(IDEA_LTP_ENV)
        except ValueError:
            log(f"IDEA_LTP env var not a number ({IDEA_LTP_ENV}) — using default ₹13.50")
    log(f"IDEA LTP reference: ₹{S.ltp}  (override with IDEA_LTP env var)")

    S.market_open = is_market_open_ist()

    # Prepare output dir
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    events_fp = open(OUT_DIR / "events.jsonl", "w")
    actions_fp = open(OUT_DIR / "actions.jsonl", "w")
    S.files["events"] = events_fp
    S.files["actions"] = actions_fp

    print()
    print("═" * 62)
    print("CODIFY WSS FULL CAPTURE")
    print("═" * 62)
    print(f"User:       {USER_ID}")
    print(f"Symbol:     IDEA ({SYMBOL})")
    print(f"Market:     {'OPEN' if S.market_open else 'CLOSED (IST)'}")
    print(f"Real money: {'SKIPPED (SKIP_REAL_MONEY=1)' if SKIP_REAL_MONEY else '~₹1-3 for A6 round-trip'}")
    print(f"Output:     {OUT_DIR}")
    print()

    # WSS listener + heartbeat + test runner
    ready = asyncio.Event()
    stop = asyncio.Event()

    listener_task = asyncio.create_task(wss_listener(ready, stop))

    # Wait for handshake
    log("Waiting for WSS handshake...")
    try:
        await asyncio.wait_for(ready.wait(), timeout=20)
    except asyncio.TimeoutError:
        log("✗ WSS handshake timeout")
        stop.set()
        await asyncio.sleep(1)
        listener_task.cancel()
        return

    log(f"✓ handshake OK. Waiting {SLEEP_HANDSHAKE}s before firing actions...")
    await asyncio.sleep(SLEEP_HANDSHAKE)

    # Fire all test actions
    try:
        await run_test_suite()
    except Exception as e:
        log(f"✗ suite error: {type(e).__name__}: {e}")

    # Drain for late events
    log(f"Draining {DRAIN_TAIL}s for late WSS events...")
    await asyncio.sleep(DRAIN_TAIL)

    # Stop listener
    stop.set()
    await asyncio.sleep(1)
    listener_task.cancel()
    try:
        await listener_task
    except (asyncio.CancelledError, Exception):
        pass

    # Write summary
    summary = build_summary()
    with open(OUT_DIR / "summary.json", "w") as f:
        json.dump(summary, f, indent=2)

    events_fp.close()
    actions_fp.close()

    log("")
    log("═" * 62)
    log("CAPTURE COMPLETE")
    log("═" * 62)
    log(f"Output directory:  {OUT_DIR}")
    log(f"  events.jsonl     ({len(S.events_log)} events)")
    log(f"  actions.jsonl    ({len(S.actions_log)} REST calls)")
    log(f"  summary.json     (state-machine inputs)")
    log("")
    log(f"Unique OrderStatus values captured: {summary['totals']['unique_OrderStatus_values']}")
    log(f"Unique OMSOrderStatus codes:        {summary['totals']['unique_OMSOrderStatus_codes']}")
    log(f"Unique MessageType values:          {summary['totals']['unique_MessageType_values']}")
    log("")
    log("Send the output directory path back and we'll build the state machine.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted — output in", OUT_DIR)
