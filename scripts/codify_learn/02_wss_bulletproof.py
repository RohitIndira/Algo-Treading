#!/usr/bin/env python3
"""
02_wss_bulletproof.py — Menu-driven WSS state-machine capture

Runs in Terminal 2 alongside 00_wss_logger.py (Terminal 1).
Each menu action fires ONE targeted REST call designed to produce ONE
specific OrderStatus event we haven't captured yet.

Goal: over ~30 min of live testing, see every state Codify's WSS can
emit. That gives us the ground truth for a bulletproof state machine.

WHY MENU-DRIVEN (not linear like 01_wss_investigate.py):
  - Some states need market OPEN, others need market CLOSED
  - Some need us to WAIT (SL trigger fire — 30-60 sec of market movement)
  - Better to pick a specific action, run it, watch logger, pick next

USAGE:
  # Terminal 1 (start FIRST):
  export JWT="eyJhbG..."
  python3 00_wss_logger.py

  # Terminal 2:
  export JWT="eyJhbG..."
  export APP_ID="8609ea49..."
  python3 02_wss_bulletproof.py
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime


JWT = os.environ.get("JWT", "").strip()
APP_ID = os.environ.get("APP_ID", "").strip()
USER_ID = os.environ.get("USER_ID", "S4450").strip()

BASE_URL = "https://livemiddleware.indiratrade.com"

# IDEA — highly liquid, cheap, S4450 doesn't hold it (safe for BUY tests)
SYMBOL = "STK_IDEA_EQ_NSE_14366"
EXC_TOKEN = "14366"
EXCHANGE = "NSE"


# ────────────────────────────────────────────────────────────
# Session state
# ────────────────────────────────────────────────────────────
class Session:
    def __init__(self):
        self.last_order_id: str | None = None
        self.last_amo_id: str | None = None
        self.ltp: float | None = None
        # Track which menu actions we've fired (not what came back from WSS —
        # user tracks that visually in the logger terminal)
        self.actions_run: list[str] = []


S = Session()


# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────
def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def log(msg: str = "") -> None:
    print(f"[{ts()}] {msg}", flush=True)


def hr(char="─", n=62):
    print(char * n)


def api(path: str, body: dict | None, method: str = "POST") -> dict:
    """Call any REST endpoint. Returns parsed JSON dict."""
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
        log(f"  HTTP {e.code}: {raw[:400]}")
        try:
            return json.loads(raw)
        except Exception:
            return {"_http_error": e.code, "_body": raw}
    except Exception as e:
        log(f"  request failed: {type(e).__name__}: {e}")
        return {"_error": str(e)}

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"_raw": raw}


def extract_order_id(resp: dict):
    """Codify puts the id under various keys — grab whichever exists."""
    if not isinstance(resp, dict):
        return None
    d = resp.get("data")
    if isinstance(d, dict):
        return (d.get("nstOrdNo")
                or d.get("ordId")
                or d.get("orderNumber")
                or d.get("brokerOrderId"))
    return None


def confirm(prompt: str = "Proceed?") -> bool:
    ans = input(f"  {prompt} [y/N] ").strip().lower()
    return ans in ("y", "yes")


def require_ltp() -> float | None:
    if S.ltp is None:
        print("  ⚠ You haven't set IDEA's LTP yet. Run option L first.")
        return None
    return S.ltp


# ────────────────────────────────────────────────────────────
# Order factories — REST call primitives
# ────────────────────────────────────────────────────────────
def place_order(**overrides) -> dict:
    """POST /order-services/api/order/v1/place-order — default is LIMIT BUY 1 share."""
    body = {
        "symbol": SYMBOL,
        "excToken": EXC_TOKEN,
        "exc": EXCHANGE,
        "ordAction": "BUY",
        "ordValidity": "DAY",
        "ordType": "Limit",
        "prdType": "DELIVERY",
        "limitPrice": 5.00,
        "triggerPrice": 0.0,
        "qty": 1,
        "disQty": 0,
        "lotSize": 1,
        "instrument": "STK",
        "amo": False,
        "boStpLoss": None,
        "boTgtPrice": None,
    }
    body.update(overrides)
    log(f"→ POST place-order  {json.dumps(body)}")
    resp = api("/order-services/api/order/v1/place-order", body)
    log(f"← RESP: {json.dumps(resp)}")
    return resp


def modify_order(ord_id: str, **overrides) -> dict:
    """POST /order-services/api/order/v1/modify-order"""
    body = {
        "ordId": ord_id,
        "symbol": SYMBOL,
        "ordAction": "BUY",
        "ordValidity": "DAY",
        "exchangeToken": EXC_TOKEN,     # NOTE: modify uses exchangeToken, not excToken
        "exc": EXCHANGE,
        "qty": 1,
        "limitPrice": 5.00,
        "triggerPrice": 0.0,
        "ordType": "Limit",
        "prdType": "DELIVERY",
        "instrument": "STK",
        "lotSize": 1,
        "disQty": 0,
        "offMktFlag": False,
        "tradedQty": 0,
        "boStpLoss": None,
        "boTgtPrice": None,
    }
    body.update(overrides)
    log(f"→ POST modify-order  {json.dumps(body)}")
    resp = api("/order-services/api/order/v1/modify-order", body)
    log(f"← RESP: {json.dumps(resp)}")
    return resp


def cancel_order(ord_id: str) -> dict:
    body = {"symbol": SYMBOL, "exc": EXCHANGE, "ordId": ord_id}
    log(f"→ POST cancel-order  {json.dumps(body)}")
    resp = api("/order-services/api/order/v1/cancel-order", body)
    log(f"← RESP: {json.dumps(resp)}")
    return resp


# ────────────────────────────────────────────────────────────
# Menu actions — each one targets a specific new state
# ────────────────────────────────────────────────────────────
def act_set_ltp():
    print()
    try:
        val = float(input("  Current IDEA LTP (₹) e.g. 13.85: ").strip())
        S.ltp = round(val, 2)
        log(f"IDEA LTP set to ₹{S.ltp}")
    except ValueError:
        log("  Not a number — cancelled.")


def act_A1_limit_buy_open():
    """LIMIT BUY at LTP-0.05 — should sit OPEN at exchange."""
    ltp = require_ltp()
    if ltp is None:
        return
    price = round(ltp - 0.05, 2)
    hr()
    log(f"A1: LIMIT BUY 1 IDEA @ ₹{price}  (LTP-0.05)")
    log("EXPECT WSS: PENDING → OPEN (order sits at exchange, doesn't fill)")
    if not confirm(f"Place BUY @ ₹{price}?"):
        return
    resp = place_order(limitPrice=price)
    oid = extract_order_id(resp)
    if oid:
        S.last_order_id = oid
        log(f"  Saved as last_order_id={oid} — you can now run A2 (modify) or A3 (cancel)")
        S.actions_run.append("A1")


def act_A2_modify_open():
    """Modify last OPEN order — should trigger MODIFIED / AMEND CONFIRMED."""
    if not S.last_order_id:
        print("  No last_order_id — run A1 first.")
        return
    ltp = require_ltp()
    if ltp is None:
        return
    new_price = round(ltp - 0.10, 2)
    hr()
    log(f"A2: MODIFY {S.last_order_id}  → limitPrice ₹{new_price}")
    log("EXPECT WSS: MODIFIED or AMEND CONFIRMED (plus maybe a 2nd status event)")
    if not confirm(f"Modify to ₹{new_price}?"):
        return
    modify_order(S.last_order_id, limitPrice=new_price)
    S.actions_run.append("A2")


def act_A3_cancel_open():
    """Cancel the OPEN order → CANCELLED event."""
    if not S.last_order_id:
        print("  No last_order_id — run A1 first.")
        return
    hr()
    log(f"A3: CANCEL {S.last_order_id}")
    log("EXPECT WSS: CANCELLED  (this is our first successful cancel!)")
    if not confirm(f"Cancel {S.last_order_id}?"):
        return
    cancel_order(S.last_order_id)
    S.actions_run.append("A3")
    # keep last_order_id so we can re-run if needed


def act_A4_limit_sell_open():
    """LIMIT SELL 1 IDEA at LTP+₹5 — sits OPEN, cost nothing to place."""
    ltp = require_ltp()
    if ltp is None:
        return
    price = round(ltp + 5.00, 2)
    hr()
    log(f"A4: LIMIT SELL 1 IDEA @ ₹{price}  (LTP+5)")
    log("EXPECT WSS: PENDING → OPEN on SELL side")
    log("            Note: broker may reject due to no holdings — captures that too")
    if not confirm(f"Place SELL @ ₹{price}?"):
        return
    resp = place_order(ordAction="SELL", limitPrice=price)
    oid = extract_order_id(resp)
    if oid:
        S.last_order_id = oid
        log(f"  Saved as last_order_id={oid} — cancel with A3 when done")
        S.actions_run.append("A4")


def act_A6_market_roundtrip():
    """MARKET BUY 1 IDEA → MARKET SELL 1 IDEA. Costs ₹1-2 spread. Real EXECUTED."""
    hr()
    log("A6: MARKET BUY 1 IDEA → MARKET SELL 1 IDEA (round-trip)")
    log("REAL MONEY: bid-ask spread ≈ ₹0.05 - ₹2.00")
    log("EXPECT WSS on BUY:  PENDING → EXECUTED (with TradedPrice)")
    log("EXPECT WSS on SELL: PENDING → EXECUTED (with TradedPrice)")
    if not confirm("Really run market round-trip?"):
        return

    log("Step 1: MARKET BUY 1 IDEA")
    buy_resp = place_order(ordType="Market", limitPrice=0.0, triggerPrice=0.0)
    buy_id = extract_order_id(buy_resp)
    if not buy_id:
        log("❌ BUY did not return an ordId. Not attempting SELL.")
        return
    log(f"  buy_id={buy_id}  — waiting 3s for position to settle...")
    time.sleep(3)

    log("Step 2: MARKET SELL 1 IDEA")
    sell_resp = place_order(ordAction="SELL", ordType="Market",
                            limitPrice=0.0, triggerPrice=0.0)
    sell_id = extract_order_id(sell_resp)
    if sell_id:
        log(f"  sell_id={sell_id}  — round-trip complete")
        S.actions_run.append("A6")
    else:
        log("❌ SELL failed! YOU MAY OWN 1 IDEA — check holdings manually.")


def act_A7_sl_trigger_fire():
    """BUY 1 IDEA (real) → place SL-Limit SELL just below LTP → wait for trigger."""
    ltp = require_ltp()
    if ltp is None:
        return
    hr()
    log("A7: Capture SL trigger fire in real time")
    log("Steps:")
    log("  1. MARKET BUY 1 IDEA (real position, ~₹" + f"{ltp}" + " cost)")
    log("  2. Place SL-Limit SELL — trigger = LTP-0.05, limit = LTP-0.10")
    log("  3. Wait 30-120 sec for market to move below trigger")
    log("EXPECT WSS on SL:  PENDING → OPEN → TRIGGERED → EXECUTED")
    log("REAL MONEY: ₹1-3 depending on how far below LTP-0.05 the market moves")
    if not confirm("Run SL trigger fire test?"):
        return

    # Buy first
    log("Step 1: MARKET BUY 1 IDEA")
    buy_resp = place_order(ordType="Market", limitPrice=0.0, triggerPrice=0.0)
    buy_id = extract_order_id(buy_resp)
    if not buy_id:
        log("❌ BUY failed — aborting.")
        return
    log(f"  buy_id={buy_id}. Waiting 3s...")
    time.sleep(3)

    # Place SL-Limit SELL
    trigger = round(ltp - 0.05, 2)
    limit = round(ltp - 0.10, 2)
    log(f"Step 2: SL-Limit SELL 1 IDEA  trigger=₹{trigger}  limit=₹{limit}")
    sl_resp = place_order(
        ordAction="SELL",
        ordType="SL",
        triggerPrice=trigger,
        limitPrice=limit,
    )
    sl_id = extract_order_id(sl_resp)
    if sl_id:
        S.last_order_id = sl_id
        log(f"  sl_id={sl_id}")
        log("")
        log("Now WATCH the logger. When LTP crosses ₹" + f"{trigger}"
            + " down, expect TRIGGERED then EXECUTED.")
        log("If market doesn't trip in 2 min, come back and hit A3 to cancel the SL,")
        log("then use A6 to sell your 1 IDEA back to close position.")
        S.actions_run.append("A7")
    else:
        log("❌ SL placement failed. You still own 1 IDEA from Step 1.")
        log("Run A6 or manually sell to close.")


def act_B1_amo_buy():
    """AMO LIMIT BUY 1 IDEA — needs market CLOSED."""
    ltp = require_ltp()
    if ltp is None:
        # AMO doesn't need LTP but let's still know it
        print("  (Using ₹5 as safe AMO price since you didn't set LTP.)")
        price = 5.00
    else:
        price = round(ltp * 0.98, 2)  # 2% below LTP = safely won't fill
    hr()
    log(f"B1: AMO LIMIT BUY 1 IDEA @ ₹{price}  (queues for tomorrow's open)")
    log("EXPECT WSS: AMO SUBMITTED or similar amo-specific status")
    log("(Market must be CLOSED for this to accept as AMO)")
    if not confirm(f"Place AMO @ ₹{price}?"):
        return
    resp = place_order(amo=True, limitPrice=price)
    oid = extract_order_id(resp)
    if oid:
        S.last_amo_id = oid
        log(f"  Saved as last_amo_id={oid} — you can now run B2 (modify) or B3 (cancel)")
        S.actions_run.append("B1")


def act_B2_modify_amo():
    if not S.last_amo_id:
        print("  No last_amo_id — run B1 first.")
        return
    hr()
    log(f"B2: MODIFY AMO {S.last_amo_id}  → limitPrice ₹4.00")
    log("EXPECT WSS: AMO MODIFIED event")
    if not confirm("Modify?"):
        return
    modify_order(S.last_amo_id, limitPrice=4.00)
    S.actions_run.append("B2")


def act_B3_cancel_amo():
    if not S.last_amo_id:
        print("  No last_amo_id — run B1 first.")
        return
    hr()
    log(f"B3: CANCEL AMO {S.last_amo_id}")
    log("EXPECT WSS: AMO CANCELLED (or plain CANCELLED)")
    if not confirm("Cancel?"):
        return
    cancel_order(S.last_amo_id)
    S.actions_run.append("B3")


def act_C1_admin_pending():
    """LIMIT far away from market — expected ADMIN PENDING."""
    hr()
    log("C1: LIMIT BUY 1 IDEA @ ₹5 (very far from market)")
    log("EXPECT WSS: ADMIN PENDING → A.REJECTED  (already captured; useful as sanity check)")
    if not confirm("Place?"):
        return
    resp = place_order(limitPrice=5.00)
    oid = extract_order_id(resp)
    if oid:
        S.last_order_id = oid
        S.actions_run.append("C1")


def act_C2_order_error():
    """SL-M with junk trigger — captures ORDER ERROR."""
    hr()
    log("C2: SL-M BUY 1 IDEA  triggerPrice=0.01 (junk)")
    log("EXPECT WSS: ORDER ERROR  (already captured)")
    log("Note: broker sometimes CLAMPS trigger to 1 → order becomes real MARKET BUY!")
    if not confirm("Place?"):
        return
    resp = place_order(ordType="SL-M", triggerPrice=0.01, limitPrice=0.0)
    oid = extract_order_id(resp)
    if oid:
        S.last_order_id = oid
        S.actions_run.append("C2")


def act_C3_sell_no_holding():
    """SELL when no holding — captures how broker handles this."""
    hr()
    log("C3: LIMIT SELL 1 IDEA @ ₹1000 (S4450 doesn't hold IDEA)")
    log("EXPECT WSS: OPEN? or REJECTED? — Codify is inconsistent here")
    if not confirm("Place?"):
        return
    resp = place_order(ordAction="SELL", limitPrice=1000.00)
    oid = extract_order_id(resp)
    if oid:
        S.last_order_id = oid
        S.actions_run.append("C3")


def act_show_status():
    hr()
    print("  Session status:")
    print(f"    IDEA LTP:         ₹{S.ltp if S.ltp else '(not set)'}")
    print(f"    Last order id:    {S.last_order_id or '(none)'}")
    print(f"    Last AMO id:      {S.last_amo_id or '(none)'}")
    print(f"    Actions run:      {', '.join(S.actions_run) if S.actions_run else '(none)'}")
    hr()


# ────────────────────────────────────────────────────────────
# Menu dispatch
# ────────────────────────────────────────────────────────────
MENU = {
    "L":  ("Set current IDEA LTP",                                act_set_ltp),
    "A1": ("LIMIT BUY @ LTP-0.05          → OPEN",                act_A1_limit_buy_open),
    "A2": ("MODIFY last order → LTP-0.10  → MODIFIED",            act_A2_modify_open),
    "A3": ("CANCEL last order             → CANCELLED",           act_A3_cancel_open),
    "A4": ("LIMIT SELL @ LTP+5            → OPEN (sell side)",    act_A4_limit_sell_open),
    "A6": ("MARKET BUY + SELL round-trip  → EXECUTED (real ₹)",   act_A6_market_roundtrip),
    "A7": ("SL trigger fire test          → TRIGGERED (real ₹)",  act_A7_sl_trigger_fire),
    "B1": ("AMO LIMIT BUY                 → AMO SUBMITTED",       act_B1_amo_buy),
    "B2": ("MODIFY AMO                    → AMO MODIFIED",        act_B2_modify_amo),
    "B3": ("CANCEL AMO                    → AMO CANCELLED",       act_B3_cancel_amo),
    "C1": ("LIMIT far away                → ADMIN PENDING",       act_C1_admin_pending),
    "C2": ("SL-M junk trigger             → ORDER ERROR",         act_C2_order_error),
    "C3": ("SELL no holding               → OPEN or REJECTED?",   act_C3_sell_no_holding),
    "S":  ("Show session status",                                 act_show_status),
}


def print_menu():
    print()
    print("╔" + "═" * 62 + "╗")
    print("║" + "  CODIFY WSS STATE-MACHINE CAPTURE".ljust(62) + "║")
    print("╚" + "═" * 62 + "╝")
    print()
    print("  Setup:")
    print(f"    L   {MENU['L'][0]}")
    print()
    print("  Phase A — Market OPEN tests:")
    for k in ("A1", "A2", "A3", "A4", "A6", "A7"):
        print(f"    {k}  {MENU[k][0]}")
    print()
    print("  Phase B — Market CLOSED tests (after 15:30 IST):")
    for k in ("B1", "B2", "B3"):
        print(f"    {k}  {MENU[k][0]}")
    print()
    print("  Phase C — Edge cases (mostly already captured):")
    for k in ("C1", "C2", "C3"):
        print(f"    {k}  {MENU[k][0]}")
    print()
    print("  Utility:")
    print(f"    S   {MENU['S'][0]}")
    print("    Q   Quit")
    print()


def main():
    if not JWT or not APP_ID:
        print("ERROR: set env vars first:", file=sys.stderr)
        print('  export JWT="eyJhbG..."', file=sys.stderr)
        print('  export APP_ID="8609ea..."', file=sys.stderr)
        sys.exit(2)

    print()
    print("═" * 62)
    print("CODIFY WSS STATE-MACHINE CAPTURE")
    print("═" * 62)
    print(f"User:   {USER_ID}")
    print(f"Symbol: IDEA (STK_IDEA_EQ_NSE_14366)")
    print()
    print("BEFORE STARTING, in another terminal:")
    print("  export JWT=\"eyJhbG...\"")
    print("  python3 00_wss_logger.py")
    print()
    print("Then come back here. Recommended sequence when market open:")
    print("  L (set LTP)  →  A1 → A2 → A3      # OPEN → MODIFIED → CANCELLED")
    print("               →  A4 → A3           # SELL OPEN → CANCELLED")
    print("               →  A6                # EXECUTED (real ₹)")
    print("               →  A7                # TRIGGERED (real ₹)")
    print("After 15:30 IST:  B1 → B2 → B3      # AMO lifecycle")
    print()

    while True:
        print_menu()
        choice = input("  Choice > ").strip().upper()
        if choice == "Q":
            print("  Bye. Log file: /tmp/wss_events_YYYYMMDD_HHMMSS.jsonl")
            break
        if choice not in MENU:
            print(f"  Unknown: {choice}")
            continue
        try:
            MENU[choice][1]()
        except KeyboardInterrupt:
            print("\n  (action interrupted)")
        except Exception as e:
            log(f"  Action error: {type(e).__name__}: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n  Bye.")
