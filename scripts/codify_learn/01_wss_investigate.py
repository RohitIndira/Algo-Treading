#!/usr/bin/env python3
"""
Codify WSS Order Investigator — Tests 1..8
==========================================

Fires 8 test orders (all on 1 share IDEA, S4450's account) so we can
capture every WSS event type the broker sends. The 00_wss_logger.py
script (run in another terminal) records the events for later analysis.

SAFETY: all default tests are ZERO-cost — orders won't fill because the
price is far from market OR the params intentionally invalid.

Test #8 does a real MARKET BUY + MARKET SELL round-trip → this is the
ONLY test that costs real money (₹0.05-2 spread). It requires you to
type 'yes' when prompted, so a mis-click can't cost you anything.

USAGE:
  # Terminal 1 (start FIRST):
  #   python3 00_wss_logger.py
  # Terminal 2:
  export JWT="eyJhbG..."
  export APP_ID="7d880cc..."
  python3 01_wss_investigate.py

Between tests, the script PAUSES and waits for you to press ENTER.
Use that pause to look at the logger output (or Postman) and copy the
events. This is a *learning* tool — go at your own pace.
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

# Test symbol — IDEA is highly liquid and cheap; safest choice for test orders.
# S4450 does NOT hold IDEA, so nothing can accidentally close a real position.
SYMBOL = "STK_IDEA_EQ_NSE_14366"
EXC_TOKEN = "14366"
EXCHANGE = "NSE"

# Prices chosen so orders never actually match the market
LOW_LIMIT = 5.00       # ~₹5, well below IDEA's typical ₹10-15
HIGH_LIMIT = 1000.00   # ~₹1000, above any conceivable IDEA price
MODIFY_TO = 5.50


# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────
def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def log(msg: str = "") -> None:
    print(f"[{ts()}] {msg}", flush=True)


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
    """Codify sometimes puts the id under different keys. Grab whichever exists."""
    if not isinstance(resp, dict):
        return None
    d = resp.get("data")
    if isinstance(d, dict):
        return (d.get("nstOrdNo")
                or d.get("ordId")
                or d.get("orderNumber")
                or d.get("brokerOrderId"))
    return None


def prompt_pause(what_to_watch: str) -> None:
    print()
    print("  ─" * 30)
    print(f"  👀 WATCH the WSS logger terminal (and Postman):")
    print(f"     {what_to_watch}")
    print("  ─" * 30)
    input("  Press ENTER when you've observed the events…")
    print()


# ────────────────────────────────────────────────────────────
# Order factories — one function per API call
# ────────────────────────────────────────────────────────────
def place_order(**overrides) -> dict:
    """POST /order-services/api/order/v1/place-order — default is LIMIT BUY."""
    body = {
        "symbol": SYMBOL,
        "excToken": EXC_TOKEN,
        "exc": EXCHANGE,
        "ordAction": "BUY",
        "ordValidity": "DAY",
        "ordType": "Limit",
        "prdType": "DELIVERY",
        "limitPrice": LOW_LIMIT,
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
        "exchangeToken": EXC_TOKEN,
        "exc": EXCHANGE,
        "qty": 1,
        "limitPrice": MODIFY_TO,
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
# The 8 tests
# ────────────────────────────────────────────────────────────
def test_1_limit_buy_low() -> str | None:
    log("═" * 60)
    log("TEST 1: LIMIT BUY 1 IDEA @ ₹5 (won't fill — price too low)")
    log("═" * 60)
    log("Expect: WSS events — SUBMITTED then OPEN (or PENDING)")
    resp = place_order()
    ord_id = extract_order_id(resp)
    if ord_id:
        prompt_pause(f"SUBMITTED / OPEN for OrderNumber={ord_id}")
    else:
        log("⚠️  Could not extract order id — check response above.")
        prompt_pause("REJECTED / error event")
    return ord_id


def test_2_modify(ord_id: str) -> None:
    log("═" * 60)
    log(f"TEST 2: MODIFY order {ord_id}  limitPrice ₹5 → ₹5.50")
    log("═" * 60)
    log("Expect: WSS event — MODIFIED / AMEND CONFIRMED / OPEN (post-amend)")
    modify_order(ord_id, limitPrice=MODIFY_TO)
    prompt_pause(f"MODIFIED event for OrderNumber={ord_id}")


def test_3_cancel(ord_id: str) -> None:
    log("═" * 60)
    log(f"TEST 3: CANCEL order {ord_id}")
    log("═" * 60)
    log("Expect: WSS event — CANCELLED")
    cancel_order(ord_id)
    prompt_pause(f"CANCELLED event for OrderNumber={ord_id}")


def test_4_amo_buy() -> str | None:
    log("═" * 60)
    log("TEST 4: AMO BUY 1 IDEA @ ₹5  (queues for tomorrow open)")
    log("═" * 60)
    log("Expect: WSS event — AMO SUBMITTED")
    resp = place_order(amo=True)
    ord_id = extract_order_id(resp)
    if ord_id:
        prompt_pause(f"AMO SUBMITTED for OrderNumber={ord_id}")
    else:
        prompt_pause("AMO placement response (check output)")
    return ord_id


def test_5_cancel_amo(ord_id: str) -> None:
    log("═" * 60)
    log(f"TEST 5: CANCEL AMO order {ord_id}")
    log("═" * 60)
    log("Expect: WSS event — CANCELLED for AMO row")
    cancel_order(ord_id)
    prompt_pause(f"AMO CANCELLED for OrderNumber={ord_id}")


def test_6_limit_sell_no_holding() -> None:
    log("═" * 60)
    log("TEST 6: LIMIT SELL 1 IDEA @ ₹1000 (S4450 doesn't own IDEA)")
    log("═" * 60)
    log("Expect: WSS event — REJECTED with reason 'insufficient quantity'/similar")
    log("        OR broker accepts the order and it sits OPEN (no shares needed")
    log("        to place a sell — only to execute; interesting to see either way)")
    resp = place_order(ordAction="SELL", limitPrice=HIGH_LIMIT)
    ord_id = extract_order_id(resp)
    if ord_id:
        prompt_pause(f"SUBMITTED / OPEN / REJECTED for OrderNumber={ord_id}")
        # Try to cancel it in case it landed as OPEN
        log("(Attempting cancel in case it accepted…)")
        cancel_order(ord_id)
        prompt_pause(f"Any follow-up CANCELLED event for {ord_id}")
    else:
        prompt_pause("REJECTED event")


def test_7_slm_buy_reject() -> None:
    log("═" * 60)
    log("TEST 7: SL-M BUY 1 IDEA  triggerPrice ₹0.01 (nonsense — should REJECT)")
    log("═" * 60)
    log("Expect: WSS event — REJECTED with reason like 'trigger price invalid'")
    resp = place_order(ordType="SL-M", triggerPrice=0.01, limitPrice=0.0)
    ord_id = extract_order_id(resp)
    prompt_pause(f"REJECTED event (or SL-M placement)  ord_id={ord_id}")


def test_8_market_roundtrip() -> None:
    log("═" * 60)
    log("TEST 8: MARKET BUY 1 IDEA → MARKET SELL 1 IDEA (real money!)")
    log("Estimated cost: bid-ask spread on IDEA ≈ ₹0.05 - ₹2.00")
    log("═" * 60)
    ans = input("  Run Test 8? Type exactly 'yes' to proceed, anything else skips: ")
    if ans.strip().lower() != "yes":
        log("Test 8 skipped.")
        return

    # BUY market
    log("─" * 40)
    log("Step 8a: MARKET BUY 1 IDEA")
    log("─" * 40)
    log("Expect WSS: SUBMITTED → OPEN → TRADED (with TradedPrice = real fill)")
    buy_resp = place_order(
        ordType="Market",
        limitPrice=0.0,
        triggerPrice=0.0,
    )
    buy_id = extract_order_id(buy_resp)
    if not buy_id:
        log("❌ Market BUY did not return an ordId — aborting Test 8.")
        return
    prompt_pause(f"TRADED event for BUY {buy_id} — **NOTE THE FILL PRICE**")

    # Give the position a beat to settle in broker's system
    log("Waiting 3s for broker to reflect the buy position…")
    time.sleep(3)

    # SELL market — sell back what we just bought
    log("─" * 40)
    log("Step 8b: MARKET SELL 1 IDEA (close the round-trip)")
    log("─" * 40)
    log("Expect WSS: SUBMITTED → OPEN → TRADED (with real fill price)")
    sell_resp = place_order(
        ordAction="SELL",
        ordType="Market",
        limitPrice=0.0,
        triggerPrice=0.0,
    )
    sell_id = extract_order_id(sell_resp)
    if not sell_id:
        log("❌ Market SELL did not return an ordId.")
        log("⚠️  YOU MAY STILL OWN 1 SHARE OF IDEA — check broker holdings manually.")
        return
    prompt_pause(f"TRADED event for SELL {sell_id} — round-trip complete")


# ────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────
def main() -> None:
    if not JWT or not APP_ID:
        print("ERROR: set env vars first:", file=sys.stderr)
        print('  export JWT="eyJhbG..."', file=sys.stderr)
        print('  export APP_ID="7d880cc..."', file=sys.stderr)
        sys.exit(2)

    print()
    print("═" * 60)
    print("CODIFY WSS ORDER INVESTIGATOR")
    print("═" * 60)
    print(f"User:   {USER_ID}")
    print(f"Symbol: {SYMBOL}  (IDEA, not held by S4450 — safe)")
    print(f"Qty:    1 share per order")
    print()
    print("BEFORE STARTING, in ANOTHER terminal:")
    print("   export JWT=\"...\"")
    print("   python3 00_wss_logger.py")
    print()
    print("That logger captures every WSS event to /tmp/wss_events_*.jsonl")
    print()
    input("Press ENTER when the logger is running…")

    # Chain
    ord_id_1 = test_1_limit_buy_low()
    if ord_id_1:
        test_2_modify(ord_id_1)
        test_3_cancel(ord_id_1)
    else:
        log("Skipping Tests 2 & 3 — no order id from Test 1.")

    amo_id = test_4_amo_buy()
    if amo_id:
        test_5_cancel_amo(amo_id)
    else:
        log("Skipping Test 5 — no AMO order id.")

    test_6_limit_sell_no_holding()
    test_7_slm_buy_reject()
    test_8_market_roundtrip()

    log("")
    log("═" * 60)
    log("ALL TESTS COMPLETE")
    log("═" * 60)
    log("Now:")
    log("  1. Ctrl+C the 00_wss_logger.py in the other terminal")
    log("  2. Its log file is at /tmp/wss_events_YYYYMMDD_HHMMSS.jsonl")
    log("  3. Paste that file path back to me — we'll analyse every event together")


if __name__ == "__main__":
    main()
