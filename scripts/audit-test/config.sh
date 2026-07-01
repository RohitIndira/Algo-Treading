#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# config.sh — paste your values here ONCE. Every script in this folder reads it.
# (Not executed directly — it's sourced by the other scripts.)
# ─────────────────────────────────────────────────────────────────────────────

# ── Broker / gateway auth ────────────────────────────────────────────────────
ACCESS_TOKEN='eyJhbGciOiJIUzUxMiJ9.eyJyb2xlIjoiQUNUSVZFIiwiYXBwSWQiOiIyYzU4MDBlNWJkMjg4MjU4MThiZGJkNDIxMTk3YzliODE3ODI4ODE1MTE4MTEiLCJ1c2VySWQiOiJJU1BMMTkwMjciLCJsb2dpblNvdXJjZSI6IkFQUCIsInN1YiI6IklTUEwxOTAyNyIsImlhdCI6MTc4Mjg4NTcyNCwiZXhwIjoxNzgyOTcyMTI0fQ.vOtMRJcTEXCFDFvTbhmTpc8_pTWEyR_E6MLo8L-_jBqzlLLtYJZsTltpdmWZEz8MRQqD1wSGc8M3lScv7XOu1w'   # refresh ~every 24h
APP_ID='2c5800e5bd28825818bdbd421197c9b81782881511811'
CLIENT_ID='ISPL19027'
SOURCE='IOS'

# ── Endpoints ────────────────────────────────────────────────────────────────
GATEWAY_URL='http://localhost:8080'     # api-gateway (change to the UAT host)
KAFKA_BROKER='localhost:9092'
TOPIC='news-events'

# ── Redis (where market data + ticks live) ───────────────────────────────────
REDIS_HOST='192.168.46.237'
REDIS_PORT='6379'
REDIS_DB='1'          # market:{exch}:{token}   (rules-engine REDIS_DB)
TICKSTORE_DB='1'      # ticks:{exch}:{token}    (TICKSTORE_REDIS_DB)

# ── Real stock for mock news that must price off LIVE LTP ────────────────────
# Used for QTY / VALUE / TRADE / AUDIT cases. MUST be an actively-traded NSE
# stock that already has live LTP in your market data (market:nse:<token>).
REAL_TOKEN='2885'        # NSE token (change to a stock you know is live)
REAL_SYMBOL='RELIANCE'

# ── Misc ─────────────────────────────────────────────────────────────────────
TRADING_MODE='LIVE'  # PAPER (safe, no real orders) | LIVE
