#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# config.sh — paste your values here ONCE. Every script in this folder reads it.
# (Not executed directly — it's sourced by the other scripts.)
# ─────────────────────────────────────────────────────────────────────────────

# ── Broker / gateway auth ────────────────────────────────────────────────────
ACCESS_TOKEN='eyJhbGciOiJIUzUxMiJ9.eyJyb2xlIjoiQUNUSVZFIiwiYXBwSWQiOiIwYmY0Y2RiNzJjODFjNzgwZDBjZjQxN2U1MmQ3OTAwZjE3ODM0ODkzMTgxOTQiLCJ1c2VySWQiOiJJU1BMMTkwMjciLCJsb2dpblNvdXJjZSI6IkFQUCIsInN1YiI6IklTUEwxOTAyNyIsImlhdCI6MTc4MzQ4OTMzMCwiZXhwIjoxNzgzNTc1NzMwfQ.Wc8u4mJ9Z6xJfVA_dBTzK0qDtFpTSe2tMPyCNBSWHyqO3TzbAv8K8z7ZG57WII5kjuST_hbPuhb98_YimlzP-Q'
APP_ID='0bf4cdb72c81c780d0cf417e52d7900f1783489318194'
CLIENT_ID='ISPL19027'
SOURCE='IOS'

# ── Endpoints ────────────────────────────────────────────────────────────────
GATEWAY_URL='http://localhost:8080'     # api-gateway (change to the UAT host)
KAFKA_BROKER='localhost:29092'   # PLAINTEXT_HOST listener — 9092 only resolves inside the Docker network
TOPIC='news-events'

# ── Redis (where market data + ticks live) ───────────────────────────────────
REDIS_HOST='15.207.203.46'
REDIS_PORT='6379'
REDIS_PASSWORD='R3d1s@Prod#2026'
REDIS_DB='1'          # market:{exch}:{token}   (rules-engine REDIS_DB)
TICKSTORE_DB='1'      # ticks:{exch}:{token}    (TICKSTORE_REDIS_DB)

# ── Real stock for mock news that must price off LIVE LTP ────────────────────
# Used for QTY / VALUE / TRADE / AUDIT cases. MUST be an actively-traded NSE
# stock that already has live LTP in your market data (market:nse:<token>).
REAL_TOKEN='2885'        # NSE token (change to a stock you know is live)
REAL_SYMBOL='RELIANCE'

# ── Misc ─────────────────────────────────────────────────────────────────────
TRADING_MODE='LIVE'  # PAPER (safe, no real orders) | LIVE
