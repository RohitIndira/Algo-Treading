# Audit / compliance-check test harness

Drives every audit + lifecycle case into the logs by publishing mock news to
Kafka and calling the gateway API. The rules-engine prices each order off the
**live LTP already in Redis** — so for most cases you seed **nothing**.

## What needs what

| Case | How it fires | Redis seed? |
|---|---|---|
| Strategy create / activate / deactivate | gateway API (`lifecycle_test.sh`) | ❌ |
| `ORDER_AUDIT` (every order) | any mock news | ❌ |
| Successful **trade** (req + res, confirmed at the broker) | mock news, qty-1 strategy | ❌ |
| QTY reject (> 50,000) | mock news, qty-60000 strategy | ❌ |
| VALUE reject (> ₹2cr) | mock news, qty-50000 strategy | ❌ |
| DPR reject | mock news + 1 crafted Redis value | ✅ |
| EXPOSURE reject (2nd order pushes cumulative value over the cap) | mock news ×2 + 2 crafted Redis values | ✅ |
| BAN reject (token in `BANNED_TOKENS`) | mock news + 1 crafted Redis value | ✅ |

**DPR**, **EXPOSURE**, and **BAN** all seed Redis on a **real** token (a real
stock never leaves its DPR band on demand, so a breach has to be crafted
either way). The script `SET`s the crafted value immediately before
publishing the news, and the compliance check reads whatever's in Redis at
that instant — see the Notes section for the one timing caveat this implies.
Everything else = mock news + strategies, no Redis.

## Setup (once)

Edit **`config.sh`**:
- paste `ACCESS_TOKEN` (refresh ~24h), set `GATEWAY_URL` to the gateway host.
- set `REAL_TOKEN` / `REAL_SYMBOL` to an **actively-traded** NSE stock that has
  live LTP (default RELIANCE/2475). This is the stock used for QTY/VALUE/TRADE.
- `REDIS_DB` **must equal the rules-engine's `REDIS_DB`**.
- `TRADING_MODE`: `PAPER` recommended for testing. **`LIVE` places a real
  1-share order in the TRADE case.**
- **EXPOSURE** and **BAN** also need env vars set **on the rules-engine
  itself**: `MAX_EXPOSURE_LIMIT` > 0 (e.g. `10000000` = ₹1cr; it defaults to
  `0` = check disabled) and `BANNED_TOKENS` must include `14366`. Without
  these the order still goes through and nothing rejects it.

Two ways to run this, sharing the same `config.sh`:
- **Linux/UAT box** (needs `redis-cli`, `kcat`, `curl`, `jq`): `bash <script>`.
- **Windows dev machine** that can reach the UAT gateway/Kafka/Redis over the
  network but doesn't have those CLI tools: `powershell -ExecutionPolicy
  Bypass -File .\<script>.ps1`. It talks Kafka via the bundled
  `kafka_publisher` Go binary (auto-built on first run — needs `go`) and Redis
  via raw TCP, so no extra tooling is required on the Windows box.
  `lifecycle_test.sh` / `cleanup_test_strategies.sh` are gateway-API-only and
  have no `.ps1` port yet; run them via `bash` (WSL/Git Bash work fine).

## Run

```bash
# watch (two terminals)
pm2 logs rules-engine    | grep -E 'STRATEGY_INITIALIZED|ORDER_AUDIT|ORDER_REJECTED'
pm2 logs trade-execution | grep -E 'Placing order|EXECUTED|response'

# create the AUDIT-* strategies (also logs STRATEGY_INITIALIZED)
bash create_test_strategies.sh        # all 6; or: bash create_test_strategies.sh trade|qty|value|dpr|exposure|ban
                                       # wait ~5s for Kafka propagation

# strategy lifecycle: create → activate → deactivate
bash lifecycle_test.sh

# all audit cases + the successful trade
bash run_audit_tests.sh               # or: bash run_audit_tests.sh trade|qty|value|dpr|exposure|ban

# remove the test strategies
bash cleanup_test_strategies.sh       # ACTION=delete for hard delete
```

From a Windows box (same `config.sh`, same cases; no `lifecycle_test`/`cleanup`
port yet):

```powershell
powershell -ExecutionPolicy Bypass -File .\create_strategies.ps1          # or: ... .ps1 trade|qty|value|dpr|exposure|ban
powershell -ExecutionPolicy Bypass -File .\run_audit_tests.ps1            # or: ... .ps1 trade|qty|value|dpr|exposure|ban|all
```

## Notes
- **Market hours**: if `ENFORCE_MARKET_HOURS=true` and off-hours, orders are
  silently skipped. Test 09:15–15:30 IST or set it false on the rules-engine.
- The **TRADE** case: qty 1 → passes all checks → `ORDER_AUDIT` (approved) in
  rules-engine, then trade-execution logs the broker **request** (`Placing order
  … payload=…`) and **response**. In LIVE that's a real 1-share order. The
  script doesn't stop at the logs — after a 6s wait it calls
  `GET /api/v1/live-orders/indira-positions`, which queries the **broker
  directly** (not our own DB), and prints the resulting `REAL_SYMBOL` position
  if found. That's the actual proof rules-engine handed the order to
  trade-execution and trade-execution placed it — logs alone only show what
  our own services *say* happened. If it prints "NOT FOUND", either the 6s
  wasn't enough (rerun the check manually against the same endpoint) or the
  hand-off broke somewhere in the pipeline.
- Rejected orders don't create a same-day lock (re-runnable). A *passing* order
  (TRADE) locks that stock for the day — use a different `REAL_TOKEN` to repeat.
- **DPR** seeds ACC (token 22) with its **real** circuit band and an `ltp` bumped
  just above the real `dpr_upper`, so the LIMIT order prices above the band. The
  `.sh` version reads ACC's live band from Redis when the feed has written it
  (rich doc with a `high` field), else falls back to ACC's known band
  `[1065.8, 1598.6]`; the `.ps1` version uses that band statically.
- **DPR / EXPOSURE** both write a crafted value to a real, actively-traded token
  (ACC, DIXON, ADANIENT) right before publishing. During market hours the live
  feed is also writing to that same token — if it lands between the seed and the
  compliance check reading it, the crafted value gets overwritten and the case
  misses. If one of these flakes, that's almost always why — just re-run it.
- **BAN** is the one exception: it seeds a real token (IDEA) too, but the ban
  check only cares whether the token is on the list, not its price, so a live
  feed overwrite doesn't affect the outcome.
