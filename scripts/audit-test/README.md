# Audit / compliance-check test harness

Drives every audit + lifecycle case into the logs by publishing mock news to
Kafka and calling the gateway API. The rules-engine prices each order off the
**live LTP already in Redis** — so for most cases you seed **nothing**.

## What needs what

| Case | How it fires | Redis seed? |
|---|---|---|
| Strategy create / activate / deactivate | gateway API (`lifecycle_test.sh`) | ❌ |
| `ORDER_AUDIT` (every order) | any mock news | ❌ |
| Successful **trade** (req + res) | mock news, qty-1 strategy | ❌ |
| QTY reject (> 50,000) | mock news, qty-60000 strategy | ❌ |
| VALUE reject (> ₹2cr) | mock news, qty-50000 strategy | ❌ |
| DPR reject | mock news + 1 crafted Redis value (fake token) | ✅ |
| VELOCITY reject | mock news + crafted ticks (fake token) | ✅ |

Only **DPR** and **VELOCITY** need Redis, because a real stock never leaves its
circuit band or jumps 1%/1s on demand — so those two use a throwaway fake token
the live feed never overwrites. Everything else = mock news + strategies.

## Setup (once)

Edit **`config.sh`**:
- paste `ACCESS_TOKEN` (refresh ~24h), set `GATEWAY_URL` to the gateway host.
- set `REAL_TOKEN` / `REAL_SYMBOL` to an **actively-traded** NSE stock that has
  live LTP (default RELIANCE/2475). This is the stock used for QTY/VALUE/TRADE.
- `REDIS_DB` **must equal the rules-engine's `REDIS_DB`**, `TICKSTORE_DB` its
  `TICKSTORE_REDIS_DB` (only matters for the DPR/velocity cases).
- `TRADING_MODE`: `PAPER` recommended for testing. **`LIVE` places a real
  1-share order in the TRADE case.**

Run everything from a **bash shell on the Linux/UAT box** (needs `redis-cli`,
`kcat`, `curl`, `jq`). Don't use PowerShell / `chmod` — just `bash <script>`.

## Run

```bash
# watch (two terminals)
pm2 logs rules-engine    | grep -E 'STRATEGY_INITIALIZED|ORDER_AUDIT|ORDER_REJECTED'
pm2 logs trade-execution | grep -E 'Placing order|EXECUTED|response'

# create the AUDIT-* strategies (also logs STRATEGY_INITIALIZED)
bash create_test_strategies.sh        # wait ~5s for Kafka propagation

# strategy lifecycle: create → activate → deactivate
bash lifecycle_test.sh

# all audit cases + the successful trade
bash run_audit_tests.sh               # or: bash run_audit_tests.sh trade|qty|value|dpr|velocity

# remove the test strategies
bash cleanup_test_strategies.sh       # ACTION=delete for hard delete
```

## Notes
- **Market hours**: if `ENFORCE_MARKET_HOURS=true` and off-hours, orders are
  silently skipped. Test 09:15–15:30 IST or set it false on the rules-engine.
- The **TRADE** case: qty 1 → passes all checks → `ORDER_AUDIT` (approved) in
  rules-engine, then trade-execution logs the broker **request** (`Placing order
  … payload=…`) and **response**. In LIVE that's a real 1-share order.
- Rejected orders don't create a same-day lock (re-runnable). A *passing* order
  (TRADE) locks that stock for the day — use a different `REAL_TOKEN` to repeat.
- Velocity uses a 1s window; if it misses on Kafka lag, set
  `VELOCITY_WINDOW_MS=5000` on rules-engine and re-run `bash run_audit_tests.sh velocity`.
