# Manthan reference data — replacing the Google Sheet

Branch `feat/manthan-refdata` (2026-10-30). Operator decisions that shaped it:

| # | Decision | Where it lives |
|---|---|---|
| 1 | Buy trigger is mechanical: live LTP (external price Redis) crosses the stored ATH | `internal/manthan/trigger_watcher.go` → `ref_buy_triggers` |
| 2 | ATH is close-based, seeded from public data, never from the sheet | `internal/refdata/ath.go` → `ref_ath_close` |
| 3 | PAT basis = preferred (consolidated where published, else standalone); all three bases stored | `ref_fundamentals.basis`, `MANTHAN_PAT_BASIS` |
| 4 | Industry = sheet's Accord names; API taxonomy mapped onto them | `ref_industry_map` (+ seed SQL) |
| 5 | EMA allocation: six ±1 checks on index closes vs EMA 21/50/100, score −6..+6 → 100/100/75/50/30/20/10 % | `internal/refdata/index.go` using `internal/ema` |
| 6 | Volume / 20-bar traded-value filter stays off | unchanged (`pipeline.go`) |

Nothing in this branch changes `Pipeline.Run`, the publisher, rules-engine or
trade-execution. The DB reader produces the same `ReadResult` the sheet reader
does; `MANTHAN_SOURCE` picks which one manthan-live uses. Default stays `sheet`.

## Data sources

| Need | Source | Loader job |
|---|---|---|
| Close, market cap, shares, codes, API industry | equities API `/eod` (T+1 by 08:30 IST) | `eod`, `eod-range` |
| PAT (TTM/FY), EPS, F-score | equities API `/fundamentals` (quarterly; newest quarter fills gradually in results season) | `fundamentals` |
| Vendor intraday ATH / 52w (cross-check only) | `/fundamental/price-levels` (no auth today — told vendor) | `price-levels` |
| Our close-based ATH + 52w closes | computed from `ref_equity_eod` | `ath` |
| Index closes (NIFTY50, NFTYMCP150, NTYSLCP250) | NSE `ind_close_all_DDMMYYYY.csv`; Yahoo 2y for seed/fallback | `index-seed`, `index-ema` |
| Live LTP for the trigger | external price Redis (`isin:` → `market:nse:<token>`) | watcher inside manthan-live |

API key: `MANTHAN_EQ_API_KEY` (env only, never in code). The key first shared on
2026-10-30 was pasted into a chat and should be rotated by the vendor before
go-live. Rate limit 120 req/min; the client honours `Retry-After`.

## Tables (migration `services/data-ingestion/migrations/013_manthan_refdata.sql`, signals_db)

`ref_equity_eod`, `ref_fundamentals`, `ref_price_levels`, `ref_ath_close`,
`ref_index_close`, `ref_index_allocation`, `ref_industry_map`, `ref_buy_triggers`,
`ref_load_runs` (audit: every loader run, rows written, error).

Seed: `services/data-ingestion/migrations/seed_ref_industry_map.sql` (275 rows,
58 noted AMBIGUOUS — review those with the strategy owner).

## One-time setup on the prod box

Postgres is the docker container `algo-dev-postgres`; data-ingestion reaches it
on host port 5442 (see `ecosystem.config.js`).

```bash
cd ~/Algo-Treading
docker exec -i algo-dev-postgres psql -U postgres -d signals_db < services/data-ingestion/migrations/013_manthan_refdata.sql
docker exec -i algo-dev-postgres psql -U postgres -d signals_db < services/data-ingestion/migrations/seed_ref_industry_map.sql
go build -o deployments/separate-namespace/bin/manthan-refdata ./services/data-ingestion/cmd/manthan-refdata
go build -o deployments/separate-namespace/bin/manthan-shadow  ./services/data-ingestion/cmd/manthan-shadow

export MANTHAN_EQ_API_KEY=...                       # from the vendor, after rotation
export MARKET_DATA_DB_HOST=localhost MARKET_DATA_DB_PORT=5442 MARKET_DATA_DB_NAME=signals_db \
       MARKET_DATA_DB_USER=postgres MARKET_DATA_DB_PASSWORD=postgres MARKET_DATA_DB_SSLMODE=disable
B=deployments/separate-namespace/bin/manthan-refdata

$B -job eod-range -from 2021-01-01 -to $(date -d yesterday +%F)   # ~70 min, resumable, one call per day
$B -job fundamentals -period 2026-09
$B -job fundamentals -period 2026-06
$B -job fundamentals -period 2026-03
$B -job price-levels
$B -job ath                                                        # close-based ATH for every symbol
$B -job index-seed                                                 # 2 years of index closes from Yahoo
$B -job index-ema                                                  # NSE closes for the latest EOD day + scoring
```

Check: `select kind, params, rows_written, ok, error from ref_load_runs order by id desc limit 20;`

## Daily schedule (PM2, box time is UTC)

Already in `deployments/separate-namespace/ecosystem.config.js` on this branch:

| App | When | What |
|---|---|---|
| `manthan-refdata` | 03:05 UTC = 08:35 IST, Mon–Fri | `-job daily`: eod → fundamentals (3 quarters, all bases) → price-levels → ath → index-ema |
| `manthan-shadow` | 10:05 UTC = 15:35 IST, Mon–Fri | sheet vs db report → `/home/ubuntu/manthan-shadow/<date>.json` (remove after the flip) |
| `data-ingestion` | unchanged 09:00 IST restart | now reads `MANTHAN_SOURCE` from `.env` (default `sheet`) |

Both new apps are one-shot (`autorestart: false`): `pm2 start` runs them once
immediately, then `cron_restart` re-runs them each weekday. Add to `.env`:

```bash
MANTHAN_EQ_API_KEY=...        # vendor key, rotated
# MANTHAN_SOURCE=db           # only at the flip
```

Start them after the one-time setup above:

```bash
cd ~/Algo-Treading/deployments/separate-namespace
pm2 start ecosystem.config.js --only manthan-refdata,manthan-shadow
pm2 save
```

## Switching manthan-live to the database (the flip)

Set `MANTHAN_SOURCE=db` in `deployments/separate-namespace/.env`, then

```bash
pm2 restart ecosystem.config.js --only data-ingestion --update-env
```

outside 09:10–09:25 IST. `MANTHAN_PAT_BASIS` (default `preferred`),
`MANTHAN_TRIGGER_WATCH` (default `1`) and `MANTHAN_TRIGGER_TICK` (default `15`
seconds) can be overridden the same way. The watcher logs `trigger watcher
started` with its window; it uses `EXT_REDIS_ADDR` from the shared BASE env.
In db mode a triggered stock's entry price is the breakout LTP, not the
previous close.

## Shadow period (two weeks) — what to look at

`manthan-shadow` prints one line per sheet Buy-list symbol: verdict under the
sheet vs under the database, and every field outside 5 % tolerance. Expected,
legitimate differences:

- **Allocation**: the sheet has fed 1.0 for every index since launch; the
  database carries the real EMA score (on 2026-10-29: NIFTY50 −4 → 20 %,
  NFTYMCP150 −6 → 10 %, NTYSLCP250 −4 → 20 %). Entries will be sized smaller
  while indices trade below their EMAs. This is the rule as specified.
- **ATH**: the sheet's LifeTimeHigh tab froze on 27 Mar 2026; the database is
  current. Stocks the sheet called "ATH breakout" against a stale high may not
  be breakouts at all.
- **Market cap / PE**: the sheet's GOOGLEFINANCE cells glitch (COFORGE showed
  12,681 Cr on 2026-10-30 and was dropped as implausible; the API has 86,924 Cr).
- **PAT**: preferred basis vs whatever basis the sheet row carried.

Unexpected differences (an ISIN mismatch, a symbol missing from the EOD feed, a
mapped industry that looks wrong) are the things to fix before the flip.

## Rollback

`MANTHAN_SOURCE=sheet` (or unset) and restart data-ingestion. The ref_* tables
keep loading in the background; nothing else changes.

## Known limits

- EOD history starts January 2021. Peaks before that come from the vendor's
  intraday ATH when it is >10 % above our max close (`source =
  seed:vendor-intraday`); the trigger is stricter for those names.
- Splits/bonuses are inferred from `shares_outstanding` jumps ≥1.2×. The vendor
  says ~0.4 % of micro-caps have inconsistent share counts.
- A new listing has no ATH until it has closes in `ref_equity_eod`; it cannot
  trigger until then (BarNo rule by construction).
- Yahoo is a convenience source for index history; NSE's file is authoritative.
