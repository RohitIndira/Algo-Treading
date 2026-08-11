#!/usr/bin/env bash
#
# sync_nifty_benchmark.sh
# ─────────────────────────────────────────────────────────────────────
# Refresh stockk_market.benchmark_daily (benchmark_id='nifty50') from
# Yahoo Finance (^NSEI), so the "Manthan vs NIFTY 50" comparison on the
# performance screen is real and current.
#
# WHY THIS EXISTS
#   benchmark_daily was seeded ONCE and never refreshed: on 2026-08-11 it
#   ended 2026-07-03 while the algo series ran to 2026-08-10. Any period
#   toggle shorter than ~6 weeks had NO benchmark rows at all, so NIFTY
#   showed 0/flat while Manthan moved. This script closes that gap and is
#   meant to run daily after close.
#
# SEMANTICS (must match the existing rows)
#   close_value = Yahoo adjusted close for the session
#   return_pct  = DAILY change %, i.e. (close/prev_close − 1) × 100
#                 (verified against seeded rows, e.g. 2026-07-03 = 0.3497)
#
# SAFETY
#   * Idempotent: ON CONFLICT (benchmark_id, date) DO UPDATE — re-run any time.
#   * Only touches benchmark_id='nifty50'.
#   * Rows with a null close (exchange holiday in Yahoo's series) are SKIPPED.
#   * The most recent row may be a PARTIAL (live) candle while the market is
#     open; the next run overwrites it with the settled close.
#
# USAGE
#   ./scripts/sync_nifty_benchmark.sh            # default 3mo (daily top-up)
#   ./scripts/sync_nifty_benchmark.sh 5y         # full backfill
#
# ENV
#   PGHOST, PGPORT, PGUSER, PGDATABASE, PGPASSWORD
#   (defaults localhost:5432/postgres/stockk_market)

set -euo pipefail

RANGE="${1:-3mo}"
BENCH_ID="nifty50"
URL="https://query1.finance.yahoo.com/v8/finance/chart/%5ENSEI?range=${RANGE}&interval=1d"

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-stockk_market}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT
JSON="$WORKDIR/nsei.json"
SQL="$WORKDIR/upsert.sql"

echo "[nifty] fetching ^NSEI range=${RANGE} from Yahoo…"
# Yahoo rejects the default curl agent.
curl -fsSL -H "User-Agent: Mozilla/5.0" --max-time 30 "$URL" -o "$JSON"

python3 - "$JSON" "$SQL" "$BENCH_ID" <<'PY'
import json, sys
from datetime import datetime, timezone

json_path, sql_path, bench_id = sys.argv[1], sys.argv[2], sys.argv[3]
doc = json.load(open(json_path))
res = (doc.get("chart") or {}).get("result") or []
if not res:
    err = (doc.get("chart") or {}).get("error")
    print(f"ERROR: Yahoo returned no result ({err})", file=sys.stderr)
    sys.exit(1)

ts = res[0].get("timestamp") or []
quote = (res[0].get("indicators", {}).get("quote") or [{}])[0]
closes = quote.get("close") or []

# Pair (date, close), dropping holiday nulls.
points = []
for t, c in zip(ts, closes):
    if c is None:
        continue
    d = datetime.fromtimestamp(t, tz=timezone.utc).date().isoformat()
    points.append((d, float(c)))

if not points:
    print("ERROR: no usable close prices in Yahoo response", file=sys.stderr)
    sys.exit(1)

with open(sql_path, "w") as out:
    # Upsert target — created once, idempotent.
    out.write("CREATE UNIQUE INDEX IF NOT EXISTS ux_benchmark_daily_id_date "
              "ON benchmark_daily (benchmark_id, date);\n")
    out.write("BEGIN;\n")
    for i, (d, c) in enumerate(points):
        # Daily change vs the previous session IN THIS BATCH; for the first
        # point we defer to whatever is already stored (COALESCE below) so a
        # short top-up run never overwrites a good value with 0.
        if i == 0:
            ret = "NULL"
        else:
            prev = points[i - 1][1]
            ret = f"{(c / prev - 1) * 100:.4f}" if prev else "NULL"
        out.write(
            "INSERT INTO benchmark_daily (benchmark_id, date, close_value, return_pct, updated_at)\n"
            f"VALUES ('{bench_id}', '{d}', {c:.4f}, {ret}, now())\n"
            "ON CONFLICT (benchmark_id, date) DO UPDATE\n"
            "  SET close_value = EXCLUDED.close_value,\n"
            "      return_pct  = COALESCE(EXCLUDED.return_pct, benchmark_daily.return_pct),\n"
            "      updated_at  = EXCLUDED.updated_at;\n"
        )
    out.write("COMMIT;\n")

print(f"[nifty] parsed {len(points)} sessions: {points[0][0]} → {points[-1][0]}", file=sys.stderr)
PY

echo "[nifty] applying to ${PGDATABASE}…"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -q -v ON_ERROR_STOP=1 -f "$SQL"

echo "[nifty] verifying…"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "
SELECT count(*) AS rows, min(date) AS first_date, max(date) AS latest_date
FROM benchmark_daily WHERE benchmark_id='${BENCH_ID}';"

echo "[nifty] done."
