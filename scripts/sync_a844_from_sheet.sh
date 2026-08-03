#!/usr/bin/env bash
#
# sync_a844_from_sheet.sh
# ─────────────────────────────────────────────────────────────────────
# Pull A844 daily performance from the LIVE Google Sheet ("Date wise
# data" tab) and append/update rows in stockk_market.algo_performance_daily.
#
# WHY THIS EXISTS
#   The xlsx report (docs/daily_mtm_report.xlsx) has clean position-level
#   MTM data but is only refreshed occasionally. This script fills the
#   tail: rows dated 2026-07-06 and later come from the live sheet so the
#   Manthan Performance tile stays current between xlsx refreshes.
#
# SAFETY
#   * Only touches (algo_id='algo_manthan_v1', reference_client_id='A844')
#   * Only rows with date >= CUTOFF (default 2026-07-06) are considered
#   * Rows containing any spreadsheet error (#REF!, #DIV/0!, #N/A,
#     #VALUE!, #NAME?, #NUM!, #NULL!) are SKIPPED — never persisted
#   * Rows with blank %Return or blank P&L are SKIPPED
#   * ON CONFLICT DO UPDATE — idempotent, re-run any time to pick up
#     corrections in the sheet
#   * All INSERTs wrapped in one transaction — either all land or none
#
# USAGE
#   ./scripts/sync_a844_from_sheet.sh
#   ./scripts/sync_a844_from_sheet.sh 2026-07-06     # custom cutoff
#
# ENV
#   PGHOST, PGPORT, PGUSER, PGDATABASE   Postgres connection (defaults
#                                        localhost:5432/postgres/stockk_market)
#   PGPASSWORD                           Optional; falls back to `postgres`

set -euo pipefail

CUTOFF="${1:-2026-07-06}"
SHEET_ID="1Facw3qxlZkua4uumY__pD6lGNFk3G2Pd5niqJP0D6m0"
GID="2105971053"                            # "Date wise data" tab
CSV_URL="https://docs.google.com/spreadsheets/d/${SHEET_ID}/export?format=csv&gid=${GID}"

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-postgres}"
PGDATABASE="${PGDATABASE:-stockk_market}"
export PGPASSWORD="${PGPASSWORD:-postgres}"

WORKDIR="$(mktemp -d)"
trap 'rm -rf "$WORKDIR"' EXIT

CSV="$WORKDIR/date_wise_data.csv"
SQL="$WORKDIR/upsert.sql"

echo "[sync] downloading live sheet…"
curl -fsSL "$CSV_URL" -o "$CSV"
rows_dl=$(wc -l <"$CSV")
echo "[sync] downloaded ${rows_dl} rows"

echo "[sync] parsing + generating SQL (cutoff ${CUTOFF})…"
python3 - "$CSV" "$SQL" "$CUTOFF" <<'PY'
import csv, sys, re
from datetime import datetime, date as _date

csv_path, sql_path, cutoff = sys.argv[1], sys.argv[2], sys.argv[3]
ERROR_MARKERS = ('#REF!', '#DIV/0!', '#N/A', '#VALUE!', '#NAME?', '#NUM!', '#NULL!')

def has_err(cells):
    for c in cells:
        s = str(c).strip() if c is not None else ''
        for m in ERROR_MARKERS:
            if m in s: return True
    return False

def num(s):
    if s is None: return None
    s = str(s).strip().replace(',', '').replace('%', '').replace('"', '')
    if s in ('', '-'): return None
    try: return float(s)
    except: return None

def parse_date(d):
    d = d.strip().replace('-', '/')
    for fmt in ('%m/%d/%Y', '%m/%d/%y'):
        try: return datetime.strptime(d, fmt).date().isoformat()
        except: pass
    return None

with open(csv_path) as f:
    rows = list(csv.reader(f))

hdr = [h.strip() for h in rows[0]]
def col(name):
    for i, h in enumerate(hdr):
        if h.lower() == name.lower(): return i
    return -1

i_date, i_cid = col('Date'), col('Client id')
i_inv, i_ns   = col('Investment'), col('No of Scripts')
i_ret, i_pnl  = col('%Return'), col('P&L')
i_mtn         = col('Daily MTN')
i_mtm, i_exp  = col('Daily MTM %'), col('% of exposure')

if min(i_date, i_cid, i_ret, i_pnl) < 0:
    print("ERROR: sheet header missing required columns", file=sys.stderr)
    sys.exit(1)

queued = skipped_bad = skipped_before_cutoff = skipped_weekend = 0
skipped_reasons = {'error_cells': 0, 'blank_ret_pnl': 0, 'bad_date': 0, 'not_a844': 0, 'weekend': 0}

with open(sql_path, 'w') as out:
    out.write("BEGIN;\n")
    for r in rows[1:]:
        if len(r) <= i_cid: continue
        if (r[i_cid] or '').strip() != 'A844':
            skipped_reasons['not_a844'] += 1
            continue
        d_iso = parse_date(r[i_date])
        if not d_iso:
            skipped_reasons['bad_date'] += 1
            skipped_bad += 1
            continue
        if d_iso < cutoff:
            skipped_before_cutoff += 1
            continue
        # Hard gate: NSE is closed on Sat/Sun. Any weekend row is a
        # source-file bug — never persist it, regardless of the fields.
        dt = _date.fromisoformat(d_iso)
        if dt.weekday() >= 5:   # 5=Sat, 6=Sun
            skipped_weekend += 1
            skipped_reasons['weekend'] += 1
            print(f"[skip] {d_iso}: {dt.strftime('%A')} — market closed", file=sys.stderr)
            continue
        # Check the exact cells we care about for #REF! etc
        care_cells = [r[i] for i in (i_inv, i_ns, i_ret, i_pnl, i_mtn, i_mtm, i_exp) if 0 <= i < len(r)]
        if has_err(care_cells):
            skipped_reasons['error_cells'] += 1
            skipped_bad += 1
            print(f"[skip] {d_iso}: contains spreadsheet error marker", file=sys.stderr)
            continue
        ret = num(r[i_ret]); pnl = num(r[i_pnl])
        if ret is None or pnl is None:
            skipped_reasons['blank_ret_pnl'] += 1
            skipped_bad += 1
            print(f"[skip] {d_iso}: blank %Return or P&L", file=sys.stderr)
            continue
        inv = num(r[i_inv]) or 500000
        ns  = num(r[i_ns]); ns = int(ns) if ns is not None else None
        mtn = num(r[i_mtn]) if i_mtn >= 0 and len(r) > i_mtn else None
        mtm = num(r[i_mtm]) if i_mtm >= 0 and len(r) > i_mtm else None
        exp = num(r[i_exp]) if i_exp >= 0 and len(r) > i_exp else None

        def n(v): return "NULL" if v is None else f"{v}"
        out.write(f"""INSERT INTO algo_performance_daily
  (algo_id, reference_client_id, date, investment_amount, num_scripts,
   return_pct, pnl_amount, daily_mtm_amount, daily_mtm_pct, exposure_pct, updated_at)
VALUES
  ('algo_manthan_v1','A844','{d_iso}',{inv},{n(ns)},{ret},{pnl},{n(mtn)},{n(mtm)},{n(exp)},now())
ON CONFLICT (algo_id, reference_client_id, date) DO UPDATE
  SET investment_amount = EXCLUDED.investment_amount,
      num_scripts       = EXCLUDED.num_scripts,
      return_pct        = EXCLUDED.return_pct,
      pnl_amount        = EXCLUDED.pnl_amount,
      daily_mtm_amount  = EXCLUDED.daily_mtm_amount,
      daily_mtm_pct     = EXCLUDED.daily_mtm_pct,
      exposure_pct      = EXCLUDED.exposure_pct,
      updated_at        = EXCLUDED.updated_at;
""")
        queued += 1
    out.write("COMMIT;\n")

print(f"[sync] queued={queued}  skipped_bad={skipped_bad}  skipped_weekend={skipped_weekend}  skipped_before_cutoff={skipped_before_cutoff}", file=sys.stderr)
print(f"[sync] reasons: {skipped_reasons}", file=sys.stderr)
PY

echo "[sync] applying to $PGDATABASE…"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -q -v ON_ERROR_STOP=1 -f "$SQL"

echo "[sync] verifying…"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "
SELECT COUNT(*) AS total_rows, MIN(date) AS first_date, MAX(date) AS latest_date
FROM algo_performance_daily
WHERE algo_id='algo_manthan_v1' AND reference_client_id='A844';"

psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" -c "
SELECT date, num_scripts, return_pct, pnl_amount, daily_mtm_amount, daily_mtm_pct, exposure_pct
FROM algo_performance_daily
WHERE algo_id='algo_manthan_v1' AND reference_client_id='A844'
  AND date >= '${CUTOFF}'
ORDER BY date;"

echo "[sync] done."
