#!/usr/bin/env bash
#
# run_all.sh — exercise the full user-config CRUD + credentials + negative suite
# through api-gateway. Sequential, self-cleaning, prints PASS/FAIL per check.
#
# Usage:
#   export JWT="<fresh broker JWT>"
#   ./run_all.sh
#
# Optional overrides (defaults shown):
#   BASE=http://localhost:8080/api/v1
#   USER_ID=S4450
#   APP_ID=<decoded from JWT automatically if python3 present, else must be set>
#   SOURCE=WEB
#
# NOTE: uses trading_mode=PAPER + activate_immediately=false so nothing arms
# real capital. jq required for pretty output; falls back to raw if absent.

set -uo pipefail

BASE="${BASE:-http://localhost:8080/api/v1}"
USER_ID="${USER_ID:-S4450}"
SOURCE="${SOURCE:-WEB}"

if [[ -z "${JWT:-}" ]]; then
  echo "ERROR: export JWT=<fresh broker JWT> first." >&2
  exit 1
fi

# Auto-derive APP_ID from the JWT payload if not provided.
if [[ -z "${APP_ID:-}" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    APP_ID="$(python3 - "$JWT" <<'PY'
import base64, json, sys
p = sys.argv[1].split('.')[1]
p += '=' * (-len(p) % 4)
print(json.loads(base64.urlsafe_b64decode(p)).get('appId', ''))
PY
)"
  fi
fi
if [[ -z "${APP_ID:-}" ]]; then
  echo "ERROR: APP_ID not set and could not derive from JWT (need python3). export APP_ID=<appId claim>." >&2
  exit 1
fi

H=(-H "Authorization: Bearer $JWT" -H "userId: $USER_ID" -H "appId: $APP_ID" -H "source: $SOURCE")
JH=("${H[@]}" -H "Content-Type: application/json")

pass=0; fail=0
check() { # check <label> <want_code> <got_code>
  if [[ "$2" == "$3" ]]; then echo "  PASS  $1  ($3)"; pass=$((pass+1));
  else echo "  FAIL  $1  (want $2, got $3)"; fail=$((fail+1)); fi
}
code() { tail -n1 <<<"$1"; }        # last line = http_code
body() { sed '$d' <<<"$1"; }        # everything but last line

echo "== user-config API suite =="
echo "base=$BASE user=$USER_ID source=$SOURCE"
echo

# 1. CREATE
R=$(curl -sS -w $'\n%{http_code}' -X POST "$BASE/strategies" "${JH[@]}" \
  -d "{\"user_id\":\"$USER_ID\",\"strategy_name\":\"CollectionTest\",\"strategy_type\":\"MANTHAN\",\"trading_mode\":\"PAPER\",\"activate_immediately\":false,\"trade_config\":{\"total_capital\":500000,\"stop_loss_pct\":20,\"trailing_sl_pct\":2}}")
check "1.create" 200 "$(code "$R")"
SID=$(body "$R" | (command -v jq >/dev/null && jq -r '.data.strategy_id // .strategy_id' || grep -o '"strategy_id":"[^"]*"' | head -1 | cut -d'"' -f4))
echo "  strategy_id=$SID"

# 2. READ
R=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE/strategies/$SID?user_id=$USER_ID" "${H[@]}")
check "2.read" 200 "$R"

# 3. LIST
R=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE/users/$USER_ID/strategies?page=1&page_size=20" "${H[@]}")
check "3.list" 200 "$R"

# 4. UPDATE (version 1 -> 2)
R=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "$BASE/strategies/$SID" "${JH[@]}" \
  -d '{"strategy_name":"Renamed","version":1}')
check "4.update" 200 "$R"

# 5. ACTIVATE
R=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE/strategies/$SID/activate" "${H[@]}")
check "5.activate" 200 "$R"

# 6. DEACTIVATE
R=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE/strategies/$SID/deactivate" "${JH[@]}" \
  -d '{"position_handling":"KEEP_POSITIONS_OPEN"}')
check "6.deactivate" 200 "$R"

# 7. DELETE (soft STOP)
R=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$BASE/strategies/$SID" "${H[@]}")
check "7.delete" 200 "$R"

# 8. CREDENTIALS
R=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE/auth/credentials" "${JH[@]}" \
  -d "{\"user_id\":\"$USER_ID\",\"bearer_token\":\"$JWT\"}")
check "8.credentials" 200 "$R"

echo
echo "-- negative tests --"

# a fresh strategy for version-based negatives
SID2=$(curl -sS -X POST "$BASE/strategies" "${JH[@]}" \
  -d "{\"user_id\":\"$USER_ID\",\"strategy_name\":\"NegTest\",\"strategy_type\":\"MANTHAN\",\"trading_mode\":\"PAPER\",\"trade_config\":{\"total_capital\":500000,\"stop_loss_pct\":20,\"trailing_sl_pct\":2}}" \
  | (command -v jq >/dev/null && jq -r '.data.strategy_id // .strategy_id' || grep -o '"strategy_id":"[^"]*"' | head -1 | cut -d'"' -f4))

R=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE/strategies" "${JH[@]}" \
  -d "{\"user_id\":\"$USER_ID\",\"strategy_name\":\"\",\"strategy_type\":\"MANTHAN\",\"trade_config\":{\"total_capital\":500000}}")
check "N1.empty_name" 400 "$R"

R=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE/strategies" "${JH[@]}" \
  -d "{\"user_id\":\"$USER_ID\",\"strategy_name\":\"LowCap\",\"strategy_type\":\"MANTHAN\",\"trade_config\":{\"total_capital\":100000}}")
check "N2.low_capital" 400 "$R"

R=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "$BASE/strategies/$SID2" "${JH[@]}" \
  -d '{"strategy_name":"X","version":999}')
check "N3.stale_version" 412 "$R"

R=$(curl -sS -o /dev/null -w '%{http_code}' -X PUT "$BASE/strategies/11111111-2222-3333-4444-555555555555" "${JH[@]}" \
  -d '{"strategy_name":"X","version":1}')
check "N4.not_found" 404 "$R"

R=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE/strategies" "${JH[@]}" \
  -d "{\"user_id\":\"SOMEONE_ELSE\",\"strategy_name\":\"Hack\",\"strategy_type\":\"MANTHAN\",\"trade_config\":{\"total_capital\":500000}}")
check "N5.idor" 403 "$R"

# cleanup the neg-test strategy
curl -sS -o /dev/null -X DELETE "$BASE/strategies/$SID2" "${H[@]}"

echo
echo "== $pass passed, $fail failed =="
exit $(( fail > 0 ? 1 : 0 ))
