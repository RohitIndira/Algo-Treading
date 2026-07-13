#!/usr/bin/env bash
#
# cleanup_test_strategies.sh — deactivates (default) or deletes the AUDIT-* test
# strategies created by create_test_strategies.sh. Run this once you've seen the
# audit cases in the logs, so they stop matching every incoming news event.
#
# Usage:
#   ./cleanup_test_strategies.sh            # deactivate AUDIT-* strategies
#   ACTION=delete ./cleanup_test_strategies.sh
#
# Requires jq.
set -euo pipefail

# Paste your values in config.sh (same folder) — this sources them.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/config.sh"
ACTION="${ACTION:-deactivate}"            # deactivate | delete

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

auth=(-H "Authorization: Bearer ${ACCESS_TOKEN}" -H "appId: ${APP_ID}" -H "userId: ${CLIENT_ID}" -H "source: ${SOURCE}")

echo "Listing strategies for ${CLIENT_ID} ..."
list=$(curl -sS "${GATEWAY_URL}/api/v1/users/${CLIENT_ID}/strategies?page=1&page_size=200" "${auth[@]}")

# Match by category, not strategy_name — create_test_strategies.sh names strategies
# freely (e.g. "High_Exp_Order-11"), but every AUDIT test strategy's condition
# category always starts with AUDIT_ (AUDIT_TRADE/DPR/QTY/VALUE/EXPOSURE/BAN/LIFECYCLE).
echo "$list" | jq -r '.strategies[]? | select((.conditions.categories // []) | any(startswith("AUDIT_"))) | "\(.strategy_id)\t\(.strategy_name)"' \
| while IFS=$'\t' read -r sid sname; do
  [ -z "$sid" ] && continue
  if [ "$ACTION" = "delete" ]; then
    echo "Deleting ${sname} (${sid})"
    curl -sS -X DELETE "${GATEWAY_URL}/api/v1/strategies/${sid}?user_id=${CLIENT_ID}" "${auth[@]}" >/dev/null
  else
    echo "Deactivating ${sname} (${sid})"
    curl -sS -X POST "${GATEWAY_URL}/api/v1/strategies/${sid}/deactivate" "${auth[@]}" \
      -H "Content-Type: application/json" -d "{\"user_id\":\"${CLIENT_ID}\"}" >/dev/null
  fi
done

echo "Done (${ACTION})."
