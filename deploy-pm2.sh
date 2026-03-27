#!/bin/bash

# PM2 deploy script for all services (excluding risk-management)
# Deployment order:
#   1. data-ingestion
#   2. user-config
#   3. rules-engine
#   4. trade-execution
#   5. api-gateway  (last)
#
# Usage:
#   ./deploy-pm2.sh            # deploy all
#   ./deploy-pm2.sh restart    # restart all running instances
#   ./deploy-pm2.sh stop       # stop all instances
#   ./deploy-pm2.sh status     # show pm2 status

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STARTUP_DELAY="${STARTUP_DELAY:-3}"   # seconds to wait between services

# ── Colour helpers ──────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; }

# ── Verify pm2 is available ─────────────────────────────────────────────────
if ! command -v pm2 &>/dev/null; then
    error "pm2 not found. Install it with: npm install -g pm2"
    exit 1
fi

# ── Deployment order (path:pm2-name:binary-name) ────────────────────────────
# risk-management is intentionally excluded
declare -a SERVICES=(
    "services/data-ingestion:data-ingestion:data-ingestion"
    "services/user-config:user-config:user-config"
    "services/rules-engine:rules-engine:rules-engine"
    "services/trade-execution:trade-execution:trade-execution"
    "api/gateway:api-gateway:gateway"
)

# ── Helper: deploy a single service ─────────────────────────────────────────
deploy_service() {
    local rel_path="$1"
    local pm2_name="$2"
    local binary="$3"
    local service_dir="${PROJECT_ROOT}/${rel_path}"
    local binary_path="${service_dir}/bin/${binary}"
    local env_file="${service_dir}/.env"
    local log_dir="${service_dir}/logs"

    echo ""
    echo "=========================================="
    info "Deploying: ${pm2_name}"
    echo "  Path  : ${rel_path}"
    echo "  Binary: ${binary_path}"
    echo "=========================================="

    # Guard: binary must exist
    if [ ! -f "${binary_path}" ]; then
        error "Binary not found: ${binary_path}"
        error "Run ./build-all.sh first."
        return 1
    fi

    mkdir -p "${log_dir}"

    # Stop existing instance if running (ignore errors if not found)
    pm2 delete "${pm2_name}" 2>/dev/null || true

    # Build pm2 start command
    local pm2_cmd=(
        pm2 start "${binary_path}"
        --name "${pm2_name}"
        --log "${log_dir}/${pm2_name}.log"
        --error "${log_dir}/${pm2_name}-error.log"
        --restart-delay 3000
        --max-restarts 10
        --no-autorestart
    )

    # Inject .env if present
    if [ -f "${env_file}" ]; then
        info "Loading env: ${env_file}"
        # Parse key=value lines (skip comments and blanks), export for the process
        set -o allexport
        # shellcheck disable=SC1090
        source "${env_file}"
        set +o allexport
    else
        warn "No .env found at ${env_file} — using system environment"
    fi

    "${pm2_cmd[@]}"
    info "${pm2_name} started."
}

# ── Subcommands ─────────────────────────────────────────────────────────────
ACTION="${1:-deploy}"

case "$ACTION" in
    stop)
        info "Stopping all managed services..."
        for entry in "${SERVICES[@]}"; do
            IFS=':' read -r _ pm2_name _ <<< "$entry"
            pm2 delete "${pm2_name}" 2>/dev/null && info "Stopped ${pm2_name}" || warn "${pm2_name} was not running"
        done
        pm2 save
        exit 0
        ;;

    restart)
        info "Restarting all managed services..."
        for entry in "${SERVICES[@]}"; do
            IFS=':' read -r _ pm2_name _ <<< "$entry"
            pm2 restart "${pm2_name}" 2>/dev/null && info "Restarted ${pm2_name}" || warn "${pm2_name} not found — run deploy first"
        done
        pm2 save
        exit 0
        ;;

    status)
        pm2 list
        exit 0
        ;;

    deploy)
        ;;  # fall through to deploy logic below

    *)
        error "Unknown action: $ACTION"
        echo "Usage: $0 [deploy|restart|stop|status]"
        exit 1
        ;;
esac

# ── Deploy in order ──────────────────────────────────────────────────────────
echo ""
echo "=========================================="
echo "  PM2 Deployment"
echo "  Project : ${PROJECT_ROOT}"
echo "  Delay   : ${STARTUP_DELAY}s between services"
echo "=========================================="

FAILED=()

for i in "${!SERVICES[@]}"; do
    IFS=':' read -r rel_path pm2_name binary <<< "${SERVICES[$i]}"

    deploy_service "$rel_path" "$pm2_name" "$binary" || { FAILED+=("$pm2_name"); continue; }

    # Wait between services (skip delay after last one)
    if [ $i -lt $(( ${#SERVICES[@]} - 1 )) ]; then
        info "Waiting ${STARTUP_DELAY}s before next service..."
        sleep "${STARTUP_DELAY}"
    fi
done

# ── Save pm2 process list so it survives reboots ─────────────────────────────
pm2 save

echo ""
echo "=========================================="
if [ ${#FAILED[@]} -eq 0 ]; then
    info "All services deployed successfully."
else
    error "The following service(s) failed to deploy:"
    for name in "${FAILED[@]}"; do
        echo "    - $name"
    done
    echo ""
    warn "Run 'pm2 logs <name>' to inspect errors."
    exit 1
fi
echo ""
info "Useful commands:"
echo "  pm2 list                  — show all processes"
echo "  pm2 logs <name>           — tail logs"
echo "  pm2 monit                 — live monitor"
echo "  $0 restart                — restart all"
echo "  $0 stop                   — stop all"
echo "=========================================="
