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
#   ./deploy-pm2.sh                  # deploy all
#   ./deploy-pm2.sh restart          # restart all running instances
#   ./deploy-pm2.sh stop             # stop all instances
#   ./deploy-pm2.sh status           # show pm2 status
#   ./deploy-pm2.sh setup-logrotate  # install/configure pm2-logrotate (run once)
#
# Logging model: each service logs ONLY to stdout; PM2's --log captures stdout
# and stderr into a single combined file per instance under <service>/logs/.
# That PM2 file is the single source of truth — `pm2 logs <name>` shows exactly
# its contents. Run `setup-logrotate` once so these files rotate daily.

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

# ── Deployment order (path:pm2-name:binary-name:instances) ──────────────────
# instances > 1 starts that many numbered PM2 processes sharing the same
# Kafka consumer group — Kafka distributes partitions across them automatically.
# risk-management is intentionally excluded.
declare -a SERVICES=(
    "services/data-ingestion:data-ingestion:data-ingestion:1"
    "services/user-config:user-config:user-config:1"
    "services/rules-engine:rules-engine:rules-engine:2"
    "services/trade-execution:trade-execution:trade-execution:2"
    "api/gateway:api-gateway:gateway:1"
)

# ── Helper: deploy a single service (optionally multiple numbered instances) ──
# Args: rel_path  pm2_name  binary  instances
# When instances > 1, starts processes named <pm2_name>-0, <pm2_name>-1, …
# Each shares the same Kafka consumer group — Kafka rebalances partitions.
deploy_service() {
    local rel_path="$1"
    local pm2_name="$2"
    local binary="$3"
    local instances="${4:-1}"
    local service_dir="${PROJECT_ROOT}/${rel_path}"
    local binary_path="${service_dir}/bin/${binary}"
    local env_file="${service_dir}/.env"
    local log_dir="${service_dir}/logs"

    echo ""
    echo "=========================================="
    info "Deploying: ${pm2_name} (x${instances})"
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

    # Inject .env if present (exported into the shell so pm2 inherits them)
    if [ -f "${env_file}" ]; then
        info "Loading env: ${env_file}"
        set -o allexport
        # shellcheck disable=SC1090
        source "${env_file}"
        set +o allexport
    else
        warn "No .env found at ${env_file} — using system environment"
    fi

    # Stop all existing instances for this service (handles both single and
    # previously-multi-instance deployments).
    if [ "${instances}" -gt 1 ]; then
        for idx in $(seq 0 $(( instances - 1 ))); do
            pm2 delete "${pm2_name}-${idx}" 2>/dev/null || true
        done
    else
        pm2 delete "${pm2_name}" 2>/dev/null || true
    fi

    # Start the requested number of instances.
    for idx in $(seq 0 $(( instances - 1 ))); do
        if [ "${instances}" -gt 1 ]; then
            local instance_name="${pm2_name}-${idx}"
        else
            local instance_name="${pm2_name}"
        fi

        # Single source of truth: --log writes BOTH stdout and stderr into one
        # combined file per instance. The app no longer writes its own file
        # (LOG_DIR is unset), so this PM2 file is the only on-disk log and
        # `pm2 logs ${instance_name}` shows exactly its contents.
        # --cwd pins the working directory so any relative paths resolve here.
        pm2 start "${binary_path}" \
            --name "${instance_name}" \
            --cwd  "${service_dir}" \
            --log  "${log_dir}/${instance_name}.log" \
            --restart-delay 3000 \
            --max-restarts 10 \
            --no-autorestart

        info "${instance_name} started → ${log_dir}/${instance_name}.log"
    done
}

# ── Subcommands ─────────────────────────────────────────────────────────────
ACTION="${1:-deploy}"

case "$ACTION" in
    stop)
        info "Stopping all managed services..."
        for entry in "${SERVICES[@]}"; do
            IFS=':' read -r _ pm2_name _ instances <<< "$entry"
            instances="${instances:-1}"
            if [ "${instances}" -gt 1 ]; then
                for idx in $(seq 0 $(( instances - 1 ))); do
                    pm2 delete "${pm2_name}-${idx}" 2>/dev/null && info "Stopped ${pm2_name}-${idx}" || warn "${pm2_name}-${idx} was not running"
                done
            else
                pm2 delete "${pm2_name}" 2>/dev/null && info "Stopped ${pm2_name}" || warn "${pm2_name} was not running"
            fi
        done
        pm2 save
        exit 0
        ;;

    restart)
        info "Restarting all managed services..."
        for entry in "${SERVICES[@]}"; do
            IFS=':' read -r _ pm2_name _ instances <<< "$entry"
            instances="${instances:-1}"
            if [ "${instances}" -gt 1 ]; then
                for idx in $(seq 0 $(( instances - 1 ))); do
                    pm2 restart "${pm2_name}-${idx}" 2>/dev/null && info "Restarted ${pm2_name}-${idx}" || warn "${pm2_name}-${idx} not found — run deploy first"
                done
            else
                pm2 restart "${pm2_name}" 2>/dev/null && info "Restarted ${pm2_name}" || warn "${pm2_name} not found — run deploy first"
            fi
        done
        pm2 save
        exit 0
        ;;

    status)
        pm2 list
        exit 0
        ;;

    setup-logrotate)
        # Since the app no longer writes dated sub-folders, PM2's pm2-logrotate
        # module handles daily rotation + retention of the combined log files.
        info "Installing and configuring pm2-logrotate..."
        pm2 install pm2-logrotate
        pm2 set pm2-logrotate:max_size 50M
        pm2 set pm2-logrotate:retain 30
        pm2 set pm2-logrotate:compress true
        pm2 set pm2-logrotate:rotateInterval '0 0 * * *'   # daily at midnight
        pm2 set pm2-logrotate:dateFormat 'YYYY-MM-DD_HH-mm-ss'
        info "pm2-logrotate configured: rotate daily, keep 30, compress, max 50M."
        exit 0
        ;;

    deploy)
        ;;  # fall through to deploy logic below

    *)
        error "Unknown action: $ACTION"
        echo "Usage: $0 [deploy|restart|stop|status|setup-logrotate]"
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
    IFS=':' read -r rel_path pm2_name binary instances <<< "${SERVICES[$i]}"
    instances="${instances:-1}"

    deploy_service "$rel_path" "$pm2_name" "$binary" "$instances" || { FAILED+=("$pm2_name"); continue; }

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
echo "  pm2 logs <name>           — tail logs (same content as the file)"
echo "  pm2 monit                 — live monitor"
echo "  $0 restart                — restart all"
echo "  $0 stop                   — stop all"
echo "  $0 setup-logrotate        — install/configure daily log rotation"
echo "=========================================="
