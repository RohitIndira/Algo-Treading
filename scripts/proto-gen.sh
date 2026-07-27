#!/usr/bin/env bash
# ============================================================================
# Regenerates the Go code for every .proto under api/proto/.
#
# WHY YOU NEED THIS: the generated *.pb.go files are gitignored (.gitignore
# lines 51-52), so they are absent from a fresh clone and go stale whenever a
# .proto changes without someone rerunning this. That is exactly how
# `undefined: pb.AMNActivation` happens — user_config.proto grew an
# AMNActivation message and the generated code on disk predated it.
#
# Run after cloning, and after every .proto edit.
#
#   ./scripts/proto-gen.sh              # all protos
#   ./scripts/proto-gen.sh user_config  # just the matching one
#
# Uses a pinned protoc toolchain container, so nothing needs installing on the
# host. Set USE_LOCAL_PROTOC=1 to use a protoc already on your PATH.
# ============================================================================
set -euo pipefail

# Git Bash / MSYS on Windows rewrites anything that looks like a Unix path in
# an argument list, so container-side paths such as /work/api become
# C:/Program Files/Git/work/api before Docker ever sees them. Every path below
# is a *container* path — turn the rewriting off for this script.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

MODULE="github.com/RohitIndira/Algo-Treading"
IMAGE="algo-trading/protoc:local"
CONTAINER="algo-trading-protogen-$$"

# Only these declare `service`; common.proto is messages-only, so running the
# grpc plugin on it would emit a stub file that is not part of the tree.
SERVICE_PROTOS=(
  api/proto/user_config/user_config.proto
  api/proto/risk_management/risk_management.proto
  api/proto/rules_engine/rules_engine.proto
  api/proto/trade_execution/trade_execution.proto
)
MESSAGE_ONLY_PROTOS=(
  api/proto/common/common.proto
)
PROTO_DIRS=(common risk_management rules_engine trade_execution user_config)

if [ $# -gt 0 ]; then
  filter="$1"
  mapfile -t SERVICE_PROTOS < <(printf '%s\n' "${SERVICE_PROTOS[@]}" | grep -- "$filter" || true)
  mapfile -t MESSAGE_ONLY_PROTOS < <(printf '%s\n' "${MESSAGE_ONLY_PROTOS[@]}" | grep -- "$filter" || true)
  if [ ${#SERVICE_PROTOS[@]} -eq 0 ] && [ ${#MESSAGE_ONLY_PROTOS[@]} -eq 0 ]; then
    echo "No .proto matched '$filter'" >&2
    exit 1
  fi
fi

# -I . is required: the protos import "api/proto/common/common.proto", a path
# that only resolves from the repo root.
# --go_opt=module=... strips the module prefix from go_package so files land
# next to their .proto instead of under a nested github.com/... tree.
GO_OUT=(--go_out=. --go_opt="module=${MODULE}")
GRPC_OUT=(--go-grpc_out=. --go-grpc_opt="module=${MODULE}")

# ---------------------------------------------------------------------------
# Local protoc
# ---------------------------------------------------------------------------
if [ "${USE_LOCAL_PROTOC:-0}" = "1" ]; then
  [ ${#SERVICE_PROTOS[@]} -gt 0 ] && \
    protoc -I . "${GO_OUT[@]}" "${GRPC_OUT[@]}" "${SERVICE_PROTOS[@]}"
  [ ${#MESSAGE_ONLY_PROTOS[@]} -gt 0 ] && \
    protoc -I . "${GO_OUT[@]}" "${MESSAGE_ONLY_PROTOS[@]}"
  echo "==> Done (local protoc)."
  exit 0
fi

# ---------------------------------------------------------------------------
# Containerised protoc
#
# Files are copied in and out with `docker cp` rather than bind-mounted. A
# bind mount is the obvious approach, but Docker Desktop on this project's
# Windows hosts fails it with
#   "error while creating mount source path ...: mkdir /run/desktop/mnt/host/d:
#    file exists"
# for repos on the D: drive. docker cp has no such dependency on file sharing.
# ---------------------------------------------------------------------------
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "==> Building protoc toolchain image (first run only)..."
  docker build -f deployments/docker/protoc.Dockerfile -t "$IMAGE" . >/dev/null
fi

cleanup() { docker rm -f "$CONTAINER" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$CONTAINER" --entrypoint sleep "$IMAGE" 600 >/dev/null
docker exec "$CONTAINER" mkdir -p /work/api
docker cp api/proto "${CONTAINER}:/work/api/proto"

if [ ${#SERVICE_PROTOS[@]} -gt 0 ]; then
  echo "==> Generating messages + gRPC stubs"
  printf '    %s\n' "${SERVICE_PROTOS[@]}"
  docker exec "$CONTAINER" protoc -I . "${GO_OUT[@]}" "${GRPC_OUT[@]}" "${SERVICE_PROTOS[@]}"
fi

if [ ${#MESSAGE_ONLY_PROTOS[@]} -gt 0 ]; then
  echo "==> Generating messages only (no services declared)"
  printf '    %s\n' "${MESSAGE_ONLY_PROTOS[@]}"
  docker exec "$CONTAINER" protoc -I . "${GO_OUT[@]}" "${MESSAGE_ONLY_PROTOS[@]}"
fi

echo "==> Copying generated code back"
for d in "${PROTO_DIRS[@]}"; do
  [ -d "api/proto/$d" ] || continue
  for f in $(docker exec "$CONTAINER" sh -c "ls /work/api/proto/$d/*.pb.go 2>/dev/null" || true); do
    name="$(basename "$f")"
    docker cp "${CONTAINER}:${f}" "api/proto/$d/$name"
    echo "    api/proto/$d/$name"
  done
done

echo "==> Done. Rebuild with: go build ./... (or docker compose --profile apps build)"
