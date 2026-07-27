# syntax=docker/dockerfile:1
# ============================================================================
# Trade Execution Service — order lifecycle and broker integration (gRPC 9004)
#
# Owns the `trading_execution` database and additionally opens a read
# connection to `trading_db` for startup strategy hydration.
#
# Build context MUST be the repo root. This module's go.mod uses relative
# `replace` directives (../../, ../../pkg/indira, ../../api/proto/*), so a
# narrower context cannot resolve them.
#
#   docker build -f services/trade-execution/Dockerfile -t algo-trading/trade-execution .
#   docker compose --profile apps build trade-execution
# ============================================================================

ARG GO_VERSION=1.25
ARG ALPINE_VERSION=3.21

# --------------------------------------------------------------------------
# Build
# --------------------------------------------------------------------------
FROM golang:${GO_VERSION}-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /app
COPY . .

WORKDIR /app/services/trade-execution

# Build WITH the Go workspace (go.work at the repo root).
#
# This service imports sibling modules — pkg/logger, pkg/indira, pkg/decisions
# and api/proto/* — that are resolved through the workspace's `use`
# directives, not through its own go.mod. The previous Dockerfile set
# GOWORK=off, which hid those modules and failed with
# "module .../pkg/logger ... is replaced but not required".
# Do not re-add GOWORK=off without first adding real `require` entries.
ENV CGO_ENABLED=0 \
    GOOS=linux

# BuildKit cache mounts: module downloads and compiled packages survive
# between builds, so a code-only change no longer re-downloads the world.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-w -s" -o /out/trade-execution ./cmd

# --------------------------------------------------------------------------
# Runtime
# --------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

# ca-certificates is required here, not optional: this service calls the
# Indira broker API and a market-data WSS endpoint over TLS.
# netcat-openbsd provides `nc -z` for the gRPC healthcheck (busybox nc has
# no -z flag).
RUN apk add --no-cache ca-certificates tzdata netcat-openbsd \
 && addgroup -S app \
 && adduser -S -G app -h /app app

# Market-hours and square-off logic is IST — keep the clock in the same zone.
ENV TZ=Asia/Kolkata

WORKDIR /app
COPY --from=builder /out/trade-execution /app/trade-execution

USER app

EXPOSE 9004

HEALTHCHECK --interval=30s --timeout=5s --start-period=25s --retries=3 \
  CMD nc -z localhost 9004 || exit 1

ENTRYPOINT ["/app/trade-execution"]
