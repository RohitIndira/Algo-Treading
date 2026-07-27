# syntax=docker/dockerfile:1
# ============================================================================
# Risk Management Service — pre-trade risk checks and scoring (gRPC 9005)
#
# Build context MUST be the repo root. This module's go.mod uses relative
# `replace` directives (../.., ../../api/proto/*), so a narrower context
# cannot resolve them.
#
#   docker build -f services/risk-management/Dockerfile -t algo-trading/risk-management .
#   docker compose --profile apps build risk-management
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

WORKDIR /app/services/risk-management

# Build WITH the Go workspace (go.work at the repo root), matching how the
# other services build. This module happens to resolve without the workspace
# today, but keeping it consistent means a new sibling-module import here
# won't suddenly break the image.
ENV CGO_ENABLED=0 \
    GOOS=linux

# BuildKit cache mounts: module downloads and compiled packages survive
# between builds, so a code-only change no longer re-downloads the world.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-w -s" -o /out/risk-management ./cmd

# --------------------------------------------------------------------------
# Runtime
# --------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

# netcat-openbsd provides `nc -z` for the gRPC TCP healthcheck below
# (busybox nc has no -z flag).
RUN apk add --no-cache ca-certificates tzdata netcat-openbsd \
 && addgroup -S app \
 && adduser -S -G app -h /app app

# Daily risk counters roll over on IST boundaries — keep the clock aligned.
ENV TZ=Asia/Kolkata

WORKDIR /app
COPY --from=builder /out/risk-management /app/risk-management

USER app

EXPOSE 9005

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD nc -z localhost 9005 || exit 1

ENTRYPOINT ["/app/risk-management"]
