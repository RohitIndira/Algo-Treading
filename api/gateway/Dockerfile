# syntax=docker/dockerfile:1
# ============================================================================
# API Gateway — REST API for the frontend (HTTP 8081)
#
# Build context MUST be the repo root. This module's go.mod uses relative
# `replace` directives (../.., ../proto/*), so a narrower context cannot
# resolve them.
#
#   docker build -f api/gateway/Dockerfile -t algo-trading/api-gateway .
#   docker compose --profile apps build api-gateway
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

WORKDIR /app/api/gateway

# Build WITH the Go workspace (go.work at the repo root).
#
# This service imports sibling modules — pkg/logger and api/proto/* — that
# are resolved through the workspace's `use` directives, not through its own
# go.mod. The previous Dockerfile set GOWORK=off, which hid those modules and
# failed with "no required module provides package .../pkg/logger".
# Do not re-add GOWORK=off without first adding real `require` entries.
ENV CGO_ENABLED=0 \
    GOOS=linux

# BuildKit cache mounts: module downloads and compiled packages survive
# between builds, so a code-only change no longer re-downloads the world.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-w -s" -o /out/gateway ./cmd

# --------------------------------------------------------------------------
# Runtime
# --------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S app \
 && adduser -S -G app -h /app app

# Market-hours logic is IST — keep the container clock in the same zone.
ENV TZ=Asia/Kolkata

WORKDIR /app
COPY --from=builder /out/gateway /app/gateway

USER app

# 8080 in local .env, 8081 in .env.live and as the code default — both are
# documented rather than picking one.
EXPOSE 8080 8081

# Shell form so $HTTP_PORT is read at runtime and the probe follows whichever
# port the env file actually sets. The route is /api/v1/health (router.go) —
# not /health, which 404s.
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget --quiet --tries=1 --spider "http://localhost:${HTTP_PORT:-8081}/api/v1/health" || exit 1

ENTRYPOINT ["/app/gateway"]
