# syntax=docker/dockerfile:1
# ============================================================================
# Rules Engine — strategy matching and trade-signal generation
#   gRPC 9003, AMN preview HTTP 8092
#
# Note: this module declares `go 1.25` in its go.mod — the highest in the
# repo. GO_VERSION must stay >= 1.25 or this build fails.
#
# Build context MUST be the repo root. This module's go.mod uses relative
# `replace` directives (../.., ../../api/proto/*), so a narrower context
# cannot resolve them.
#
#   docker build -f services/rules-engine/Dockerfile -t algo-trading/rules-engine .
#   docker compose --profile apps build rules-engine
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

WORKDIR /app/services/rules-engine

# Build WITH the Go workspace (go.work at the repo root).
#
# This service imports sibling modules — pkg/logger, pkg/decisions and
# api/proto/* — that are resolved through the workspace's `use` directives,
# not through its own go.mod. The previous Dockerfile set GOWORK=off, which
# hid those modules and failed with
# "no required module provides package .../pkg/logger".
# Do not re-add GOWORK=off without first adding real `require` entries.
ENV CGO_ENABLED=0 \
    GOOS=linux

# BuildKit cache mounts: module downloads and compiled packages survive
# between builds, so a code-only change no longer re-downloads the world.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-w -s" -o /out/rules-engine ./cmd

# --------------------------------------------------------------------------
# Runtime
# --------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

# tzdata matters here specifically: this service enforces NSE/BSE market
# hours in Asia/Kolkata (MARKET_TIMEZONE).
# netcat-openbsd provides `nc -z` for the TCP healthcheck (busybox nc has
# no -z flag). The preview HTTP server only serves POST /internal/amn-preview,
# so there is no GET endpoint to probe — hence a TCP check on the gRPC port.
RUN apk add --no-cache ca-certificates tzdata netcat-openbsd \
 && addgroup -S app \
 && adduser -S -G app -h /app app

ENV TZ=Asia/Kolkata

WORKDIR /app
COPY --from=builder /out/rules-engine /app/rules-engine

USER app

EXPOSE 9003 8092

HEALTHCHECK --interval=30s --timeout=5s --start-period=25s --retries=3 \
  CMD nc -z localhost 9003 || exit 1

ENTRYPOINT ["/app/rules-engine"]
