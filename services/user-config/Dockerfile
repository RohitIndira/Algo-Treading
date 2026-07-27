# syntax=docker/dockerfile:1
# ============================================================================
# User Config Service — strategy CRUD + Kafka config events (gRPC 50051)
#
# Build context MUST be the repo root. This module's go.mod uses relative
# `replace` directives (../.., ../../pkg/logger, ../../api/proto/*), so a
# narrower context cannot resolve them.
#
#   docker build -f services/user-config/Dockerfile -t algo-trading/user-config .
#   docker compose --profile apps build user-config
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

WORKDIR /app/services/user-config

# Build WITH the Go workspace (go.work at the repo root).
#
# This service imports sibling modules — pkg/logger and api/proto/* — that
# are resolved through the workspace's `use` directives, not through its own
# go.mod. The previous Dockerfile set GOWORK=off, which hid those modules.
# Do not re-add GOWORK=off without first adding real `require` entries.
#
# The *.pb.go files are gitignored, so they must exist in the build context
# before this runs — see scripts/proto-gen.sh.
ENV CGO_ENABLED=0 \
    GOOS=linux

# BuildKit cache mounts: module downloads and compiled packages survive
# between builds, so a code-only change no longer re-downloads the world.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-w -s" -o /out/user-config ./cmd

# --------------------------------------------------------------------------
# Runtime
# --------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

# netcat-openbsd provides `nc -z` for the gRPC TCP healthcheck below
# (busybox nc has no -z flag).
RUN apk add --no-cache ca-certificates tzdata netcat-openbsd \
 && addgroup -S app \
 && adduser -S -G app -h /app app

# Market-hours logic is IST — keep the container clock in the same zone.
ENV TZ=Asia/Kolkata

WORKDIR /app
COPY --from=builder /out/user-config /app/user-config

USER app

EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD nc -z localhost 50051 || exit 1

ENTRYPOINT ["/app/user-config"]
