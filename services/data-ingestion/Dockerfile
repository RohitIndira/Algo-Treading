# syntax=docker/dockerfile:1
# ============================================================================
# Data Ingestion Service — MongoDB change streams -> Kafka
#
# No listening port: this is a pure consumer/producer worker, so there is no
# EXPOSE and no endpoint to probe.
#
# Build context MUST be the repo root. This module's go.mod uses a relative
# `replace` directive (../..), so a narrower context cannot resolve it.
#
#   docker build -f services/data-ingestion/Dockerfile -t algo-trading/data-ingestion .
#   docker compose --profile apps build data-ingestion
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

WORKDIR /app/services/data-ingestion

# Build WITH the Go workspace (go.work at the repo root).
#
# This service imports the sibling pkg/logger module, which is resolved
# through the workspace's `use` directives, not through its own go.mod. The
# previous Dockerfile set GOWORK=off, which hid that module and failed with
# "no required module provides package .../pkg/logger".
# Do not re-add GOWORK=off without first adding real `require` entries.
ENV CGO_ENABLED=0 \
    GOOS=linux

# BuildKit cache mounts: module downloads and compiled packages survive
# between builds, so a code-only change no longer re-downloads the world.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags="-w -s" -o /out/data-ingestion ./cmd

# --------------------------------------------------------------------------
# Runtime
# --------------------------------------------------------------------------
FROM alpine:${ALPINE_VERSION}

# ca-certificates is required here, not optional: this service connects to
# MongoDB Atlas over TLS (mongodb+srv://).
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S app \
 && adduser -S -G app -h /app app

# Market-hours logic is IST — keep the container clock in the same zone.
ENV TZ=Asia/Kolkata

WORKDIR /app
COPY --from=builder /out/data-ingestion /app/data-ingestion

USER app

ENTRYPOINT ["/app/data-ingestion"]
