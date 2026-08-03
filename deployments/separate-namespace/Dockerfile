# Shared multi-stage build for any Go service in this go.work monorepo.
#
# Why root context: every service module resolves its siblings via
# `replace => ../../<module>` directives, so a service can ONLY build when the
# whole workspace (go.work + all modules) is present. Build with:
#
#   docker build -f deployments/separate-namespace/Dockerfile \
#     --build-arg SERVICE=orderstatus --build-arg CMD_PATH=cmd \
#     -t algo-dev/orderstatus:local .          # context = REPO ROOT
#
# Pinned to go1.25.12 — the patched toolchain that clears the stdlib CVEs
# (see go.work `toolchain go1.25.12`).
FROM golang:1.25.12-alpine AS builder

RUN apk add --no-cache git ca-certificates
WORKDIR /src

# Copy the entire workspace so go.work + all replace-target modules resolve.
COPY . .

ARG SERVICE
ARG CMD_PATH=cmd
# GOFLAGS=-mod=mod lets the workspace resolve without a committed vendor dir.
RUN CGO_ENABLED=0 GOOS=linux GOTOOLCHAIN=local \
    go build -trimpath -o /out/app ./services/${SERVICE}/${CMD_PATH}

# ── runtime ──────────────────────────────────────────────────────────────
FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
ENV TZ=Asia/Kolkata
WORKDIR /app
COPY --from=builder /out/app /app/app
# Services are configured entirely via environment variables (set in compose);
# no config dir is baked in. Mount one as a volume if a service needs a file.
CMD ["/app/app"]
