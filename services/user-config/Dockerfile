## Multi-stage build for User Config Service
FROM golang:1.25.4-alpine AS builder

WORKDIR /app

## Build from monorepo root so go.work and replace directives work
COPY . .

WORKDIR /app/services/user-config

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/user-config ./cmd/main.go

FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates curl

COPY --from=builder /app/bin/user-config ./user-config

## Copy service-specific config/migrations if present
COPY --from=builder /app/services/user-config/config ./config
COPY --from=builder /app/services/user-config/migrations ./migrations

EXPOSE 50051

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:50051/health || exit 1

CMD ["./user-config"]
