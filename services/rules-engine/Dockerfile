## Multi-stage build for Rules Engine Service
FROM golang:1.25.4-alpine AS builder

WORKDIR /app

## Build from monorepo root so go.work and replace directives work
COPY . .

WORKDIR /app/services/rules-engine

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/rules-engine ./cmd/main.go

FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates

COPY --from=builder /app/bin/rules-engine ./rules-engine

## Copy service-specific config if present
COPY --from=builder /app/services/rules-engine/config ./config

CMD ["./rules-engine"]
