# Build context must be repo root: docker build -f api/gateway/Dockerfile .
FROM golang:1.24.6-alpine AS builder

WORKDIR /app

COPY . .

WORKDIR /app/api/gateway

RUN GOWORK=off CGO_ENABLED=0 GOOS=linux go mod download

RUN GOWORK=off CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/gateway ./cmd/main.go

FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/bin/gateway .

EXPOSE 8081

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:8081/health || exit 1

CMD ["./gateway"]
