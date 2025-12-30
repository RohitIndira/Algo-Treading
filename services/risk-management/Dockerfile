## Multi-stage build for Risk Management Service
FROM golang:1.25.4-alpine AS builder

WORKDIR /app

## Build from monorepo root so go.work and replace directives work
COPY . .

WORKDIR /app/services/risk-management

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/risk-management ./cmd/main.go

FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates

COPY --from=builder /app/bin/risk-management ./risk-management

## Copy service-specific config if it exists
COPY --from=builder /app/services/risk-management/config ./config

CMD ["./risk-management"]
