# Build context must be repo root: docker build -f services/trade-execution/Dockerfile .
FROM golang:1.24.6-alpine AS builder

WORKDIR /app

COPY . .

WORKDIR /app/services/trade-execution

RUN GOWORK=off CGO_ENABLED=0 GOOS=linux go mod download

RUN GOWORK=off CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/bin/trade-execution ./cmd/main.go

FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata

COPY --from=builder /app/bin/trade-execution .

CMD ["./trade-execution"]
