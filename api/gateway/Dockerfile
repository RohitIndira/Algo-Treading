## Multi-stage build for API Gateway
FROM golang:1.25.4-alpine AS builder

WORKDIR /app

## Build from monorepo root so go.work and replace directives work
COPY . .

WORKDIR /app/api/gateway

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/gateway ./cmd/main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

# Copy binary from builder
COPY --from=builder /app/bin/gateway ./gateway

# Copy config files if they exist (service-specific)
COPY --from=builder /app/api/gateway/config ./config

# Expose port (adjust based on your gateway port)
EXPOSE 8081

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:8081/health || exit 1

# Run the application
CMD ["./gateway"]
