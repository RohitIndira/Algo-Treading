# Multi-stage build for User Config Service
FROM golang:1.24.6-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/user-config ./cmd/main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Install ca-certificates and curl for health checks
RUN apk --no-cache add ca-certificates curl

# Copy binary from builder
COPY --from=builder /app/bin/user-config .

# Copy config and migrations if they exist
COPY --from=builder /app/config ./config 2>/dev/null || true
COPY --from=builder /app/migrations ./migrations 2>/dev/null || true

# Expose port (adjust based on your service port)
EXPOSE 50051

# Health check using grpcurl or simple curl
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:50051/health || exit 1

# Run the application
CMD ["./user-config"]
