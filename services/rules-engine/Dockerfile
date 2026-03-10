# Multi-stage build for Rules Engine Service
FROM golang:1.24.6-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/rules-engine ./cmd/main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Install ca-certificates
RUN apk --no-cache add ca-certificates

# Copy binary from builder
COPY --from=builder /app/bin/rules-engine .

# Copy config if it exists
COPY --from=builder /app/config ./config 2>/dev/null || true

# Run the application
CMD ["./rules-engine"]
