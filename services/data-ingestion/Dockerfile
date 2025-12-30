## Multi-stage build for Data Ingestion Service
FROM golang:1.25.4-alpine AS builder

WORKDIR /app

## Build from monorepo root so go.work and replace directives work
COPY . .

WORKDIR /app/services/data-ingestion

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/data-ingestion ./cmd/main.go

# Final stage
FROM alpine:latest

WORKDIR /app

# Install ca-certificates
RUN apk --no-cache add ca-certificates

# Copy binary from builder
COPY --from=builder /app/bin/data-ingestion ./data-ingestion

# Copy service-specific config if it exists
COPY --from=builder /app/services/data-ingestion/config ./config

# Run the application
CMD ["./data-ingestion"]
