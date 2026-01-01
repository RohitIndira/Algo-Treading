# ## Multi-stage build for Data Ingestion Service
# FROM golang:1.25.4-alpine AS builder

# WORKDIR /app

# ## Build from monorepo root so go.work and replace directives work
# COPY . .

# WORKDIR /app/services/data-ingestion

# RUN go mod download

# RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/data-ingestion ./cmd/main.go

# # Final stage
# FROM alpine:latest

# WORKDIR /app

# # Install ca-certificates
# RUN apk --no-cache add ca-certificates

# # Copy binary from builder
# COPY --from=builder /app/bin/data-ingestion ./data-ingestion

# # Copy service-specific config if it exists
# COPY --from=builder /app/services/data-ingestion/config ./config

# # Run the application
# CMD ["./data-ingestion"]


## Multi-stage build for Data Ingestion Service
FROM golang:1.25.4-alpine AS builder

WORKDIR /app

# Build from monorepo root so go.work and replace directives work
COPY . .

WORKDIR /app/services/data-ingestion

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/data-ingestion ./cmd/main.go


# ---------------- Runtime stage ----------------
FROM alpine:latest

WORKDIR /app

# Install runtime dependencies
RUN apk --no-cache add \
    ca-certificates \
    python3

# Copy Go binary
COPY --from=builder /app/bin/data-ingestion ./data-ingestion

# Copy config
COPY --from=builder /app/services/data-ingestion/config ./config

# ✅ Copy B2C Python bridge
COPY --from=builder /app/b2c-api-python ./b2c-api-python

# Optional: make python command predictable
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Run the application
CMD ["./data-ingestion"]

## =========================
## Builder stage
## =========================


## =========================
## Builder stage
## =========================
## =========================
## Builder stage
## =========================
## =========================
## Builder stage
## =========================

## =========================
## Builder stage
## =========================
## =========================
## Builder stage
## =========================
# FROM golang:1.25.4-alpine AS builder

# WORKDIR /app

# RUN apk add --no-cache git

# # Copy entire repo (go.work + b2c-api-python)
# COPY . .

# # Go service directory
# WORKDIR /app/services/data-ingestion

# # Build Go binary (cmd/main.go exists here)
# RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
#     go build -o /app/bin/data-ingestion ./cmd

# ## =========================
# ## Runtime stage
# ## =========================
# FROM alpine:latest

# WORKDIR /app

# # Install Python3 and expose `python`
# RUN apk --no-cache add \
#     ca-certificates \
#     python3 \
#     py3-pip \
#  && ln -sf /usr/bin/python3 /usr/bin/python

# # Copy Go binary
# COPY --from=builder /app/bin/data-ingestion ./data-ingestion

# # Copy Python bridge
# COPY --from=builder /app/b2c-api-python /app/b2c-api-python

# # Copy config
# COPY --from=builder /app/services/data-ingestion/config ./config

# RUN chmod +x /app/b2c-api-python/b2c_bridge.py

# ENV PYTHONUNBUFFERED=1

# CMD ["./data-ingestion"]

