.PHONY: proto build test docker-up docker-down lint clean help

# Variables
PROTO_DIR=api/proto
SERVICES=api-gateway user-config data-ingestion rules-engine trade-execution risk-management
BIN_DIR=bin

help: ## Display this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

proto: ## Generate protobuf code
	@echo "Generating protobuf code..."
	@./scripts/proto-gen.sh

build: ## Build all services
	@echo "Building all services..."
	@./scripts/build.sh

build-service: ## Build specific service (usage: make build-service SERVICE=user-config)
	@echo "Building $(SERVICE)..."
	@cd services/$(SERVICE) && go build -o ../../$(BIN_DIR)/$(SERVICE) ./cmd/main.go

test: ## Run unit tests
	@echo "Running unit tests..."
	@go test -v ./...

test-coverage: ## Run tests with coverage
	@echo "Running tests with coverage..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

test-integration: ## Run integration tests
	@echo "Running integration tests..."
	@go test -v ./tests/integration/...

docker-up: ## Start all services with docker-compose
	@echo "Starting services with docker-compose..."
	@docker-compose -f deployments/docker-compose.yml up -d

docker-down: ## Stop all services
	@echo "Stopping services..."
	@docker-compose -f deployments/docker-compose.yml down

docker-build: ## Build Docker images
	@echo "Building Docker images..."
	@docker-compose -f deployments/docker-compose.yml build

docker-logs: ## View logs from all services
	@docker-compose -f deployments/docker-compose.yml logs -f

lint: ## Run linter
	@echo "Running linter..."
	@golangci-lint run ./...

fmt: ## Format code
	@echo "Formatting code..."
	@go fmt ./...
	@gofmt -s -w .

