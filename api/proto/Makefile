# Protocol Buffer Generation Makefile
# Generates Go code from .proto files

# Variables
PROTO_ROOT = ../..
COMMON_PROTO = api/proto/common/common.proto
USER_CONFIG_PROTO = api/proto/user_config/user_config.proto
RISK_MGMT_PROTO = api/proto/risk_management/risk_management.proto
TRADE_EXEC_PROTO = api/proto/trade_execution/trade_execution.proto
RULES_ENGINE_PROTO = api/proto/rules_engine/rules_engine.proto
HFT_ENGINE_PROTO = api/proto/hft_engine/hft_engine.proto
PORTFOLIO_PROTO = api/proto/portfolio/portfolio.proto

# Output directories (relative to proto root)
GO_OUT = $(PROTO_ROOT)
GO_GRPC_OUT = $(PROTO_ROOT)

# Protoc flags
PROTOC = protoc
PROTO_FLAGS = --proto_path=$(PROTO_ROOT) --go_out=$(GO_OUT) --go_opt=paths=source_relative --go-grpc_out=$(GO_GRPC_OUT) --go-grpc_opt=paths=source_relative --experimental_allow_proto3_optional

.PHONY: all clean install-tools generate-common generate-user-config generate-risk-management generate-trade-execution generate-rules-engine generate-hft-engine generate-portfolio help

# Default target
all: generate-all

# Install required tools
install-tools:
	@echo "Installing protoc-gen-go and protoc-gen-go-grpc..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Tools installed successfully!"

# Generate all proto files
generate-all: generate-common generate-user-config generate-risk-management generate-trade-execution generate-rules-engine generate-hft-engine generate-portfolio
	@echo "All protobuf files generated successfully!"

# Generate common proto
generate-common:
	@echo "Generating common proto..."
	$(PROTOC) $(PROTO_FLAGS) $(COMMON_PROTO)

# Generate user config proto
generate-user-config:
	@echo "Generating user config proto..."
	$(PROTOC) $(PROTO_FLAGS) $(USER_CONFIG_PROTO)

# Generate risk management proto
generate-risk-management:
	@echo "Generating risk management proto..."
	$(PROTOC) $(PROTO_FLAGS) $(RISK_MGMT_PROTO)

# Generate trade execution proto
generate-trade-execution:
	@echo "Generating trade execution proto..."
	$(PROTOC) $(PROTO_FLAGS) $(TRADE_EXEC_PROTO)

# Generate rules engine proto
generate-rules-engine:
	@echo "Generating rules engine proto..."
	$(PROTOC) $(PROTO_FLAGS) $(RULES_ENGINE_PROTO)

# Generate HFT engine proto
generate-hft-engine:
	@echo "Generating HFT engine proto..."
	$(PROTOC) $(PROTO_FLAGS) $(HFT_ENGINE_PROTO)

# Generate portfolio proto
generate-portfolio:
	@echo "Generating portfolio proto..."
	$(PROTOC) $(PROTO_FLAGS) $(PORTFOLIO_PROTO)

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	find . -name "*.pb.go" -type f -delete
	@echo "Clean complete!"

# Display help
help:
	@echo "Available targets:"
	@echo "  install-tools              - Install protoc-gen-go and protoc-gen-go-grpc"
	@echo "  generate-all               - Generate all protobuf files"
	@echo "  generate-common            - Generate common proto only"
	@echo "  generate-user-config       - Generate user config proto only"
	@echo "  generate-risk-management   - Generate risk management proto only"
	@echo "  generate-trade-execution   - Generate trade execution proto only"
	@echo "  generate-rules-engine      - Generate rules engine proto only"
	@echo "  clean                      - Remove all generated .pb.go files"
	@echo "  help                       - Display this help message"
