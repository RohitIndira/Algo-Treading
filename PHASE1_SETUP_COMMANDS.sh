#!/bin/bash
# Phase 1: Cash52W Enhanced Configuration - Complete Setup Script

set -e  # Exit on error

echo "============================================"
echo "Phase 1 Cash52W Setup - Complete"
echo "============================================"

# Step 1: Regenerate Proto Files
echo ""
echo "Step 1: Regenerating proto files..."
cd /home/ubuntu/Algo-Treading/api/proto
make

# Step 2: Install Missing Dependencies
echo ""
echo "Step 2: Installing dependencies..."
cd /home/ubuntu/Algo-Treading/services/user-config
go get github.com/IBM/sarama
go mod tidy

# Step 3: Apply Database Migration
echo ""
echo "Step 3: Applying database migration..."
sudo -u postgres psql -U postgres -d trading_db -f migrations/004_enhance_cash52w_config.sql

# Step 4: Build User-Config Service
echo ""
echo "Step 4: Building user-config service..."
go build -o bin/user-config ./cmd/main.go

# Step 5: Build API Gateway
echo ""
echo "Step 5: Building API gateway..."
cd /home/ubuntu/Algo-Treading/api/gateway
go mod tidy
go build -o bin/gateway ./cmd/main.go

echo ""
echo "============================================"
echo "✅ Phase 1 Setup Complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "1. Start user-config service:"
echo "   cd /home/ubuntu/Algo-Treading/services/user-config"
echo "   ./bin/user-config"
echo ""
echo "2. Start API gateway (in another terminal):"
echo "   cd /home/ubuntu/Algo-Treading/api/gateway"
echo "   ./bin/gateway"
echo ""
echo "3. Test APIs (see docs/api/PHASE1_CASH52W_API_TESTING.md)"
echo "   curl -X POST http://localhost:8080/api/v1/strategies/cash52w/configure-enhanced ..."
echo ""
