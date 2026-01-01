#!/bin/bash

# Script to re-index strategies in Elasticsearch after fixing exchange format
# This script will trigger the rules-engine to reload all strategies from PostgreSQL

set -e

echo "======================================"
echo "Strategy Re-indexing Script"
echo "======================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Elasticsearch is running
echo "1. Checking Elasticsearch status..."
if curl -s "http://localhost:9200/_cluster/health" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Elasticsearch is running${NC}"
else
    echo -e "${RED}✗ Elasticsearch is not running${NC}"
    echo "Please start Elasticsearch first"
    exit 1
fi

# Check if PostgreSQL is running
echo "2. Checking PostgreSQL status..."
if pg_isready -h localhost -p 5432 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ PostgreSQL is running${NC}"
else
    echo -e "${RED}✗ PostgreSQL is not running${NC}"
    echo "Please start PostgreSQL first"
    exit 1
fi

echo ""
echo "======================================"
echo "Current Index Status"
echo "======================================"

# Show current strategies in Elasticsearch
STRATEGY_COUNT=$(curl -s "http://localhost:9200/trading-strategies/_count" | jq -r '.count' 2>/dev/null || echo "0")
echo "Current strategies in Elasticsearch: $STRATEGY_COUNT"

# Show a sample strategy to check exchange format
echo ""
echo "Sample strategy (checking exchange format):"
curl -s "http://localhost:9200/trading-strategies/_search?size=1&pretty" | jq '.hits.hits[0]._source | {strategy_id, exchange}' 2>/dev/null || echo "No strategies found"

echo ""
echo "======================================"
echo "Re-indexing Options"
echo "======================================"
echo ""
echo "Choose an option:"
echo "1. Delete index and let rules-engine recreate (RECOMMENDED)"
echo "2. Restart rules-engine service to re-index all strategies"
echo "3. Cancel"
echo ""
read -p "Enter your choice (1-3): " choice

case $choice in
    1)
        echo ""
        echo -e "${YELLOW}Deleting Elasticsearch index...${NC}"
        
        # Delete the index
        if curl -X DELETE "http://localhost:9200/trading-strategies" 2>/dev/null; then
            echo -e "${GREEN}✓ Index deleted successfully${NC}"
        else
            echo -e "${RED}✗ Failed to delete index (it may not exist)${NC}"
        fi
        
        echo ""
        echo -e "${YELLOW}Now restart the rules-engine service:${NC}"
        echo "  cd services/rules-engine"
        echo "  go run cmd/main.go"
        echo ""
        echo "The service will:"
        echo "  1. Create a new index with proper mapping"
        echo "  2. Load all strategies from PostgreSQL"
        echo "  3. Index them with normalized exchange values (NSE/BSE)"
        ;;
        
    2)
        echo ""
        echo -e "${YELLOW}Please restart the rules-engine service manually:${NC}"
        echo ""
        echo "Option A - If running with systemd:"
        echo "  sudo systemctl restart rules-engine"
        echo ""
        echo "Option B - If running manually:"
        echo "  1. Stop the current rules-engine process (Ctrl+C)"
        echo "  2. cd services/rules-engine"
        echo "  3. go run cmd/main.go"
        echo ""
        echo "The service will re-index all strategies on startup."
        ;;
        
    3)
        echo ""
        echo "Operation cancelled."
        exit 0
        ;;
        
    *)
        echo ""
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo ""
echo "======================================"
echo "Verification Commands"
echo "======================================"
echo ""
echo "After restarting rules-engine, verify the fix:"
echo ""
echo "1. Check strategy count:"
echo "   curl -s 'http://localhost:9200/trading-strategies/_count' | jq"
echo ""
echo "2. Check exchange format (should be 'NSE' or 'BSE', not 'EXCHANGE_NSE'):"
echo "   curl -s 'http://localhost:9200/trading-strategies/_search?size=5&pretty' | jq '.hits.hits[]._source | {strategy_id, exchange}'"
echo ""
echo "3. Check if active strategies are indexed:"
echo "   curl -s 'http://localhost:9200/trading-strategies/_search?q=active:true&pretty' | jq '.hits.total.value'"
echo ""
echo "======================================"
echo "Done!"
echo "======================================"
