#!/bin/bash

# Script to check Elasticsearch index status for trading strategies
# This helps verify if the rules-engine has successfully indexed strategies

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "======================================"
echo "Elasticsearch Index Status Checker"
echo "======================================"
echo ""

# Check if Elasticsearch is running
echo -e "${BLUE}1. Checking Elasticsearch connectivity...${NC}"
if curl -s "http://localhost:9200" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Elasticsearch is running${NC}"
    ES_VERSION=$(curl -s "http://localhost:9200" | jq -r '.version.number' 2>/dev/null || echo "unknown")
    echo "  Version: $ES_VERSION"
else
    echo -e "${RED}✗ Elasticsearch is not running or not accessible${NC}"
    echo "  Please start Elasticsearch: sudo systemctl start elasticsearch"
    exit 1
fi

echo ""
echo "======================================"
echo -e "${BLUE}2. Checking trading-strategies index...${NC}"
echo "======================================"

# Check if index exists
if curl -s "http://localhost:9200/trading-strategies" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Index 'trading-strategies' exists${NC}"
    
    # Get index stats
    INDEX_SIZE=$(curl -s "http://localhost:9200/trading-strategies/_stats" | jq -r '.indices."trading-strategies".total.store.size_in_bytes' 2>/dev/null || echo "0")
    INDEX_SIZE_MB=$(echo "scale=2; $INDEX_SIZE / 1024 / 1024" | bc 2>/dev/null || echo "0")
    echo "  Index size: ${INDEX_SIZE_MB} MB"
    
else
    echo -e "${RED}✗ Index 'trading-strategies' does NOT exist${NC}"
    echo ""
    echo -e "${YELLOW}Action Required:${NC}"
    echo "  The rules-engine service needs to be started to create the index."
    echo "  Run: cd services/rules-engine && go run cmd/main.go"
    echo ""
    exit 1
fi

echo ""
echo "======================================"
echo -e "${BLUE}3. Strategy Count${NC}"
echo "======================================"

# Get total count
TOTAL_COUNT=$(curl -s "http://localhost:9200/trading-strategies/_count" | jq -r '.count' 2>/dev/null || echo "0")
echo "Total strategies indexed: ${TOTAL_COUNT}"

# Get active count
ACTIVE_COUNT=$(curl -s "http://localhost:9200/trading-strategies/_count" -H 'Content-Type: application/json' -d '{"query":{"term":{"active":true}}}' | jq -r '.count' 2>/dev/null || echo "0")
echo "Active strategies: ${ACTIVE_COUNT}"

# Get inactive count
INACTIVE_COUNT=$((TOTAL_COUNT - ACTIVE_COUNT))
echo "Inactive strategies: ${INACTIVE_COUNT}"

if [ "$ACTIVE_COUNT" -eq 0 ]; then
    echo ""
    echo -e "${YELLOW}⚠ Warning: No active strategies found!${NC}"
    echo "  Strategies must be active to match events."
fi

echo ""
echo "======================================"
echo -e "${BLUE}4. Index Mapping (Schema)${NC}"
echo "======================================"

# Check index mapping
MAPPING_EXISTS=$(curl -s "http://localhost:9200/trading-strategies/_mapping" | jq -r '.["trading-strategies"].mappings.properties | length' 2>/dev/null || echo "0")

if [ "$MAPPING_EXISTS" -gt 0 ]; then
    echo -e "${GREEN}✓ Index mapping is configured${NC}"
    echo "  Fields count: $MAPPING_EXISTS"
    echo ""
    echo "  Key fields:"
    curl -s "http://localhost:9200/trading-strategies/_mapping" | jq -r '.["trading-strategies"].mappings.properties | keys[]' 2>/dev/null | head -10 | while read field; do
        echo "    - $field"
    done
else
    echo -e "${RED}✗ No mapping found${NC}"
fi

echo ""
echo "======================================"
echo -e "${BLUE}5. Sample Strategies${NC}"
echo "======================================"

# Show 3 sample strategies
echo "Showing 3 sample strategies:"
echo ""

SAMPLE=$(curl -s "http://localhost:9200/trading-strategies/_search?size=3&pretty" | jq -r '.hits.hits[]._source | {strategy_id, user_id, strategy_name, active, exchange, impact_score_min}' 2>/dev/null)

if [ -n "$SAMPLE" ] && [ "$SAMPLE" != "null" ]; then
    echo "$SAMPLE"
else
    echo -e "${YELLOW}No strategies to display${NC}"
fi

echo ""
echo "======================================"
echo -e "${BLUE}6. Exchange Values Check${NC}"
echo "======================================"

# Check exchange format (should be NSE/BSE, not EXCHANGE_NSE)
echo "Checking exchange field values..."
EXCHANGES=$(curl -s "http://localhost:9200/trading-strategies/_search?size=100" | jq -r '.hits.hits[]._source.exchange' 2>/dev/null | sort | uniq)

if [ -n "$EXCHANGES" ]; then
    echo "Exchange values found:"
    echo "$EXCHANGES" | while read exchange; do
        if [ -n "$exchange" ] && [ "$exchange" != "null" ]; then
            # Check if it starts with EXCHANGE_ (old format)
            if [[ "$exchange" == EXCHANGE_* ]]; then
                echo -e "  ${RED}✗ $exchange (WRONG FORMAT - should be normalized)${NC}"
            else
                echo -e "  ${GREEN}✓ $exchange (correct)${NC}"
            fi
        fi
    done
else
    echo -e "${YELLOW}No exchange values found${NC}"
fi

echo ""
echo "======================================"
echo -e "${BLUE}7. Index Health${NC}"
echo "======================================"

# Get index health
HEALTH=$(curl -s "http://localhost:9200/_cat/indices/trading-strategies?v&h=health,status,index,docs.count,store.size" 2>/dev/null)

if [ -n "$HEALTH" ]; then
    echo "$HEALTH"
    
    HEALTH_STATUS=$(echo "$HEALTH" | tail -1 | awk '{print $1}')
    
    case $HEALTH_STATUS in
        green)
            echo -e "${GREEN}✓ Index health is GREEN (optimal)${NC}"
            ;;
        yellow)
            echo -e "${YELLOW}⚠ Index health is YELLOW (functional but not optimal)${NC}"
            ;;
        red)
            echo -e "${RED}✗ Index health is RED (problems detected)${NC}"
            ;;
    esac
else
    echo -e "${YELLOW}Unable to retrieve health status${NC}"
fi

echo ""
echo "======================================"
echo -e "${BLUE}8. Comparison with PostgreSQL${NC}"
echo "======================================"

# Compare with PostgreSQL count
if command -v psql &> /dev/null; then
    PG_ACTIVE=$(psql -U postgres -d trading_db -t -c "SELECT COUNT(*) FROM strategies WHERE active = true;" 2>/dev/null | xargs)
    PG_TOTAL=$(psql -U postgres -d trading_db -t -c "SELECT COUNT(*) FROM strategies;" 2>/dev/null | xargs)
    
    if [ -n "$PG_ACTIVE" ] && [ "$PG_ACTIVE" != "" ]; then
        echo "PostgreSQL active strategies: ${PG_ACTIVE}"
        echo "Elasticsearch active strategies: ${ACTIVE_COUNT}"
        
        if [ "$PG_ACTIVE" -eq "$ACTIVE_COUNT" ]; then
            echo -e "${GREEN}✓ Counts match! All active strategies are indexed.${NC}"
        else
            echo -e "${YELLOW}⚠ Count mismatch! Re-indexing may be needed.${NC}"
            echo "  Difference: $((PG_ACTIVE - ACTIVE_COUNT))"
        fi
        
        echo ""
        echo "PostgreSQL total strategies: ${PG_TOTAL}"
        echo "Elasticsearch total strategies: ${TOTAL_COUNT}"
    else
        echo -e "${YELLOW}Unable to query PostgreSQL (password required or DB not accessible)${NC}"
    fi
else
    echo -e "${YELLOW}psql not available - skipping PostgreSQL comparison${NC}"
fi

echo ""
echo "======================================"
echo -e "${GREEN}Summary${NC}"
echo "======================================"

if [ "$ACTIVE_COUNT" -gt 0 ]; then
    echo -e "${GREEN}✓ Index is set up correctly${NC}"
    echo -e "${GREEN}✓ ${ACTIVE_COUNT} active strategies ready for matching${NC}"
    echo ""
    echo "Your rules-engine should now be able to:"
    echo "  - Receive market events from Kafka"
    echo "  - Query Elasticsearch for matching strategies"
    echo "  - Evaluate conditions and generate trade orders"
else
    echo -e "${RED}✗ Issue detected: No active strategies${NC}"
    echo ""
    echo "Troubleshooting steps:"
    echo "  1. Ensure rules-engine service is running"
    echo "  2. Check rules-engine logs for errors"
    echo "  3. Verify strategies in PostgreSQL are active"
    echo "  4. Try re-indexing: bash scripts/reindex_strategies.sh"
fi

echo ""
echo "======================================"
echo "Quick Commands"
echo "======================================"
echo ""
echo "View all strategies:"
echo "  curl -s 'http://localhost:9200/trading-strategies/_search?size=100&pretty'"
echo ""
echo "Search by user:"
echo "  curl -s 'http://localhost:9200/trading-strategies/_search?q=user_id:IS14415&pretty'"
echo ""
echo "Count active strategies:"
echo "  curl -s 'http://localhost:9200/trading-strategies/_count' -H 'Content-Type: application/json' -d '{\"query\":{\"term\":{\"active\":true}}}' | jq"
echo ""
echo "Delete and recreate index:"
echo "  curl -X DELETE 'http://localhost:9200/trading-strategies'"
echo "  # Then restart rules-engine service"
echo ""
