#!/bin/bash

# Test script to verify Kafka topics and connection for trade execution service

echo "=========================================="
echo "Kafka Connection and Topics Test"
echo "=========================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Kafka configuration
KAFKA_BROKER="localhost:9092"
TRADE_SIGNALS_TOPIC="trade-signals"
TRADE_EXECUTIONS_TOPIC="trade-executions"

echo ""
echo "1. Checking if Kafka broker is accessible..."
if timeout 5 bash -c "echo > /dev/tcp/localhost/9092" 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Kafka broker is accessible at ${KAFKA_BROKER}"
else
    echo -e "${RED}✗${NC} Cannot connect to Kafka broker at ${KAFKA_BROKER}"
    echo "Please ensure Kafka is running"
    exit 1
fi

echo ""
echo "2. Listing all Kafka topics..."
TOPICS=$(kafka-topics --bootstrap-server ${KAFKA_BROKER} --list 2>/dev/null)
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC} Successfully retrieved topic list:"
    echo "$TOPICS" | sed 's/^/  - /'
else
    echo -e "${RED}✗${NC} Failed to list Kafka topics"
    echo "Please check if Kafka is running and kafka-topics command is available"
    exit 1
fi

echo ""
echo "3. Checking '${TRADE_SIGNALS_TOPIC}' topic..."
if echo "$TOPICS" | grep -q "^${TRADE_SIGNALS_TOPIC}$"; then
    echo -e "${GREEN}✓${NC} Topic '${TRADE_SIGNALS_TOPIC}' exists"
    
    # Get topic details
    TOPIC_INFO=$(kafka-topics --bootstrap-server ${KAFKA_BROKER} --describe --topic ${TRADE_SIGNALS_TOPIC} 2>/dev/null)
    PARTITIONS=$(echo "$TOPIC_INFO" | grep -o "PartitionCount: [0-9]*" | cut -d' ' -f2)
    REPLICATION=$(echo "$TOPIC_INFO" | grep -o "ReplicationFactor: [0-9]*" | cut -d' ' -f2)
    
    echo "  - Partitions: ${PARTITIONS}"
    echo "  - Replication Factor: ${REPLICATION}"
    
    # Check message count (last offset)
    echo ""
    echo "  Checking message count in topic..."
    OFFSETS=$(kafka-run-class kafka.tools.GetOffsetShell --broker-list ${KAFKA_BROKER} --topic ${TRADE_SIGNALS_TOPIC} --time -1 2>/dev/null)
    TOTAL_MESSAGES=0
    while IFS=: read -r partition offset; do
        if [[ "$offset" =~ ^[0-9]+$ ]]; then
            TOTAL_MESSAGES=$((TOTAL_MESSAGES + offset))
        fi
    done <<< "$OFFSETS"
    
    echo "  - Total messages: ${TOTAL_MESSAGES}"
    
    if [ "$TOTAL_MESSAGES" -gt 0 ]; then
        echo -e "  ${GREEN}✓${NC} Topic contains messages ready for processing"
    else
        echo -e "  ${YELLOW}!${NC} Topic is empty"
    fi
else
    echo -e "${RED}✗${NC} Topic '${TRADE_SIGNALS_TOPIC}' does not exist"
fi

echo ""
echo "4. Checking '${TRADE_EXECUTIONS_TOPIC}' topic..."
if echo "$TOPICS" | grep -q "^${TRADE_EXECUTIONS_TOPIC}$"; then
    echo -e "${GREEN}✓${NC} Topic '${TRADE_EXECUTIONS_TOPIC}' exists"
    
    # Get topic details
    TOPIC_INFO=$(kafka-topics --bootstrap-server ${KAFKA_BROKER} --describe --topic ${TRADE_EXECUTIONS_TOPIC} 2>/dev/null)
    PARTITIONS=$(echo "$TOPIC_INFO" | grep -o "PartitionCount: [0-9]*" | cut -d' ' -f2)
    REPLICATION=$(echo "$TOPIC_INFO" | grep -o "ReplicationFactor: [0-9]*" | cut -d' ' -f2)
    
    echo "  - Partitions: ${PARTITIONS}"
    echo "  - Replication Factor: ${REPLICATION}"
    
    # Check message count
    echo ""
    echo "  Checking message count in topic..."
    OFFSETS=$(kafka-run-class kafka.tools.GetOffsetShell --broker-list ${KAFKA_BROKER} --topic ${TRADE_EXECUTIONS_TOPIC} --time -1 2>/dev/null)
    TOTAL_MESSAGES=0
    while IFS=: read -r partition offset; do
        if [[ "$offset" =~ ^[0-9]+$ ]]; then
            TOTAL_MESSAGES=$((TOTAL_MESSAGES + offset))
        fi
    done <<< "$OFFSETS"
    
    echo "  - Total messages: ${TOTAL_MESSAGES}"
    
    if [ "$TOTAL_MESSAGES" -gt 0 ]; then
        echo -e "  ${GREEN}✓${NC} Topic contains execution results"
    else
        echo -e "  ${YELLOW}!${NC} Topic is empty (no executions yet)"
    fi
else
    echo -e "${YELLOW}!${NC} Topic '${TRADE_EXECUTIONS_TOPIC}' does not exist"
    echo "  This topic will be created when first execution result is published"
fi

echo ""
echo "5. Testing consumer group connectivity..."
TEST_GROUP="trade-execution-test-group"
echo "  Attempting to describe consumer group '${TEST_GROUP}'..."
kafka-consumer-groups --bootstrap-server ${KAFKA_BROKER} --describe --group ${TEST_GROUP} 2>/dev/null
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓${NC} Consumer group commands working"
else
    echo -e "${YELLOW}!${NC} Consumer group not found (expected if service hasn't started yet)"
fi

echo ""
echo "6. Sample messages from '${TRADE_SIGNALS_TOPIC}' (if available)..."
if echo "$TOPICS" | grep -q "^${TRADE_SIGNALS_TOPIC}$"; then
    echo "  Fetching latest 3 messages..."
    kafka-console-consumer --bootstrap-server ${KAFKA_BROKER} \
        --topic ${TRADE_SIGNALS_TOPIC} \
        --max-messages 3 \
        --timeout-ms 5000 \
        --from-beginning 2>/dev/null | head -3
    
    if [ $? -eq 0 ]; then
        echo -e "  ${GREEN}✓${NC} Successfully read messages from topic"
    else
        echo -e "  ${YELLOW}!${NC} Could not read messages (topic might be empty)"
    fi
fi

echo ""
echo "=========================================="
echo "Summary:"
echo "-------------------------------------------"
echo "Kafka Broker:            ${KAFKA_BROKER}"
echo "Trade Signals Topic:     ${TRADE_SIGNALS_TOPIC}"
echo "Trade Executions Topic:  ${TRADE_EXECUTIONS_TOPIC}"
echo "-------------------------------------------"

echo ""
if timeout 5 bash -c "echo > /dev/tcp/localhost/9092" 2>/dev/null; then
    echo -e "${GREEN}✓ Kafka is accessible and ready${NC}"
else
    echo -e "${RED}✗ Kafka connection issues detected${NC}"
fi

echo ""
echo "=========================================="
