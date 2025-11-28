# Kafka Setup Guide for Trading System

This guide will help you set up Apache Kafka for the trading system's message queue functionality.

## Prerequisites

- Docker and Docker Compose installed
- Port 9092 available (default Kafka port)
- Port 2181 available (default Zookeeper port)

## Quick Start with Docker Compose

### 1. Create Docker Compose File

We'll use Docker to run Kafka and Zookeeper easily.

Create `deployments/docker/docker-compose-kafka.yml`:

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    hostname: zookeeper
    container_name: trading-zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - trading-network
    healthcheck:
      test: ["CMD", "nc", "-z", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    hostname: kafka
    container_name: trading-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data
    networks:
      - trading-network
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
      interval: 10s
      timeout: 10s
      retries: 5

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: trading-kafka-ui
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: trading-cluster
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181
    networks:
      - trading-network

volumes:
  kafka-data:
    driver: local

networks:
  trading-network:
    driver: bridge
```

### 2. Start Kafka Services

```bash
# Navigate to docker directory
cd deployments/docker

# Start Kafka and Zookeeper
docker-compose -f docker-compose-kafka.yml up -d

# Check if containers are running
docker-compose -f docker-compose-kafka.yml ps

# View logs
docker-compose -f docker-compose-kafka.yml logs -f kafka
```

### 3. Verify Kafka is Running

```bash
# Check if Kafka is accepting connections
docker exec -it trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Create a test topic
docker exec -it trading-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic test-topic

# List topics
docker exec -it trading-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 4. Create Required Topics for Trading System

```bash
# User configs topic
docker exec -it trading-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic user-configs

# News events topic
docker exec -it trading-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic news-events

# Trade signals topic
docker exec -it trading-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic trade-signals

# Trade executions topic
docker exec -it trading-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic trade-executions

# List all topics
docker exec -it trading-kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Access Kafka UI

Once started, you can access Kafka UI at: http://localhost:8080

Features:
- View topics and messages
- Monitor consumer groups
- View broker information
- Send test messages

## Testing Kafka Connection

### Test with Command Line

```bash
# Produce messages
docker exec -it trading-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic test-topic

# (Type messages and press Enter, Ctrl+C to exit)

# Consume messages
docker exec -it trading-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

### Test from Host Machine

```bash
# Test connection from your application
nc -zv localhost 9092
```

## Configuration for Trading Services

Update your service's `.env` file:

```env
# Kafka Configuration
KAFKA_ENABLED=true
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=user-configs
```

## Common Commands

### Start/Stop Services

```bash
# Start
docker-compose -f deployments/docker/docker-compose-kafka.yml up -d

# Stop
docker-compose -f deployments/docker/docker-compose-kafka.yml down

# Stop and remove volumes (clears all data)
docker-compose -f deployments/docker/docker-compose-kafka.yml down -v

# Restart
docker-compose -f deployments/docker/docker-compose-kafka.yml restart
```

### View Logs

```bash
# All services
docker-compose -f deployments/docker/docker-compose-kafka.yml logs -f

# Just Kafka
docker-compose -f deployments/docker/docker-compose-kafka.yml logs -f kafka

# Just Zookeeper
docker-compose -f deployments/docker/docker-compose-kafka.yml logs -f zookeeper
```

### Topic Management

```bash
# List all topics
docker exec trading-kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe a topic
docker exec trading-kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic user-configs

# Delete a topic
docker exec trading-kafka kafka-topics --delete \
  --bootstrap-server localhost:9092 \
  --topic test-topic

# Get topic details
docker exec trading-kafka kafka-topics --describe \
  --bootstrap-server localhost:9092
```

### Consumer Groups

```bash
# List consumer groups
docker exec trading-kafka kafka-consumer-groups --list \
  --bootstrap-server localhost:9092

# Describe a consumer group
docker exec trading-kafka kafka-consumer-groups --describe \
  --bootstrap-server localhost:9092 \
  --group your-consumer-group

# Reset consumer group offset
docker exec trading-kafka kafka-consumer-groups --reset-offsets \
  --bootstrap-server localhost:9092 \
  --group your-consumer-group \
  --topic user-configs \
  --to-earliest \
  --execute
```

## Troubleshooting

### Issue 1: Connection Refused

**Error**: `dial tcp 127.0.0.1:9092: connect: connection refused`

**Solution**:
```bash
# Check if Kafka container is running
docker ps | grep kafka

# If not running, start it
cd deployments/docker
docker-compose -f docker-compose-kafka.yml up -d

# Check logs for errors
docker logs trading-kafka

# Wait for Kafka to be ready (may take 30-60 seconds)
docker exec trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Issue 2: Topics Not Created

**Solution**:
```bash
# Kafka auto-creates topics by default, but you can create them manually
docker exec trading-kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic user-configs
```

### Issue 3: Can't Connect from Application

**Check**:
1. Ensure `KAFKA_ENABLED=true` in `.env`
2. Ensure `KAFKA_BROKERS=localhost:9092` is correct
3. Check if port 9092 is accessible:
   ```bash
   nc -zv localhost 9092
   ```

### Issue 4: Zookeeper Connection Issues

**Solution**:
```bash
# Check Zookeeper is running
docker ps | grep zookeeper

# Check Zookeeper logs
docker logs trading-zookeeper

# Restart Zookeeper
docker restart trading-zookeeper
```

### Issue 5: Disk Space Issues

**Solution**:
```bash
# Check disk usage
docker system df

# Clean up old data
docker system prune -a

# Remove Kafka data volume
docker-compose -f deployments/docker/docker-compose-kafka.yml down -v
```

## Performance Tuning

### For Production

Update `docker-compose-kafka.yml` with:

```yaml
environment:
  # Increase retention
  KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
  
  # Increase segment size
  KAFKA_LOG_SEGMENT_BYTES: 1073741824  # 1GB
  
  # Enable compression
  KAFKA_COMPRESSION_TYPE: 'snappy'
  
  # Increase buffer sizes
  KAFKA_SOCKET_SEND_BUFFER_BYTES: 102400
  KAFKA_SOCKET_RECEIVE_BUFFER_BYTES: 102400
  KAFKA_SOCKET_REQUEST_MAX_BYTES: 104857600
```

## Monitoring

### Check Kafka Health

```bash
# Check broker status
docker exec trading-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check topic lag
docker exec trading-kafka kafka-consumer-groups --describe \
  --bootstrap-server localhost:9092 \
  --all-groups

# Check disk usage
docker exec trading-kafka df -h /var/lib/kafka/data
```

### Kafka UI Dashboard

Access http://localhost:8080 to view:
- Broker information
- Topic details and messages
- Consumer group lag
- Configuration settings

## Integration with Trading System

### Service Configuration

Each service that uses Kafka should have in its `.env`:

```env
KAFKA_ENABLED=true
KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=<service-specific-topic>
```

### Topics Architecture

- **user-configs**: Strategy creation/updates from user-config service
- **news-events**: Market news from data-ingestion service  
- **trade-signals**: Signals from rules-engine service
- **trade-executions**: Execution results from trade-execution service

## Next Steps

1. Start Kafka with Docker Compose
2. Create required topics
3. Update service `.env` files to enable Kafka
4. Restart your services
5. Monitor Kafka UI to see messages flowing

## Quick Setup Script

Run this script to set everything up:

```bash
#!/bin/bash
cd deployments/docker
docker-compose -f docker-compose-kafka.yml up -d
sleep 30  # Wait for Kafka to start
docker exec trading-kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic user-configs
docker exec trading-kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic news-events
docker exec trading-kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic trade-signals
docker exec trading-kafka kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic trade-executions
echo "Kafka setup complete! Access UI at http://localhost:8080"
