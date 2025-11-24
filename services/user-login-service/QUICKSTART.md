# User Login Service - Quick Start Guide

Step-by-step commands to get the service running.

## Prerequisites Check

```bash
# Check Go version (need 1.21+)
go version

# Check PostgreSQL
psql --version

# Check if Docker is running (for Kafka - optional)
docker ps
```

## Step 1: Database Setup

### Start PostgreSQL (if using Docker)

```bash
# Navigate to deployments directory
cd /home/rohitt/Desktop/trading-system/deployments/docker

# Start PostgreSQL
docker-compose -f docker-compose-postgres.yml up -d

# Wait for PostgreSQL to be ready
sleep 5

# Verify it's running
docker ps | grep postgres
```

### Create Database and Run Migrations

```bash
# Go to service directory
