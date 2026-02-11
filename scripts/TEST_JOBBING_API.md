# Jobbing Strategy API Test Documentation

## Overview
This document describes the test scripts for the Jobbing Strategy API endpoints.

## Test Scripts

### PowerShell: `test_jobbing_api.ps1`
**Location**: `scripts/test_jobbing_api.ps1`

**Usage**:
```powershell
# Basic usage (defaults: http://localhost:8080, user ISPL19027)
.\scripts\test_jobbing_api.ps1

# Custom configuration
$env:API_BASE_URL = "http://192.168.1.100:8080"
$env:TEST_USER_ID = "CUSTOM_USER"
.\scripts\test_jobbing_api.ps1
```

### Bash: `test_jobbing_api.sh`
**Location**: `scripts/test_jobbing_api.sh`

**Usage**:
```bash
# Make executable
chmod +x scripts/test_jobbing_api.sh

# Basic usage (defaults: http://localhost:8080, user ISPL19027)
./scripts/test_jobbing_api.sh

# Custom configuration
API_BASE_URL="http://192.168.1.100:8080" TEST_USER_ID="CUSTOM_USER" ./scripts/test_jobbing_api.sh
```

## Test Coverage

### Test 1: Configure Jobbing Strategy
- **Endpoint**: `POST /api/v1/strategies/jobbing/configure`
- **Purpose**: Create jobbing configurations for multiple tokens
- **Test Data**: 
  - SILVERCASE (token 30274): range 10-15, offset 0.01, max_qty 10
  - RELIANCE (token 500325): range 2400-2600, offset 0.50, max_qty 5

### Test 2: Get All Jobbing Configs
- **Endpoint**: `GET /api/v1/strategies/jobbing?user_id={user_id}`
- **Purpose**: Retrieve all jobbing configurations for a user
- **Expected**: Returns list of all configured tokens

### Test 3: Get Single Jobbing Config
- **Endpoint**: `GET /api/v1/strategies/jobbing/{token}?user_id={user_id}`
- **Purpose**: Retrieve configuration for specific token
- **Test Token**: 30274 (SILVERCASE)

### Test 4: Update Jobbing Config
- **Endpoint**: `PUT /api/v1/strategies/jobbing/{token}`
- **Purpose**: Partially update existing configuration
- **Changes**: 
  - lower_range: 10.0 → 11.0
  - higher_range: 15.0 → 14.0
  - max_quantity: 10 → 15

### Test 5: Disable Jobbing Config
- **Endpoint**: `POST /api/v1/strategies/jobbing/{token}/disable`
- **Purpose**: Disable strategy for specific token
- **Test Token**: 30274 (SILVERCASE)

### Test 6: Get Enabled Configs Only
- **Endpoint**: `GET /api/v1/strategies/jobbing?user_id={user_id}&enabled_only=true`
- **Purpose**: Retrieve only enabled configurations
- **Expected**: Should NOT include token 30274 (disabled in Test 5)

### Test 7: Enable Jobbing Config
- **Endpoint**: `POST /api/v1/strategies/jobbing/{token}/enable`
- **Purpose**: Re-enable previously disabled strategy
- **Test Token**: 30274 (SILVERCASE)

### Test 8: Update Trading Mode
- **Endpoint**: `PUT /api/v1/strategies/jobbing/{token}`
- **Purpose**: Switch between PAPER and LIVE modes
- **Changes**: trading_mode: PAPER → LIVE

### Test 9: Delete Jobbing Config (Optional)
- **Endpoint**: `DELETE /api/v1/strategies/jobbing/{token}?user_id={user_id}`
- **Purpose**: Remove configuration (commented out to preserve test data)
- **To Enable**: Uncomment the test in the script

## Test Output

### Success
```
Test 1: Configure jobbing strategy for 2 tokens
  Method: POST
  URL: http://localhost:8080/api/v1/strategies/jobbing/configure
  Response:
  {
    "message": "Jobbing strategy configured successfully",
    "configs_count": 2
  }
  ✓ PASSED

...

Test Summary
Total Tests: 8
Passed: 8
Failed: 0
✓ All tests passed!
```

### Failure
```
Test 2: Get all jobbing configs for user
  Method: GET
  URL: http://localhost:8080/api/v1/strategies/jobbing?user_id=ISPL19027
  ✗ FAILED (HTTP 500)
  Response: {"error": "database connection failed"}

Test Summary
Total Tests: 8
Passed: 7
Failed: 1
✗ Some tests failed!
```

## Prerequisites

### Before Running Tests

1. **Database Setup**:
   ```powershell
   # Windows
   .\scripts\apply_jobbing_migration.ps1

   # Linux/Mac
   ./scripts/apply_jobbing_migration.sh
   ```

2. **Start Services**:
   - PostgreSQL (port 5432)
   - Kafka (port 9092)
   - User Config Service (gRPC + Kafka publisher)
   - API Gateway (port 8080)

3. **Create Kafka Topic**:
   ```bash
   kafka-topics.sh --create --topic user-configs.jobbing \
     --bootstrap-server localhost:9092 \
     --partitions 3 \
     --replication-factor 1
   ```

4. **Verify Proto Compilation**:
   ```bash
   # From api/proto/user_config directory
   protoc --go_out=. --go-grpc_out=. user_config.proto
   ```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `API_BASE_URL` | `http://localhost:8080` | API Gateway base URL |
| `TEST_USER_ID` | `ISPL19027` | User ID for testing |

## Dependencies

### PowerShell
- PowerShell 5.1+ or PowerShell Core 7+
- `Invoke-RestMethod` cmdlet (built-in)

### Bash
- Bash 4.0+
- `curl` command
- `jq` for JSON formatting (optional but recommended)

Install jq:
```bash
# Ubuntu/Debian
sudo apt-get install jq

# macOS
brew install jq

# Windows (Git Bash)
# Download from https://stedolan.github.io/jq/download/
```

## Troubleshooting

### Connection Refused
```
Error: Failed to connect to localhost:8080
```
**Solution**: Ensure API Gateway is running:
```bash
cd api/gateway
go run cmd/gateway/main.go
```

### User Not Found
```
Error: user not found
```
**Solution**: Verify user exists in `users` table or use valid TEST_USER_ID

### Database Error
```
Error: database connection failed
```
**Solution**: 
1. Check PostgreSQL is running
2. Verify connection string in `.env`
3. Run migration: `apply_jobbing_migration.ps1`

### Kafka Publishing Failed
```
Warning: Kafka event publishing failed (config saved)
```
**Solution**:
1. Check Kafka broker is running: `docker ps | grep kafka`
2. Verify topic exists: `kafka-topics.sh --list --bootstrap-server localhost:9092`
3. Check Kafka connection in user-config service

## Next Steps

After successful API tests:

1. **Verify Database**: Query `jobbing_configs` table
   ```sql
   SELECT * FROM jobbing_configs WHERE user_id = 'ISPL19027';
   ```

2. **Check Kafka Events**: Monitor `user-configs.jobbing` topic
   ```bash
   kafka-console-consumer.sh --bootstrap-server localhost:9092 \
     --topic user-configs.jobbing --from-beginning
   ```

3. **Phase 2 Integration**: Implement Rules Engine consumer
   - Subscribe to `user-configs.jobbing` topic
   - Process CREATED/UPDATED/ENABLED events
   - Initialize order placement logic for active configurations

4. **Load Testing**: Use Apache Bench or k6 for concurrent requests
   ```bash
   ab -n 100 -c 10 -p config.json -T application/json \
     http://localhost:8080/api/v1/strategies/jobbing/configure
   ```

## API Reference

For complete API documentation, see:
- [FRONTEND_API_DOCUMENTATION.md](../docs/guides/FRONTEND_API_DOCUMENTATION.md)
- [CREATE_STRATEGY_API.md](../docs/api/CREATE_STRATEGY_API.md)
