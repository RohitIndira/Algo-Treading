# Odin Trading API HTTP Wrapper

FastAPI-based HTTP wrapper for the b2c-api-python SDK (Odin Trading API).

## Overview

This service provides a RESTful HTTP interface to the Odin Trading API, allowing Go microservices to interact with the trading platform without directly managing Python dependencies.

## Features

- ✅ Session management with automatic cleanup
- ✅ All order types (Regular, Cover, Bracket, Multileg)
- ✅ Portfolio management (Positions, Holdings)
- ✅ Order book and trade history
- ✅ Pre-trade validation support
- ✅ Health check endpoint
- ✅ Comprehensive error handling
- ✅ Request/response logging

## Installation

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Copy environment file
cp .env.example .env
# Edit .env with your credentials

# Run the service
python main.py
```

### Docker

```bash
# Build image
docker build -t odin-api-wrapper:latest .

# Run container
docker run -d \
  -p 8000:8000 \
  --env-file .env \
  --name odin-api-wrapper \
  odin-api-wrapper:latest
```

## API Endpoints

### Authentication

#### Login
```http
POST /auth/login
Content-Type: application/json

{
  "user_id": "USER123",
  "password": "password",
  "totp_secret": "BASE32SECRET"
}
```

#### Get Balance
```http
GET /auth/balance
X-User-ID: USER123
```

#### Logout
```http
DELETE /auth/logout
X-User-ID: USER123
```

### Order Management

#### Place Order
```http
POST /orders/place
X-User-ID: USER123
Content-Type: application/json

{
  "scrip_info": {
    "exchange": "NSE_EQ",
    "scrip_token": 2885,
    "symbol": "RELIANCE",
    "series": "EQ"
  },
  "transaction_type": "BUY",
  "product_type": "INTRADAY",
  "order_type": "RL",
  "quantity": 10,
  "price": 2500.50
}
```

#### Modify Order
```http
PUT /orders/modify
X-User-ID: USER123
Content-Type: application/json

{
  "exchange": "NSE_EQ",
  "order_id": "ORDER123",
  "quantity": 20,
  "price": 2505.00
}
```

#### Cancel Order
```http
DELETE /orders/cancel
X-User-ID: USER123
Content-Type: application/json

{
  "exchange": "NSE_EQ",
  "order_id": "ORDER123"
}
```

#### Get Order Book
```http
GET /orders/book?offset=1&limit=20&order_status=OPEN
X-User-ID: USER123
```

#### Get Trade Book
```http
GET /orders/trades?offset=1&limit=20
X-User-ID: USER123
```

### Portfolio Management

#### Get Positions
```http
GET /portfolio/positions?position_type=DAY
X-User-ID: USER123
```

#### Get Holdings
```http
GET /portfolio/holdings
X-User-ID: USER123
```

#### Convert Position
```http
PUT /portfolio/positions/convert?exchange=NSE_EQ&token=2885&from_product=INTRADAY&to_product=DELIVERY&quantity=10&transaction_type=BUY
X-User-ID: USER123
```

### Health Check

```http
GET /health
```

## Response Format

All endpoints return standardized responses:

### Success Response
```json
{
  "success": true,
  "data": { ... },
  "message": "Operation successful"
}
```

### Error Response
```json
{
  "success": false,
  "error": {
    "code": "ERROR_CODE",
    "message": "Error description",
    "details": { ... }
  },
  "message": "Operation failed"
}
```

## Session Management

- Sessions are stored in-memory using user_id as key
- All authenticated endpoints require `X-User-ID` header
- Sessions automatically cleaned up on logout
- Sessions expire based on Odin API timeout (24 hours)

## Error Codes

| HTTP Code | Description |
|-----------|-------------|
| 200 | Success |
| 400 | Bad Request |
| 401 | Unauthorized (not logged in) |
| 403 | Forbidden (insufficient permissions/margin) |
| 404 | Not Found |
| 422 | Validation Error |
| 500 | Internal Server Error |

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| ODIN_API_URL | Odin API endpoint | Yes |
| ODIN_API_KEY | API key | Yes |
| ODIN_X_API_KEY | X-API-Key header | Yes |
| SERVICE_PORT | Port to run service on | No (default: 8000) |
| LOG_LEVEL | Logging level | No (default: INFO) |
| ENVIRONMENT | Environment name | No (default: development) |

## Integration with Go Services

Use the Go client package at `pkg/odin/client.go`:

```go
import "github.com/RohitIndira/Algo-Treading/pkg/odin"

// Create client
client := odin.NewClient("http://localhost:8000")

// Login
err := client.Login(ctx, &odin.LoginRequest{
    UserID:     "USER123",
    Password:   "password",
    TOTPSecret: "BASE32SECRET",
})

// Place order
orderID, err := client.PlaceOrder(ctx, "USER123", &odin.OrderRequest{
    ScripInfo: odin.ScripInfo{
        Exchange:    "NSE_EQ",
        ScripToken:  2885,
        Symbol:      "RELIANCE",
        Series:      "EQ",
    },
    TransactionType: "BUY",
    ProductType:     "INTRADAY",
    OrderType:       "RL",
    Quantity:        10,
    Price:           2500.50,
})

// Get holdings
holdings, err := client.GetHoldings(ctx, "USER123")
```

## Security Considerations

1. **TOTP Secrets**: Store securely, never commit to repository
2. **Session Management**: In-memory storage is not suitable for multi-instance deployment
3. **API Keys**: Use environment variables, not hardcoded values
4. **HTTPS**: Always use HTTPS in production
5. **Rate Limiting**: Consider adding rate limiting middleware
6. **Authentication**: Add API key authentication for wrapper endpoints

## Monitoring

### Health Check
```bash
curl http://localhost:8000/health
```

### Logs
All requests and errors are logged with timestamps:
```
2025-01-10 10:30:00 - INFO - User USER123 logged in successfully
2025-01-10 10:31:00 - INFO - Place order request for USER123
```

## Testing

### Manual Testing
```bash
# Install httpie
pip install httpie

# Test login
http POST localhost:8000/auth/login \
  user_id=USER123 \
  password=password \
  totp_secret=BASE32SECRET

# Test order placement
http POST localhost:8000/orders/place \
  X-User-ID:USER123 \
  scrip_info:='{"exchange":"NSE_EQ","scrip_token":2885,"symbol":"RELIANCE","series":"EQ"}' \
  transaction_type=BUY \
  product_type=INTRADAY \
  order_type=RL \
  quantity:=10 \
  price:=2500.50
```

### API Documentation
Once running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

## Production Deployment

### Considerations

1. **Session Store**: Move to Redis for multi-instance support
2. **Rate Limiting**: Add per-user rate limits
3. **Caching**: Cache holdings/positions for 1-2 seconds
4. **Monitoring**: Add Prometheus metrics
5. **Logging**: Use structured logging with correlation IDs
6. **Circuit Breaker**: Add circuit breaker for Odin API calls

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: odin-api-wrapper
spec:
  replicas: 3
  selector:
    matchLabels:
      app: odin-api-wrapper
  template:
    metadata:
      labels:
        app: odin-api-wrapper
    spec:
      containers:
      - name: odin-api-wrapper
        image: odin-api-wrapper:latest
        ports:
        - containerPort: 8000
        envFrom:
        - secretRef:
            name: odin-api-secrets
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 30
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 10
```

## Troubleshooting

### Common Issues

**1. ImportError: No module named 'pycloudrestapi'**
- Ensure b2c-api-python is in the correct path
- Check sys.path.append in main.py

**2. Unauthorized errors**
- Verify user is logged in
- Check X-User-ID header is present
- Ensure session hasn't expired

**3. TOTP validation failed**
- Verify TOTP secret is correct (BASE32)
- Check system clock is synchronized
- TOTP codes expire every 30 seconds

**4. Order placement fails**
- Check balance is sufficient
- Verify holdings for SELL orders
- Ensure market is open
- Check circuit limits

## Contributing

1. Follow PEP 8 style guide
2. Add type hints to all functions
3. Write docstrings for all public methods
4. Add tests for new endpoints
5. Update this README with changes

## License

[Your License Here]
