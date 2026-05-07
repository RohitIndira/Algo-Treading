# API Gateway - Knowledge Transfer Document

## 📋 Table of Contents

1. [Overview](#overview)
2. [Architecture & Design](#architecture--design)
3. [Project Structure](#project-structure)
4. [Core Components](#core-components)
5. [Configuration](#configuration)
6. [API Endpoints](#api-endpoints)
7. [WebSocket Implementation](#websocket-implementation)
8. [CORS Configuration](#cors-configuration)
9. [Error Handling](#error-handling)
10. [Setup & Deployment](#setup--deployment)
11. [Testing](#testing)
12. [Troubleshooting](#troubleshooting)
13. [Best Practices](#best-practices)

---

## Overview

### Purpose
The API Gateway serves as the **unified entry point** for all frontend applications to interact with the trading system's backend microservices. It translates RESTful HTTP/JSON requests into gRPC calls and proxies HTTP requests to other REST services.

### Key Responsibilities
- **REST to gRPC Translation**: Converts JSON requests to Protocol Buffer format for backend microservices
- **HTTP Proxying**: Forwards authentication requests to the User Login Service
- **CORS Management**: Handles Cross-Origin Resource Sharing for frontend applications
- **WebSocket Support**: Provides real-time streaming of trading match events via WebSocket
- **Request Routing**: Routes incoming requests to appropriate backend services
- **Error Translation**: Converts backend errors to user-friendly HTTP responses

### Technology Stack
- **Language**: Go 1.23+
- **Web Framework**: Gorilla Mux (router)
- **RPC**: gRPC (for microservice communication)
- **Serialization**: Protocol Buffers
- **Real-time**: WebSocket + Redis Pub/Sub
- **Configuration**: Environment variables + `.env` file

---

## Architecture & Design

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Frontend Applications                 │
│            (React/Vue/Angular - Port 3000/5173)         │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP/JSON + WebSocket
                     ▼
┌─────────────────────────────────────────────────────────┐
│              API Gateway (Port 8081)                     │
│  ┌──────────────────────────────────────────────────┐  │
│  │         Router (Gorilla Mux)                     │  │
│  └──────┬───────────────────────────────┬───────────┘  │
│         │                                 │              │
│    ┌────▼─────────┐              ┌──────▼───────────┐  │
│    │   Handlers   │              │   WebSocket      │  │
│    │              │              │   Handler        │  │
│    └────┬─────────┘              └──────┬───────────┘  │
│         │                                 │              │
│    ┌────▼─────────┐              ┌──────▼───────────┐  │
│    │ gRPC Clients │              │  Redis Client    │  │
│    │              │              │  (Pub/Sub)       │  │
│    └────┬─────────┘              └──────────────────┘  │
│         │                                                │
│    ┌────▼─────────┐                                     │
│    │ HTTP Proxy   │                                     │
│    └────┬─────────┘                                     │
└─────────┼─────────────────────────────────────────────┘
          │
    ┌─────▼──────────────────────────────────┐
    │                                          │
    ▼                                          ▼
┌──────────────────┐              ┌───────────────────┐
│  User Config     │              │  User Login       │
│  Service         │              │  Service          │
│  (gRPC:50051)    │              │  (REST:8002)      │
└──────────────────┘              └───────────────────┘
```

### Request Flow

#### 1. REST → gRPC Flow (User Config Service)
```
Frontend → API Gateway → User Config Handler → gRPC Client → User Config Service
    ↓                                                              ↓
  JSON                                                      Protobuf
    ↑                                                              ↑
Frontend ← API Gateway ← User Config Handler ← gRPC Client ← User Config Service
```

#### 2. HTTP Proxy Flow (User Login Service)
```
Frontend → API Gateway → Auth Proxy Handler → User Login Service
    ↓                                              ↓
  JSON                                           JSON
    ↑                                              ↑
Frontend ← API Gateway ← Auth Proxy Handler ← User Login Service
```

#### 3. WebSocket Flow (Real-time Match Events)
```
Rules Engine → Redis Pub/Sub → WebSocket Handler → Frontend
                                      ↓
                               (Active WebSocket
                                Connections)
```

---

## Project Structure

```
api/gateway/
├── cmd/
│   └── main.go                    # Application entry point
├── config/
│   └── config.go                  # Configuration loading & management
├── internal/
│   ├── handlers/                  # HTTP request handlers
│   │   ├── user_config_handler.go # Strategy CRUD operations
│   │   ├── auth_proxy_handler.go  # Auth service proxy
│   │   └── websocket_handler.go   # WebSocket connection handler
│   ├── grpc_clients/              # gRPC client wrappers
│   │   └── user_config_client.go  # User Config Service client
│   ├── middleware/                # HTTP middleware
│   │   └── cors.go                # CORS configuration
│   └── router/                    # Route definitions
│       └── router.go              # Route setup & registration
├── bin/                           # Compiled binaries
├── .env                           # Environment configuration
├── go.mod                         # Go module dependencies
├── README.md                      # Setup instructions
├── API_DOCUMENTATION.md           # API endpoint documentation
├── WEBSOCKET_DOCUMENTATION.md     # WebSocket documentation
└── CURL_COMMANDS.md              # cURL testing examples
```

---

## Core Components

### 1. Main Application (`cmd/main.go`)

**Responsibilities:**
- Load configuration from environment
- Initialize gRPC clients
- Initialize HTTP handlers
- Set up router with middleware
- Start HTTP server
- Handle graceful shutdown

**Key Code Flow:**
```go
func main() {
    // 1. Load configuration
    cfg, err := config.Load()
    
    // 2. Initialize gRPC client for User Config Service
    userConfigClient, err := grpc_clients.NewUserConfigClient(
        cfg.Services.UserConfigAddr,
        cfg.Server.GRPCTimeout,
    )
    
    // 3. Initialize Redis for WebSocket pub/sub
    redisClient := redis.NewClient(&redis.Options{...})
    
    // 4. Create handlers
    userConfigHandler := handlers.NewUserConfigHandler(userConfigClient)
    authProxyHandler := handlers.NewAuthProxyHandler(cfg.Services.UserLoginServiceURL)
    websocketHandler := handlers.NewWebSocketHandler(redisClient)
    
    // 5. Setup router with CORS
    r := router.NewRouter(userConfigHandler, authProxyHandler, websocketHandler, corsConfig)
    
    // 6. Start HTTP server
    srv := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Server.HTTPPort), Handler: r}
    go srv.ListenAndServe()
    
    // 7. Wait for shutdown signal
    <-quit
    srv.Shutdown(ctx)
}
```

### 2. Configuration (`config/config.go`)

**Purpose:** Centralized configuration management from environment variables.

**Configuration Structure:**
```go
type Config struct {
    Server   ServerConfig     // HTTP port, timeouts
    Services ServicesConfig   // Backend service addresses
    CORS     CORSConfig      // CORS settings
    Logging  LoggingConfig   // Log level
}
```

**Environment Variables:**
- `HTTP_PORT`: API Gateway listening port (default: 8081)
- `GRPC_TIMEOUT`: Timeout for gRPC calls (default: 30s)
- `USER_CONFIG_GRPC_ADDR`: User Config Service address (default: localhost:50051)
- `USER_LOGIN_SERVICE_URL`: User Login Service URL (default: http://localhost:8002)
- `CORS_ALLOWED_ORIGINS`: Comma-separated allowed origins
- `CORS_ALLOWED_METHODS`: Comma-separated HTTP methods
- `CORS_ALLOWED_HEADERS`: Comma-separated allowed headers
- `LOG_LEVEL`: Logging level (default: INFO)

### 3. Router (`internal/router/router.go`)

**Purpose:** Define and register all HTTP routes with appropriate handlers.

**Route Groups:**

#### Strategy Management Routes (`/api/v1/strategies`)
- `POST /api/v1/strategies` - Create new strategy
- `GET /api/v1/strategies/{strategy_id}` - Get strategy by ID
- `PUT /api/v1/strategies/{strategy_id}` - Update strategy
- `DELETE /api/v1/strategies/{strategy_id}` - Delete strategy
- `POST /api/v1/strategies/{strategy_id}/activate` - Activate strategy
- `POST /api/v1/strategies/{strategy_id}/deactivate` - Deactivate strategy

#### User Routes (`/api/v1/users`)
- `GET /api/v1/users/{user_id}/strategies` - List user's strategies

#### Authentication Proxy Routes (`/api/v1/auth`, `/credentials`, etc.)
- All requests under these paths are proxied to User Login Service

#### WebSocket Routes
- `GET /ws/matches?user_id={user_id}` - User-specific match feed
- `GET /ws/matches/all` - All users match feed (admin)

#### Health Check
- `GET /api/v1/health` - Service health status

### 4. Handlers

#### a. User Config Handler (`internal/handlers/user_config_handler.go`)

**Purpose:** Handle strategy CRUD operations by calling User Config Service via gRPC.

**Key Methods:**
```go
type UserConfigHandler struct {
    client *grpc_clients.UserConfigClient
}

// Create a new strategy
func (h *UserConfigHandler) CreateStrategy(w http.ResponseWriter, r *http.Request)

// Update existing strategy
func (h *UserConfigHandler) UpdateStrategy(w http.ResponseWriter, r *http.Request)

// Delete strategy
func (h *UserConfigHandler) DeleteStrategy(w http.ResponseWriter, r *http.Request)

// Get strategy details
func (h *UserConfigHandler) GetStrategy(w http.ResponseWriter, r *http.Request)

// List user's strategies with pagination
func (h *UserConfigHandler) ListUserStrategies(w http.ResponseWriter, r *http.Request)

// Activate/Deactivate strategy
func (h *UserConfigHandler) ActivateStrategy(w http.ResponseWriter, r *http.Request)
func (h *UserConfigHandler) DeactivateStrategy(w http.ResponseWriter, r *http.Request)
```

**Request Flow Example (CreateStrategy):**
```
1. Receive HTTP POST with JSON body
2. Parse JSON into pb.CreateStrategyRequest
3. Call gRPC client: client.CreateStrategy(ctx, req)
4. Receive pb.CreateStrategyResponse
5. Check response.Success
6. Return JSON response with appropriate status code
```

#### b. Auth Proxy Handler (`internal/handlers/auth_proxy_handler.go`)

**Purpose:** Forward authentication-related requests to User Login Service.

**Key Method:**
```go
func (h *AuthProxyHandler) ProxyRequest(w http.ResponseWriter, r *http.Request) {
    // 1. Build target URL
    targetURL := h.loginServiceURL + r.URL.Path + r.URL.RawQuery
    
    // 2. Read request body
    bodyBytes, _ := io.ReadAll(r.Body)
    
    // 3. Create proxy request
    proxyReq, _ := http.NewRequest(r.Method, targetURL, bytes.NewBuffer(bodyBytes))
    
    // 4. Copy headers (excluding hop-by-hop)
    for key, values := range r.Header {...}
    
    // 5. Execute request
    resp, _ := h.client.Do(proxyReq)
    
    // 6. Copy response headers and body
    w.WriteHeader(resp.StatusCode)
    io.Copy(w, resp.Body)
}
```

**Features:**
- Transparent proxying (frontend sees no difference)
- Header forwarding (preserves authentication tokens)
- Excludes hop-by-hop headers
- Prevents CORS header duplication

#### c. WebSocket Handler (`internal/handlers/websocket_handler.go`)

**Purpose:** Stream real-time match events from Redis to WebSocket clients.

**Key Methods:**
```go
type WebSocketHandler struct {
    redisClient *redis.Client
    upgrader    websocket.Upgrader
}

// Handle user-specific match feed
func (h *WebSocketHandler) HandleMatchesFeed(w http.ResponseWriter, r *http.Request)

// Handle all users match feed
func (h *WebSocketHandler) HandleAllMatchesFeed(w http.ResponseWriter, r *http.Request)
```

**WebSocket Flow:**
```
1. Client connects: ws://localhost:8081/ws/matches?user_id=IS14415
2. Upgrade HTTP to WebSocket connection
3. Subscribe to Redis channel: user:{user_id}:matches
4. Send "connected" message to client
5. Listen for Redis messages in goroutine
6. Forward each message to WebSocket client
7. On error/disconnect: cleanup and close
```

**Message Format:**
```json
{
  "type": "match",
  "user_id": "IS14415",
  "strategy_id": "strat_123",
  "match_details": {...},
  "timestamp": "2025-12-10T10:30:00Z"
}
```

### 5. gRPC Clients (`internal/grpc_clients/user_config_client.go`)

**Purpose:** Wrapper for User Config Service gRPC calls with timeout management.

**Implementation:**
```go
type UserConfigClient struct {
    client  pb.UserConfigServiceClient  // Generated gRPC client
    conn    *grpc.ClientConn            // gRPC connection
    timeout time.Duration               // Call timeout
}

// Initialize client with connection
func NewUserConfigClient(addr string, timeout time.Duration) (*UserConfigClient, error) {
    conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
    return &UserConfigClient{
        client:  pb.NewUserConfigServiceClient(conn),
        conn:    conn,
        timeout: timeout,
    }, nil
}

// All methods follow this pattern:
func (c *UserConfigClient) CreateStrategy(ctx context.Context, req *pb.CreateStrategyRequest) (*pb.CreateStrategyResponse, error) {
    ctx, cancel := context.WithTimeout(ctx, c.timeout)
    defer cancel()
    return c.client.CreateStrategy(ctx, req)
}
```

**Features:**
- Automatic timeout handling
- Connection management
- Graceful shutdown support

### 6. Middleware (`internal/middleware/cors.go`)

**Purpose:** Handle CORS preflight requests and add CORS headers to responses.

**Implementation:**
```go
func CORS(config CORSConfig) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            origin := r.Header.Get("Origin")
            
            // Check if origin is allowed
            if originAllowed(origin, config.AllowedOrigins) {
                w.Header().Set("Access-Control-Allow-Origin", origin)
            }
            
            // Set CORS headers
            w.Header().Set("Access-Control-Allow-Methods", ...)
            w.Header().Set("Access-Control-Allow-Headers", ...)
            w.Header().Set("Access-Control-Allow-Credentials", "true")
            
            // Handle OPTIONS preflight
            if r.Method == http.MethodOptions {
                w.WriteHeader(http.StatusOK)
                return
            }
            
            next.ServeHTTP(w, r)
        })
    }
}
```

**Key Features:**
- Wildcard origin support (reflects requesting origin)
- Trailing slash normalization
- Credentials support
- OPTIONS preflight handling

---

## Configuration

### Environment Setup

Create or edit `.env` file in `api/gateway/` directory:

```bash
# Server Configuration
HTTP_PORT=8081
GRPC_TIMEOUT=30s

# Backend Service Endpoints
USER_CONFIG_GRPC_ADDR=localhost:50051
USER_LOGIN_SERVICE_URL=http://localhost:8002

# CORS Configuration
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173,http://localhost:5174
CORS_ALLOWED_METHODS=GET,POST,PUT,DELETE,OPTIONS
CORS_ALLOWED_HEADERS=Content-Type,Authorization,X-Request-ID

# Logging
LOG_LEVEL=INFO

# Redis Configuration (for WebSocket)
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
```

### Configuration Best Practices

1. **Development**: Use `localhost` addresses
2. **Staging**: Use internal service discovery or container names
3. **Production**: Use fully qualified domain names with TLS
4. **CORS**: Be specific with allowed origins in production (avoid wildcards)
5. **Timeouts**: Adjust based on expected response times
6. **Logging**: Use DEBUG in development, INFO/WARN in production

---

## API Endpoints

### Base URL
```
http://localhost:8081/api/v1
```

### 1. Health Check

**Endpoint:** `GET /api/v1/health`

**Response:**
```json
{
  "status": "healthy",
  "service": "api-gateway",
  "timestamp": "2025-12-10T10:30:00Z"
}
```

### 2. Create Strategy

**Endpoint:** `POST /api/v1/strategies`

**Request Body:**
```json
{
  "user_id": "IS14415",
  "strategy_name": "Apple News Trading",
  "instrument_token": "AAPL",
  "segment": "NSE_EQ",
  "action_type": "BUY",
  "quantity": 100,
  "order_type": "MARKET",
  "product_type": "INTRADAY",
  "news_keywords": ["Apple", "iPhone", "earnings"],
  "sentiment": "POSITIVE"
}
```

**Response:**
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "strat_123abc",
    "user_id": "IS14415",
    "strategy_name": "Apple News Trading",
    "is_active": false,
    "created_at": "2025-12-10T10:30:00Z"
  }
}
```

### 3. Get Strategy

**Endpoint:** `GET /api/v1/strategies/{strategy_id}`

**Query Parameters:**
- `user_id` (required): User ID

**Response:**
```json
{
  "success": true,
  "strategy": {
    "strategy_id": "strat_123abc",
    "user_id": "IS14415",
    "strategy_name": "Apple News Trading",
    "is_active": true,
    "instrument_token": "AAPL",
    "segment": "NSE_EQ",
    "action_type": "BUY",
    "quantity": 100,
    "order_type": "MARKET",
    "product_type": "INTRADAY",
    "news_keywords": ["Apple", "iPhone"],
    "sentiment": "POSITIVE",
    "created_at": "2025-12-10T10:30:00Z",
    "updated_at": "2025-12-10T11:00:00Z"
  }
}
```

### 4. Update Strategy

**Endpoint:** `PUT /api/v1/strategies/{strategy_id}`

**Request Body:**
```json
{
  "user_id": "IS14415",
  "strategy_name": "Apple News Trading V2",
  "quantity": 150,
  "news_keywords": ["Apple", "iPhone", "earnings", "revenue"]
}
```

### 5. Delete Strategy

**Endpoint:** `DELETE /api/v1/strategies/{strategy_id}?user_id=IS14415`

**Response:**
```json
{
  "success": true,
  "message": "Strategy deleted successfully"
}
```

### 6. List User Strategies

**Endpoint:** `GET /api/v1/users/{user_id}/strategies`

**Query Parameters:**
- `page` (optional, default: 1): Page number
- `page_size` (optional, default: 10): Items per page

**Response:**
```json
{
  "success": true,
  "strategies": [
    {
      "strategy_id": "strat_123abc",
      "strategy_name": "Apple News Trading",
      "is_active": true,
      "created_at": "2025-12-10T10:30:00Z"
    }
  ],
  "pagination": {
    "total": 25,
    "page": 1,
    "page_size": 10,
    "total_pages": 3
  }
}
```

### 7. Activate Strategy

**Endpoint:** `POST /api/v1/strategies/{strategy_id}/activate`

**Request Body:**
```json
{
  "user_id": "IS14415"
}
```

### 8. Authentication Endpoints (Proxied)

All auth endpoints are proxied to User Login Service:

- `POST /api/v1/credentials/register` - Register user credentials
- `POST /api/v1/auth/login` - Perform login
- `GET /api/v1/session/active-sessions` - Get active sessions
- `POST /api/v1/totp/generate` - Generate TOTP
- `POST /api/v1/totp/verify` - Verify TOTP

See `API_DOCUMENTATION.md` for complete auth API details.

---

## WebSocket Implementation

### Connection URLs

#### User-Specific Feed
```
ws://localhost:8081/ws/matches?user_id=IS14415
```

#### All Users Feed (Admin)
```
ws://localhost:8081/ws/matches/all
```

### Connection Flow

1. **Client initiates WebSocket connection**
2. **Server upgrades HTTP connection to WebSocket**
3. **Server sends connected confirmation:**
   ```json
   {
     "type": "connected",
     "message": "Connected to live match feed",
     "user_id": "IS14415"
   }
   ```
4. **Server subscribes to Redis channel:** `user:{user_id}:matches`
5. **Rules Engine publishes match events to Redis**
6. **Server forwards events to WebSocket client**

### Message Types

#### Connection Confirmation
```json
{
  "type": "connected",
  "message": "Connected to live match feed",
  "user_id": "IS14415"
}
```

#### Match Event
```json
{
  "type": "match",
  "user_id": "IS14415",
  "strategy_id": "strat_123abc",
  "strategy_name": "Apple News Trading",
  "news_event": {
    "headline": "Apple announces record earnings",
    "sentiment": "POSITIVE",
    "timestamp": "2025-12-10T10:30:00Z"
  },
  "order_request": {
    "instrument_token": "AAPL",
    "action": "BUY",
    "quantity": 100,
    "order_type": "MARKET"
  },
  "match_timestamp": "2025-12-10T10:30:05Z"
}
```

#### Error Message
```json
{
  "type": "error",
  "message": "Connection lost, reconnecting...",
  "code": "WS_RECONNECT"
}
```

### JavaScript Client Example

```javascript
const ws = new WebSocket('ws://localhost:8081/ws/matches?user_id=IS14415');

ws.onopen = () => {
    console.log('WebSocket connected');
};

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    
    if (data.type === 'connected') {
        console.log('Connected:', data.message);
    } else if (data.type === 'match') {
        console.log('New match:', data);
        // Update UI with match information
        displayMatchNotification(data);
    } else if (data.type === 'error') {
        console.error('WebSocket error:', data.message);
    }
};

ws.onerror = (error) => {
    console.error('WebSocket error:', error);
};

ws.onclose = () => {
    console.log('WebSocket closed, attempting reconnect...');
    setTimeout(() => {
        // Reconnect logic
    }, 5000);
};
```

### React Hook Example

```javascript
import { useEffect, useState } from 'react';

function useMatchFeed(userId) {
    const [matches, setMatches] = useState([]);
    const [connected, setConnected] = useState(false);

    useEffect(() => {
        const ws = new WebSocket(`ws://localhost:8081/ws/matches?user_id=${userId}`);

        ws.onopen = () => setConnected(true);
        
        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (data.type === 'match') {
                setMatches(prev => [data, ...prev]);
            }
        };

        ws.onclose = () => setConnected(false);

        return () => ws.close();
    }, [userId]);

    return { matches, connected };
}
```

---

## CORS Configuration

### Overview
CORS (Cross-Origin Resource Sharing) allows frontend applications running on different domains to make requests to the API Gateway.

### Configuration Strategy

The gateway supports multiple CORS origins through environment configuration:

```bash
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173,http://localhost:5174
```

### Implementation Details

#### Origin Matching
- Exact match: `http://localhost:3000`
- Wildcard (`*`): Reflects requesting origin (with credentials support)
- Trailing slash normalization: `http://localhost:3000/` → `http://localhost:3000`

#### Headers Added
```
Access-Control-Allow-Origin: http://localhost:3000
Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization, X-Request-ID
Access-Control-Allow-Credentials: true
Access-Control-Max-Age: 3600
```

#### Preflight Handling
- All `OPTIONS` requests return `200 OK` immediately
- CORS headers are added to all responses
- No authentication required for preflight

### Common CORS Issues & Solutions

| Issue | Cause | Solution |
|-------|-------|----------|
| `No 'Access-Control-Allow-Origin' header` | Origin not in allowed list | Add origin to `CORS_ALLOWED_ORIGINS` |
| `Credentials flag is 'true', but origin is '*'` | Using wildcard with credentials | Use specific origins or reflect origin |
| `Method not allowed` | Method not in `CORS_ALLOWED_METHODS` | Add method to config |
| `Header not allowed` | Custom header not in `CORS_ALLOWED_HEADERS` | Add header to config |

---

## Error Handling

### HTTP Status Codes

| Code | Meaning | When Used |
|------|---------|-----------|
| 200 | OK | Successful GET, PUT operations |
| 201 | Created | Successful POST (resource created) |
| 204 | No Content | Successful DELETE |
| 400 | Bad Request | Invalid request body or parameters |
| 401 | Unauthorized | Authentication required |
| 403 | Forbidden | Authenticated but not authorized |
| 404 | Not Found | Resource doesn't exist |
| 500 | Internal Server Error | Server-side error |
| 502 | Bad Gateway | Backend service unavailable |
| 503 | Service Unavailable | Gateway overloaded |

### Error Response Format

```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid strategy configuration",
    "details": {
      "field": "quantity",
      "reason": "must be positive integer"
    }
  }
}
```

### Error Handling in Handlers

```go
func (h *UserConfigHandler) CreateStrategy(w http.ResponseWriter, r *http.Request) {
    var req pb.CreateStrategyRequest
    
    // Parse error
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        respondWithError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
        return
    }

    // gRPC call error
    resp, err := h.client.CreateStrategy(r.Context(), &req)
    if err != nil {
        respondWithError(w, http.StatusInternalServerError, "Failed to create strategy: "+err.Error())
        return
    }

    // Business logic error
    if !resp.Success {
        respondWithError(w, http.StatusBadRequest, resp.Error.Message)
        return
    }

    respondWithJSON(w, http.StatusCreated, resp)
}
```

### Helper Functions

```go
func respondWithError(w http.ResponseWriter, code int, message string) {
    respondWithJSON(w, code, map[string]interface{}{
        "success": false,
        "error": map[string]string{
            "message": message,
        },
    })
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
    response, _ := json.Marshal(payload)
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    w.Write(response)
}
```

---

## Setup & Deployment

### Prerequisites

1. **Go 1.23+** installed
2. **User Config Service** running on port 50051
3. **User Login Service** running on port 8002
4. **Redis** running on port 6379 (for WebSocket)
5. **PostgreSQL** for User Config Service

### Development Setup

```bash
# 1. Navigate to gateway directory
cd /home/stockkask/algo-trading/Algo-Treading/api/gateway

# 2. Install dependencies
go mod download
go mod tidy

# 3. Configure environment
cp .env.example .env
# Edit .env with your configuration

# 4. Generate protocol buffers (if proto files changed)
cd ../proto
make generate

# 5. Run the gateway
cd ../gateway
go run cmd/main.go
```

### Production Build

```bash
# Build binary
go build -o bin/api-gateway cmd/main.go

# Run binary
./bin/api-gateway
```

### Docker Deployment

```dockerfile
# Dockerfile
FROM golang:1.23-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o api-gateway cmd/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

COPY --from=builder /app/api-gateway .
COPY --from=builder /app/.env .

EXPOSE 8081
CMD ["./api-gateway"]
```

```bash
# Build and run
docker build -t api-gateway:latest .
docker run -p 8081:8081 --env-file .env api-gateway:latest
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-gateway
spec:
  replicas: 3
  selector:
    matchLabels:
      app: api-gateway
  template:
    metadata:
      labels:
        app: api-gateway
    spec:
      containers:
      - name: api-gateway
        image: api-gateway:latest
        ports:
        - containerPort: 8081
        env:
        - name: HTTP_PORT
          value: "8081"
        - name: USER_CONFIG_GRPC_ADDR
          value: "user-config-service:50051"
        - name: USER_LOGIN_SERVICE_URL
          value: "http://user-login-service:8002"
        - name: REDIS_ADDR
          value: "redis-service:6379"
        livenessProbe:
          httpGet:
            path: /api/v1/health
            port: 8081
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /api/v1/health
            port: 8081
          initialDelaySeconds: 5
          periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: api-gateway
spec:
  selector:
    app: api-gateway
  ports:
  - protocol: TCP
    port: 80
    targetPort: 8081
  type: LoadBalancer
```

### Environment-Specific Configuration

#### Development
```bash
HTTP_PORT=8081
USER_CONFIG_GRPC_ADDR=localhost:50051
USER_LOGIN_SERVICE_URL=http://localhost:8002
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173
LOG_LEVEL=DEBUG
```

#### Staging
```bash
HTTP_PORT=8081
USER_CONFIG_GRPC_ADDR=user-config-service.staging:50051
USER_LOGIN_SERVICE_URL=http://user-login-service.staging:8002
CORS_ALLOWED_ORIGINS=https://staging.tradingapp.com
LOG_LEVEL=INFO
```

#### Production
```bash
HTTP_PORT=8081
USER_CONFIG_GRPC_ADDR=user-config-service.prod:50051
USER_LOGIN_SERVICE_URL=https://auth.tradingapp.com
CORS_ALLOWED_ORIGINS=https://app.tradingapp.com,https://www.tradingapp.com
LOG_LEVEL=WARN
```

---

## Testing

### Manual Testing with cURL

#### Health Check
```bash
curl http://localhost:8081/api/v1/health
```

#### Create Strategy
```bash
curl -X POST http://localhost:8081/api/v1/strategies \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "IS14415",
    "strategy_name": "Test Strategy",
    "instrument_token": "AAPL",
    "segment": "NSE_EQ",
    "action_type": "BUY",
    "quantity": 100,
    "order_type": "MARKET",
    "product_type": "INTRADAY",
    "news_keywords": ["Apple"],
    "sentiment": "POSITIVE"
  }'
```

#### Get Strategy
```bash
curl http://localhost:8081/api/v1/strategies/strat_123abc?user_id=IS14415
```

#### List Strategies
```bash
curl http://localhost:8081/api/v1/users/IS14415/strategies?page=1&page_size=10
```

#### WebSocket Test (JavaScript)
```javascript
const ws = new WebSocket('ws://localhost:8081/ws/matches?user_id=IS14415');
ws.onmessage = (event) => console.log(JSON.parse(event.data));
```

### Automated Testing

#### Unit Tests

```go
// handlers/user_config_handler_test.go
package handlers_test

import (
    "bytes"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"
)

func TestCreateStrategy(t *testing.T) {
    // Setup
    mockClient := &mockUserConfigClient{}
    handler := handlers.NewUserConfigHandler(mockClient)
    
    req := pb.CreateStrategyRequest{
        UserId: "IS14415",
        StrategyName: "Test Strategy",
    }
    body, _ := json.Marshal(req)
    
    // Execute
    r := httptest.NewRequest("POST", "/api/v1/strategies", bytes.NewBuffer(body))
    w := httptest.NewRecorder()
    handler.CreateStrategy(w, r)
    
    // Assert
    if w.Code != http.StatusCreated {
        t.Errorf("Expected status 201, got %d", w.Code)
    }
}
```

#### Integration Tests

```bash
# test_api.sh
#!/bin/bash

BASE_URL="http://localhost:8081/api/v1"

# Test health check
echo "Testing health check..."
curl -s $BASE_URL/health | jq

# Test create strategy
echo "Testing create strategy..."
STRATEGY_ID=$(curl -s -X POST $BASE_URL/strategies \
  -H "Content-Type: application/json" \
  -d '{"user_id":"IS14415","strategy_name":"Test"}' \
  | jq -r '.strategy.strategy_id')

echo "Created strategy: $STRATEGY_ID"

# Test get strategy
echo "Testing get strategy..."
curl -s "$BASE_URL/strategies/$STRATEGY_ID?user_id=IS14415" | jq

# Cleanup
echo "Cleaning up..."
curl -s -X DELETE "$BASE_URL/strategies/$STRATEGY_ID?user_id=IS14415"
```

### Load Testing

```bash
# Using Apache Bench
ab -n 1000 -c 10 http://localhost:8081/api/v1/health

# Using wrk
wrk -t4 -c100 -d30s http://localhost:8081/api/v1/health
```

---

## Troubleshooting

### Common Issues

#### 1. Gateway Won't Start

**Error:** `Failed to initialize user config client`

**Cause:** User Config Service not running or unreachable

**Solution:**
```bash
# Check User Config Service
curl http://localhost:50051

# Start User Config Service
cd services/user-config
go run cmd/main.go
```

#### 2. CORS Errors in Browser

**Error:** `Access to XMLHttpRequest has been blocked by CORS policy`

**Cause:** Frontend origin not in allowed list

**Solution:**
```bash
# Add origin to .env
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://localhost:5173

# Restart gateway
```

#### 3. WebSocket Connection Fails

**Error:** `WebSocket connection failed`

**Cause:** Redis not running or incorrect configuration

**Solution:**
```bash
# Check Redis
redis-cli ping

# Start Redis
redis-server

# Check Redis connection in logs
```

#### 4. gRPC Timeout Errors

**Error:** `context deadline exceeded`

**Cause:** Backend service too slow or not responding

**Solution:**
```bash
# Increase timeout in .env
GRPC_TIMEOUT=60s

# Check backend service health
```

#### 5. 502 Bad Gateway

**Error:** HTTP 502 on proxy endpoints

**Cause:** User Login Service not running

**Solution:**
```bash
# Start User Login Service
cd services/user-login-service
python -m uvicorn src.main:app --port 8002
```

### Debug Mode

Enable debug logging:

```bash
LOG_LEVEL=DEBUG go run cmd/main.go
```

### Health Checks

```bash
# Gateway health
curl http://localhost:8081/api/v1/health

# User Config Service health (via gateway)
curl http://localhost:8081/api/v1/health

# Direct gRPC health check
grpcurl -plaintext localhost:50051 health.Health/Check
```

---

## Best Practices

### 1. Error Handling
- Always return proper HTTP status codes
- Include descriptive error messages
- Log errors with context (user_id, strategy_id, etc.)
- Don't expose internal error details to clients

### 2. Request Validation
- Validate all input parameters
- Check required fields
- Validate data types and ranges
- Use Protocol Buffer validation

### 3. Timeout Management
- Set reasonable timeouts for all operations
- Use context.WithTimeout for all gRPC calls
- Handle timeout errors gracefully
- Configure timeouts per environment

### 4. Logging
- Log all incoming requests
- Log request/response for debugging
- Use structured logging (JSON format)
- Include correlation IDs

### 5. Security
- Validate authentication tokens
- Implement rate limiting
- Use HTTPS in production
- Sanitize user input
- Implement request size limits

### 6. Performance
- Use connection pooling for gRPC
- Cache frequently accessed data
- Implement pagination for list operations
- Use HTTP/2 for better performance
- Monitor response times

### 7. Monitoring
- Track request counts and latency
- Monitor error rates
- Set up alerts for anomalies
- Use distributed tracing
- Monitor WebSocket connection count

### 8. Deployment
- Use health checks in load balancers
- Implement graceful shutdown
- Use rolling deployments
- Keep multiple replicas for HA
- Use blue-green deployment

---

## Additional Resources

### Related Documentation
- [API Documentation](./API_DOCUMENTATION.md) - Complete API reference
- [WebSocket Documentation](./WEBSOCKET_DOCUMENTATION.md) - WebSocket guide
- [CURL Commands](./CURL_COMMANDS.md) - Testing examples
- [User Config Service](../../services/user-config/README.md) - Backend service docs
- [User Login Service](../../services/user-login-service/README.md) - Auth service docs

### External References
- [Gorilla Mux Documentation](https://github.com/gorilla/mux)
- [gRPC Go Tutorial](https://grpc.io/docs/languages/go/)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)
- [WebSocket Protocol](https://datatracker.ietf.org/doc/html/rfc6455)
- [Redis Pub/Sub](https://redis.io/topics/pubsub)

### Support
- **Issues:** Report bugs or request features via GitHub Issues
- **Slack:** #trading-system-dev channel
- **Email:** dev-team@tradingapp.com

---

**Last Updated:** December 10, 2025  
**Version:** 1.0  
**Maintained by:** Backend Development Team
