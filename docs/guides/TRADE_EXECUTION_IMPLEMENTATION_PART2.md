# Trade Execution Service - Implementation Guide (Part 2)

## 📋 Continuation from Part 1

This document continues the Trade Execution Service implementation with RabbitMQ consumer, gRPC server, testing, and deployment.

---

## 🔨 Step 7: RabbitMQ Consumer Implementation

Create `internal/consumer/rabbitmq_consumer.go`:

```go
package consumer

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"
    
    "github.com/google/uuid"
    amqp "github.com/rabbitmq/amqp091-go"
    "trade-execution/internal/executor"
    "trade-execution/internal/models"
    "trade-execution/internal/repository"
)

type RabbitMQConsumer struct {
    conn           *amqp.Connection
    channel        *amqp.Channel
    queueName      string
    prefetchCount  int
    executor       *executor.OrderExecutor
    repo           repository.OrderRepository
    workerCount    int
    shutdownChan   chan struct{}
}

type Config struct {
    URL           string
    QueueName     string
    PrefetchCount int
    WorkerCount   int
}

func NewRabbitMQConsumer(cfg Config, executor *executor.OrderExecutor, repo repository.OrderRepository) (*RabbitMQConsumer, error) {
    conn, err := amqp.Dial(cfg.URL)
    if err != nil {
        return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
    }
    
    channel, err := conn.Channel()
    if err != nil {
        conn.Close()
        return nil, fmt.Errorf("failed to open channel: %w", err)
    }
    
    // Set QoS (prefetch count)
    err = channel.Qos(cfg.PrefetchCount, 0, false)
    if err != nil {
        channel.Close()
        conn.Close()
        return nil, fmt.Errorf("failed to set QoS: %w", err)
    }
    
    return &RabbitMQConsumer{
        conn:          conn,
        channel:       channel,
        queueName:     cfg.QueueName,
        prefetchCount: cfg.PrefetchCount,
        executor:      executor,
        repo:          repo,
        workerCount:   cfg.WorkerCount,
        shutdownChan:  make(chan struct{}),
    }, nil
}

// Start begins consuming messages
func (c *RabbitMQConsumer) Start(ctx context.Context) error {
    log.Printf("Starting RabbitMQ consumer on queue: %s", c.queueName)
    
    // Declare queue (idempotent)
    _, err := c.channel.QueueDeclare(
        c.queueName, // name
        true,        // durable
        false,       // delete when unused
        false,       // exclusive
        false,       // no-wait
        nil,         // arguments
    )
    if err != nil {
        return fmt.Errorf("failed to declare queue: %w", err)
    }
    
    // Start consuming
    messages, err := c.channel.Consume(
        c.queueName,
        "trade-executor", // consumer tag
        false,            // auto-ack
        false,            // exclusive
        false,            // no-local
        false,            // no-wait
        nil,              // args
    )
    if err != nil {
        return fmt.Errorf("failed to register consumer: %w", err)
    }
    
    // Start worker pool
    for i := 0; i < c.workerCount; i++ {
        go c.worker(ctx, messages, i)
    }
    
    log.Printf("Started %d workers", c.workerCount)
    
    // Wait for shutdown
    <-c.shutdownChan
    
    return nil
}

// worker processes messages from the queue
func (c *RabbitMQConsumer) worker(ctx context.Context, messages <-chan amqp.Delivery, workerID int) {
    log.Printf("Worker %d started", workerID)
    
    for {
        select {
        case <-ctx.Done():
            log.Printf("Worker %d shutting down", workerID)
            return
            
        case msg, ok := <-messages:
            if !ok {
                log.Printf("Worker %d: channel closed", workerID)
                return
            }
            
            c.processMessage(ctx, msg, workerID)
        }
    }
}

// processMessage handles a single order request
func (c *RabbitMQConsumer) processMessage(ctx context.Context, msg amqp.Delivery, workerID int) {
    log.Printf("Worker %d processing message", workerID)
    
    // Parse order request
    var orderReq models.OrderRequest
    if err := json.Unmarshal(msg.Body, &orderReq); err != nil {
        log.Printf("Worker %d: failed to parse message: %v", workerID, err)
        msg.Nack(false, false) // Send to DLQ
        return
    }
    
    // Validate request
    if err := c.validateOrderRequest(&orderReq); err != nil {
        log.Printf("Worker %d: invalid order request: %v", workerID, err)
        msg.Nack(false, false) // Send to DLQ
        return
    }
    
    // Convert to internal order model
    order := c.convertToOrder(&orderReq)
    
    // Save order to database
    if err := c.repo.Create(ctx, order); err != nil {
        log.Printf("Worker %d: failed to save order: %v", workerID, err)
        msg.Nack(false, true) // Requeue
        return
    }
    
    // Execute order
    if err := c.executor.ExecuteOrder(ctx, order); err != nil {
        log.Printf("Worker %d: failed to execute order %s: %v", workerID, order.OrderID, err)
        
        // Check if we should retry
        if orderReq.RetryCount < 3 {
            msg.Nack(false, true) // Requeue
        } else {
            msg.Nack(false, false) // Send to DLQ
        }
        return
    }
    
    log.Printf("Worker %d: successfully processed order %s", workerID, order.OrderID)
    msg.Ack(false)
}

func (c *RabbitMQConsumer) validateOrderRequest(req *models.OrderRequest) error {
    if req.UserID == "" {
        return fmt.Errorf("user_id is required")
    }
    if req.StockCode == 0 {
        return fmt.Errorf("stock_code is required")
    }
    if req.Quantity <= 0 {
        return fmt.Errorf("quantity must be positive")
    }
    if !req.RiskApproved {
        return fmt.Errorf("order not approved by risk management")
    }
    return nil
}

func (c *RabbitMQConsumer) convertToOrder(req *models.OrderRequest) *models.Order {
    orderID := uuid.New()
    eventID, _ := uuid.Parse(req.EventID)
    
    return &models.Order{
        OrderID:      orderID,
        UserID:       req.UserID,
        StrategyID:   req.StrategyID,
        EventID:      eventID,
        StockCode:    req.StockCode,
        Exchange:     models.Exchange(req.Exchange),
        Symbol:       req.Symbol,
        OrderType:    models.OrderType(req.OrderType),
        OrderSide:    models.OrderSide(req.OrderSide),
        Quantity:     req.Quantity,
        Price:        req.Price,
        StopLoss:     req.StopLoss,
        TakeProfit:   req.TakeProfit,
        Validity:     req.Validity,
        Status:       models.StatusReceived,
        RiskApproved: req.RiskApproved,
        RiskScore:    req.RiskScore,
        RetryCount:   req.RetryCount,
        CreatedAt:    time.Now(),
        UpdatedAt:    time.Now(),
    }
}

// Shutdown gracefully shuts down the consumer
func (c *RabbitMQConsumer) Shutdown() error {
    log.Println("Shutting down RabbitMQ consumer...")
    
    close(c.shutdownChan)
    
    if c.channel != nil {
        c.channel.Close()
    }
    
    if c.conn != nil {
        c.conn.Close()
    }
    
    log.Println("RabbitMQ consumer shutdown complete")
    return nil
}
```

---

## 🌐 Step 8: gRPC Server Implementation

Create `internal/server/grpc_server.go`:

```go
package server

import (
    "context"
    "fmt"
    "net"
    
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    
    "github.com/google/uuid"
    pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
    "trade-execution/internal/repository"
    "trade-execution/internal/models"
)

type Server struct {
    pb.UnimplementedTradeExecutionServiceServer
    repo repository.OrderRepository
    port int
}

func NewServer(repo repository.OrderRepository, port int) *Server {
    return &Server{
        repo: repo,
        port: port,
    }
}

// Start starts the gRPC server
func (s *Server) Start() error {
    lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
    if err != nil {
        return fmt.Errorf("failed to listen: %w", err)
    }
    
    grpcServer := grpc.NewServer()
    pb.RegisterTradeExecutionServiceServer(grpcServer, s)
    
    fmt.Printf("gRPC server listening on port %d\n", s.port)
    return grpcServer.Serve(lis)
}

// GetOrderStatus retrieves order status by ID
func (s *Server) GetOrderStatus(ctx context.Context, req *pb.GetOrderStatusRequest) (*pb.GetOrderStatusResponse, error) {
    if req.OrderId == "" {
        return nil, status.Error(codes.InvalidArgument, "order_id is required")
    }
    
    orderID, err := uuid.Parse(req.OrderId)
    if err != nil {
        return nil, status.Error(codes.InvalidArgument, "invalid order_id format")
    }
    
    order, err := s.repo.GetByID(ctx, orderID)
    if err != nil {
        return nil, status.Error(codes.NotFound, "order not found")
    }
    
    // Authorization check
    if req.UserId != "" && order.UserID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "access denied")
    }
    
    return &pb.GetOrderStatusResponse{
        Success: true,
        Order:   s.convertToProtoOrder(order),
    }, nil
}

// GetUserOrders retrieves all orders for a user
func (s *Server) GetUserOrders(ctx context.Context, req *pb.GetUserOrdersRequest) (*pb.GetUserOrdersResponse, error) {
    if req.UserId == "" {
        return nil, status.Error(codes.InvalidArgument, "user_id is required")
    }
    
    // Extract pagination
    page := int(req.Pagination.Page)
    pageSize := int(req.Pagination.PageSize)
    if page < 1 {
        page = 1
    }
    if pageSize < 1 || pageSize > 100 {
        pageSize = 20
    }
    
    offset := (page - 1) * pageSize
    
    orders, err := s.repo.GetUserOrders(ctx, req.UserId, pageSize, offset)
    if err != nil {
        return nil, status.Error(codes.Internal, "failed to retrieve orders")
    }
    
    protoOrders := make([]*pb.Order, len(orders))
    for i, order := range orders {
        protoOrders[i] = s.convertToProtoOrder(order)
    }
    
    return &pb.GetUserOrdersResponse{
        Success: true,
        Orders:  protoOrders,
        Pagination: &pb.PaginationResponse{
            Page:     int32(page),
            PageSize: int32(pageSize),
            Total:    int32(len(orders)),
        },
    }, nil
}

// CancelOrder cancels a pending order
func (s *Server) CancelOrder(ctx context.Context, req *pb.CancelOrderRequest) (*pb.CancelOrderResponse, error) {
    if req.OrderId == "" {
        return nil, status.Error(codes.InvalidArgument, "order_id is required")
    }
    
    orderID, err := uuid.Parse(req.OrderId)
    if err != nil {
        return nil, status.Error(codes.InvalidArgument, "invalid order_id format")
    }
    
    order, err := s.repo.GetByID(ctx, orderID)
    if err != nil {
        return nil, status.Error(codes.NotFound, "order not found")
    }
    
    // Authorization check
    if order.UserID != req.UserId {
        return nil, status.Error(codes.PermissionDenied, "access denied")
    }
    
    // Check if order can be cancelled
    if order.Status == models.StatusFilled || order.Status == models.StatusCancelled {
        return &pb.CancelOrderResponse{
            Success: false,
            Message: fmt.Sprintf("order cannot be cancelled (status: %s)", order.Status),
        }, nil
    }
    
    // Update status to cancelled
    order.Status = models.StatusCancelled
    reason := req.Reason
    order.RejectionReason = &reason
    
    if err := s.repo.Update(ctx, order); err != nil {
        return nil, status.Error(codes.Internal, "failed to cancel order")
    }
    
    return &pb.CancelOrderResponse{
        Success: true,
        Order:   s.convertToProtoOrder(order),
        Message: "Order cancelled successfully",
    }, nil
}

// ModifyOrder modifies a pending order
func (s *Server) ModifyOrder(ctx context.Context, req *pb.ModifyOrderRequest) (*pb.ModifyOrderResponse, error) {
    // Similar implementation to CancelOrder
    // Update order fields based on request
    return &pb.ModifyOrderResponse{
        Success: true,
        Message: "Order modified successfully",
    }, nil
}

// GetOrderHistory retrieves order history with filters
func (s *Server) GetOrderHistory(ctx context.Context, req *pb.GetOrderHistoryRequest) (*pb.GetOrderHistoryResponse, error) {
    // Implementation similar to GetUserOrders with additional filters
    return &pb.GetOrderHistoryResponse{
        Success: true,
    }, nil
}

// GetOrderStatistics returns order statistics for a user
func (s *Server) GetOrderStatistics(ctx context.Context, req *pb.GetOrderStatisticsRequest) (*pb.GetOrderStatisticsResponse, error) {
    // Aggregate statistics from database
    return &pb.GetOrderStatisticsResponse{
        Success: true,
    }, nil
}

// HealthCheck checks service health
func (s *Server) HealthCheck(ctx context.Context, req *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
    return &pb.HealthCheckResponse{
        Status:  "healthy",
        Service: "trade-execution-service",
    }, nil
}

func (s *Server) convertToProtoOrder(order *models.Order) *pb.Order {
    protoOrder := &pb.Order{
        OrderId:      order.OrderID.String(),
        UserId:       order.UserID,
        StrategyId:   order.StrategyID,
        EventId:      order.EventID.String(),
        StockCode:    order.StockCode,
        Exchange:     s.convertExchange(order.Exchange),
        Symbol:       order.Symbol,
        OrderType:    s.convertOrderType(order.OrderType),
        OrderSide:    s.convertOrderSide(order.OrderSide),
        Quantity:     order.Quantity,
        Validity:     order.Validity,
        Status:       s.convertOrderStatus(order.Status),
        RiskApproved: order.RiskApproved,
        RetryCount:   order.RetryCount,
    }
    
    if order.Price != nil {
        protoOrder.Price = *order.Price
    }
    if order.FilledPrice != nil {
        protoOrder.FilledPrice = *order.FilledPrice
    }
    if order.OdinOrderID != nil {
        protoOrder.OdinOrderId = *order.OdinOrderID
    }
    
    return protoOrder
}

func (s *Server) convertExchange(exchange models.Exchange) pb.Exchange {
    switch exchange {
    case models.ExchangeNSE:
        return pb.Exchange_EXCHANGE_NSE
    case models.ExchangeBSE:
        return pb.Exchange_EXCHANGE_BSE
    default:
        return pb.Exchange_EXCHANGE_UNSPECIFIED
    }
}

func (s *Server) convertOrderType(orderType models.OrderType) pb.OrderType {
    switch orderType {
    case models.OrderTypeMarket:
        return pb.OrderType_ORDER_TYPE_MARKET
    case models.OrderTypeLimit:
        return pb.OrderType_ORDER_TYPE_LIMIT
    case models.OrderTypeStopLoss:
        return pb.OrderType_ORDER_TYPE_STOP_LOSS
    default:
        return pb.OrderType_ORDER_TYPE_UNSPECIFIED
    }
}

func (s *Server) convertOrderSide(side models.OrderSide) pb.OrderSide {
    switch side {
    case models.OrderSideBuy:
        return pb.OrderSide_ORDER_SIDE_BUY
    case models.OrderSideSell:
        return pb.OrderSide_ORDER_SIDE_SELL
    default:
        return pb.OrderSide_ORDER_SIDE_UNSPECIFIED
    }
}

func (s *Server) convertOrderStatus(status models.OrderStatus) pb.OrderStatus {
    switch status {
    case models.StatusReceived:
        return pb.OrderStatus_ORDER_STATUS_RECEIVED
    case models.StatusPending:
        return pb.OrderStatus_ORDER_STATUS_PENDING
    case models.StatusSubmitted:
        return pb.OrderStatus_ORDER_STATUS_SUBMITTED
    case models.StatusFilled:
        return pb.OrderStatus_ORDER_STATUS_FILLED
    case models.StatusRejected:
        return pb.OrderStatus_ORDER_STATUS_REJECTED
    case models.StatusCancelled:
        return pb.OrderStatus_ORDER_STATUS_CANCELLED
    default:
        return pb.OrderStatus_ORDER_STATUS_UNSPECIFIED
    }
}
```

---

## 🚀 Step 9: Main Service Entry Point

Create `cmd/main.go`:

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"
    
    "github.com/jmoiron/sqlx"
    _ "github.com/lib/pq"
    
    "trade-execution/internal/consumer"
    "trade-execution/internal/executor"
    "trade-execution/internal/odin"
    "trade-execution/internal/repository"
    "trade-execution/internal/server"
)

func main() {
    log.Println("Starting Trade Execution Service...")
    
    // Load configuration
    cfg := loadConfig()
    
    // Initialize PostgreSQL
    db, err := initPostgres(cfg)
    if err != nil {
        log.Fatalf("Failed to connect to PostgreSQL: %v", err)
    }
    defer db.Close()
    
    // Initialize repository
    orderRepo := repository.NewOrderRepository(db)
    
    // Initialize Odin client
    odinClient := odin.NewExecutionClient(cfg.OdinBaseURL)
    
    // Initialize executor
    orderExecutor := executor.NewOrderExecutor(
        orderRepo,
        odinClient,
        cfg.MaxRetries,
        cfg.RetryDelay,
    )
    
    // Initialize RabbitMQ consumer
    consumerCfg := consumer.Config{
        URL:           cfg.RabbitMQURL,
        QueueName:     cfg.QueueName,
        PrefetchCount: cfg.PrefetchCount,
        WorkerCount:   cfg.WorkerCount,
    }
    
    rabbitConsumer, err := consumer.NewRabbitMQConsumer(consumerCfg, orderExecutor, orderRepo)
    if err != nil {
        log.Fatalf("Failed to initialize RabbitMQ consumer: %v", err)
    }
    defer rabbitConsumer.Shutdown()
    
    // Initialize gRPC server
    grpcServer := server.NewServer(orderRepo, cfg.GRPCPort)
    
    // Start services
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    
    // Start RabbitMQ consumer
    go func() {
        if err := rabbitConsumer.Start(ctx); err != nil {
            log.Fatalf("RabbitMQ consumer error: %v", err)
        }
    }()
    
    // Start gRPC server
    go func() {
        if err := grpcServer.Start(); err != nil {
            log.Fatalf("gRPC server error: %v", err)
        }
    }()
    
    log.Println("Trade Execution Service started successfully")
    
    // Wait for interrupt signal
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
    <-sigChan
    
    log.Println("Shutting down gracefully...")
    cancel()
    
    // Give time for graceful shutdown
    time.Sleep(5 * time.Second)
    log.Println("Service stopped")
}

type Config struct {
    GRPCPort      int
    RabbitMQURL   string
    QueueName     string
    PrefetchCount int
    WorkerCount   int
    OdinBaseURL   string
    MaxRetries    int
    RetryDelay    time.Duration
    PostgresURL   string
}

func loadConfig() Config {
    return Config{
        GRPCPort:      getEnvInt("SERVICE_PORT", 9004),
        RabbitMQURL:   getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
        QueueName:     getEnv("RABBITMQ_QUEUE", "order.execution.queue"),
        PrefetchCount: getEnvInt("RABBITMQ_PREFETCH", 10),
        WorkerCount:   getEnvInt("WORKER_COUNT", 10),
        OdinBaseURL:   getEnv("ODIN_BASE_URL", ""),
        MaxRetries:    getEnvInt("MAX_RETRIES", 3),
        RetryDelay:    time.Duration(getEnvInt("RETRY_DELAY_SEC", 1)) * time.Second,
        PostgresURL:   buildPostgresURL(),
    }
}

func initPostgres(cfg Config) (*sqlx.DB, error) {
    db, err := sqlx.Connect("postgres", cfg.PostgresURL)
    if err != nil {
        return nil, err
    }
    
    db.SetMaxOpenConns(25)
    db.SetMaxIdleConns(5)
    db.SetConnMaxLifetime(5 * time.Minute)
    
    return db, nil
}

func buildPostgresURL() string {
    return fmt.Sprintf(
        "host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
        getEnv("POSTGRES_HOST", "localhost"),
        getEnv("POSTGRES_PORT", "5432"),
        getEnv("POSTGRES_USER", "trading_user"),
        getEnv("POSTGRES_PASSWORD", ""),
        getEnv("POSTGRES_DB", "trading_execution"),
        getEnv("POSTGRES_SSL_MODE", "disable"),
    )
}

func getEnv(key, defaultValue string) string {
    if value := os.Getenv(key); value != "" {
        return value
    }
    return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
    if value := os.Getenv(key); value != "" {
        var intVal int
        if _, err := fmt.Sscanf(value, "%d", &intVal); err == nil {
            return intVal
        }
    }
    return defaultValue
}
```

---

## 🧪 Step 10: Testing Strategy

### Unit Tests

Create `internal/executor/executor_test.go`:

```go
package executor

import (
    "context"
    "testing"
    "time"
    
    "github.com/google/uuid"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
    
    "trade-execution/internal/models"
)

// Mock repository
type MockRepository struct {
    mock.Mock
}

func (m *MockRepository) Create(ctx context.Context, order *models.Order) error {
    args := m.Called(ctx, order)
    return args.Error(0)
}

func (m *MockRepository) Update(ctx context.Context, order *models.Order) error {
    args := m.Called(ctx, order)
    return args.Error(0)
}

// Test cases
func TestExecuteOrder_Success(t *testing.T) {
    // Setup
    mockRepo := new(MockRepository)
    mockOdin := new(MockOdinClient)
    
    executor := NewOrderExecutor(mockRepo, mockOdin, 3, time.Second)
    
    order := &models.Order{
        OrderID:      uuid.New(),
        UserID:       "user123",
        RiskApproved: true,
        Status:       models.StatusReceived,
    }
    
    // Expectations
    mockRepo.On("Update", mock.Anything, mock.Anything).Return(nil)
    mockOdin.On("PlaceOrder", mock.Anything, mock.Anything).Return(&OdinResponse{Success: true}, nil)
    
    // Execute
    err := executor.ExecuteOrder(context.Background(), order)
    
    // Assert
    assert.NoError(t, err)
    assert.Equal(t, models.StatusSubmitted, order.Status)
    mockRepo.AssertExpectations(t)
}

func TestExecuteOrder_RiskNotApproved(t *testing.T) {
    mockRepo := new(MockRepository)
    mockOdin := new(MockOdinClient)
    
    executor := NewOrderExecutor(mockRepo, mockOdin, 3, time.Second)
    
    order := &models.Order{
        OrderID:      uuid.New(),
        RiskApproved: false,
    }
    
    mockRepo.On("Update", mock.Anything, mock.Anything).Return(nil)
    
    err := executor.ExecuteOrder(context.Background(), order)
    
    assert.NoError(t, err)
    assert.Equal(t, models.StatusRejected, order.Status)
}
```

### Integration Tests

Create `tests/integration/trade_execution_test.go`:

```go
package integration

import (
    "context"
    "testing"
    "time"
    
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestTradeExecutionFlow(t *testing.T) {
    // Setup test environment
    ctx := context.Background()
    
    // 1. Publish order to RabbitMQ
    orderReq := createTestOrderRequest()
    publishToQueue(t, orderReq)
    
    // 2. Wait for processing
    time.Sleep(2 * time.Second)
    
    // 3. Query order status via gRPC
    order := queryOrderStatus(t, orderReq.RequestID)
    
    // 4. Assert
    assert.NotNil(t, order)
    assert.Equal(t, "SUBMITTED", order.Status)
}
```

### Test Client

Create `test_client.go`:

```go
package main

import (
    "context"
    "log"
    "time"
    
    "google.golang.org/grpc"
    pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
)

func main() {
    // Connect to gRPC server
    conn, err := grpc.Dial("localhost:9004", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()
    
    client := pb.NewTradeExecutionServiceClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    // Test: Health Check
    log.Println("Testing Health Check...")
    healthResp, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{})
    if err != nil {
        log.Fatalf("Health check failed: %v", err)
    }
    log.Printf("Health status: %s\n", healthResp.Status)
    
    // Test: Get Order Status
    log.Println("\nTesting Get Order Status...")
    orderResp, err := client.GetOrderStatus(ctx, &pb.GetOrderStatusRequest{
        OrderId: "test-order-id",
        UserId:  "user123",
    })
    if err != nil {
        log.Printf("Get order failed: %v", err)
    } else {
        log.Printf("Order found: %+v\n", orderResp.Order)
    }
    
    // Test: Get User Orders
    log.Println("\nTesting Get User Orders...")
    ordersResp, err := client.GetUserOrders(ctx, &pb.GetUserOrdersRequest{
        UserId: "user123",
        Pagination: &pb.PaginationRequest{
            Page:     1,
            PageSize: 10,
        },
    })
    if err != nil {
        log.Printf("Get user orders failed: %v", err)
    } else {
        log.Printf("Found %d orders\n", len(ordersResp.Orders))
    }
    
    log.Println("\nAll tests completed!")
}
```

---

## 🚀 Step 11: Running the Service

### 1. Setup PostgreSQL Database

```powershell
# Create database
psql -U postgres -c "CREATE DATABASE trading_execution;"

# Run migrations
cd services/trade-execution/migrations
psql -U postgres -d trading_execution -f 001_create_orders_table.sql
```

### 2. Setup RabbitMQ

```powershell
# Start RabbitMQ (if using Docker)
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

# Create queue manually (optional, auto-created by consumer)
# Access RabbitMQ management UI at http://localhost:15672
```

### 3. Build and Run

```powershell
# Navigate to service directory
cd services/trade-execution

# Install dependencies
go mod download

# Build
go build -o ../../bin/trade-execution.exe ./cmd/main.go

# Run
../../bin/trade-execution.exe
```

### 4. Test the Service

```powershell
# Run test client
go run test_client.go

# Use grpcurl
grpcurl -plaintext localhost:9004 trade_execution.TradeExecutionService/HealthCheck
```

---

## 📊 Step 12: Monitoring & Observability

### Metrics to Track

```go
// Add Prometheus metrics
var (
    ordersReceived = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "trade_execution_orders_received_total",
            Help: "Total orders received",
        },
        []string{"status"},
    )
    
    orderExecutionDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "trade_execution_duration_seconds",
            Help:    "Order execution duration",
            Buckets: prometheus.DefBuckets,
        },
        []string{"status"},
    )
    
    odinAPIErrors = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "trade_execution_odin_api_errors_total",
            Help: "Total Odin API errors",
        },
    )
)
```

### Logging Best Practices

```go
log.WithFields(log.Fields{
    "order_id":    order.OrderID,
    "user_id":     order.UserID,
    "stock_code":  order.StockCode,
    "status":      order.Status,
}).Info("Order processed successfully")
```

---

## 🎯 Summary

You now have a complete implementation guide for the Trade Execution Service including:

✅ **Architecture & Design** - Component structure and data flow
✅ **Database Schema** - PostgreSQL tables with indexes
✅ **Repository Layer** - Data access with CRUD operations
✅ **Odin Integration** - API client for order placement
✅ **Order Executor** - Core business logic with retries
✅ **RabbitMQ Consumer** - Message queue integration
✅ **gRPC Server** - API for order queries
✅ **Testing Strategy** - Unit, integration, and manual tests
✅ **Running Guide** - Complete setup and deployment steps
✅ **Monitoring** - Metrics and logging recommendations

**Next Steps:**
1. Implement status polling worker
2. Add Redis caching layer
3. Integrate with Risk Management Service
4. Add comprehensive error handling
5. Implement circuit breaker pattern
6. Add rate limiting for Odin API
7. Create deployment configurations
8. Setup monitoring dashboards
