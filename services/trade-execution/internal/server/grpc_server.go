package server

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	commonpb "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
)

// Server implements the TradeExecutionService gRPC server
type Server struct {
	pb.UnimplementedTradeExecutionServiceServer
	repo     repository.OrderRepository
	executor *executor.OrderExecutor
	port     int
}

// NewServer creates a new gRPC server
func NewServer(repo repository.OrderRepository, exec *executor.OrderExecutor, port int) *Server {
	return &Server{
		repo:     repo,
		executor: exec,
		port:     port,
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

	// Register reflection service for grpcurl
	reflection.Register(grpcServer)

	log.Printf("gRPC server listening on port %d", s.port)
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
		log.Printf("Failed to get order %s: %v", orderID, err)
		return &pb.GetOrderStatusResponse{
			Success: false,
			Error: &commonpb.Error{
				Code:    "NOT_FOUND",
				Message: "Order not found",
			},
		}, nil
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
		log.Printf("Failed to get user orders: %v", err)
		return &pb.GetUserOrdersResponse{
			Success: false,
			Error: &commonpb.Error{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to retrieve orders",
			},
		}, nil
	}

	protoOrders := make([]*pb.Order, len(orders))
	for i, order := range orders {
		protoOrders[i] = s.convertToProtoOrder(order)
	}

	return &pb.GetUserOrdersResponse{
		Success: true,
		Orders:  protoOrders,
		Pagination: &commonpb.PaginationResponse{
			Page:       int32(page),
			PageSize:   int32(pageSize),
			TotalItems: int64(len(orders)),
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
		return &pb.CancelOrderResponse{
			Success: false,
			Message: "Order not found",
		}, nil
	}

	// Authorization check
	if order.UserID != req.UserId {
		return nil, status.Error(codes.PermissionDenied, "access denied")
	}

	// Cancel order
	reason := req.Reason
	if reason == "" {
		reason = "User requested cancellation"
	}

	if err := s.executor.CancelOrder(ctx, order, reason); err != nil {
		log.Printf("Failed to cancel order %s: %v", orderID, err)
		return &pb.CancelOrderResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}

	// Reload order to get updated status
	order, _ = s.repo.GetByID(ctx, orderID)

	return &pb.CancelOrderResponse{
		Success: true,
		Order:   s.convertToProtoOrder(order),
		Message: "Order cancelled successfully",
	}, nil
}

// ModifyOrder modifies a pending order (placeholder implementation)
func (s *Server) ModifyOrder(ctx context.Context, req *pb.ModifyOrderRequest) (*pb.ModifyOrderResponse, error) {
	// TODO: Implement order modification logic
	return &pb.ModifyOrderResponse{
		Success: false,
		Message: "Order modification not yet implemented",
	}, nil
}

// GetOrderHistory retrieves order history with filters
func (s *Server) GetOrderHistory(ctx context.Context, req *pb.GetOrderHistoryRequest) (*pb.GetOrderHistoryResponse, error) {
	if req.UserId == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	// For now, use the same implementation as GetUserOrders
	// TODO: Add filtering logic based on req.Filter
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
		return &pb.GetOrderHistoryResponse{
			Success: false,
			Error: &commonpb.Error{
				Code:    "INTERNAL_ERROR",
				Message: "Failed to retrieve order history",
			},
		}, nil
	}

	protoOrders := make([]*pb.Order, len(orders))
	for i, order := range orders {
		protoOrders[i] = s.convertToProtoOrder(order)
	}

	return &pb.GetOrderHistoryResponse{
		Success: true,
		Orders:  protoOrders,
		Pagination: &commonpb.PaginationResponse{
			Page:       int32(page),
			PageSize:   int32(pageSize),
			TotalItems: int64(len(orders)),
		},
	}, nil
}

// GetOrderStatistics returns order statistics for a user (placeholder)
func (s *Server) GetOrderStatistics(ctx context.Context, req *pb.GetOrderStatisticsRequest) (*pb.GetOrderStatisticsResponse, error) {
	// TODO: Implement statistics aggregation
	return &pb.GetOrderStatisticsResponse{
		Success: true,
		Statistics: &pb.OrderStatistics{
			UserId: req.UserId,
		},
	}, nil
}

// HealthCheck checks service health
func (s *Server) HealthCheck(ctx context.Context, req *commonpb.HealthCheckRequest) (*commonpb.HealthCheckResponse, error) {
	return &commonpb.HealthCheckResponse{
		Healthy: true,
		Service: "trade-execution-service",
		Version: "1.0.0",
	}, nil
}

func (s *Server) convertToProtoOrder(order *models.Order) *pb.Order {
	protoOrder := &pb.Order{
		OrderId:        order.OrderID.String(),
		UserId:         order.UserID,
		StrategyId:     order.StrategyID,
		EventId:        order.EventID.String(),
		StockCode:      order.StockCode,
		Exchange:       s.convertExchange(order.Exchange),
		Symbol:         order.Symbol,
		OrderType:      s.convertOrderType(order.OrderType),
		OrderSide:      s.convertOrderSide(order.OrderSide),
		Quantity:       order.Quantity,
		Validity:       order.Validity,
		Status:         s.convertOrderStatus(order.Status),
		FilledQuantity: order.FilledQuantity,
	}

	if order.Price != nil {
		protoOrder.Price = *order.Price
	}
	if order.FilledPrice != nil {
		protoOrder.FilledPrice = *order.FilledPrice
	}
	if order.IndiraOrderID != nil {
		protoOrder.IndiraOrderId = *order.IndiraOrderID
	}
	if order.Commission != nil {
		protoOrder.Commission = *order.Commission
	}
	if order.TotalCost != nil {
		protoOrder.TotalCost = *order.TotalCost
	}

	// Convert timestamps
	protoOrder.CreatedAt = &commonpb.Timestamp{
		Seconds: order.CreatedAt.Unix(),
		Nanos:   int32(order.CreatedAt.Nanosecond()),
	}
	protoOrder.UpdatedAt = &commonpb.Timestamp{
		Seconds: order.UpdatedAt.Unix(),
		Nanos:   int32(order.UpdatedAt.Nanosecond()),
	}

	return protoOrder
}

func (s *Server) convertExchange(exchange models.Exchange) commonpb.Exchange {
	switch exchange {
	case models.ExchangeNSE:
		return commonpb.Exchange_EXCHANGE_NSE
	case models.ExchangeBSE:
		return commonpb.Exchange_EXCHANGE_BSE
	default:
		return commonpb.Exchange_EXCHANGE_UNSPECIFIED
	}
}

func (s *Server) convertOrderType(orderType models.OrderType) commonpb.OrderType {
	switch orderType {
	case models.OrderTypeMarket:
		return commonpb.OrderType_ORDER_TYPE_MARKET
	case models.OrderTypeLimit:
		return commonpb.OrderType_ORDER_TYPE_LIMIT
	case models.OrderTypeStopLoss:
		return commonpb.OrderType_ORDER_TYPE_STOP_LOSS
	default:
		return commonpb.OrderType_ORDER_TYPE_UNSPECIFIED
	}
}

func (s *Server) convertOrderSide(side models.OrderSide) commonpb.OrderSide {
	switch side {
	case models.OrderSideBuy:
		return commonpb.OrderSide_ORDER_SIDE_BUY
	case models.OrderSideSell:
		return commonpb.OrderSide_ORDER_SIDE_SELL
	default:
		return commonpb.OrderSide_ORDER_SIDE_UNSPECIFIED
	}
}

func (s *Server) convertOrderStatus(status models.OrderStatus) commonpb.OrderStatus {
	switch status {
	case models.StatusReceived:
		return commonpb.OrderStatus_ORDER_STATUS_RECEIVED
	case models.StatusPending:
		return commonpb.OrderStatus_ORDER_STATUS_PENDING
	case models.StatusSubmitted:
		return commonpb.OrderStatus_ORDER_STATUS_SUBMITTED
	case models.StatusPartiallyFilled:
		return commonpb.OrderStatus_ORDER_STATUS_PARTIAL_FILLED
	case models.StatusFilled:
		return commonpb.OrderStatus_ORDER_STATUS_FILLED
	case models.StatusRejected:
		return commonpb.OrderStatus_ORDER_STATUS_REJECTED
	case models.StatusCancelled:
		return commonpb.OrderStatus_ORDER_STATUS_CANCELLED
	case models.StatusFailed:
		return commonpb.OrderStatus_ORDER_STATUS_FAILED
	default:
		return commonpb.OrderStatus_ORDER_STATUS_UNSPECIFIED
	}
}
