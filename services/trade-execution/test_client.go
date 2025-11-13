package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonpb "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
)

func main() {
	log.Println("========================================")
	log.Println("Trade Execution Service - Test Client")
	log.Println("========================================")

	// Connect to gRPC server
	log.Println("Connecting to gRPC server at localhost:9004...")
	conn, err := grpc.Dial("localhost:9004", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	log.Println("✓ Connected")

	client := pb.NewTradeExecutionServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test 1: Health Check
	log.Println("\n--- Test 1: Health Check ---")
	healthResp, err := client.HealthCheck(ctx, &commonpb.HealthCheckRequest{})
	if err != nil {
		log.Printf("✗ Health check failed: %v", err)
	} else {
		log.Printf("✓ Health check passed")
		log.Printf("  Healthy: %v", healthResp.Healthy)
		log.Printf("  Service: %s", healthResp.Service)
		log.Printf("  Version: %s", healthResp.Version)
	}

	// Test 2: Get Order Status (will fail if no orders exist)
	log.Println("\n--- Test 2: Get Order Status ---")
	orderResp, err := client.GetOrderStatus(ctx, &pb.GetOrderStatusRequest{
		OrderId: "123e4567-e89b-12d3-a456-426614174000", // Random UUID
		UserId:  "test-user-123",
	})
	if err != nil {
		log.Printf("✗ Get order status failed (expected if no order exists): %v", err)
	} else {
		if orderResp.Success {
			log.Printf("✓ Order found: %+v", orderResp.Order)
		} else {
			log.Printf("✓ Order not found (expected): %s", orderResp.Error.Message)
		}
	}

	// Test 3: Get User Orders
	log.Println("\n--- Test 3: Get User Orders ---")
	ordersResp, err := client.GetUserOrders(ctx, &pb.GetUserOrdersRequest{
		UserId: "test-user-123",
		Pagination: &commonpb.PaginationRequest{
			Page:     1,
			PageSize: 10,
		},
	})
	if err != nil {
		log.Printf("✗ Get user orders failed: %v", err)
	} else {
		log.Printf("✓ Found %d orders", len(ordersResp.Orders))
		if len(ordersResp.Orders) > 0 {
			log.Printf("  First order ID: %s", ordersResp.Orders[0].OrderId)
			log.Printf("  Status: %s", ordersResp.Orders[0].Status)
		}
	}

	// Test 4: Get Order History
	log.Println("\n--- Test 4: Get Order History ---")
	historyResp, err := client.GetOrderHistory(ctx, &pb.GetOrderHistoryRequest{
		UserId: "test-user-123",
		Pagination: &commonpb.PaginationRequest{
			Page:     1,
			PageSize: 20,
		},
		IncludeCancelled: true,
	})
	if err != nil {
		log.Printf("✗ Get order history failed: %v", err)
	} else {
		log.Printf("✓ Found %d historical orders", len(historyResp.Orders))
	}

	// Test 5: Get Order Statistics
	log.Println("\n--- Test 5: Get Order Statistics ---")
	statsResp, err := client.GetOrderStatistics(ctx, &pb.GetOrderStatisticsRequest{
		UserId: "test-user-123",
	})
	if err != nil {
		log.Printf("✗ Get order statistics failed: %v", err)
	} else {
		log.Printf("✓ Statistics retrieved")
		log.Printf("  User ID: %s", statsResp.Statistics.UserId)
	}

	// Test 6: Cancel Order (will fail if order doesn't exist or can't be cancelled)
	log.Println("\n--- Test 6: Cancel Order ---")
	cancelResp, err := client.CancelOrder(ctx, &pb.CancelOrderRequest{
		OrderId: "123e4567-e89b-12d3-a456-426614174000", // Random UUID
		UserId:  "test-user-123",
		Reason:  "Test cancellation",
	})
	if err != nil {
		log.Printf("✗ Cancel order failed (expected if no order exists): %v", err)
	} else {
		if cancelResp.Success {
			log.Printf("✓ Order cancelled: %s", cancelResp.Message)
		} else {
			log.Printf("✓ Cancel failed (expected): %s", cancelResp.Message)
		}
	}

	log.Println("\n========================================")
	log.Println("All tests completed!")
	log.Println("========================================")
	log.Println("\nNote: Some tests may fail if no orders exist in the database.")
	log.Println("To create test orders, publish a message to RabbitMQ queue:")
	log.Println("  Queue: order.execution.queue")
	log.Println("\nExample order message:")
	log.Println(`{
  "request_id": "req-123",
  "user_id": "test-user-123",
  "strategy_id": "test-strategy-1",
  "event_id": "event-456",
  "stock_code": 517170,
  "exchange": "BSE",
  "symbol": "EDVENSWA",
  "order_type": "MARKET",
  "order_side": "BUY",
  "quantity": 10,
  "risk_approved": true,
  "risk_score": 3.5,
  "timestamp": "2025-11-13T10:00:00Z"
}`)
}
