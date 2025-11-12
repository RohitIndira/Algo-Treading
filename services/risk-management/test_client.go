package main

import (
	"context"
	"fmt"
	"log"
	"time"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/risk_management"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to the risk management service
	conn, err := grpc.Dial("localhost:9005", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewRiskManagementServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("=== Risk Management Service Test Client ===\n")

	// 1. Health Check
	fmt.Println("1. Testing Health Check...")
	healthResp, err := client.HealthCheck(ctx, &common.HealthCheckRequest{})
	if err != nil {
		log.Printf("Health check failed: %v\n", err)
	} else {
		fmt.Printf("   ✓ Service: %s\n", healthResp.Service)
		fmt.Printf("   ✓ Healthy: %v\n", healthResp.Healthy)
		fmt.Printf("   ✓ Version: %s\n\n", healthResp.Version)
	}

	// 2. Set Risk Limits for a user
	fmt.Println("2. Setting Risk Limits for user 'test-user-123'...")
	setLimitsResp, err := client.SetRiskLimits(ctx, &pb.SetRiskLimitsRequest{
		Limits: &pb.CustomRiskLimits{
			UserId:          "test-user-123",
			MaxDailyTrades:  func(i int32) *int32 { return &i }(10),
			MaxDailyLoss:    func(f float64) *float64 { return &f }(5000.0),
			MaxPositionSize: func(f float64) *float64 { return &f }(50000.0),
			MaxPerTradeRisk: func(f float64) *float64 { return &f }(1000.0),
		},
	})
	if err != nil {
		log.Printf("Set limits failed: %v\n", err)
	} else if setLimitsResp.Success {
		fmt.Printf("   ✓ Risk limits set successfully\n")
		fmt.Printf("   - Max Daily Trades: %d\n", *setLimitsResp.Limits.MaxDailyTrades)
		fmt.Printf("   - Max Daily Loss: ₹%.2f\n", *setLimitsResp.Limits.MaxDailyLoss)
		fmt.Printf("   - Max Position Size: ₹%.2f\n\n", *setLimitsResp.Limits.MaxPositionSize)
	}

	// 3. Perform Pre-Trade Risk Check (Valid Trade)
	fmt.Println("3. Pre-Trade Risk Check - Valid Trade...")
	preTradeResp, err := client.CheckPreTradeRisk(ctx, &pb.PreTradeRiskRequest{
		UserId:          "test-user-123",
		StrategyId:      "momentum-strategy-1",
		StockCode:       3456, // Reliance Industries example
		Exchange:        common.Exchange_EXCHANGE_NSE,
		OrderType:       common.OrderType_ORDER_TYPE_LIMIT,
		OrderSide:       common.OrderSide_ORDER_SIDE_BUY,
		Quantity:        10,
		Price:           2500.50,
		StopLoss:        2450.00,
		TakeProfit:      2600.00,
		MaxDailyTrades:  10,
		MaxLossPerDay:   5000.0,
		MaxPositionSize: 50000.0,
		MaxPerTradeRisk: 1000.0,
	})
	if err != nil {
		log.Printf("Pre-trade check failed: %v\n", err)
	} else {
		fmt.Printf("   Approval: %v\n", preTradeResp.Approved)
		fmt.Printf("   Risk Score: %.2f/100\n", preTradeResp.RiskScore)
		if len(preTradeResp.Violations) > 0 {
			fmt.Println("   ✗ Violations:")
			for _, v := range preTradeResp.Violations {
				fmt.Printf("     - %s: %s\n", v.Type, v.Message)
			}
		} else {
			fmt.Println("   ✓ No violations")
		}
		if len(preTradeResp.Suggestions) > 0 {
			fmt.Println("   Suggestions:")
			for _, s := range preTradeResp.Suggestions {
				fmt.Printf("     - %s\n", s)
			}
		}
		fmt.Println()
	}

	// 4. Simulate a trade execution by updating post-trade metrics
	if preTradeResp != nil && preTradeResp.Approved {
		fmt.Println("4. Updating Post-Trade Metrics (Simulating Order Fill)...")
		postTradeResp, err := client.UpdatePostTradeMetrics(ctx, &pb.PostTradeMetricsRequest{
			UserId:         "test-user-123",
			OrderId:        "ORD-20251112-001",
			StockCode:      3456,
			Exchange:       common.Exchange_EXCHANGE_NSE,
			OrderSide:      common.OrderSide_ORDER_SIDE_BUY,
			FilledQuantity: 10,
			FilledPrice:    2500.50,
			Commission:     15.50,
			RealizedPnl:    0.0,
			ExecutedAt:     &common.Timestamp{Seconds: time.Now().Unix()},
		})
		if err != nil {
			log.Printf("Post-trade update failed: %v\n", err)
		} else if postTradeResp.Success {
			fmt.Printf("   ✓ Trade metrics updated: %s\n\n", postTradeResp.Message)
		}
	}

	// 5. Get Risk Metrics
	fmt.Println("5. Getting Current Risk Metrics...")
	metricsResp, err := client.GetRiskMetrics(ctx, &pb.GetRiskMetricsRequest{
		UserId: "test-user-123",
	})
	if err != nil {
		log.Printf("Get metrics failed: %v\n", err)
	} else if metricsResp.Success {
		m := metricsResp.Metrics
		fmt.Printf("   User: %s\n", m.UserId)
		fmt.Printf("   Daily Stats:\n")
		fmt.Printf("     - Trades: %d\n", m.DailyTradeCount)
		fmt.Printf("     - Net P&L: ₹%.2f\n", m.DailyNetPnl)
		fmt.Printf("     - Profit: ₹%.2f\n", m.DailyProfit)
		fmt.Printf("     - Loss: ₹%.2f\n", m.DailyLoss)
		fmt.Printf("   Portfolio:\n")
		fmt.Printf("     - Open Positions: %d\n", m.OpenPositionsCount)
		fmt.Printf("     - Invested Value: ₹%.2f\n", m.TotalInvestedValue)
		fmt.Printf("     - Current Value: ₹%.2f\n", m.TotalCurrentValue)
		fmt.Printf("     - Unrealized P&L: ₹%.2f\n", m.TotalUnrealizedPnl)
		fmt.Println()
	}

	// 6. Get User Positions
	fmt.Println("6. Getting User Positions...")
	positionsResp, err := client.GetUserPositions(ctx, &pb.GetUserPositionsRequest{
		UserId:        "test-user-123",
		IncludeClosed: false,
	})
	if err != nil {
		log.Printf("Get positions failed: %v\n", err)
	} else if positionsResp.Success {
		if len(positionsResp.Positions) == 0 {
			fmt.Println("   No open positions")
		} else {
			for i, pos := range positionsResp.Positions {
				fmt.Printf("   Position %d:\n", i+1)
				fmt.Printf("     - Stock: %s (%d)\n", pos.Symbol, pos.StockCode)
				fmt.Printf("     - Quantity: %d\n", pos.Quantity)
				fmt.Printf("     - Avg Price: ₹%.2f\n", pos.AveragePrice)
				fmt.Printf("     - Current Price: ₹%.2f\n", pos.CurrentPrice)
				fmt.Printf("     - Unrealized P&L: ₹%.2f (%.2f%%)\n", pos.UnrealizedPnl, pos.UnrealizedPnlPct)
			}
		}
		fmt.Println()
	}

	// 7. Test a risk violation (exceeding position size)
	fmt.Println("7. Pre-Trade Risk Check - Should Violate Position Size...")
	violationResp, err := client.CheckPreTradeRisk(ctx, &pb.PreTradeRiskRequest{
		UserId:          "test-user-123",
		StrategyId:      "momentum-strategy-1",
		StockCode:       3456,
		Exchange:        common.Exchange_EXCHANGE_NSE,
		OrderType:       common.OrderType_ORDER_TYPE_LIMIT,
		OrderSide:       common.OrderSide_ORDER_SIDE_BUY,
		Quantity:        100, // Large quantity
		Price:           2500.50,
		StopLoss:        2450.00,
		TakeProfit:      2600.00,
		MaxDailyTrades:  10,
		MaxLossPerDay:   5000.0,
		MaxPositionSize: 50000.0, // Will be violated
		MaxPerTradeRisk: 1000.0,
	})
	if err != nil {
		log.Printf("Pre-trade check failed: %v\n", err)
	} else {
		fmt.Printf("   Approval: %v\n", violationResp.Approved)
		fmt.Printf("   Risk Score: %.2f/100\n", violationResp.RiskScore)
		if len(violationResp.Violations) > 0 {
			fmt.Println("   ✗ Violations Detected:")
			for _, v := range violationResp.Violations {
				fmt.Printf("     - %s: %s (Current: %.2f, Limit: %.2f)\n",
					v.Type, v.Message, v.CurrentValue, v.LimitValue)
			}
		}
		fmt.Println()
	}

	fmt.Println("=== Test Complete ===")
}
