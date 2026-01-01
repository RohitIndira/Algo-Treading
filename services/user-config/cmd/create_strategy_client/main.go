package main

import (
    "context"
    "flag"
    "fmt"
    "os"
    "time"

    userconfigpb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
    common "github.com/RohitIndira/Algo-Treading/api/proto/common"
    "google.golang.org/grpc"
)

func main() {
    addr := flag.String("addr", "localhost:50051", "user-config gRPC address")
    flag.Parse()

    // Dial gRPC
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    conn, err := grpc.DialContext(ctx, *addr, grpc.WithInsecure(), grpc.WithBlock())
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to dial user-config: %v\n", err)
        os.Exit(1)
    }
    defer conn.Close()

    client := userconfigpb.NewUserConfigServiceClient(conn)

    // Build a depth-only strategy request
    req := &userconfigpb.CreateStrategyRequest{
        UserId:       "demo-user",
        StrategyName: "DemoDepthBuy",
        Description:  "Demo depth-based BUY strategy",
        ActivateImmediately: true,
        Conditions: &userconfigpb.StrategyConditions{
            StockCodes: []int64{12345},
            Exchanges:  []common.Exchange{common.Exchange_EXCHANGE_NSE},
            MatchAllNews: false,
            ImpactScoreThreshold: 1,
            MinBidQuantity: 1000,
            MinAskQuantity: 500,
            MaxSpreadPct: 0.2,
            DepthOnly: true,
            RequireLtpBetweenSpread: true,
        },
        TradeConfig: &userconfigpb.TradeConfig{
            OrderType: common.OrderType_ORDER_TYPE_MARKET,
            Quantity: 10,
            Exchange: common.Exchange_EXCHANGE_NSE,
            OrderSide: common.OrderSide_ORDER_SIDE_BUY,
            Validity: "DAY",
        },
        RiskLimits: &userconfigpb.RiskLimits{
            PositionSizing: common.PositionSizing_POSITION_SIZING_FIXED,
            EnableRiskChecks: false,
        },
    }

    // Call CreateStrategy
    ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel2()

    resp, err := client.CreateStrategy(ctx2, req)
    if err != nil {
        fmt.Fprintf(os.Stderr, "CreateStrategy call failed: %v\n", err)
        os.Exit(1)
    }

    if !resp.Success {
        fmt.Fprintf(os.Stderr, "CreateStrategy failed: %v\n", resp.Error)
        os.Exit(1)
    }

    fmt.Printf("Strategy created: ID=%s, name=%s\n", resp.Strategy.StrategyId, resp.Strategy.StrategyName)
}
