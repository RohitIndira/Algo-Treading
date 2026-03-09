package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	proto "github.com/RohitIndira/Algo-Treading/api/proto/indira_wrapper"
	"github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/indira-wrapper/config"
	"github.com/RohitIndira/Algo-Treading/services/indira-wrapper/internal/server"
	"github.com/RohitIndira/Algo-Treading/services/indira-wrapper/internal/wss"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	indiraClient := indira.NewClient(indira.Config{BaseURL: cfg.IndiraBaseURL, Timeout: cfg.HTTPTimeout})
	log.Printf("Indira HTTP client ready base_url=%s timeout=%v", cfg.IndiraBaseURL, cfg.HTTPTimeout)

	pollAuth := buildPollAuth()
	pollFn := func(pctx context.Context) ([]wss.PollOrder, error) {
		if pollAuth == nil {
			return nil, nil
		}
		orders, err := indiraClient.GetOrderBook(pctx, pollAuth)
		if err != nil {
			return nil, err
		}
		out := make([]wss.PollOrder, 0, len(orders))
		for _, o := range orders {
			out = append(out, wss.PollOrder{
				OrdId:     o.OrdId,
				Status:    o.Status,
				TradedQty: int32(o.TradedQty),
				Price:     o.LimitPrice,
				RejReason: "",
			})
		}
		return out, nil
	}

	wssClient := wss.NewClient(wss.Config{
		WSSURL:         cfg.IndiraWSSURL,
		InitialBackoff: cfg.WSSInitialBackoff,
		MaxBackoff:     cfg.WSSMaxBackoff,
		EventBufSize:   cfg.WSSEventBufSize,
		PollFn:         pollFn,
	})
	go wssClient.Start(ctx)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalf("FATAL: listen :%s: %v", cfg.GRPCPort, err)
	}

	grpcSrv := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 5 * time.Minute, Time: 1 * time.Minute, Timeout: 20 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 30 * time.Second, PermitWithoutStream: true}),
	)

	proto.RegisterIndiraWrapperServiceServer(grpcSrv, server.NewGRPCServer(indiraClient, wssClient))

	go func() {
		log.Printf("Indira Wrapper gRPC server listening on :%s", cfg.GRPCPort)
		if err := grpcSrv.Serve(lis); err != nil {
			log.Fatalf("gRPC serve error: %v", err)
		}
	}()

	log.Println("Indira Wrapper is LIVE")
	<-ctx.Done()
	log.Println("Shutdown signal — stopping")
	grpcSrv.GracefulStop()
	log.Println("Shutdown complete")
}

func buildPollAuth() *indira.AuthContext {
	userID := os.Getenv("INDIRA_POLL_USER_ID")
	appID := os.Getenv("INDIRA_POLL_APP_ID")
	token := os.Getenv("INDIRA_POLL_TOKEN")
	if userID == "" || appID == "" || token == "" {
		log.Printf("[WSS-POLL] polling disabled (missing INDIRA_POLL_USER_ID/APP_ID/TOKEN)")
		return nil
	}
	return &indira.AuthContext{UserId: userID, AppId: appID, Source: "WEB", BearerToken: token}
}
