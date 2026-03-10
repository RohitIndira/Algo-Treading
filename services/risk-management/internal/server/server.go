package server

import (
	"context"
	"fmt"
	"net"

	common "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/risk_management"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/checker"
	"github.com/RohitIndira/Algo-Treading/services/risk-management/internal/repository"
	"google.golang.org/grpc"
)

type RiskManagementServer struct {
	pb.UnimplementedRiskManagementServiceServer
	preTradeChecker *checker.PreTradeChecker
	redisRepo       *repository.RedisRepository
}

func NewRiskManagementServer(redisRepo *repository.RedisRepository) *RiskManagementServer {
	return &RiskManagementServer{preTradeChecker: checker.NewPreTradeChecker(redisRepo), redisRepo: redisRepo}
}

func (s *RiskManagementServer) CheckPreTradeRisk(ctx context.Context, req *pb.PreTradeRiskRequest) (*pb.PreTradeRiskResponse, error) {
	return s.preTradeChecker.CheckPreTrade(ctx, req)
}

func (s *RiskManagementServer) UpdatePostTradeMetrics(ctx context.Context, req *pb.PostTradeMetricsRequest) (*pb.PostTradeMetricsResponse, error) {
	if err := s.redisRepo.IncrementDailyTradeCount(ctx, req.UserId, req.OrderId); err != nil {
		return &pb.PostTradeMetricsResponse{Success: false, Error: &common.Error{Code: "INTERNAL_ERROR", Message: fmt.Sprintf("Failed to update metrics: %v", err)}}, nil
	}
	// update position (best-effort)
	s.redisRepo.SetPosition(ctx, req.UserId, req.StockCode, int(req.FilledQuantity), req.FilledPrice)
	return &pb.PostTradeMetricsResponse{Success: true}, nil
}

func (s *RiskManagementServer) GetRiskMetrics(ctx context.Context, req *pb.GetRiskMetricsRequest) (*pb.GetRiskMetricsResponse, error) {
	// minimal implementation placeholder
	return &pb.GetRiskMetricsResponse{Success: true, Metrics: &pb.RiskMetrics{}}, nil
}

func (s *RiskManagementServer) SetRiskLimits(ctx context.Context, req *pb.SetRiskLimitsRequest) (*pb.SetRiskLimitsResponse, error) {
	// placeholder: persist to DB in a real implementation
	return &pb.SetRiskLimitsResponse{Success: true, Limits: req.Limits}, nil
}

func (s *RiskManagementServer) ResetDailyCounters(ctx context.Context, req *pb.ResetDailyCountersRequest) (*pb.ResetDailyCountersResponse, error) {
	if req.UserId == "" {
		return &pb.ResetDailyCountersResponse{Success: false, UsersReset: 0}, nil
	}
	if err := s.redisRepo.ResetDailyCounters(ctx, req.UserId); err != nil {
		return &pb.ResetDailyCountersResponse{Success: false, UsersReset: 0}, nil
	}
	return &pb.ResetDailyCountersResponse{Success: true, UsersReset: 1}, nil
}

func (s *RiskManagementServer) HealthCheck(ctx context.Context, req *common.HealthCheckRequest) (*common.HealthCheckResponse, error) {
	return &common.HealthCheckResponse{Healthy: true, Service: "RiskManagementService", Version: "0.1.0"}, nil
}

func (s *RiskManagementServer) Start(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	srv := grpc.NewServer()
	pb.RegisterRiskManagementServiceServer(srv, s)
	fmt.Printf("Risk Management Server listening on :%s\n", port)
	return srv.Serve(listener)
}
