// Package server — gRPC surface for portfolio svc. Projects the Store
// structs into wire envelopes per api/proto/portfolio/portfolio.proto.
//
// Kept THIN: no business logic, no LTP fetch, no auth. The Store owns
// the SQL. api-gateway owns auth. LTP enrichment happens in api-gateway
// after this service's response returns.
package server

import (
	"context"

	commonpb "github.com/RohitIndira/Algo-Treading/api/proto/common"
	pb "github.com/RohitIndira/Algo-Treading/api/proto/portfolio"
	"github.com/RohitIndira/Algo-Treading/services/portfolio/internal/store"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements pb.PortfolioServiceServer.
type Server struct {
	pb.UnimplementedPortfolioServiceServer
	store  *store.Store
	logger *zap.Logger
}

// New wires the gRPC server. main.go registers this via
// pb.RegisterPortfolioServiceServer.
func New(s *store.Store, logger *zap.Logger) *Server {
	return &Server{store: s, logger: logger}
}

// ------------------------------------------------------------------------------
// GetPortfolioSummary
// ------------------------------------------------------------------------------

func (s *Server) GetPortfolioSummary(ctx context.Context, req *pb.GetPortfolioSummaryRequest) (*pb.GetPortfolioSummaryResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	sum, err := s.store.SummaryFor(ctx, req.GetUserId())
	if err != nil {
		s.logger.Warn("SummaryFor failed",
			zap.String("user_id", req.GetUserId()), zap.Error(err))
		return &pb.GetPortfolioSummaryResponse{
			Success: false,
			Error: &commonpb.Error{
				Code:    "INTERNAL",
				Message: err.Error(),
			},
		}, nil
	}

	return &pb.GetPortfolioSummaryResponse{
		Success: true,
		Summary: &pb.Summary{
			TotalInvested:            sum.TotalInvested,
			TotalRealizedPnlLifetime: sum.TotalRealizedPnLLifetime,
			TodayRealizedPnl:         sum.TodayRealizedPnL,
			ActiveLotCount:           int32(sum.ActiveLotCount),
			ClosedLotCount:           int32(sum.ClosedLotCount),
			ManthanInvested:          sum.ManthanInvested,
			UserManualInvested:       sum.UserManualInvested,
		},
	}, nil
}

// ------------------------------------------------------------------------------
// GetActivePositions
// ------------------------------------------------------------------------------

func (s *Server) GetActivePositions(ctx context.Context, req *pb.GetActivePositionsRequest) (*pb.GetActivePositionsResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	lots, err := s.store.ActiveLotsFor(ctx, req.GetUserId())
	if err != nil {
		s.logger.Warn("ActiveLotsFor failed",
			zap.String("user_id", req.GetUserId()), zap.Error(err))
		return &pb.GetActivePositionsResponse{
			Success: false,
			Error: &commonpb.Error{
				Code:    "INTERNAL",
				Message: err.Error(),
			},
		}, nil
	}

	out := make([]*pb.ActivePosition, 0, len(lots))
	for _, p := range lots {
		out = append(out, &pb.ActivePosition{
			PositionId:      p.PositionID.String(),
			Origin:          p.Origin,
			Symbol:          p.Symbol,
			Exchange:        p.Exchange,
			StrategyId:      p.StrategyID,
			SignalId:        p.SignalID,
			EntryTimeMs:     p.EntryTime.UnixMilli(),
			EntryPrice:      p.EntryPrice,
			Quantity:        int32(p.Quantity),
			InvestedAmount:  p.InvestedAmount,
			CurrentSl:       p.CurrentSL,
			HighSinceEntry:  p.HighSinceEntry,
		})
	}
	return &pb.GetActivePositionsResponse{
		Success:   true,
		Positions: out,
	}, nil
}

// ------------------------------------------------------------------------------
// GetClosedPositions
// ------------------------------------------------------------------------------

func (s *Server) GetClosedPositions(ctx context.Context, req *pb.GetClosedPositionsRequest) (*pb.GetClosedPositionsResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}

	// Coercion happens in the store — passing raw pb values through is safe.
	rows, total, err := s.store.ClosedLotsPaged(ctx,
		req.GetUserId(),
		int(req.GetPage()),
		int(req.GetPageSize()),
	)
	if err != nil {
		s.logger.Warn("ClosedLotsPaged failed",
			zap.String("user_id", req.GetUserId()), zap.Error(err))
		return &pb.GetClosedPositionsResponse{
			Success: false,
			Error: &commonpb.Error{
				Code:    "INTERNAL",
				Message: err.Error(),
			},
		}, nil
	}

	out := make([]*pb.ClosedPosition, 0, len(rows))
	for _, p := range rows {
		out = append(out, &pb.ClosedPosition{
			PositionId:     p.PositionID.String(),
			Origin:         p.Origin,
			Symbol:         p.Symbol,
			EntryTimeMs:    p.EntryTime.UnixMilli(),
			EntryPrice:     p.EntryPrice,
			Quantity:       int32(p.Quantity),
			ExitTimeMs:     p.ExitTime.UnixMilli(),
			ExitPrice:      p.ExitPrice,
			ExitReason:     p.ExitReason,
			RealizedPnl:    p.RealizedPnL,
			InvestedAmount: p.InvestedAmount,
		})
	}
	return &pb.GetClosedPositionsResponse{
		Success:    true,
		Positions:  out,
		TotalCount: int32(total),
	}, nil
}

// ------------------------------------------------------------------------------
// HealthCheck
// ------------------------------------------------------------------------------

func (s *Server) HealthCheck(_ context.Context, _ *commonpb.HealthCheckRequest) (*commonpb.HealthCheckResponse, error) {
	return &commonpb.HealthCheckResponse{
		Healthy: true,
		Service: "portfolio-service",
		Version: "1.0.0",
	}, nil
}
