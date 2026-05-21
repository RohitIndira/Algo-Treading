// Package grpcserver implements the HFTService gRPC interface defined in
// api/proto/hft_engine/hft_engine.proto.
//
// Phase 1: Ping is real; Entry/Exit/GetState route to manager (which
// is itself a stub). StreamState and SubmitFills are declared but
// return Unimplemented — they're filled in Phase 5/6.
//
// Concurrency: gRPC's server runtime spawns one goroutine per RPC. All
// handlers must be safe to call from multiple goroutines — they are,
// because they only touch the Manager (which holds its own mutex).
package grpcserver

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/hft_engine"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/manager"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// Version is set at build time via -ldflags; runtime fallback is "dev".
var Version = "dev"

// Server implements pb.HFTServiceServer.
type Server struct {
	pb.UnimplementedHFTServiceServer

	mgr       *manager.Manager
	logger    *zap.Logger
	startedAt time.Time
}

// New wires the server. mgr is the strategy registry; logger is named.
func New(mgr *manager.Manager, logger *zap.Logger) *Server {
	return &Server{
		mgr:       mgr,
		logger:    logger.Named("grpc"),
		startedAt: time.Now(),
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Ping — proof of life. Returns version + uptime.
// ─────────────────────────────────────────────────────────────────────────

func (s *Server) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Pong:          true,
		Version:       Version,
		UptimeSeconds: int64(time.Since(s.startedAt).Seconds()),
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────
// Entry — start running a strategy.
// ─────────────────────────────────────────────────────────────────────────

func (s *Server) Entry(ctx context.Context, req *pb.EntryRequest) (*pb.EntryResponse, error) {
	if req.GetStrategyId() == "" {
		return nil, status.Error(codes.InvalidArgument, "strategy_id required")
	}

	err := s.mgr.Start(ctx, req.GetStrategyId(), req.GetSide(), int(req.GetLots()))
	switch {
	case err == nil:
		return &pb.EntryResponse{
			Success:    true,
			Status:     "RUNNING",
			StrategyId: req.GetStrategyId(),
		}, nil
	case errors.Is(err, manager.ErrAlreadyRunning):
		return &pb.EntryResponse{
			Success:    false,
			Status:     "ALREADY_RUNNING",
			Error:      err.Error(),
			StrategyId: req.GetStrategyId(),
		}, nil
	default:
		s.logger.Error("Entry failed", zap.String("strategy_id", req.GetStrategyId()), zap.Error(err))
		return &pb.EntryResponse{
			Success:    false,
			Status:     "ERROR",
			Error:      err.Error(),
			StrategyId: req.GetStrategyId(),
		}, nil
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Exit — halt a running strategy.
// ─────────────────────────────────────────────────────────────────────────

func (s *Server) Exit(ctx context.Context, req *pb.ExitRequest) (*pb.ExitResponse, error) {
	if req.GetStrategyId() == "" {
		return nil, status.Error(codes.InvalidArgument, "strategy_id required")
	}

	err := s.mgr.Stop(ctx, req.GetStrategyId())
	switch {
	case err == nil:
		return &pb.ExitResponse{
			Success:    true,
			Status:     "STOPPED",
			StrategyId: req.GetStrategyId(),
		}, nil
	case errors.Is(err, manager.ErrNotRunning):
		return &pb.ExitResponse{
			Success:    false,
			Status:     "NOT_RUNNING",
			Error:      err.Error(),
			StrategyId: req.GetStrategyId(),
		}, nil
	default:
		s.logger.Error("Exit failed", zap.String("strategy_id", req.GetStrategyId()), zap.Error(err))
		return &pb.ExitResponse{
			Success:    false,
			Status:     "ERROR",
			Error:      err.Error(),
			StrategyId: req.GetStrategyId(),
		}, nil
	}
}

// ─────────────────────────────────────────────────────────────────────────
// GetState — snapshot for the dashboard.
// ─────────────────────────────────────────────────────────────────────────

func (s *Server) GetState(ctx context.Context, req *pb.GetStateRequest) (*pb.GetStateResponse, error) {
	if req.GetStrategyId() == "" {
		return nil, status.Error(codes.InvalidArgument, "strategy_id required")
	}
	st := s.mgr.Get(req.GetStrategyId())
	if st == nil {
		return &pb.GetStateResponse{
			Success: false,
			Error:   "strategy not running",
		}, nil
	}
	return &pb.GetStateResponse{
		Success:  true,
		Snapshot: snapshotFromState(st),
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────
// StreamState — Phase 6. Returns Unimplemented for now.
// ─────────────────────────────────────────────────────────────────────────

func (s *Server) StreamState(req *pb.StreamStateRequest, stream pb.HFTService_StreamStateServer) error {
	return status.Error(codes.Unimplemented, "StreamState: Phase 6")
}

// ─────────────────────────────────────────────────────────────────────────
// SubmitFills — Phase 5. Returns Unimplemented for now.
// ─────────────────────────────────────────────────────────────────────────

func (s *Server) SubmitFills(stream pb.HFTService_SubmitFillsServer) error {
	return status.Error(codes.Unimplemented, "SubmitFills: Phase 5")
}

// ─────────────────────────────────────────────────────────────────────────
// snapshotFromState converts our internal *state.Strategy into the
// proto StateSnapshot shape the frontend understands.
// ─────────────────────────────────────────────────────────────────────────

func snapshotFromState(s *state.Strategy) *pb.StateSnapshot {
	if s == nil {
		return nil
	}
	return &pb.StateSnapshot{
		StrategyId:     s.Cfg.StrategyID,
		UserId:         s.Cfg.UserID,
		Symbol:         s.Cfg.Symbol,
		Active:         s.Active,
		Status:         s.Status(),
		Mode:           string(s.Cfg.Mode),
		StartedAtUnix:  s.StartedAt.Unix(),
		LastTickAtUnix: s.LastTickAt.Unix(),
		LastBid:        s.LastMD.Bid,
		LastAsk:        s.LastMD.Ask,
		Buy:            sideSnapshot(&s.Buy, s.Cfg.MaxBuyQty),
		Sell:           sideSnapshot(&s.Sell, s.Cfg.MaxSellQty),
	}
}

func sideSnapshot(side *state.SideState, maxQty int) *pb.SideSnapshot {
	out := &pb.SideSnapshot{
		Position:   int32(side.Position),
		MaxQty:     int32(maxQty),
		Done:       side.Done,
		HaltReason: string(side.HaltReason),
		Armed:      side.Armed,
		History:    make([]*pb.ChunkSnapshot, 0, len(side.History)),
	}
	if side.Current != nil {
		out.Current = chunkSnapshot(side.Current)
	}
	for i := range side.History {
		out.History = append(out.History, chunkSnapshot(&side.History[i]))
	}
	return out
}

func chunkSnapshot(c *state.ChunkState) *pb.ChunkSnapshot {
	return &pb.ChunkSnapshot{
		Seq:           int32(c.Seq),
		Qty:           int32(c.Qty),
		Filled:        int32(c.Filled),
		LimitPrice:    c.LimitPrice,
		BrokerOrderId: c.BrokerOrderID,
		Status:        string(c.Status),
		ModifyCount:   int32(c.ModifyCount),
		PlacedAtUnix:  c.PlacedAt.Unix(),
	}
}
