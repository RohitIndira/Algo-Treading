package server

import (
	"context"
	"fmt"
	"log"

	proto "github.com/RohitIndira/Algo-Treading/api/proto/indira_wrapper"
	"github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/indira-wrapper/internal/wss"
)

// GRPCServer implements IndiraWrapperService.
// It forwards unary calls to pkg/indira HTTP client and streams WSS/poll events.
// Errors are embedded in responses; gRPC error is nil unless stream send fails.
type GRPCServer struct {
	proto.UnimplementedIndiraWrapperServiceServer
	indira    *indira.Client
	wssClient *wss.Client
}

func NewGRPCServer(indiraClient *indira.Client, wssClient *wss.Client) *GRPCServer {
	return &GRPCServer{indira: indiraClient, wssClient: wssClient}
}

func buildAuth(userID, appID, token, source string) *indira.AuthContext {
	if source == "" {
		source = "WEB"
	}
	return &indira.AuthContext{UserId: userID, AppId: appID, Source: source, BearerToken: token}
}

func (s *GRPCServer) PlaceOrder(ctx context.Context, req *proto.PlaceOrderRequest) (*proto.PlaceOrderResponse, error) {
	auth := buildAuth(req.UserId, req.AppId, req.Token, req.Source)

	indiraReq := &indira.PlaceOrderRequest{
		Symbol:       req.Symbol,
		ExcToken:     fmtInt64(req.ExcToken),
		Exc:          req.Exc,
		OrdAction:    req.OrdAction,
		OrdValidity:  req.OrdValidity,
		OrdType:      req.OrdType,
		PrdType:      req.PrdType,
		LimitPrice:   req.LimitPrice,
		TriggerPrice: req.TriggerPrice,
		Qty:          int(req.Qty),
		DisQty:       0,
		LotSize:      1,
		Instrument:   req.Instrument,
		Amo:          req.Amo,
	}

	resp, err := s.indira.PlaceOrder(ctx, auth, indiraReq)
	if err != nil {
		log.Printf("[PlaceOrder] http error: %v", err)
		return &proto.PlaceOrderResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	brokerID := resp.OrdId
	if brokerID == "" {
		brokerID = resp.OrderId
	}

	// pkg/indira does not model infoID/infoMsg; we only know call succeeded if err==nil.
	return &proto.PlaceOrderResponse{
		BrokerOrderId: brokerID,
		OrdStatus:     firstNonEmpty(resp.OrdStatus, resp.Status),
		RejReason:     resp.RejReason,
		Success:       true,
		ErrorMessage:  "",
	}, nil
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

func (s *GRPCServer) CancelOrder(ctx context.Context, req *proto.CancelOrderRequest) (*proto.CancelOrderResponse, error) {
	auth := buildAuth(req.UserId, req.AppId, req.Token, req.Source)
	indiraReq := &indira.CancelOrderRequest{Symbol: req.Symbol, Exc: req.Exc, OrdId: req.BrokerOrderId}
	if err := s.indira.CancelOrder(ctx, auth, indiraReq); err != nil {
		return &proto.CancelOrderResponse{BrokerOrderId: req.BrokerOrderId, Success: false, ErrorMessage: err.Error()}, nil
	}
	return &proto.CancelOrderResponse{BrokerOrderId: req.BrokerOrderId, Success: true}, nil
}

func (s *GRPCServer) ModifyOrder(ctx context.Context, req *proto.ModifyOrderRequest) (*proto.ModifyOrderResponse, error) {
	auth := buildAuth(req.UserId, req.AppId, req.Token, req.Source)
	indiraReq := &indira.ModifyOrderRequest{
		OrdId:         req.BrokerOrderId,
		Symbol:        req.Symbol,
		OrdAction:     req.OrdAction,
		OrdValidity:   req.OrdValidity,
		ExchangeToken: req.ExchangeToken,
		Exc:           req.Exc,
		Qty:           int(req.Qty),
		TradedQty:     int(req.TradedQty),
		LimitPrice:    req.LimitPrice,
		TriggerPrice:  req.TriggerPrice,
		OrdType:       req.OrdType,
		PrdType:       req.PrdType,
		Instrument:    req.Instrument,
		LotSize:       1,
		DisQty:        0,
		OffMktFlag:    false,
	}
	if err := s.indira.ModifyOrder(ctx, auth, indiraReq); err != nil {
		return &proto.ModifyOrderResponse{BrokerOrderId: req.BrokerOrderId, Success: false, ErrorMessage: err.Error()}, nil
	}
	return &proto.ModifyOrderResponse{BrokerOrderId: req.BrokerOrderId, OrdStatus: "", Success: true}, nil
}

func (s *GRPCServer) QueryOrder(ctx context.Context, req *proto.QueryOrderRequest) (*proto.QueryOrderResponse, error) {
	auth := buildAuth(req.UserId, req.AppId, req.Token, req.Source)
	orders, err := s.indira.GetOrderBook(ctx, auth)
	if err != nil {
		return &proto.QueryOrderResponse{BrokerOrderId: req.BrokerOrderId, Success: false, ErrorMessage: err.Error()}, nil
	}
	for _, o := range orders {
		if o.OrdId == req.BrokerOrderId {
			remain := int32(o.Qty - o.TradedQty)
			return &proto.QueryOrderResponse{
				BrokerOrderId: o.OrdId,
				Status:        o.Status,
				Qty:           int32(o.Qty),
				TradedQty:     int32(o.TradedQty),
				RemainQty:     remain,
				Price:         o.LimitPrice,
				RejReason:     "",
				Found:         true,
				Success:       true,
			}, nil
		}
	}
	return &proto.QueryOrderResponse{BrokerOrderId: req.BrokerOrderId, Found: false, Success: true}, nil
}

func (s *GRPCServer) GetPositions(ctx context.Context, req *proto.GetPositionsRequest) (*proto.GetPositionsResponse, error) {
	auth := buildAuth(req.UserId, req.AppId, req.Token, req.Source)
	pos, err := s.indira.GetPositions(ctx, auth)
	if err != nil {
		return &proto.GetPositionsResponse{Success: false, ErrorMessage: err.Error()}, nil
	}

	out := make([]*proto.Position, 0, len(pos))
	for _, p := range pos {
		out = append(out, &proto.Position{
			Symbol:    p.Symbol,
			DispSym:   p.Symbol,
			PrdType:   p.PrdType,
			Type:      "",
			Ltp:       p.CurrentPrice,
			NetPnl:    p.PnL,
			NetQty:    int32(p.NetQty),
			BuyAvg:    p.BuyAvgPrice,
			SellAvg:   p.SellAvgPrice,
			SquareOff: false,
		})
	}

	return &proto.GetPositionsResponse{Positions: out, Success: true}, nil
}

func (s *GRPCServer) GetFundLimits(ctx context.Context, req *proto.GetFundLimitsRequest) (*proto.GetFundLimitsResponse, error) {
	auth := buildAuth(req.UserId, req.AppId, req.Token, req.Source)
	fl, err := s.indira.GetFundLimit(ctx, auth)
	if err != nil {
		return &proto.GetFundLimitsResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &proto.GetFundLimitsResponse{
		AvailableMargin: fl.AvailableMargin,
		UtilizedMargin:  fl.UsedMargin,
		Deposit:         fl.TotalBalance,
		Success:         true,
	}, nil
}

func (s *GRPCServer) SubscribeOrderEvents(req *proto.SubscribeOrderEventsRequest, stream proto.IndiraWrapperService_SubscribeOrderEventsServer) error {
	log.Printf("[SubscribeOrderEvents] client connected")
	ch := s.wssClient.EventCh()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case evt, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(evt); err != nil {
				return err
			}
		}
	}
}

func (s *GRPCServer) GetWSSStatus(ctx context.Context, _ *proto.WSSStatusRequest) (*proto.WSSStatusResponse, error) {
	connected, statusMsg, lastEventNs, eventsRx := s.wssClient.Stats()
	_ = ctx
	return &proto.WSSStatusResponse{Connected: connected, StatusMessage: statusMsg, LastEventNs: lastEventNs, EventsReceived: eventsRx}, nil
}

func fmtInt64(v int64) string {
	// pkg/indira expects ExcToken as string
	return fmt.Sprintf("%d", v)
}
