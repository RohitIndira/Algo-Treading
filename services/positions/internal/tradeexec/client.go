package tradeexec

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
)

// OrderMeta is the domain shape positions svc consumes — mirrors
// the fields we care about from trade-execution's LookupOrderMetaResponse.
//
// Found=false is a legitimate "not a Manthan order" answer, not an error.
// Positions svc treats those as USER_MANUAL origin per §7.1 of the design doc.
type OrderMeta struct {
	Found              bool
	SignalID           string
	OrderType          string
	StrategyID         string
	UserID             string
	EntrySignalID      string
	EntryBrokerOrderID string
}

// Client is positions svc's gRPC client for trade-execution.LookupOrderMeta.
// Wraps in a bounded-TTL cache so repeat lookups (state machine hot path) don't
// hammer trade-exec — see cache.go.
type Client struct {
	conn   *grpc.ClientConn
	rpc    pb.TradeExecutionServiceClient
	cache  *Cache
	logger *zap.Logger
}

// New dials trade-execution at addr with a 10s timeout. Same fast-fail pattern
// as rebalancer / rules-engine / orderstatus svc's user-config client — better
// to crash-loop with a visible error than to silently start with a broken
// dependency.
//
// TODO(mTLS): swap insecure.NewCredentials() when service-mesh certs land.
func New(ctx context.Context, addr string, cache *Cache, logger *zap.Logger) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("trade-execution gRPC address is required")
	}

	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial trade-execution %s: %w", addr, err)
	}

	return &Client{
		conn:   conn,
		rpc:    pb.NewTradeExecutionServiceClient(conn),
		cache:  cache,
		logger: logger,
	}, nil
}

// Close shuts down the gRPC connection + stops the cache sweeper.
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if c.cache != nil {
		c.cache.Stop()
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// LookupOrderMeta resolves broker_order_id → OrderMeta, using cache first.
//
// Returns:
//
//	(meta{Found:true},  nil) — Manthan order, all fields populated
//	(meta{Found:false}, nil) — not a Manthan order (positions svc treats as USER_MANUAL)
//	(zero, err)             — RPC / network failure, caller decides retry
//
// Cache TTLs per §6 of docs/positions_service_design.md — see cache.go.
func (c *Client) LookupOrderMeta(ctx context.Context, brokerOrderID string) (OrderMeta, error) {
	if brokerOrderID == "" {
		return OrderMeta{}, fmt.Errorf("brokerOrderID is required")
	}

	// Cache lookup
	if c.cache != nil {
		if meta, ok := c.cache.Get(brokerOrderID); ok {
			return meta, nil
		}
	}

	// Cache miss — RPC
	resp, err := c.rpc.LookupOrderMeta(ctx, &pb.LookupOrderMetaRequest{
		BrokerOrderId: brokerOrderID,
	})
	if err != nil {
		return OrderMeta{}, fmt.Errorf("LookupOrderMeta RPC: %w", err)
	}

	// Bubble up server-side error (DB unreachable etc). NOT_FOUND uses
	// found=false with no error — treated as a legitimate answer below.
	if resp.GetError() != nil && resp.GetError().Code != "" {
		return OrderMeta{}, fmt.Errorf("LookupOrderMeta server error: %s %s",
			resp.GetError().Code, resp.GetError().Message)
	}

	meta := OrderMeta{
		Found:              resp.GetFound(),
		SignalID:           resp.GetSignalId(),
		OrderType:          resp.GetOrderType(),
		StrategyID:         resp.GetStrategyId(),
		UserID:             resp.GetUserId(),
		EntrySignalID:      resp.GetEntrySignalId(),
		EntryBrokerOrderID: resp.GetEntryBrokerOrderId(),
	}

	// Cache both found and not-found results — different TTLs handled inside Put.
	if c.cache != nil {
		c.cache.Put(brokerOrderID, meta)
	}

	return meta, nil
}

// GetBrokerHoldings fetches the user's Indira delivery holdings map from
// trade-execution's BrokerAdapter (freeQty-safe: delivery NetQty + holdings.Qty).
// No caching — the reconciler polls at ~5 min intervals, cache would just
// serve staler data. Called by the reconciler ticker; not on the hot path.
//
// Returns:
//
//	(map, nil)         — user has holdings (may be empty map = zero delivery)
//	(nil, err)         — RPC failure OR broker fetch failure OR auth lookup failure.
//	                     Reconciler must SKIP this user's sweep (do NOT publish
//	                     false BROKER_ONLY drifts on transient failure).
func (c *Client) GetBrokerHoldings(ctx context.Context, userID string) (map[string]int, error) {
	if userID == "" {
		return nil, fmt.Errorf("userID is required")
	}

	resp, err := c.rpc.GetBrokerHoldings(ctx, &pb.GetBrokerHoldingsRequest{
		UserId: userID,
	})
	if err != nil {
		return nil, fmt.Errorf("GetBrokerHoldings RPC: %w", err)
	}
	if !resp.GetSuccess() {
		e := resp.GetError()
		return nil, fmt.Errorf("GetBrokerHoldings server error: %s %s", e.GetCode(), e.GetMessage())
	}

	// Convert int32 wire → int for the reconciler (which speaks native int).
	out := make(map[string]int, len(resp.GetHoldings()))
	for sym, qty := range resp.GetHoldings() {
		out[sym] = int(qty)
	}
	return out, nil
}
