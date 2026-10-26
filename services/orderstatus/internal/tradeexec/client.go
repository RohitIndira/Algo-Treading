// Package tradeexec is orderstatus svc's thin gRPC client for trade-execution,
// used by the broadcast listener in own_orders mode to answer one question:
// "is this broker order id one of OURS (a row in manthan_orders)?"
//
// Why gRPC and not a local guess: the per-user REST order-book poll stores
// every order the client placed, manual ones included, so broker_events alone
// cannot tell our orders from a client's own. trade-execution's
// LookupOrderMeta is the only authoritative answer (it is what positions svc
// already uses for origin=USER_MANUAL). Fail closed: when trade-execution is
// unreachable the frame is NOT written in own_orders mode (counted instead);
// the REST poll remains the safety net exactly as today.
package tradeexec

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/trade_execution"
)

// Client answers IsOurOrder via trade-execution with a small cache:
// positive answers are kept for the process lifetime (an order never stops
// being ours), negative answers for negativeTTL (an AMO id may be adopted
// a little after its first frame).
type Client struct {
	conn *grpc.ClientConn
	rpc  pb.TradeExecutionServiceClient

	mu          sync.Mutex
	positive    map[string]struct{}
	negative    map[string]time.Time
	negativeTTL time.Duration
}

// New dials trade-execution gRPC at addr (non-blocking dial; the first RPC
// surfaces connectivity errors).
func New(addr string) (*Client, error) {
	if addr == "" {
		return nil, fmt.Errorf("trade-execution gRPC address is required")
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial trade-execution %s: %w", addr, err)
	}
	c := newWithRPC(pb.NewTradeExecutionServiceClient(conn))
	c.conn = conn
	return c, nil
}

// newWithRPC builds a Client over an existing RPC stub (tests).
func newWithRPC(rpc pb.TradeExecutionServiceClient) *Client {
	return &Client{
		rpc:         rpc,
		positive:    map[string]struct{}{},
		negative:    map[string]time.Time{},
		negativeTTL: 60 * time.Second,
	}
}

// Close shuts the gRPC connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// IsOurOrder reports whether brokerOrderID is a Manthan-placed order
// (found in manthan_orders). Errors mean "could not decide" — callers in
// own_orders mode treat that as not ours.
func (c *Client) IsOurOrder(ctx context.Context, brokerOrderID string) (bool, error) {
	if brokerOrderID == "" {
		return false, nil
	}
	now := time.Now()
	c.mu.Lock()
	if _, ok := c.positive[brokerOrderID]; ok {
		c.mu.Unlock()
		return true, nil
	}
	c.evictExpiredLocked(now)
	if _, ok := c.negative[brokerOrderID]; ok {
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()

	rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := c.rpc.LookupOrderMeta(rpcCtx, &pb.LookupOrderMetaRequest{BrokerOrderId: brokerOrderID})
	if err != nil {
		return false, fmt.Errorf("LookupOrderMeta %s: %w", brokerOrderID, err)
	}
	if resp == nil {
		return false, fmt.Errorf("LookupOrderMeta %s: empty response", brokerOrderID)
	}
	// A service-level error is "could not decide", never "not ours": no
	// negative cache, so the next frame asks again.
	if e := resp.GetError(); e != nil {
		return false, fmt.Errorf("LookupOrderMeta %s: %s %s", brokerOrderID, e.GetCode(), e.GetMessage())
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if resp.Found {
		c.positive[brokerOrderID] = struct{}{}
		delete(c.negative, brokerOrderID)
		return true, nil
	}
	c.negative[brokerOrderID] = now.Add(c.negativeTTL)
	return false, nil
}

// evictExpiredLocked drops negative entries past their TTL. Called with c.mu held.
func (c *Client) evictExpiredLocked(now time.Time) {
	for id, until := range c.negative {
		if !now.Before(until) {
			delete(c.negative, id)
		}
	}
}
