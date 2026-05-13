// IndiraBroker — production implementation of the Broker interface against
// the Indira Securities REST API. Wraps pkg/indira and adds three pieces of
// HFT-specific behaviour on top of what trade-execution's broker_adapter.go
// does for Manthan:
//
//   1. SymbolSpec resolution lives upstream (the strategy.Runner is created
//      with a fully-populated SymbolSpec — IndiraSymbol + ExchangeToken).
//      We don't reach into Redis here.
//
//   2. AU004 (auth-expired) is handled INLINE with one refresh + retry,
//      matching the Manthan SLHandler pattern. After that single retry,
//      we surface ErrAuthExpired so the strategy can HALT cleanly.
//
//   3. Every call is short-context-bound (3 s) so a stuck broker can't
//      pin a tick loop. The Indira HTTP client has its own 30 s default;
//      we tighten it locally because HFT can't afford to wait.
//
// Concurrency: each method is safe to call from many goroutines because
// pkg/indira.Client is itself stateless + uses an HTTP/2 connection pool.
package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	indira "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// ErrAuthExpired is returned by IndiraBroker when the broker rejects an
// order due to session expiry AND a single refresh-retry also failed.
// The strategy.Runner is expected to halt with HaltAuthExpired on this.
var ErrAuthExpired = errors.New("broker: session expired, refresh did not recover")

// AuthRefresher is the callback IndiraBroker uses to re-fetch credentials
// from the user_credentials table. Returns the freshest broker creds or an
// error if the user has no usable creds (e.g. never logged in).
//
// IndiraBroker calls this at most ONCE per failing request, then either
// retries with the new auth (and returns success/normal error) or fails
// with ErrAuthExpired.
type AuthRefresher func(ctx context.Context, userID string) (*AuthContext, error)

// IndiraBroker satisfies broker.Broker against pkg/indira.
type IndiraBroker struct {
	client    *indira.Client
	refresh   AuthRefresher
	logger    *zap.Logger
	callTimeout time.Duration // per HTTP call

	// authCache lets the AU004 refresh path mutate the caller's auth in
	// place so subsequent calls use the new JWT without the strategy
	// having to plumb it through. Keyed by user_id.
	authMu    sync.RWMutex
	authCache map[string]*AuthContext
}

// IndiraConfig tunes the broker.
type IndiraConfig struct {
	CallTimeout time.Duration // per-RPC timeout. Default 3s.
}

// NewIndiraBroker wires the underlying pkg/indira client + a refresh
// callback. refresh may be nil — in that case AU004 always surfaces
// ErrAuthExpired immediately (acceptable for tests; not for prod).
func NewIndiraBroker(client *indira.Client, refresh AuthRefresher, cfg IndiraConfig, logger *zap.Logger) *IndiraBroker {
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = 3 * time.Second
	}
	return &IndiraBroker{
		client:      client,
		refresh:     refresh,
		logger:      logger.Named("indira-broker"),
		callTimeout: cfg.CallTimeout,
		authCache:   make(map[string]*AuthContext),
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Broker interface implementation
// ─────────────────────────────────────────────────────────────────────────

// PlaceLimit submits a LIMIT order.
//
// Indira symbol format: "STK_{SYMBOL}_EQ_NSE_{TOKEN}" — built upstream by
// manager.Start when it composes the SymbolSpec from cfg.Symbol +
// ResolvedSymbol.SecurityCode.
func (b *IndiraBroker) PlaceLimit(
	ctx context.Context, auth *AuthContext, sym SymbolSpec,
	side state.Side, qty int, price float64,
) (string, error) {

	ordAction := "BUY"
	if side == state.SideSell {
		ordAction = "SELL"
	}

	req := &indira.PlaceOrderRequest{
		Symbol:       buildIndiraSymbol(sym),
		ExcToken:     sym.ExchangeToken,
		Exc:          sym.Exchange,
		OrdAction:    ordAction,
		OrdValidity:  "DAY",
		OrdType:      "Limit",
		PrdType:      productType(sym),
		LimitPrice:   indira.Price2DP(price),
		TriggerPrice: 0,
		Qty:          qty,
		DisQty:       0,
		LotSize:      1,
		Instrument:   "STK",
		Amo:          false,
	}

	resp, err := b.placeWithRetry(ctx, auth, req)
	if err != nil {
		return "", err
	}
	id := resp.OrderId
	if id == "" {
		id = resp.OrdId
	}
	b.logger.Info("placed LIMIT",
		zap.String("symbol", sym.Symbol),
		zap.String("side", ordAction),
		zap.Int("qty", qty),
		zap.Float64("price", price),
		zap.String("broker_order_id", id))
	return id, nil
}

// ModifyLimit changes the price (and optionally qty) of a resting order.
func (b *IndiraBroker) ModifyLimit(
	ctx context.Context, auth *AuthContext, sym SymbolSpec,
	brokerOrderID string, qty int, newPrice float64,
) error {
	req := &indira.ModifyOrderRequest{
		OrdId:         brokerOrderID,
		Symbol:        buildIndiraSymbol(sym),
		OrdAction:     "BUY", // see note below
		OrdValidity:   "DAY",
		ExchangeToken: sym.ExchangeToken,
		Exc:           sym.Exchange,
		Qty:           qty,
		TradedQty:     0,
		LimitPrice:    newPrice, // ModifyOrderRequest takes float64, not Price2DP (asymmetry with Place — kept upstream)
		TriggerPrice:  0,
		OrdType:       "Limit",
		PrdType:       productType(sym),
		Instrument:    "STK",
		LotSize:       1,
		DisQty:        0,
	}
	// NOTE on OrdAction: Indira's modify-order accepts the original side;
	// we don't have it on hand from just the brokerOrderID. In practice
	// Indira ignores OrdAction during modify (the original order owns the
	// side). We pass "BUY" to keep the request well-formed.
	if err := b.modifyWithRetry(ctx, auth, req); err != nil {
		return err
	}
	b.logger.Info("modified LIMIT",
		zap.String("symbol", sym.Symbol),
		zap.String("broker_order_id", brokerOrderID),
		zap.Int("qty", qty),
		zap.Float64("new_price", newPrice))
	return nil
}

// Cancel removes a resting order. Idempotent on the engine side — if
// Indira replies "already cancelled / already filled", we treat that as
// success.
func (b *IndiraBroker) Cancel(
	ctx context.Context, auth *AuthContext, sym SymbolSpec, brokerOrderID string,
) error {
	req := &indira.CancelOrderRequest{
		Symbol: buildIndiraSymbol(sym),
		Exc:    sym.Exchange,
		OrdId:  brokerOrderID,
	}
	if err := b.cancelWithRetry(ctx, auth, req); err != nil {
		return err
	}
	b.logger.Info("cancelled order",
		zap.String("symbol", sym.Symbol),
		zap.String("broker_order_id", brokerOrderID))
	return nil
}

// ─────────────────────────────────────────────────────────────────────────
// AU004 retry wrappers — one extra refresh on auth failure, then fail.
// ─────────────────────────────────────────────────────────────────────────

func (b *IndiraBroker) placeWithRetry(ctx context.Context, auth *AuthContext, req *indira.PlaceOrderRequest) (*indira.PlaceOrderResponse, error) {
	cctx, cancel := context.WithTimeout(ctx, b.callTimeout)
	defer cancel()
	resp, err := b.client.PlaceOrder(cctx, b.toIndiraAuth(auth), req)
	if err == nil {
		return resp, nil
	}
	if !errors.Is(err, indira.ErrAuthExpired) || b.refresh == nil {
		return nil, err
	}
	fresh, rerr := b.refresh(ctx, auth.UserID)
	if rerr != nil || fresh == nil {
		b.logger.Warn("auth refresh failed after AU004",
			zap.String("user_id", auth.UserID),
			zap.Error(rerr))
		return nil, ErrAuthExpired
	}
	b.cacheAuth(auth.UserID, fresh)
	*auth = *fresh // mutate caller's auth so the next tick uses the new JWT
	cctx2, cancel2 := context.WithTimeout(ctx, b.callTimeout)
	defer cancel2()
	resp, err = b.client.PlaceOrder(cctx2, b.toIndiraAuth(fresh), req)
	if err != nil && errors.Is(err, indira.ErrAuthExpired) {
		return nil, ErrAuthExpired
	}
	return resp, err
}

func (b *IndiraBroker) modifyWithRetry(ctx context.Context, auth *AuthContext, req *indira.ModifyOrderRequest) error {
	cctx, cancel := context.WithTimeout(ctx, b.callTimeout)
	defer cancel()
	err := b.client.ModifyOrder(cctx, b.toIndiraAuth(auth), req)
	if err == nil {
		return nil
	}
	if !errors.Is(err, indira.ErrAuthExpired) || b.refresh == nil {
		return err
	}
	fresh, rerr := b.refresh(ctx, auth.UserID)
	if rerr != nil || fresh == nil {
		return ErrAuthExpired
	}
	b.cacheAuth(auth.UserID, fresh)
	*auth = *fresh
	cctx2, cancel2 := context.WithTimeout(ctx, b.callTimeout)
	defer cancel2()
	err = b.client.ModifyOrder(cctx2, b.toIndiraAuth(fresh), req)
	if err != nil && errors.Is(err, indira.ErrAuthExpired) {
		return ErrAuthExpired
	}
	return err
}

func (b *IndiraBroker) cancelWithRetry(ctx context.Context, auth *AuthContext, req *indira.CancelOrderRequest) error {
	cctx, cancel := context.WithTimeout(ctx, b.callTimeout)
	defer cancel()
	err := b.client.CancelOrder(cctx, b.toIndiraAuth(auth), req)
	if err == nil {
		return nil
	}
	// Treat "order already executed / cancelled" as success (idempotent cancel).
	if isAlreadyTerminal(err) {
		b.logger.Debug("cancel: order already terminal — accepting",
			zap.String("broker_order_id", req.OrdId),
			zap.Error(err))
		return nil
	}
	if !errors.Is(err, indira.ErrAuthExpired) || b.refresh == nil {
		return err
	}
	fresh, rerr := b.refresh(ctx, auth.UserID)
	if rerr != nil || fresh == nil {
		return ErrAuthExpired
	}
	b.cacheAuth(auth.UserID, fresh)
	*auth = *fresh
	cctx2, cancel2 := context.WithTimeout(ctx, b.callTimeout)
	defer cancel2()
	err = b.client.CancelOrder(cctx2, b.toIndiraAuth(fresh), req)
	if err != nil {
		if isAlreadyTerminal(err) {
			return nil
		}
		if errors.Is(err, indira.ErrAuthExpired) {
			return ErrAuthExpired
		}
	}
	return err
}

// cacheAuth records the freshest creds we've seen for this user. Phase 5
// will let the fill-bridge stream consult this to keep the user-config
// service's view in sync; for now it's just a defensive cache.
func (b *IndiraBroker) cacheAuth(userID string, a *AuthContext) {
	b.authMu.Lock()
	defer b.authMu.Unlock()
	b.authCache[userID] = a
}

// ─────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────

// buildIndiraSymbol composes "STK_{SYMBOL}_EQ_NSE_{TOKEN}". Indira's order
// endpoints require this exact format — they don't accept just the symbol.
func buildIndiraSymbol(sym SymbolSpec) string {
	return fmt.Sprintf("STK_%s_EQ_%s_%s", sym.Symbol, sym.Exchange, sym.ExchangeToken)
}

// productType maps our config to Indira's wire value. "INTRADAY" → "MIS",
// "DELIVERY" → "DELIVERY". Anything else falls back to DELIVERY (safer).
func productType(sym SymbolSpec) string {
	switch sym.ProductType {
	case "INTRADAY", "MIS":
		return "MIS"
	case "CNC", "DELIVERY":
		return "DELIVERY"
	default:
		return "DELIVERY"
	}
}

// toIndiraAuth bridges our AuthContext to pkg/indira's.
func (b *IndiraBroker) toIndiraAuth(a *AuthContext) *indira.AuthContext {
	if a == nil {
		return nil
	}
	return &indira.AuthContext{
		UserId:      a.UserID,
		AppId:       a.AppID,
		Source:      a.Source,
		BearerToken: a.BearerToken,
	}
}

// isAlreadyTerminal returns true when a cancel error indicates the order
// was already filled or cancelled at the broker — both are terminal-good
// for our purposes (the chunk is gone, that's what we wanted).
func isAlreadyTerminal(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// pkg/indira surfaces broker errors as opaque strings. The two we
	// care about are observed live during Manthan ops:
	//   - "order already executed"
	//   - "Order already in terminal status"
	for _, frag := range []string{
		"already executed",
		"already cancelled",
		"already filled",
		"terminal status",
		"order not found",
	} {
		if containsFold(msg, frag) {
			return true
		}
	}
	return false
}

// containsFold is strings.Contains(strings.ToLower(a), strings.ToLower(b))
// without pulling in strings — keeps the hot path allocation-free.
func containsFold(s, sub string) bool {
	if len(sub) == 0 || len(sub) > len(s) {
		return len(sub) == 0
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		match := true
		for j := 0; j < len(sub); j++ {
			a, b := s[i+j], sub[j]
			if 'A' <= a && a <= 'Z' {
				a += 'a' - 'A'
			}
			if 'A' <= b && b <= 'Z' {
				b += 'a' - 'A'
			}
			if a != b {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
