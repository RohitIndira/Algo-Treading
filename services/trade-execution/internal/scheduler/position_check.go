package scheduler

import (
	"context"
	"fmt"
	"strings"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
)

// CredentialsResolver returns fresh broker credentials for a user.
// Implemented by *executor.CredentialsCache.
type CredentialsResolver interface {
	Get(ctx context.Context, userID string) (userId, appId, source, bearerToken string, err error)
}

// PositionLookup fetches the broker position book.
// Implemented by *indira.ExecutionClient.
type PositionLookup interface {
	GetPositions(ctx context.Context, auth *indiraClient.AuthContext) ([]indiraClient.Position, error)
}

// PositionChecker reports whether a position is already flat (NetQty == 0) at
// the broker. Used to skip redundant square-off orders that would otherwise
// open a fresh short.
//
// Thread-safe; safe for concurrent use.
type PositionChecker struct {
	indiraClient PositionLookup
	creds        CredentialsResolver
}

// NewPositionChecker constructs a checker. Either dep being nil disables the
// check (FetchOpenPositions returns an error so callers fail-open).
func NewPositionChecker(client PositionLookup, creds CredentialsResolver) *PositionChecker {
	return &PositionChecker{indiraClient: client, creds: creds}
}

// posKey identifies a broker position by (exchange, exchange-token, product type).
// Exchange-token is the broker's authoritative instrument id; product-type
// prevents an MIS position from being confused with a CNC holding of the same stock.
type posKey struct {
	exchange string
	token    int
	prodType string
}

func normalizePrd(s string) string {
	return strings.ToUpper(indiraClient.MapProductType(s))
}

func orderKey(o *models.Order) posKey {
	return posKey{
		exchange: strings.ToUpper(string(o.Exchange)),
		token:    int(o.StockCode),
		prodType: normalizePrd(o.ProductType),
	}
}

// OpenPositions is a snapshot of a user's broker positions with NetQty != 0.
// Returned by PositionChecker.FetchOpenPositions; consumed via IsExited.
//
// A nil receiver is safe and reports "not exited" — i.e. the snapshot is
// unavailable so the caller should proceed with square-off (fail-open).
type OpenPositions struct {
	m map[posKey]int
}

// IsExited reports whether the order's underlying position is already flat at
// the broker (NetQty == 0 or the symbol is missing from the position book).
// In both cases a square-off would open a fresh short, so the caller should
// skip the order.
//
// Returns false when called on a nil snapshot — fail-open semantics for the
// case where the position book could not be fetched.
func (o *OpenPositions) IsExited(order *models.Order) bool {
	if o == nil || order == nil {
		return false
	}
	_, stillOpen := o.m[orderKey(order)]
	return !stillOpen
}

// GetNetQty returns how many units the broker currently shows open for the
// order's symbol+exchange+product-type. Returns 0 when the snapshot is nil
// or the symbol is not present (position is flat).
//
// Used by position-book square-off to calculate the safe exit quantity:
//
//	squareQty = min(GetNetQty(order), order.FilledQuantity)
//
// This ensures we only close what our algo placed and never open a short
// even when the user holds additional manual qty in the same symbol.
func (o *OpenPositions) GetNetQty(order *models.Order) int {
	if o == nil || order == nil {
		return 0
	}
	return o.m[orderKey(order)]
}

// FetchOpenPositions returns the broker's open positions (NetQty != 0) for
// userID. EXPIRY rows are dropped; only DAILY (intraday) positions are kept.
//
// On any failure (no creds, broker error, timeout) returns (nil, err). Callers
// should log and proceed with square-off (fail-open) — better to over-attempt
// than strand an intraday position because the broker was momentarily slow.
func (p *PositionChecker) FetchOpenPositions(ctx context.Context, userID string) (*OpenPositions, error) {
	if p == nil || p.indiraClient == nil || p.creds == nil {
		return nil, fmt.Errorf("position checker not configured")
	}
	if userID == "" {
		return nil, fmt.Errorf("empty userID")
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	uid, appId, source, bearerToken, err := p.creds.Get(callCtx, userID)
	if err != nil {
		return nil, fmt.Errorf("fetch credentials: %w", err)
	}

	auth := &indiraClient.AuthContext{
		UserId:      uid,
		AppId:       appId,
		Source:      source,
		BearerToken: bearerToken,
	}

	positions, err := p.indiraClient.GetPositions(callCtx, auth)
	if err != nil {
		return nil, fmt.Errorf("broker GetPositions: %w", err)
	}

	open := make(map[posKey]int, len(positions))
	for _, pos := range positions {
		// Indira returns both DAILY and EXPIRY rows per symbol; keep only DAILY.
		if pos.Type != "" && pos.Type != "DAILY" {
			continue
		}
		if pos.NetQty == 0 {
			continue
		}
		key := posKey{
			exchange: strings.ToUpper(pos.Symbol.Exc),
			token:    pos.Symbol.ExcTkn,
			prodType: normalizePrd(pos.PrdType),
		}
		open[key] = pos.NetQty
	}
	return &OpenPositions{m: open}, nil
}
