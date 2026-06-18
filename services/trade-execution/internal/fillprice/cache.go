// Package fillprice resolves the actual execution (fill) price of a broker order
// from the REST trade book, with per-user batching and short-TTL caching so the
// resolution scales to many concurrent strategies.
//
// Why it exists: the order WebSocket usually carries the fill price (TradedPrice),
// but on MARKET orders the WS may report EXECUTED with no usable price, and the
// broker can briefly lag. SL/TP must be computed from the real execution price —
// not the inflated entry-limit price — so when the WS price is missing we fall
// back to the trade book through this cache.
//
// Scale design (1000+ strategies):
//   - One trade-book call returns a user's ENTIRE book (GetTradeBook with no IDs),
//     so N fills for a user cost 1 broker round-trip, not N.
//   - A per-user lock + TTL acts as single-flight: concurrent callers for the same
//     user share one in-flight fetch; different users never block each other.
package fillprice

import (
	"context"
	"sync"
	"time"

	indiraClient "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/timezone"
)

// defaultTTL is how long a fetched trade book is reused before a refetch.
const defaultTTL = 1500 * time.Millisecond

// TradeBookFetcher is the slice of the broker client this package needs.
// Satisfied by *indira.ExecutionClient.
type TradeBookFetcher interface {
	GetTradeBook(ctx context.Context, auth *indiraClient.AuthContext, orderIds ...string) ([]indiraClient.TradeBook, error)
}

// Fill is the aggregated execution of a single broker order.
type Fill struct {
	Qty   int
	Price float64 // volume-weighted average across the order's trades
	Time  time.Time
}

// Cache batches and caches trade-book lookups per user.
type Cache struct {
	fetcher TradeBookFetcher
	ttl     time.Duration

	mu    sync.Mutex
	users map[string]*userBook
}

type userBook struct {
	mu        sync.Mutex // serializes fetch for this user (single-flight)
	fetchedAt time.Time
	byOrder   map[string]Fill
}

// NewCache builds a trade-book fill cache. ttl<=0 uses the default (1.5s).
func NewCache(fetcher TradeBookFetcher, ttl time.Duration) *Cache {
	if ttl <= 0 {
		ttl = defaultTTL
	}
	return &Cache{
		fetcher: fetcher,
		ttl:     ttl,
		users:   make(map[string]*userBook),
	}
}

func (c *Cache) bookFor(userID string) *userBook {
	c.mu.Lock()
	defer c.mu.Unlock()
	ub, ok := c.users[userID]
	if !ok {
		ub = &userBook{}
		c.users[userID] = ub
	}
	return ub
}

// FillForOrder returns the aggregated fill for one broker order. It fetches the
// user's whole trade book at most once per TTL window (batching + per-user
// single-flight). ok=false means the order has no booked trade yet — the caller
// should retry later. A non-nil error means the broker call itself failed.
func (c *Cache) FillForOrder(ctx context.Context, auth *indiraClient.AuthContext, userID, brokerOrderID string) (Fill, bool, error) {
	if c == nil || c.fetcher == nil || auth == nil || brokerOrderID == "" {
		return Fill{}, false, nil
	}

	ub := c.bookFor(userID)
	ub.mu.Lock()
	defer ub.mu.Unlock()

	if ub.byOrder != nil && time.Since(ub.fetchedAt) < c.ttl {
		f, ok := ub.byOrder[brokerOrderID]
		return f, ok, nil
	}

	trades, err := c.fetcher.GetTradeBook(ctx, auth) // whole book, one call
	if err != nil {
		return Fill{}, false, err
	}
	ub.byOrder = aggregateByOrder(trades)
	ub.fetchedAt = time.Now()
	f, ok := ub.byOrder[brokerOrderID]
	return f, ok, nil
}

// aggregateByOrder reduces a trade-book response into a per-order fill: summed
// qty, volume-weighted average price, and the latest trade time.
func aggregateByOrder(trades []indiraClient.TradeBook) map[string]Fill {
	type acc struct {
		qty      int
		notional float64
		latest   time.Time
	}
	accs := make(map[string]*acc, len(trades))
	for _, t := range trades {
		if t.OrdId == "" || t.TradedQty <= 0 {
			continue
		}
		a := accs[t.OrdId]
		if a == nil {
			a = &acc{}
			accs[t.OrdId] = a
		}
		a.qty += t.TradedQty
		a.notional += float64(t.TradedQty) * t.TradedPrice
		if tt := parseTradeBookTime(t.TradeTime); tt.After(a.latest) {
			a.latest = tt
		}
	}
	out := make(map[string]Fill, len(accs))
	for id, a := range accs {
		if a.qty <= 0 {
			continue
		}
		out[id] = Fill{Qty: a.qty, Price: a.notional / float64(a.qty), Time: a.latest}
	}
	return out
}

// parseTradeBookTime parses the broker trade-book TradeTime. The REST book uses
// "2006-01-02 15:04:05"; some payloads use the order-event style "02-Jan-2006
// 15:04:05" / "02-Jan-2006 15.04.05". All are interpreted in IST.
func parseTradeBookTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05",
		"02-Jan-2006 15:04:05",
		"02-Jan-2006 15.04.05",
	} {
		if t, err := time.ParseInLocation(layout, s, timezone.IST); err == nil {
			return t
		}
	}
	return time.Time{}
}
