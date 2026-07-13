// Package portfolio owns api-gateway's orchestration of portfolio svc
// responses: gRPC fetch → symbol→token resolution → LTP enrichment →
// unrealized P&L math → wire envelopes.
//
// portfolio svc itself has NONE of this — it's pure positions_db reads.
// The reason token+LTP live here (not there) is that only api-gateway
// already knows the tokens (via stockk_trading.manthan_orders) and owns
// the Redis tunnel to the LTP feed. Duplicating either would reproduce
// the memory-noted "silent tunnel" bug in a second service.
package portfolio

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
)

// TokenLookup resolves a batch of symbols → exchange_tokens. Interface
// keeps the handler testable without dragging in a real Postgres.
type TokenLookup interface {
	// TokensBySymbols returns a map[symbol]exchange_token for as many of
	// the requested symbols as it can resolve. Missing symbols are silently
	// omitted (not an error) — the enricher treats "no token" the same
	// as "no LTP" and renders the position without LTP fields.
	TokensBySymbols(ctx context.Context, symbols []string) (map[string]string, error)
}

// TradingDBTokenLookup is a TokenLookup backed by manthan_orders in
// execution_db — the authoritative writer for exchange_token (owned
// by the trade-execution service). Previously this pointed at a
// stockk_trading replica that drifted silently; killed in Chunk DB.1.
//
// exchange_token is stable per symbol regardless of user or strategy,
// so we DISTINCT ON (symbol) — one row per symbol regardless of how
// many orders exist. FILLED filter drops rejected/pending orders whose
// exchange_token may not be authoritative.
type TradingDBTokenLookup struct {
	db *sql.DB
}

// NewTradingDBTokenLookup wraps an already-opened *sql.DB pointing at
// execution_db. Kept the "TradingDB" name for API stability — the
// underlying DB target changed in DB.1 but existing callers don't need
// to know.
func NewTradingDBTokenLookup(db *sql.DB) *TradingDBTokenLookup {
	return &TradingDBTokenLookup{db: db}
}

func (l *TradingDBTokenLookup) TokensBySymbols(ctx context.Context, symbols []string) (map[string]string, error) {
	if l == nil || l.db == nil || len(symbols) == 0 {
		return map[string]string{}, nil
	}
	rows, err := l.db.QueryContext(ctx, `
		SELECT DISTINCT ON (symbol) symbol, exchange_token
		  FROM public.manthan_orders
		 WHERE symbol = ANY($1::text[])
		   AND status = 'FILLED'
		   AND exchange_token IS NOT NULL
		 ORDER BY symbol, filled_at ASC`,
		pq.Array(symbols),
	)
	if err != nil {
		return nil, fmt.Errorf("TokensBySymbols query: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string, len(symbols))
	for rows.Next() {
		var sym, tok string
		if err := rows.Scan(&sym, &tok); err != nil {
			return nil, fmt.Errorf("TokensBySymbols scan: %w", err)
		}
		out[sym] = tok
	}
	return out, rows.Err()
}
