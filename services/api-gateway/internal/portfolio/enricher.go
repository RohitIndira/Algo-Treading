package portfolio

// Enrichment glue: takes portfolio svc's gRPC response + adds LTP-derived
// fields (currentValue, unrealizedPnL) using the api-gateway LTP client.
//
// Design intent (from docs/portfolio_service_design.md §3): portfolio svc
// stays pure positions_db reads; api-gateway owns Redis, tokens, and the
// LTP subsystem health signal. Everything below runs on the api-gateway
// side. Nothing here talks to positions_db directly.

import (
	"context"

	pfpb "github.com/RohitIndira/Algo-Treading/api/proto/portfolio"
	"github.com/RohitIndira/Algo-Treading/services/api-gateway/internal/livealgos"
)

// LTPSource is the subset of livealgos.LTPStore we need. Kept small so
// tests can plug a stub.
type LTPSource interface {
	FetchByTokens(ctx context.Context, tokens []string) (map[string]livealgos.LTPQuote, livealgos.Status)
}

// Enricher is stateless — one instance for the whole api-gateway.
type Enricher struct {
	tokens TokenLookup
	ltp    LTPSource
}

func NewEnricher(tokens TokenLookup, ltp LTPSource) *Enricher {
	return &Enricher{tokens: tokens, ltp: ltp}
}

// ---------------------------------------------------------------------
// EnrichPositions — the workhorse. Given portfolio svc's raw active-lots
// list, produces the wire []ActivePositionRow + top-level ltpStatus.
//
// Behavior on LTP unavailable:
//   - Per-row LTP / CurrentValue / UnrealizedPnL fields stay nil
//   - Top-level LTPStatus = UNAVAILABLE
//   - Non-LTP fields (position_id, symbol, entry, qty, invested) still
//     render — user can still see WHAT they hold, just not the price.
// ---------------------------------------------------------------------

func (e *Enricher) EnrichPositions(ctx context.Context, lots []*pfpb.ActivePosition) (PositionsResponse, error) {
	rows := make([]ActivePositionRow, 0, len(lots))
	if len(lots) == 0 {
		// No positions to enrich; LTP subsystem status is irrelevant here.
		return PositionsResponse{
			Positions: rows,
			LTPStatus: string(livealgos.StatusHealthy),
		}, nil
	}

	// Symbol → LTP via token lookup.
	symbols := collectSymbols(lots)
	tokenMap, err := e.tokens.TokensBySymbols(ctx, symbols)
	if err != nil {
		return PositionsResponse{}, err
	}
	tokens := distinctValues(tokenMap)
	quotes, status := e.ltp.FetchByTokens(ctx, tokens)

	for _, lot := range lots {
		row := baseActiveRow(lot)
		// Best-effort LTP annotation. Missing token OR missing quote OR
		// unhealthy subsystem all fall through the same "leave nil" path.
		if status == livealgos.StatusHealthy {
			if tok, ok := tokenMap[lot.GetSymbol()]; ok {
				if q, ok := quotes[tok]; ok {
					ltp := q.LTP
					qty := float64(lot.GetQuantity())
					cur := qty * ltp
					unr := (ltp - lot.GetEntryPrice()) * qty
					row.LTP = &ltp
					row.CurrentValue = &cur
					row.UnrealizedPnL = &unr
				}
			}
		}
		rows = append(rows, row)
	}

	return PositionsResponse{
		Positions: rows,
		LTPStatus: string(status),
	}, nil
}

// ---------------------------------------------------------------------
// EnrichSummary — takes the raw Summary + ActivePositions and produces
// the wire SummaryResponse with the top-level CurrentValue / UnrealizedPnL
// aggregates when LTP is available.
//
// The current-value calc walks positions the same way EnrichPositions
// would — so callers that need both should call EnrichSummaryAndPositions
// (below) to avoid a duplicate LTP round-trip.
// ---------------------------------------------------------------------

func (e *Enricher) EnrichSummary(ctx context.Context, sum *pfpb.Summary, lots []*pfpb.ActivePosition) (SummaryResponse, error) {
	resp := SummaryResponse{
		TotalInvested:            sum.GetTotalInvested(),
		TotalRealizedPnLLifetime: sum.GetTotalRealizedPnlLifetime(),
		TodayRealizedPnL:         sum.GetTodayRealizedPnl(),
		ActiveLotCount:           sum.GetActiveLotCount(),
		ClosedLotCount:           sum.GetClosedLotCount(),
		ManthanInvested:          sum.GetManthanInvested(),
		UserManualInvested:       sum.GetUserManualInvested(),
		LTPStatus:                string(livealgos.StatusHealthy),
	}
	if len(lots) == 0 {
		return resp, nil
	}

	symbols := collectSymbols(lots)
	tokenMap, err := e.tokens.TokensBySymbols(ctx, symbols)
	if err != nil {
		return SummaryResponse{}, err
	}
	tokens := distinctValues(tokenMap)
	quotes, status := e.ltp.FetchByTokens(ctx, tokens)
	resp.LTPStatus = string(status)

	if status != livealgos.StatusHealthy {
		// Leave CurrentValue + UnrealizedPnL nil so the UI renders "—".
		return resp, nil
	}

	var current float64
	var hasAny bool
	for _, lot := range lots {
		tok, ok := tokenMap[lot.GetSymbol()]
		if !ok {
			continue
		}
		q, ok := quotes[tok]
		if !ok {
			continue
		}
		current += float64(lot.GetQuantity()) * q.LTP
		hasAny = true
	}
	if hasAny {
		unrealized := current - resp.TotalInvested
		resp.CurrentValue = &current
		resp.UnrealizedPnL = &unrealized
	}
	return resp, nil
}

// ---------------------------------------------------------------------
// EnrichHistory — no LTP; realized_pnl is the historical truth. This
// is here (not inline in the handler) purely so all wire-envelope
// building lives in one package.
// ---------------------------------------------------------------------

func (e *Enricher) EnrichHistory(rows []*pfpb.ClosedPosition, page, pageSize, total int32) HistoryResponse {
	out := make([]ClosedPositionRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, ClosedPositionRow{
			PositionID:     r.GetPositionId(),
			Origin:         r.GetOrigin(),
			Symbol:         r.GetSymbol(),
			EntryTimeMs:    r.GetEntryTimeMs(),
			EntryPrice:     r.GetEntryPrice(),
			Quantity:       r.GetQuantity(),
			ExitTimeMs:     r.GetExitTimeMs(),
			ExitPrice:      r.GetExitPrice(),
			ExitReason:     r.GetExitReason(),
			RealizedPnL:    r.GetRealizedPnl(),
			InvestedAmount: r.GetInvestedAmount(),
		})
	}
	return HistoryResponse{
		Positions:  out,
		Page:       page,
		PageSize:   pageSize,
		TotalCount: total,
	}
}

// ---------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------

func baseActiveRow(lot *pfpb.ActivePosition) ActivePositionRow {
	return ActivePositionRow{
		PositionID:     lot.GetPositionId(),
		Origin:         lot.GetOrigin(),
		Symbol:         lot.GetSymbol(),
		Exchange:       lot.GetExchange(),
		StrategyID:     lot.GetStrategyId(),
		SignalID:       lot.GetSignalId(),
		EntryTimeMs:    lot.GetEntryTimeMs(),
		EntryPrice:     lot.GetEntryPrice(),
		Quantity:       lot.GetQuantity(),
		InvestedAmount: lot.GetInvestedAmount(),
		CurrentSL:      lot.GetCurrentSl(),
		HighSinceEntry: lot.GetHighSinceEntry(),
	}
}

func collectSymbols(lots []*pfpb.ActivePosition) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(lots))
	for _, l := range lots {
		s := l.GetSymbol()
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func distinctValues(m map[string]string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(m))
	for _, v := range m {
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
