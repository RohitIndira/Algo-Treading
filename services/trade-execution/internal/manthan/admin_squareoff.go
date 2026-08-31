package manthan

// M7.3 — admin square-off: cancel-SL-then-sell as ONE supervised
// operation, under the same per-position advisory lock every other
// mutation of that position takes.
//
// Why this shape (2026-06-12 lesson): a naked SL cancel is what the
// safety layer reads as manual-exit intent — cancelling stops without
// the paired sell is how S4450 got liquidated in June. Holding
// WithPositionLock across cancel→sell serializes us against the inbox
// worker; the emergency-sell latch (one per symbol per window, the
// 2026-08-26 IOLCP fix) refuses double-fires; lower-circuit falls back
// to AMO SELL exactly like the validated emergency path; and the FIX-5
// FILLED SELL row is written for market sells so the net-qty view closes
// the position instead of diverging from broker reality.

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// SquareOffReport is what the admin gateway renders and audits.
type SquareOffReport struct {
	UserID       string   `json:"user_id"`
	StrategyID   string   `json:"strategy_id"`
	Symbol       string   `json:"symbol"`
	Qty          int      `json:"qty"`
	CancelledSLs []string `json:"cancelled_sls"` // broker ids of stops removed first
	SellBrokerID string   `json:"sell_broker_id,omitempty"`
	Mode         string   `json:"mode"` // MARKET | AMO_LOWER_CIRCUIT
}

// GetNetPosition returns the order-derived net holding for one
// (strategy, symbol) plus the entry-order context needed to act on it —
// the same net-qty arithmetic the naked scan uses (FILLED BUY − FILLED
// SELL). ok=false when nothing is held.
func (r *Repository) GetNetPosition(ctx context.Context, strategyID, symbol string) (*PositionNeedingProtection, bool, error) {
	var p PositionNeedingProtection
	err := r.db.QueryRowContext(ctx, `
		WITH net_qty AS (
		    SELECT COALESCE(SUM(CASE WHEN order_side='BUY'  AND status='FILLED' THEN filled_qty
		                              WHEN order_side='SELL' AND status='FILLED' THEN -filled_qty
		                              ELSE 0 END), 0) AS net
		      FROM manthan_orders
		     WHERE strategy_id = $1 AND symbol = $2
		)
		SELECT e.id, e.signal_id, e.strategy_id, e.user_id, e.symbol,
		       COALESCE(e.isin,''), COALESCE(e.indira_symbol,''),
		       COALESCE(e.exchange_token,''), COALESCE(e.exchange,'NSE'),
		       n.net
		  FROM manthan_orders e, net_qty n
		 WHERE e.strategy_id = $1 AND e.symbol = $2
		   AND e.order_side = 'BUY' AND e.status = 'FILLED'
		 ORDER BY e.filled_at DESC NULLS LAST, e.id DESC
		 LIMIT 1`, strategyID, symbol).
		Scan(&p.EntryOrderID, &p.EntrySignalID, &p.StrategyID, &p.UserID, &p.Symbol,
			&p.ISIN, &p.IndiraSymbol, &p.ExchangeToken, &p.Exchange, &p.NetQty)
	if err == sql.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if p.NetQty <= 0 {
		return nil, false, nil
	}
	return &p, true, nil
}

// ListStandingSLs returns broker-resident protection rows for one
// (strategy, symbol): SL_PLACED / SL_MODIFY_PENDING with a broker id.
func (r *Repository) ListStandingSLs(ctx context.Context, strategyID, symbol string) ([]*ManthanOrder, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, COALESCE(broker_order_id,''), COALESCE(indira_symbol,''),
		       COALESCE(exchange_token,''), COALESCE(isin,''), qty
		  FROM manthan_orders
		 WHERE strategy_id = $1 AND symbol = $2
		   AND order_type IN ('SL_SELL','SL_SELL_AMO')
		   AND status IN ('SL_PLACED','SL_MODIFY_PENDING','AMO_PENDING')
		 ORDER BY id`, strategyID, symbol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*ManthanOrder
	for rows.Next() {
		o := &ManthanOrder{Symbol: symbol, StrategyID: strategyID}
		if err := rows.Scan(&o.ID, &o.BrokerOrderID, &o.IndiraSymbol, &o.ExchangeToken, &o.ISIN, &o.Qty); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// AdminSquareOff closes one Manthan position on operator authority:
// cancel every standing stop, then sell the net holding — one locked,
// supervised sequence. Refuses when nothing is held or auth is dead.
func (h *SLHandler) AdminSquareOff(ctx context.Context, userID, strategyID, symbol string) (*SquareOffReport, error) {
	var report *SquareOffReport
	err := h.repo.WithPositionLock(ctx, strategyID, symbol, func(ctx context.Context) error {
		pos, held, err := h.repo.GetNetPosition(ctx, strategyID, symbol)
		if err != nil {
			return fmt.Errorf("net position: %w", err)
		}
		if !held {
			return fmt.Errorf("no held quantity for %s/%s (net<=0) — nothing to square off", strategyID, symbol)
		}
		if pos.UserID != userID {
			return fmt.Errorf("position belongs to %s, not %s — refusing", pos.UserID, userID)
		}

		auth, ok := h.resolveAuth(userID, symbol, "AdminSquareOff")
		if !ok {
			return fmt.Errorf("no working broker credentials for %s — square-off needs a live session", userID)
		}
		info := &SymbolInfo{
			Symbol: pos.Symbol, IndiraSymbol: pos.IndiraSymbol,
			ExchangeToken: pos.ExchangeToken, Exchange: pos.Exchange,
		}
		if info.IndiraSymbol == "" || info.ExchangeToken == "" {
			resolved, rerr := h.broker.ResolveSymbol(ctx, pos.Symbol, pos.ISIN)
			if rerr != nil {
				return fmt.Errorf("resolve symbol: %w", rerr)
			}
			info = resolved
		}

		report = &SquareOffReport{UserID: userID, StrategyID: strategyID, Symbol: symbol, Qty: pos.NetQty}

		// 1) Remove every standing stop FIRST — inside the lock, paired
		// with the sell below, so no window exists where the naked-cancel
		// heuristics can misread intent (and freeQty is released for the
		// sell).
		standing, err := h.repo.ListStandingSLs(ctx, strategyID, symbol)
		if err != nil {
			return fmt.Errorf("standing SLs: %w", err)
		}
		for _, sl := range standing {
			if sl.BrokerOrderID != "" {
				if cerr := h.broker.CancelOrder(ctx, auth, info, sl.BrokerOrderID); cerr != nil {
					// A vanished order (already terminal at the broker) is
					// fine — the sell is what matters. A live cancel failure
					// is NOT: selling with a standing SL double-commits qty.
					if !IsOrderNotFoundError(cerr) {
						return fmt.Errorf("cancel standing SL %s failed — aborting BEFORE any sell (position still protected): %w", sl.BrokerOrderID, cerr)
					}
				}
			}
			_ = h.repo.UpdateOrderCancelled(ctx, sl.ID)
			_ = h.repo.InsertEvent(ctx, sl.ID, "ADMIN_SQUAREOFF_SL_CANCEL", "SL_PLACED", "CANCELLED",
				"", 0, sl.Qty, "admin square-off: stop removed as part of supervised cancel+sell")
			report.CancelledSLs = append(report.CancelledSLs, sl.BrokerOrderID)
		}

		// 2) Sell the net holding — same latch + lower-circuit logic as the
		// validated emergency path.
		brokerID, mode, serr := h.placeSquareOffSell(ctx, info, auth, pos.NetQty)
		if serr != nil {
			return fmt.Errorf("square-off sell failed (stops already cancelled — position is UNPROTECTED, re-arm or retry NOW): %w", serr)
		}
		report.SellBrokerID, report.Mode = brokerID, mode

		// 3) FIX-5 row for market sells: nets the position to 0 downstream;
		// true fill price reconciles later via WSS/tradebook. AMO mode gets
		// its row when the conversion confirms — inserting FILLED now would
		// lie overnight.
		if mode == "MARKET" {
			slLike := &ManthanOrder{StrategyID: strategyID, UserID: userID, Symbol: symbol,
				ISIN: pos.ISIN, IndiraSymbol: info.IndiraSymbol, ExchangeToken: info.ExchangeToken}
			_ = h.repo.InsertEmergencySellFilled(ctx, slLike, brokerID, pos.NetQty)
		}
		_ = h.repo.InsertEvent(ctx, pos.EntryOrderID, "ADMIN_SQUAREOFF", "FILLED", "EXIT_PENDING",
			"", 0, pos.NetQty, fmt.Sprintf("admin square-off: %s sell %s placed (%d stops cancelled first)", mode, brokerID, len(report.CancelledSLs)))

		h.logger.Warn("ADMIN SQUARE-OFF executed",
			zap.String("user_id", userID), zap.String("symbol", symbol),
			zap.Int("qty", pos.NetQty), zap.Strings("cancelled_sls", report.CancelledSLs),
			zap.String("sell_broker_id", brokerID), zap.String("mode", mode))
		return nil
	})
	return report, err
}

// placeSquareOffSell mirrors emergencySellInternal but reports the broker
// id + mode back to the operator (the internal path deliberately discards
// them; an audited admin action must not).
func (h *SLHandler) placeSquareOffSell(ctx context.Context, info *SymbolInfo, auth BrokerAuth, qty int) (string, string, error) {
	h.emergencyMu.Lock()
	if last, ok := h.lastEmergency[info.Symbol]; ok && time.Since(last) < emergencyLatchWindow {
		h.emergencyMu.Unlock()
		return "", "", fmt.Errorf("sell for %s suppressed by emergency latch: already fired at %s — the position is likely already closing", info.Symbol, last.Format(time.RFC3339))
	}
	h.lastEmergency[info.Symbol] = time.Now()
	h.emergencyMu.Unlock()

	_, atLower, _ := h.broker.CheckCircuit(ctx, info.ExchangeToken)
	if atLower {
		id, err := h.broker.PlaceAMOSell(ctx, auth, info, qty)
		return id, "AMO_LOWER_CIRCUIT", err
	}
	id, err := h.broker.PlaceMarketSell(ctx, auth, info, qty)
	return id, "MARKET", err
}
