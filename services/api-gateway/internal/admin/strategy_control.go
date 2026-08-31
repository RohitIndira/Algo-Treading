package admin

// M4 — Strategy control: pause/resume/delete on behalf of any user,
// lifecycle timeline, and the cooldown/embargo manager.
//
// Review decisions (2026-08-31):
//   - Delete is KEEP_POSITIONS_OPEN only. A square-off delete places real
//     market orders and needs the target user's live auth plus the
//     safety-monitor-aware flow — that is M7.3, not a strategy-CRUD call.
//     The TYPED blast radius states the open-position count and that they
//     remain open (protected by their standing stops).
//   - Capital / SL% / trail% are NOT editable here. Capital lives in
//     rules-engine's portfolio state with no write RPC (bodging cross-
//     service writes is how books diverge), and the 20% stop is a declared
//     hard rule (operator 2026-08-19 + the NSE writeup).
//   - 4.4 covers BOTH invisible block mechanisms in signals_db: SL-exit
//     re-entry cooldowns (manthan_cooldown) and manual-exit embargoes
//     (manthan_signal_decisions.user_override_until).

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pb "github.com/RohitIndira/Algo-Treading/api/proto/user_config"
)

// strategyRPC is the slice of the user-config client M4 needs.
type strategyRPC interface {
	ActivateStrategy(ctx context.Context, req *pb.ActivateStrategyRequest) (*pb.ActivateStrategyResponse, error)
	DeactivateStrategy(ctx context.Context, req *pb.DeactivateStrategyRequest) (*pb.DeactivateStrategyResponse, error)
	DeleteStrategy(ctx context.Context, req *pb.DeleteStrategyRequest) (*pb.DeleteStrategyResponse, error)
}

// StrategyControl bundles M4's dependencies.
type StrategyControl struct {
	rpc       strategyRPC
	fleet     *FleetStore
	signalsDB *sql.DB // manthan_cooldown, manthan_signal_decisions (rules-engine's DB)
}

func NewStrategyControl(rpc strategyRPC, fleet *FleetStore, signalsDB *sql.DB) *StrategyControl {
	return &StrategyControl{rpc: rpc, fleet: fleet, signalsDB: signalsDB}
}

// StrategyRef resolves a strategy to its owner + live context — every M4
// action starts here so the admin acts on verified identity, and the
// TYPED blast radius carries real numbers.
type StrategyRef struct {
	StrategyID    string
	UserID        string
	Active        bool
	TradingMode   string
	OpenPositions int
}

func (sc *StrategyControl) Resolve(ctx context.Context, strategyID string) (*StrategyRef, error) {
	ref := &StrategyRef{StrategyID: strategyID}
	err := sc.fleet.tradingDB.QueryRowContext(ctx, `
		SELECT user_id, active, trading_mode FROM strategies
		 WHERE strategy_id = $1 AND deleted_at IS NULL`, strategyID).
		Scan(&ref.UserID, &ref.Active, &ref.TradingMode)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("strategy %s not found (or deleted)", strategyID)
	}
	if err != nil {
		return nil, fmt.Errorf("strategy lookup: %w", err)
	}
	if err := sc.fleet.posDB.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT symbol) FROM positions
		 WHERE strategy_id = $1 AND status = 'ACTIVE'`, strategyID).
		Scan(&ref.OpenPositions); err != nil {
		return nil, fmt.Errorf("open positions count: %w", err)
	}
	return ref, nil
}

// Pause / Resume ride the existing, mock-drill-validated gRPC paths, with
// the TARGET user's id (admin-on-behalf) — the same lifecycle events and
// config-sync fan-out fire as if the user had clicked it.
func (sc *StrategyControl) Pause(ctx context.Context, ref *StrategyRef) error {
	resp, err := sc.rpc.DeactivateStrategy(ctx, &pb.DeactivateStrategyRequest{
		StrategyId: ref.StrategyID, UserId: ref.UserID,
	})
	return rpcOutcome("pause", resp2ok(resp == nil || !resp.Success, respErr(resp)), err)
}

func (sc *StrategyControl) Resume(ctx context.Context, ref *StrategyRef) error {
	resp, err := sc.rpc.ActivateStrategy(ctx, &pb.ActivateStrategyRequest{
		StrategyId: ref.StrategyID, UserId: ref.UserID,
	})
	return rpcOutcome("resume", resp2ok(resp == nil || !resp.Success, activateErr(resp)), err)
}

// Delete removes the strategy KEEPING positions open (their standing stops
// keep guarding them; square-off delete is M7.3).
func (sc *StrategyControl) Delete(ctx context.Context, ref *StrategyRef) error {
	resp, err := sc.rpc.DeleteStrategy(ctx, &pb.DeleteStrategyRequest{
		StrategyId:       ref.StrategyID,
		UserId:           ref.UserID,
		PositionHandling: pb.PositionHandling_KEEP_POSITIONS_OPEN,
	})
	return rpcOutcome("delete", resp2ok(resp == nil || !resp.Success, deleteErr(resp)), err)
}

func rpcOutcome(action string, failMsg string, err error) error {
	if err != nil {
		return fmt.Errorf("%s rpc: %w", action, err)
	}
	if failMsg != "" {
		return fmt.Errorf("%s refused: %s", action, failMsg)
	}
	return nil
}

func resp2ok(failed bool, msg string) string {
	if !failed {
		return ""
	}
	if msg == "" {
		msg = "user-config returned success=false"
	}
	return msg
}

func respErr(r *pb.DeactivateStrategyResponse) string {
	if r != nil && r.Error != nil {
		return r.Error.Message
	}
	return ""
}
func activateErr(r *pb.ActivateStrategyResponse) string {
	if r != nil && r.Error != nil {
		return r.Error.Message
	}
	return ""
}
func deleteErr(r *pb.DeleteStrategyResponse) string {
	if r != nil && r.Error != nil {
		return r.Error.Message
	}
	return ""
}

// DeleteConfirmation is the TYPED blast-radius text for a strategy delete.
func DeleteConfirmation(ref *StrategyRef) string {
	return fmt.Sprintf("DELETE STRATEGY %s FOR %s — %d OPEN POSITIONS STAY OPEN",
		shortID(ref.StrategyID), ref.UserID, ref.OpenPositions)
}

func shortID(id string) string {
	if i := strings.IndexByte(id, '-'); i > 0 {
		return strings.ToUpper(id[:i])
	}
	return id
}

// ── 4.3 Lifecycle timeline ──────────────────────────────────────────────

type LifecycleEvent struct {
	ID        int64           `json:"id"`
	EventType string          `json:"event_type"`
	Details   json.RawMessage `json:"details"`
	CreatedAt time.Time       `json:"created_at"`
}

func (sc *StrategyControl) Timeline(ctx context.Context, strategyID string) ([]LifecycleEvent, error) {
	rows, err := sc.fleet.tradingDB.QueryContext(ctx, `
		SELECT id, event_type, details, created_at
		  FROM strategy_lifecycle_events
		 WHERE strategy_id = $1 ORDER BY id DESC LIMIT 200`, strategyID)
	if err != nil {
		return nil, fmt.Errorf("timeline: %w", err)
	}
	defer rows.Close()
	var out []LifecycleEvent
	for rows.Next() {
		var e LifecycleEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.Details, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ── 4.4 Cooldowns & embargoes ───────────────────────────────────────────

// Block is one re-entry restriction currently in force.
type Block struct {
	Kind         string     `json:"kind"` // COOLDOWN | OVERRIDE
	Symbol       string     `json:"symbol"`
	Detail       string     `json:"detail"`
	Until        *time.Time `json:"until,omitempty"`         // OVERRIDE: expiry
	ReentryBelow *float64   `json:"reentry_below,omitempty"` // COOLDOWN: price condition
}

// Blocks lists both mechanisms for one strategy.
func (sc *StrategyControl) Blocks(ctx context.Context, strategyID string) ([]Block, error) {
	var out []Block

	cd, err := sc.signalsDB.QueryContext(ctx, `
		SELECT symbol, exit_price, reentry_below, exit_time
		  FROM manthan_cooldown
		 WHERE strategy_id = $1::uuid AND cleared = false
		 ORDER BY symbol`, strategyID)
	if err != nil {
		return nil, fmt.Errorf("cooldowns: %w", err)
	}
	for cd.Next() {
		var b Block
		var exitPrice, reentry float64
		var exitTime time.Time
		if err := cd.Scan(&b.Symbol, &exitPrice, &reentry, &exitTime); err != nil {
			cd.Close()
			return nil, err
		}
		b.Kind, b.ReentryBelow = "COOLDOWN", &reentry
		b.Detail = fmt.Sprintf("SL-exit %s at %.2f — re-entry only below %.2f (20%% ATH correction rule)",
			exitTime.Format("2006-01-02"), exitPrice, reentry)
		out = append(out, b)
	}
	cd.Close()
	if err := cd.Err(); err != nil {
		return nil, err
	}

	ov, err := sc.signalsDB.QueryContext(ctx, `
		SELECT symbol, MAX(user_override_until)
		  FROM manthan_signal_decisions
		 WHERE strategy_id = $1::uuid
		   AND user_override_until IS NOT NULL
		   AND user_override_until > now()
		 GROUP BY symbol ORDER BY symbol`, strategyID)
	if err != nil {
		return nil, fmt.Errorf("overrides: %w", err)
	}
	for ov.Next() {
		var b Block
		var until time.Time
		if err := ov.Scan(&b.Symbol, &until); err != nil {
			ov.Close()
			return nil, err
		}
		b.Kind, b.Until = "OVERRIDE", &until
		b.Detail = fmt.Sprintf("manual-exit embargo — blocked until %s", until.Format("2006-01-02 15:04 MST"))
		out = append(out, b)
	}
	ov.Close()
	return out, ov.Err()
}

// ClearBlock lifts one restriction early. kind selects the mechanism;
// refuses when nothing matched (clearing a ghost is a UI bug to surface).
func (sc *StrategyControl) ClearBlock(ctx context.Context, strategyID, symbol, kind string) error {
	var res sql.Result
	var err error
	switch kind {
	case "COOLDOWN":
		res, err = sc.signalsDB.ExecContext(ctx, `
			UPDATE manthan_cooldown SET cleared = true, cleared_at = now()
			 WHERE strategy_id = $1::uuid AND symbol = $2 AND cleared = false`,
			strategyID, symbol)
	case "OVERRIDE":
		res, err = sc.signalsDB.ExecContext(ctx, `
			UPDATE manthan_signal_decisions SET user_override_until = NULL
			 WHERE strategy_id = $1::uuid AND symbol = $2
			   AND user_override_until IS NOT NULL AND user_override_until > now()`,
			strategyID, symbol)
	default:
		return fmt.Errorf("unknown block kind %q (want COOLDOWN or OVERRIDE)", kind)
	}
	if err != nil {
		return fmt.Errorf("clear %s: %w", kind, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("no active %s block for %s on this strategy", kind, symbol)
	}
	return nil
}
