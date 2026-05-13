// Package repo is the ONLY place in hft-engine that talks to Postgres.
// Every query lives here so future changes (caching, sharding, replicas,
// switching ORMs) happen in one file.
//
// Phase 1 surface — three methods:
//   LoadConfig(strategyID)          → loads the strategy + parses trade_config JSON
//   LoadCredentials(userID)         → loads the user's Indira JWT + appId + source
//   InsertAuditOrder(ctx, event)    → appends one row to hft_audit_orders
//                                     (used by audit.Writer, not called directly
//                                     from the tick loop)
//
// Phase 2+ will add ListActiveHFTStrategies (for restart recovery) and
// batch-insert variants.
package repo

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/hft-engine/internal/state"
)

// Repo bundles the two DB handles the engine needs.
//   tradingDB     — holds `strategies` + `trade_configs` (one row per strategy)
//   tradingExecDB — holds `user_credentials` + `hft_audit_orders`
type Repo struct {
	tradingDB     *sql.DB
	tradingExecDB *sql.DB
}

// New wires the two DB handles. main.go opens both, passes them here.
func New(tradingDB, tradingExecDB *sql.DB) *Repo {
	return &Repo{tradingDB: tradingDB, tradingExecDB: tradingExecDB}
}

// ─────────────────────────────────────────────────────────────────────────
// Strategy config loading
// ─────────────────────────────────────────────────────────────────────────

// LoadConfig fetches one strategy by id and returns its parsed Config.
// Returns sql.ErrNoRows if the strategy doesn't exist or has been soft-deleted.
//
// Schema note: the `strategies` table PK is `strategy_id` (not `id`), and
// `trade_configs.config_extra` is a JSONB column (added by migration 013)
// holding HFT-specific fields. The typed columns on trade_configs
// (order_type, product_type, etc.) are used by NEWS/52W/MANTHAN; HFT puts
// everything in config_extra.
//
// We trust the row's user_id over anything inside the JSON to prevent
// a malformed/spoofed JSON from changing identity.
func (r *Repo) LoadConfig(ctx context.Context, strategyID string) (*state.Config, error) {
	const q = `
		SELECT s.user_id, s.strategy_type, COALESCE(tc.config_extra, '{}'::jsonb)
		FROM strategies s
		JOIN trade_configs tc ON tc.strategy_id = s.strategy_id
		WHERE s.strategy_id = $1
		  AND s.deleted_at IS NULL
		  AND s.strategy_type = 'HFT_BIDDING'`

	var (
		userID       string
		strategyType string
		configJSON   []byte
	)
	err := r.tradingDB.QueryRowContext(ctx, q, strategyID).
		Scan(&userID, &strategyType, &configJSON)
	if err != nil {
		return nil, fmt.Errorf("load strategy %s: %w", strategyID, err)
	}

	var cfg state.Config
	if err := json.Unmarshal(configJSON, &cfg); err != nil {
		return nil, fmt.Errorf("parse trade_config for %s: %w", strategyID, err)
	}

	// Trust the row, not the JSON, for identity fields.
	cfg.StrategyID = strategyID
	cfg.UserID = userID

	// Apply engine-wide safety defaults if the JSON didn't set them.
	if cfg.Mode == "" {
		cfg.Mode = state.ModePaper
	}
	if cfg.Side == "" {
		// BUY+SELL legs implied when side is missing — safe default.
		cfg.Side = "BOTH"
	}
	if cfg.TickSize <= 0 {
		cfg.TickSize = 0.05 // NSE equity default; broker_adapter falls back to this too
	}

	return &cfg, nil
}

// ─────────────────────────────────────────────────────────────────────────
// Broker credentials loading
// ─────────────────────────────────────────────────────────────────────────

// Credentials are the four fields needed for any Indira REST/WS call.
type Credentials struct {
	UserID      string
	AppID       string
	Source      string
	BearerToken string
}

// LoadCredentials reads the user's Indira creds row. trade-execution writes
// to this same table every time the user re-logs via /api/v1/auth/credentials,
// so we always get the freshest JWT here.
func (r *Repo) LoadCredentials(ctx context.Context, userID string) (*Credentials, error) {
	const q = `
		SELECT indira_user_id, indira_app_id, indira_source, indira_bearer_token
		FROM user_credentials
		WHERE user_id = $1`

	var c Credentials
	err := r.tradingExecDB.QueryRowContext(ctx, q, userID).
		Scan(&c.UserID, &c.AppID, &c.Source, &c.BearerToken)
	if err != nil {
		return nil, fmt.Errorf("load creds for %s: %w", userID, err)
	}
	return &c, nil
}

// ─────────────────────────────────────────────────────────────────────────
// Audit log writer
// ─────────────────────────────────────────────────────────────────────────

// AuditRow is the persisted shape of one event. audit.Writer batches these
// and calls InsertAuditOrder for each (Phase 1) or InsertAuditOrderBatch
// (Phase 2+ optimisation).
type AuditRow struct {
	StrategyID    string
	UserID        string
	Symbol        string
	Side          string // "B" | "S"
	Action        string // PLACE | MODIFY | CANCEL | FILL | REJECT
	ChunkSeq      int
	Qty           int
	Price         float64
	BrokerOrderID string
	BrokerStatus  string
	ErrorMsg      string
	Mode          string // PAPER | LIVE
	CreatedAt     time.Time
}

// InsertAuditOrder appends one row to hft_audit_orders. Called by audit.Writer.
// Never call this directly from a tick path — it'll block on a DB roundtrip.
func (r *Repo) InsertAuditOrder(ctx context.Context, e AuditRow) error {
	const q = `
		INSERT INTO hft_audit_orders
			(strategy_id, user_id, symbol, side, action, chunk_seq,
			 qty, price, broker_order_id, broker_status, error_msg, mode, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`
	_, err := r.tradingExecDB.ExecContext(ctx, q,
		e.StrategyID, e.UserID, e.Symbol, e.Side, e.Action, e.ChunkSeq,
		e.Qty, e.Price, nilIfEmpty(e.BrokerOrderID), nilIfEmpty(e.BrokerStatus),
		nilIfEmpty(e.ErrorMsg), e.Mode, e.CreatedAt,
	)
	return err
}

// nilIfEmpty maps "" to NULL — keeps the audit table tidy for grepping.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
