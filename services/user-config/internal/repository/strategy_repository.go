package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/apperr"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// StrategyRepository handles database operations for strategies
type StrategyRepository struct {
	db *sqlx.DB
}

// ListAllActive returns a page of active, non-deleted strategies.
// Ordered by updated_at DESC for stable pagination.
func (r *StrategyRepository) ListAllActive(ctx context.Context, limit int, offset int) ([]*models.Strategy, error) {
	if limit <= 0 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	strategies := []*models.Strategy{}
	// stopped_at IS NULL is critical: a STOPPED strategy (terminal) must NOT be
	// returned here. rules-engine's bulk-load forces Active=true on every row it
	// receives (startup/bootstrap.go), so without this guard a strategy the user
	// explicitly Stopped would resurrect and start trading again on the next
	// rules-engine restart. See docs/known_issues_user_config.md for the still-open
	// PAUSED-resurrection case, which needs a coordinated rules-engine change.
	query := `
		SELECT *
		FROM strategies
		WHERE (active = true OR strategy_type = 'MANTHAN')
		  AND deleted_at IS NULL
		  AND stopped_at IS NULL
		ORDER BY updated_at DESC
		LIMIT $1 OFFSET $2`

	if err := r.db.SelectContext(ctx, &strategies, query, limit, offset); err != nil {
		return nil, fmt.Errorf("failed to list active strategies: %w", err)
	}

	// Load related data for each strategy
	for _, strategy := range strategies {
		condition := &models.StrategyCondition{}
		condQuery := `SELECT * FROM strategy_conditions WHERE strategy_id = $1`
		err := r.db.GetContext(ctx, condition, condQuery, strategy.StrategyID)
		if err == nil {
			strategy.Conditions = condition
		}

		tradeConfig := &models.TradeConfig{}
		tradeQuery := `SELECT * FROM trade_configs WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, tradeConfig, tradeQuery, strategy.StrategyID)
		if err == nil {
			strategy.TradeConfig = tradeConfig
		}

		riskLimits := &models.RiskLimits{}
		riskQuery := `SELECT * FROM risk_limits WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, riskLimits, riskQuery, strategy.StrategyID)
		if err == nil {
			strategy.RiskLimits = riskLimits
		}
	}

	return strategies, nil
}

// NewStrategyRepository creates a new strategy repository
func NewStrategyRepository(db *sqlx.DB) *StrategyRepository {
	return &StrategyRepository{db: db}
}

// Create creates a new strategy with its conditions, trade config, and risk limits
func (r *StrategyRepository) Create(ctx context.Context, req *models.CreateStrategyRequest) (*models.Strategy, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Insert strategy
	strategyType := req.StrategyType
	if strategyType == "" {
		strategyType = models.StrategyTypeManthan
	}

	strategy := &models.Strategy{
		StrategyID:   uuid.New(),
		UserID:       req.UserID,
		StrategyName: req.StrategyName,
		Description:  req.Description,
		Active:       req.ActivateImmediately,
		StrategyType: strategyType,
		TradingMode:  req.TradingMode,
		Version:      1,
	}

	query := `
		INSERT INTO strategies (strategy_id, user_id, strategy_name, description, active, strategy_type, trading_mode, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING created_at, updated_at`

	err = tx.QueryRowxContext(ctx, query,
		strategy.StrategyID, strategy.UserID, strategy.StrategyName,
		strategy.Description, strategy.Active, strategy.StrategyType, strategy.TradingMode, strategy.Version,
	).Scan(&strategy.CreatedAt, &strategy.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert strategy: %w", err)
	}

	// Insert conditions — placeholder row only (news-specific fields dropped 2026-07-20)
	if req.Conditions != nil {
		conditionID := uuid.New()
		condQuery := `
			INSERT INTO strategy_conditions (condition_id, strategy_id)
			VALUES ($1, $2)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, condQuery, conditionID, strategy.StrategyID).
			Scan(&req.Conditions.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to insert conditions: %w", err)
		}
		req.Conditions.ConditionID = conditionID
		req.Conditions.StrategyID = strategy.StrategyID
		strategy.Conditions = req.Conditions
	}

	// Insert trade config
	if req.TradeConfig != nil {
		tradeConfigID := uuid.New()
		tradeQuery := `
			INSERT INTO trade_configs (
				trade_config_id, strategy_id, order_type, product_type, validity, quantity,
				exchange, order_side, stop_loss_pct, take_profit_pct, trailing_sl_pct, stop_loss_type, limit_price,
				position_sizing_mode, total_capital, max_positions, per_stock_amount
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, tradeQuery,
			tradeConfigID, strategy.StrategyID, req.TradeConfig.OrderType,
			req.TradeConfig.ProductType, req.TradeConfig.Validity, req.TradeConfig.Quantity,
			req.TradeConfig.Exchange, req.TradeConfig.OrderSide,
			req.TradeConfig.StopLossPct, req.TradeConfig.TakeProfitPct,
			req.TradeConfig.TrailingSLPct, req.TradeConfig.StopLossType, req.TradeConfig.LimitPrice,
			req.TradeConfig.PositionSizingMode, req.TradeConfig.TotalCapital,
			req.TradeConfig.MaxPositions, req.TradeConfig.PerStockAmount,
		).Scan(&req.TradeConfig.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to insert trade config: %w", err)
		}
		req.TradeConfig.TradeConfigID = tradeConfigID
		req.TradeConfig.StrategyID = strategy.StrategyID
		strategy.TradeConfig = req.TradeConfig
	}

	// Insert risk limits — placeholder row only (7 risk fields dropped 2026-07-30,
	// migration 017). Kept one row per strategy for envelope/read parity.
	if req.RiskLimits != nil {
		riskLimitID := uuid.New()
		riskQuery := `
			INSERT INTO risk_limits (risk_limit_id, strategy_id)
			VALUES ($1, $2)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, riskQuery, riskLimitID, strategy.StrategyID).
			Scan(&req.RiskLimits.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to insert risk limits: %w", err)
		}
		req.RiskLimits.RiskLimitID = riskLimitID
		req.RiskLimits.StrategyID = strategy.StrategyID
		strategy.RiskLimits = req.RiskLimits
	}

	// Insert into Execution Outbox
	payload, err := json.Marshal(strategy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal strategy for outbox: %w", err)
	}

	outboxQuery := `
		INSERT INTO execution_outbox (aggregate_id, event_type, payload)
		VALUES ($1, $2, $3)`

	_, err = tx.ExecContext(ctx, outboxQuery, strategy.StrategyID, "STRATEGY_CREATED", payload)
	if err != nil {
		return nil, fmt.Errorf("failed to insert into execution outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return strategy, nil
}

// GetByID retrieves a strategy by its ID
func (r *StrategyRepository) GetByID(ctx context.Context, strategyID uuid.UUID, userID string) (*models.Strategy, error) {
	strategy := &models.Strategy{}

	// Get strategy
	query := `SELECT * FROM strategies WHERE strategy_id = $1 AND user_id = $2 AND deleted_at IS NULL`
	err := r.db.GetContext(ctx, strategy, query, strategyID, userID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w (id=%s)", apperr.ErrNotFound, strategyID)
		}
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	// Get conditions
	condition := &models.StrategyCondition{}
	condQuery := `SELECT * FROM strategy_conditions WHERE strategy_id = $1`
	err = r.db.GetContext(ctx, condition, condQuery, strategyID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get conditions: %w", err)
	}
	if err == nil {
		strategy.Conditions = condition
	}

	// Get trade config
	tradeConfig := &models.TradeConfig{}
	tradeQuery := `SELECT * FROM trade_configs WHERE strategy_id = $1`
	err = r.db.GetContext(ctx, tradeConfig, tradeQuery, strategyID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get trade config: %w", err)
	}
	if err == nil {
		strategy.TradeConfig = tradeConfig
	}

	// Get risk limits
	riskLimits := &models.RiskLimits{}
	riskQuery := `SELECT * FROM risk_limits WHERE strategy_id = $1`
	err = r.db.GetContext(ctx, riskLimits, riskQuery, strategyID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get risk limits: %w", err)
	}
	if err == nil {
		strategy.RiskLimits = riskLimits
	}

	return strategy, nil
}

// ListByUserID lists all strategies for a user
func (r *StrategyRepository) ListByUserID(ctx context.Context, userID string, activeOnly bool, limit, offset int) ([]*models.Strategy, int, error) {
	strategies := []*models.Strategy{}

	// Filter out deleted strategies
	query := `SELECT * FROM strategies WHERE user_id = $1 AND deleted_at IS NULL`
	args := []interface{}{userID}

	if activeOnly {
		query += ` AND active = true`
	}

	// Get total count
	countQuery := `SELECT COUNT(*) FROM strategies WHERE user_id = $1 AND deleted_at IS NULL`
	if activeOnly {
		countQuery += ` AND active = true`
	}
	var total int
	err := r.db.GetContext(ctx, &total, countQuery, userID)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to count strategies: %w", err)
	}

	// Add pagination
	query += ` ORDER BY created_at DESC LIMIT $2 OFFSET $3`
	args = append(args, limit, offset)

	err = r.db.SelectContext(ctx, &strategies, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list strategies: %w", err)
	}

	// Load related data for each strategy
	// Optimize this with joins or batch queries in future if N+1 becomes issue
	for _, strategy := range strategies {
		// Load conditions
		condition := &models.StrategyCondition{}
		condQuery := `SELECT * FROM strategy_conditions WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, condition, condQuery, strategy.StrategyID)
		if err == nil {
			strategy.Conditions = condition
		}

		// Load trade config
		tradeConfig := &models.TradeConfig{}
		tradeQuery := `SELECT * FROM trade_configs WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, tradeConfig, tradeQuery, strategy.StrategyID)
		if err == nil {
			strategy.TradeConfig = tradeConfig
		}

		// Load risk limits
		riskLimits := &models.RiskLimits{}
		riskQuery := `SELECT * FROM risk_limits WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, riskLimits, riskQuery, strategy.StrategyID)
		if err == nil {
			strategy.RiskLimits = riskLimits
		}
	}

	return strategies, total, nil
}

// Update updates a strategy
func (r *StrategyRepository) Update(ctx context.Context, req *models.UpdateStrategyRequest) (*models.Strategy, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Update strategy with optimistic locking. RETURNING carries the FULL
	// strategies row — including the already-incremented version (N+1) and
	// strategy_type — so we can build the outbox payload + response entirely
	// from in-transaction state. Do NOT reload via r.GetByID afterwards: that
	// uses a separate, non-transactional connection which under READ COMMITTED
	// can't see this uncommitted UPDATE, so it would publish stale data at the
	// OLD version N (which rules-engine's version dedup then silently drops).
	query := `
		UPDATE strategies
		SET strategy_name = COALESCE($1, strategy_name),
		    description = COALESCE($2, description),
		    trading_mode = COALESCE($3, trading_mode),
		    version = version + 1,
		    updated_at = CURRENT_TIMESTAMP
		WHERE strategy_id = $4 AND user_id = $5 AND version = $6 AND deleted_at IS NULL
		RETURNING strategy_id, user_id, strategy_name, description, active, strategy_type, trading_mode, version, created_at, updated_at, deleted_at, stopped_at`

	strategy := &models.Strategy{}
	err = tx.QueryRowxContext(ctx, query,
		req.StrategyName, req.Description, req.TradingMode, req.StrategyID, req.UserID, req.Version,
	).StructScan(strategy)
	if err != nil {
		if err == sql.ErrNoRows {
			// 0 rows updated: either the strategy doesn't exist for this user
			// (404) or the optimistic-lock version didn't match (412). Split
			// them with a same-tx read so the caller gets the right status.
			exists, exErr := r.existsForUserTx(ctx, tx, req.StrategyID, req.UserID)
			if exErr != nil {
				return nil, fmt.Errorf("failed to update strategy: %w", exErr)
			}
			if !exists {
				return nil, apperr.ErrNotFound
			}
			var currentVersion int32
			if vErr := tx.GetContext(ctx, &currentVersion,
				`SELECT version FROM strategies WHERE strategy_id = $1 AND user_id = $2 AND deleted_at IS NULL`,
				req.StrategyID, req.UserID); vErr != nil {
				return nil, fmt.Errorf("failed to update strategy: %w", vErr)
			}
			return nil, fmt.Errorf("%w: current version is %d, request sent %d", apperr.ErrVersionConflict, currentVersion, req.Version)
		}
		return nil, fmt.Errorf("failed to update strategy: %w", err)
	}

	// Update conditions — no-op after 2026-07-20 (only placeholder row remains,
	// nothing to modify). Kept the req.Conditions guard so callers can still
	// pass a non-nil Conditions without breaking anything.

	// Update trade config if provided
	if req.TradeConfig != nil {
		tradeQuery := `
			UPDATE trade_configs
			SET order_type = $1, product_type = $2, validity = $3, quantity = $4,
			    exchange = $5, order_side = $6, stop_loss_pct = $7, take_profit_pct = $8,
			    trailing_sl_pct = $9, stop_loss_type = $10, limit_price = $11
			WHERE strategy_id = $12`

		_, err = tx.ExecContext(ctx, tradeQuery,
			req.TradeConfig.OrderType, req.TradeConfig.ProductType, req.TradeConfig.Validity,
			req.TradeConfig.Quantity, req.TradeConfig.Exchange, req.TradeConfig.OrderSide,
			req.TradeConfig.StopLossPct, req.TradeConfig.TakeProfitPct,
			req.TradeConfig.TrailingSLPct, req.TradeConfig.StopLossType, req.TradeConfig.LimitPrice, req.StrategyID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update trade config: %w", err)
		}
	}

	// Risk limits — no updatable fields after the 2026-07-30 cleanup (only the
	// placeholder row remains). Nothing to write here.

	// Load the child rows (conditions / trade config / risk limits) via the
	// SAME transaction so the outbox payload + response reflect the values
	// just written in this tx — not a stale non-transactional snapshot. The
	// MANTHAN Kafka mapper reads TradeConfig (capital, SL) off this payload,
	// so it must be present and current.
	if err := r.loadRelatedTx(ctx, tx, strategy); err != nil {
		return nil, fmt.Errorf("failed to load strategy relations for outbox: %w", err)
	}

	// Insert into Execution Outbox — UNCONDITIONAL inside the tx. If the
	// marshal or insert fails we return an error, the deferred Rollback fires,
	// and neither the strategy update nor the event is persisted. This closes
	// the old "commit the update but emit no event" divergence.
	payload, err := json.Marshal(strategy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal strategy for outbox: %w", err)
	}
	outboxQuery := `
		INSERT INTO execution_outbox (aggregate_id, event_type, payload)
		VALUES ($1, $2, $3)`
	if _, err := tx.ExecContext(ctx, outboxQuery, req.StrategyID, "STRATEGY_UPDATED", payload); err != nil {
		return nil, fmt.Errorf("failed to insert into execution outbox: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return strategy, nil
}

// existsForUserTx reports whether a non-deleted strategy row exists for
// (strategyID, userID), using the given transaction so it sees this tx's own
// uncommitted writes and can't race a concurrent create. Used to split a
// lifecycle UPDATE that affected 0 rows into "row genuinely missing"
// (apperr.ErrNotFound → 404) vs "row exists but precondition failed" (version
// mismatch / already stopped → 412).
func (r *StrategyRepository) existsForUserTx(ctx context.Context, tx *sqlx.Tx, strategyID uuid.UUID, userID string) (bool, error) {
	var exists bool
	err := tx.GetContext(ctx, &exists,
		`SELECT EXISTS(SELECT 1 FROM strategies WHERE strategy_id = $1 AND user_id = $2 AND deleted_at IS NULL)`,
		strategyID, userID)
	return exists, err
}

// loadRelatedTx hydrates a strategy's conditions / trade config / risk limits
// using the provided transaction, so callers inside an open write-tx see the
// rows as modified within that same tx (unlike r.GetByID, which reads on a
// separate connection and cannot see uncommitted changes). A missing child row
// (sql.ErrNoRows) is not an error — the field is simply left nil.
func (r *StrategyRepository) loadRelatedTx(ctx context.Context, tx *sqlx.Tx, strategy *models.Strategy) error {
	condition := &models.StrategyCondition{}
	if err := tx.GetContext(ctx, condition, `SELECT * FROM strategy_conditions WHERE strategy_id = $1`, strategy.StrategyID); err != nil {
		if err != sql.ErrNoRows {
			return fmt.Errorf("load conditions: %w", err)
		}
	} else {
		strategy.Conditions = condition
	}

	tradeConfig := &models.TradeConfig{}
	if err := tx.GetContext(ctx, tradeConfig, `SELECT * FROM trade_configs WHERE strategy_id = $1`, strategy.StrategyID); err != nil {
		if err != sql.ErrNoRows {
			return fmt.Errorf("load trade config: %w", err)
		}
	} else {
		strategy.TradeConfig = tradeConfig
	}

	riskLimits := &models.RiskLimits{}
	if err := tx.GetContext(ctx, riskLimits, `SELECT * FROM risk_limits WHERE strategy_id = $1`, strategy.StrategyID); err != nil {
		if err != sql.ErrNoRows {
			return fmt.Errorf("load risk limits: %w", err)
		}
	} else {
		strategy.RiskLimits = riskLimits
	}

	return nil
}

// Delete deletes a strategy.
//
// positionHandling is stamped on the outbox payload and propagates via
// Kafka to trade-execution so it knows whether user-config already
// placed exit orders (SQUARE_OFF_AT_MARKET) — in which case it MUST
// NOT run its classic closeStrategyPositions cleanup, otherwise those
// exit orders get cancelled before they fill. See events/config_event.go
// PositionHandling docstring for the full contract.
func (r *StrategyRepository) Delete(ctx context.Context, strategyID uuid.UUID, userID string, positionHandling string) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// STOP is a terminal transition — set stopped_at (NOT deleted_at)
	// so the row keeps showing in reads with status=STOPPED. Reject any
	// row that's ALREADY been stopped (idempotency + safety) and any
	// row that's been soft-deleted (compliance / admin escape hatch).
	// See migrations/015_add_stopped_at.sql for the design rationale.
	query := `
		UPDATE strategies
		SET stopped_at = CURRENT_TIMESTAMP, active = false, updated_at = CURRENT_TIMESTAMP, version = version + 1
		WHERE strategy_id = $1 AND user_id = $2 AND deleted_at IS NULL AND stopped_at IS NULL
		RETURNING strategy_id, version`

	var deletedID uuid.UUID
	var currentVersion int32
	err = tx.QueryRowxContext(ctx, query, strategyID, userID).Scan(&deletedID, &currentVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			// 0 rows: missing (404) vs already stopped/deleted (412).
			exists, exErr := r.existsForUserTx(ctx, tx, strategyID, userID)
			if exErr != nil {
				return fmt.Errorf("failed to stop strategy: %w", exErr)
			}
			if !exists {
				return apperr.ErrNotFound
			}
			return fmt.Errorf("%w: strategy already stopped", apperr.ErrTerminalState)
		}
		return fmt.Errorf("failed to stop strategy: %w", err)
	}

	// Insert into Execution Outbox (Deactivation/Deletion event)
	eventPayload := map[string]interface{}{
		"strategy_id":       strategyID,
		"user_id":           userID,
		"version":           uint64(currentVersion),
		"active":            false,
		"deleted":           true,
		"position_handling": positionHandling,
	}
	payload, _ := json.Marshal(eventPayload)
	outboxQuery := `
		INSERT INTO execution_outbox (aggregate_id, event_type, payload)
		VALUES ($1, $2, $3)`

	_, err = tx.ExecContext(ctx, outboxQuery, strategyID, "STRATEGY_DELETED", payload)
	if err != nil {
		return fmt.Errorf("failed to insert into execution outbox: %w", err)
	}

	return tx.Commit()
}

// Activate activates a strategy
func (r *StrategyRepository) Activate(ctx context.Context, strategyID uuid.UUID, userID string) error {
	// Re-using Update logic or similar transaction pattern
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// RESUME is only valid for a PAUSED (non-stopped, non-deleted) row.
	// stopped_at IS NULL guard makes STOP terminal — a STOPPED strategy
	// cannot be resumed (user redeploys instead). This surfaces as
	// "failed to activate strategy: sql: no rows in result set" from
	// the caller's perspective; service layer translates.
	query := `UPDATE strategies SET active = true, updated_at = CURRENT_TIMESTAMP, version = version + 1 WHERE strategy_id = $1 AND user_id = $2 AND deleted_at IS NULL AND stopped_at IS NULL RETURNING strategy_id, version`
	var updatedID uuid.UUID
	var currentVersion int32
	err = tx.QueryRowxContext(ctx, query, strategyID, userID).Scan(&updatedID, &currentVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			// 0 rows: missing (404) vs stopped/deleted — cannot resume (412).
			exists, exErr := r.existsForUserTx(ctx, tx, strategyID, userID)
			if exErr != nil {
				return fmt.Errorf("failed to activate strategy: %w", exErr)
			}
			if !exists {
				return apperr.ErrNotFound
			}
			return fmt.Errorf("%w: cannot resume a stopped strategy", apperr.ErrTerminalState)
		}
		return fmt.Errorf("failed to activate strategy: %w", err)
	}

	// Outbox
	eventPayload := map[string]interface{}{
		"strategy_id": strategyID,
		"user_id":     userID,
		"version":     uint64(currentVersion),
		"active":      true,
	}
	payload, _ := json.Marshal(eventPayload)
	outboxQuery := `INSERT INTO execution_outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`
	_, err = tx.ExecContext(ctx, outboxQuery, strategyID, "STRATEGY_ACTIVATED", payload)
	if err != nil {
		return fmt.Errorf("failed to outbox: %w", err)
	}

	return tx.Commit()
}

// Deactivate deactivates a strategy.
//
// positionHandling is stamped on the outbox payload and propagates via
// Kafka to trade-execution so it knows whether user-config already
// placed exit orders. See Delete's docstring + events/config_event.go
// for the full contract.
func (r *StrategyRepository) Deactivate(ctx context.Context, strategyID uuid.UUID, userID string, positionHandling string) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// PAUSE is only valid for a non-stopped, non-deleted row. Refusing
	// to pause a STOPPED row keeps the version chain clean and prevents
	// an unnecessary outbox event downstream.
	query := `UPDATE strategies SET active = false, updated_at = CURRENT_TIMESTAMP, version = version + 1 WHERE strategy_id = $1 AND user_id = $2 AND deleted_at IS NULL AND stopped_at IS NULL RETURNING strategy_id, version`
	var updatedID uuid.UUID
	var currentVersion int32
	err = tx.QueryRowxContext(ctx, query, strategyID, userID).Scan(&updatedID, &currentVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			// 0 rows: missing (404) vs stopped/deleted — cannot pause (412).
			exists, exErr := r.existsForUserTx(ctx, tx, strategyID, userID)
			if exErr != nil {
				return fmt.Errorf("failed to deactivate strategy: %w", exErr)
			}
			if !exists {
				return apperr.ErrNotFound
			}
			return fmt.Errorf("%w: cannot pause a stopped strategy", apperr.ErrTerminalState)
		}
		return fmt.Errorf("failed to deactivate strategy: %w", err)
	}

	// Outbox
	eventPayload := map[string]interface{}{
		"strategy_id":       strategyID,
		"user_id":           userID,
		"version":           uint64(currentVersion),
		"active":            false,
		"position_handling": positionHandling,
	}
	payload, _ := json.Marshal(eventPayload)
	outboxQuery := `INSERT INTO execution_outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`
	_, err = tx.ExecContext(ctx, outboxQuery, strategyID, "STRATEGY_DEACTIVATED", payload)
	if err != nil {
		return fmt.Errorf("failed to outbox: %w", err)
	}

	return tx.Commit()
}

// GetByIDs retrieves multiple strategies by their IDs
func (r *StrategyRepository) GetByIDs(ctx context.Context, strategyIDs []uuid.UUID) ([]*models.Strategy, error) {
	if len(strategyIDs) == 0 {
		return []*models.Strategy{}, nil
	}

	strategies := []*models.Strategy{}
	query := `SELECT * FROM strategies WHERE strategy_id = ANY($1) AND deleted_at IS NULL`

	err := r.db.SelectContext(ctx, &strategies, query, pq.Array(strategyIDs))
	if err != nil {
		return nil, fmt.Errorf("failed to get strategies: %w", err)
	}

	for _, strategy := range strategies {
		// Load conditions
		condition := &models.StrategyCondition{}
		condQuery := `SELECT * FROM strategy_conditions WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, condition, condQuery, strategy.StrategyID)
		if err == nil {
			strategy.Conditions = condition
		}

		// Load trade config
		tradeConfig := &models.TradeConfig{}
		tradeQuery := `SELECT * FROM trade_configs WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, tradeConfig, tradeQuery, strategy.StrategyID)
		if err == nil {
			strategy.TradeConfig = tradeConfig
		}

		// Load risk limits
		riskLimits := &models.RiskLimits{}
		riskQuery := `SELECT * FROM risk_limits WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, riskLimits, riskQuery, strategy.StrategyID)
		if err == nil {
			strategy.RiskLimits = riskLimits
		}
	}

	return strategies, nil
}

// ListPendingOutboxEvents retrieves pending outbox events
func (r *StrategyRepository) ListPendingOutboxEvents(ctx context.Context, limit int) ([]*models.ExecutionOutbox, error) {
	events := []*models.ExecutionOutbox{}
	query := `
		SELECT * FROM execution_outbox
		WHERE processed = false
		ORDER BY created_at ASC
		LIMIT $1`

	err := r.db.SelectContext(ctx, &events, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pending outbox events: %w", err)
	}
	return events, nil
}

// MarkOutboxEventsProcessed marks events as processed
func (r *StrategyRepository) MarkOutboxEventsProcessed(ctx context.Context, eventIDs []int64) error {
	if len(eventIDs) == 0 {
		return nil
	}

	query := `UPDATE execution_outbox SET processed = true WHERE id = ANY($1)`
	_, err := r.db.ExecContext(ctx, query, pq.Array(eventIDs))
	if err != nil {
		return fmt.Errorf("failed to mark events as processed: %w", err)
	}
	return nil
}
