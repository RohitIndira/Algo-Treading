package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

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
	query := `
		SELECT *
		FROM strategies
		WHERE active = true AND deleted_at IS NULL
		ORDER BY updated_at DESC
		LIMIT $1 OFFSET $2`

	if err := r.db.SelectContext(ctx, &strategies, query, limit, offset); err != nil {
		return nil, fmt.Errorf("failed to list active strategies: %w", err)
	}

	// Load related data for each strategy
	for _, strategy := range strategies {
		condition := &models.StrategyCondition{}
		condQuery := `SELECT condition_id, strategy_id, match_all_news, impact_score_min, impact_score_max, sentiments, news_categories, min_market_cap, max_market_cap, market_cap_types, min_price_change_pct, max_price_change_pct, exchanges, created_at FROM strategy_conditions WHERE strategy_id = $1`
		err := r.db.GetContext(ctx, condition, condQuery, strategy.StrategyID)
		if err == nil {
			strategy.Conditions = condition
		}

		tradeConfig := &models.TradeConfig{}
		tradeQuery := `SELECT trade_config_id, strategy_id, order_type, product_type, validity, quantity, exchange, order_side, stop_loss_pct, take_profit_pct, trailing_sl_pct, stop_loss_type, limit_price, take_profit_type, trade_window_start, trade_window_end, created_at FROM trade_configs WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, tradeConfig, tradeQuery, strategy.StrategyID)
		if err == nil {
			r.loadMultiLevelConfig(ctx, tradeConfig)
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
	strategy := &models.Strategy{
		StrategyID:   uuid.New(),
		UserID:       req.UserID,
		StrategyName: req.StrategyName,
		Description:  req.Description,
		Active:       req.ActivateImmediately,
		TradingMode:  req.TradingMode,
		Version:      1,
	}

	query := `
		INSERT INTO strategies (strategy_id, user_id, strategy_name, description, active, trading_mode, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING created_at, updated_at`

	err = tx.QueryRowxContext(ctx, query,
		strategy.StrategyID, strategy.UserID, strategy.StrategyName,
		strategy.Description, strategy.Active, strategy.TradingMode, strategy.Version,
	).Scan(&strategy.CreatedAt, &strategy.UpdatedAt)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, fmt.Errorf("strategy name %q already exists for this user", strategy.StrategyName)
		}
		return nil, fmt.Errorf("failed to insert strategy: %w", err)
	}

	// Insert conditions
	if req.Conditions != nil {
		conditionID := uuid.New()
		condQuery := `
			INSERT INTO strategy_conditions (
				condition_id, strategy_id, match_all_news, impact_score_min, impact_score_max,
				sentiments, news_categories, min_market_cap, max_market_cap,
				market_cap_types, min_price_change_pct, max_price_change_pct, exchanges
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, condQuery,
			conditionID, strategy.StrategyID, req.Conditions.MatchAllNews,
			req.Conditions.ImpactScoreMin, req.Conditions.ImpactScoreMax,
			req.Conditions.Sentiments, req.Conditions.Categories,
			req.Conditions.MinMarketCap, req.Conditions.MaxMarketCap, req.Conditions.MarketCapTypes,
			req.Conditions.MinPriceChangePct, req.Conditions.MaxPriceChangePct,
			req.Conditions.Exchanges,
		).Scan(&req.Conditions.CreatedAt)
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

		mlSLStr, err := marshalMultiLevel(req.TradeConfig.MultiLevelSL)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal multi_level_sl: %w", err)
		}
		mlTPStr, err := marshalMultiLevel(req.TradeConfig.MultiLevelTP)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal multi_level_tp: %w", err)
		}

		tradeQuery := `
			INSERT INTO trade_configs (
				trade_config_id, strategy_id, order_type, product_type, validity, quantity,
				exchange, order_side, stop_loss_pct, take_profit_pct, trailing_sl_pct,
				stop_loss_type, limit_price, take_profit_type, multi_level_sl, multi_level_tp,
				trade_window_start, trade_window_end
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, tradeQuery,
			tradeConfigID, strategy.StrategyID, req.TradeConfig.OrderType,
			req.TradeConfig.ProductType, req.TradeConfig.Validity, req.TradeConfig.Quantity,
			req.TradeConfig.Exchange, req.TradeConfig.OrderSide,
			req.TradeConfig.StopLossPct, req.TradeConfig.TakeProfitPct,
			req.TradeConfig.TrailingSLPct, req.TradeConfig.StopLossType, req.TradeConfig.LimitPrice,
			req.TradeConfig.TakeProfitType, mlSLStr, mlTPStr,
			req.TradeConfig.TradeWindowStart, req.TradeConfig.TradeWindowEnd,
		).Scan(&req.TradeConfig.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to insert trade config: %w", err)
		}
		req.TradeConfig.TradeConfigID = tradeConfigID
		req.TradeConfig.StrategyID = strategy.StrategyID
		strategy.TradeConfig = req.TradeConfig
	}

	// Insert risk limits
	if req.RiskLimits != nil {
		riskLimitID := uuid.New()
		riskQuery := `
			INSERT INTO risk_limits (
				risk_limit_id, strategy_id, max_daily_trades, max_per_trade_risk,
				max_portfolio_exposure_pct, max_loss_per_day, enable_risk_checks,
				enable_auto_square_off, auto_square_off_time
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, riskQuery,
			riskLimitID, strategy.StrategyID, req.RiskLimits.MaxDailyTrades,
			req.RiskLimits.MaxPerTradeRisk, req.RiskLimits.MaxPortfolioExposurePct,
			req.RiskLimits.MaxLossPerDay, req.RiskLimits.EnableRiskChecks,
			req.RiskLimits.EnableAutoSquareOff, req.RiskLimits.AutoSquareOffTime,
		).Scan(&req.RiskLimits.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to insert risk limits: %w", err)
		}
		req.RiskLimits.RiskLimitID = riskLimitID
		req.RiskLimits.StrategyID = strategy.StrategyID
		strategy.RiskLimits = req.RiskLimits
	}

	// Insert into Execution Outbox
	payloadBytes, err := json.Marshal(strategy)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal strategy for outbox: %w", err)
	}

	outboxQuery := `
		INSERT INTO execution_outbox (aggregate_id, event_type, payload)
		VALUES ($1, $2, $3)`

	_, err = tx.ExecContext(ctx, outboxQuery, strategy.StrategyID, "STRATEGY_CREATED", string(payloadBytes))
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
			return nil, fmt.Errorf("strategy not found")
		}
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	// Get conditions
	condition := &models.StrategyCondition{}
	condQuery := `SELECT condition_id, strategy_id, match_all_news, impact_score_min, impact_score_max, sentiments, news_categories, min_market_cap, max_market_cap, market_cap_types, min_price_change_pct, max_price_change_pct, exchanges, created_at FROM strategy_conditions WHERE strategy_id = $1`
	err = r.db.GetContext(ctx, condition, condQuery, strategyID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get conditions: %w", err)
	}
	if err == nil {
		strategy.Conditions = condition
	}

	// Get trade config
	tradeConfig := &models.TradeConfig{}
	tradeQuery := `SELECT trade_config_id, strategy_id, order_type, product_type, validity, quantity, exchange, order_side, stop_loss_pct, take_profit_pct, trailing_sl_pct, stop_loss_type, limit_price, take_profit_type, trade_window_start, trade_window_end, created_at FROM trade_configs WHERE strategy_id = $1`
	err = r.db.GetContext(ctx, tradeConfig, tradeQuery, strategyID)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get trade config: %w", err)
	}
	if err == nil {
		r.loadMultiLevelConfig(ctx, tradeConfig)
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
		condQuery := `SELECT condition_id, strategy_id, match_all_news, impact_score_min, impact_score_max, sentiments, news_categories, min_market_cap, max_market_cap, market_cap_types, min_price_change_pct, max_price_change_pct, exchanges, created_at FROM strategy_conditions WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, condition, condQuery, strategy.StrategyID)
		if err == nil {
			strategy.Conditions = condition
		}

		// Load trade config
		tradeConfig := &models.TradeConfig{}
		tradeQuery := `SELECT trade_config_id, strategy_id, order_type, product_type, validity, quantity, exchange, order_side, stop_loss_pct, take_profit_pct, trailing_sl_pct, stop_loss_type, limit_price, take_profit_type, trade_window_start, trade_window_end, created_at FROM trade_configs WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, tradeConfig, tradeQuery, strategy.StrategyID)
		if err == nil {
			r.loadMultiLevelConfig(ctx, tradeConfig)
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

	// Update strategy with optimistic locking
	query := `
		UPDATE strategies
		SET strategy_name = COALESCE($1, strategy_name),
		    description = COALESCE($2, description),
		    trading_mode = COALESCE($3, trading_mode),
		    version = version + 1,
		    updated_at = CURRENT_TIMESTAMP
		WHERE strategy_id = $4 AND user_id = $5 AND version = $6 AND deleted_at IS NULL
		RETURNING strategy_id, user_id, strategy_name, description, active, trading_mode, version, created_at, updated_at`

	strategy := &models.Strategy{}
	err = tx.QueryRowxContext(ctx, query,
		req.StrategyName, req.Description, req.TradingMode, req.StrategyID, req.UserID, req.Version,
	).StructScan(strategy)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("strategy not found or version mismatch")
		}
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, fmt.Errorf("strategy name %q already exists for this user", req.StrategyName)
		}
		return nil, fmt.Errorf("failed to update strategy: %w", err)
	}

	// Update conditions if provided
	if req.Conditions != nil {
		condQuery := `
			UPDATE strategy_conditions
			SET match_all_news = $1, impact_score_min = $2, impact_score_max = $3,
			    sentiments = $4, news_categories = $5,
			    min_market_cap = $6, max_market_cap = $7, market_cap_types = $8,
			    min_price_change_pct = $9, max_price_change_pct = $10,
			    exchanges = $11
			WHERE strategy_id = $12`

		_, err = tx.ExecContext(ctx, condQuery,
			req.Conditions.MatchAllNews, req.Conditions.ImpactScoreMin, req.Conditions.ImpactScoreMax,
			req.Conditions.Sentiments, req.Conditions.Categories,
			req.Conditions.MinMarketCap, req.Conditions.MaxMarketCap, req.Conditions.MarketCapTypes,
			req.Conditions.MinPriceChangePct, req.Conditions.MaxPriceChangePct,
			req.Conditions.Exchanges, req.StrategyID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update conditions: %w", err)
		}
	}

	// Update trade config if provided
	if req.TradeConfig != nil {
		mlSLStr, err := marshalMultiLevel(req.TradeConfig.MultiLevelSL)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal multi_level_sl: %w", err)
		}
		mlTPStr, err := marshalMultiLevel(req.TradeConfig.MultiLevelTP)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal multi_level_tp: %w", err)
		}

		tradeQuery := `
			UPDATE trade_configs
			SET order_type = $1, product_type = $2, validity = $3, quantity = $4,
			    exchange = $5, order_side = $6, stop_loss_pct = $7, take_profit_pct = $8,
			    trailing_sl_pct = $9, stop_loss_type = $10, limit_price = $11,
			    take_profit_type = $12, multi_level_sl = $13, multi_level_tp = $14,
			    trade_window_start = $15, trade_window_end = $16
			WHERE strategy_id = $17`

		_, err = tx.ExecContext(ctx, tradeQuery,
			req.TradeConfig.OrderType, req.TradeConfig.ProductType, req.TradeConfig.Validity,
			req.TradeConfig.Quantity, req.TradeConfig.Exchange, req.TradeConfig.OrderSide,
			req.TradeConfig.StopLossPct, req.TradeConfig.TakeProfitPct,
			req.TradeConfig.TrailingSLPct, req.TradeConfig.StopLossType, req.TradeConfig.LimitPrice,
			req.TradeConfig.TakeProfitType, mlSLStr, mlTPStr,
			req.TradeConfig.TradeWindowStart, req.TradeConfig.TradeWindowEnd, req.StrategyID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update trade config: %w", err)
		}
	}

	// Update risk limits if provided
	if req.RiskLimits != nil {
		riskQuery := `
			UPDATE risk_limits
			SET max_daily_trades = $1, max_per_trade_risk = $2, max_portfolio_exposure_pct = $3,
			    max_loss_per_day = $4, enable_risk_checks = $5, enable_auto_square_off = $6,
			    auto_square_off_time = $7
			WHERE strategy_id = $8`

		_, err = tx.ExecContext(ctx, riskQuery,
			req.RiskLimits.MaxDailyTrades, req.RiskLimits.MaxPerTradeRisk,
			req.RiskLimits.MaxPortfolioExposurePct, req.RiskLimits.MaxLossPerDay,
			req.RiskLimits.EnableRiskChecks, req.RiskLimits.EnableAutoSquareOff,
			req.RiskLimits.AutoSquareOffTime, req.StrategyID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update risk limits: %w", err)
		}
	}

	// Reload full strategy for Outbox
	fullStrategy, err := r.GetByID(ctx, req.StrategyID, req.UserID)
	if err == nil {
		// Insert into Execution Outbox
		payloadBytes, err := json.Marshal(fullStrategy)
		if err == nil {
			outboxQuery := `
				INSERT INTO execution_outbox (aggregate_id, event_type, payload)
				VALUES ($1, $2, $3)`

			_, ctxErr := tx.ExecContext(ctx, outboxQuery, req.StrategyID, "STRATEGY_UPDATED", string(payloadBytes))
			if ctxErr != nil {
				return nil, fmt.Errorf("failed to insert into execution outbox: %w", ctxErr)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return fullStrategy, nil
}

// Delete deletes a strategy
func (r *StrategyRepository) Delete(ctx context.Context, strategyID uuid.UUID, userID string) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Soft delete: Update deleted_at and set active = false
	query := `
		UPDATE strategies 
		SET deleted_at = CURRENT_TIMESTAMP, active = false, updated_at = CURRENT_TIMESTAMP, version = version + 1 
		WHERE strategy_id = $1 AND user_id = $2 
		RETURNING strategy_id, version`

	var deletedID uuid.UUID
	var currentVersion int32
	err = tx.QueryRowxContext(ctx, query, strategyID, userID).Scan(&deletedID, &currentVersion)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("strategy not found")
		}
		return fmt.Errorf("failed to delete strategy: %w", err)
	}

	// Insert into Execution Outbox (Deactivation/Deletion event)
	eventPayload := map[string]interface{}{
		"strategy_id": strategyID,
		"user_id":     userID,
		"version":     uint64(currentVersion),
		"active":      false,
		"deleted":     true,
	}
	payloadBytes, _ := json.Marshal(eventPayload)
	outboxQuery := `
		INSERT INTO execution_outbox (aggregate_id, event_type, payload)
		VALUES ($1, $2, $3)`

	_, err = tx.ExecContext(ctx, outboxQuery, strategyID, "STRATEGY_DELETED", string(payloadBytes))
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

	query := `UPDATE strategies SET active = true, updated_at = CURRENT_TIMESTAMP, version = version + 1 WHERE strategy_id = $1 AND user_id = $2 AND deleted_at IS NULL RETURNING strategy_id, version`
	var updatedID uuid.UUID
	var currentVersion int32
	err = tx.QueryRowxContext(ctx, query, strategyID, userID).Scan(&updatedID, &currentVersion)
	if err != nil {
		return fmt.Errorf("failed to activate strategy: %w", err)
	}

	// Outbox
	eventPayload := map[string]interface{}{
		"strategy_id": strategyID,
		"user_id":     userID,
		"version":     uint64(currentVersion),
		"active":      true,
	}
	payloadBytes, _ := json.Marshal(eventPayload)
	outboxQuery := `INSERT INTO execution_outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`
	_, err = tx.ExecContext(ctx, outboxQuery, strategyID, "STRATEGY_ACTIVATED", string(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to outbox: %w", err)
	}

	return tx.Commit()
}

// Deactivate deactivates a strategy
func (r *StrategyRepository) Deactivate(ctx context.Context, strategyID uuid.UUID, userID string) error {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	query := `UPDATE strategies SET active = false, updated_at = CURRENT_TIMESTAMP, version = version + 1 WHERE strategy_id = $1 AND user_id = $2 AND deleted_at IS NULL RETURNING strategy_id, version`
	var updatedID uuid.UUID
	var currentVersion int32
	err = tx.QueryRowxContext(ctx, query, strategyID, userID).Scan(&updatedID, &currentVersion)
	if err != nil {
		return fmt.Errorf("failed to deactivate strategy: %w", err)
	}

	// Outbox
	eventPayload := map[string]interface{}{
		"strategy_id": strategyID,
		"user_id":     userID,
		"version":     uint64(currentVersion),
		"active":      false,
	}
	payloadBytes, _ := json.Marshal(eventPayload)
	outboxQuery := `INSERT INTO execution_outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`
	_, err = tx.ExecContext(ctx, outboxQuery, strategyID, "STRATEGY_DEACTIVATED", string(payloadBytes))
	if err != nil {
		return fmt.Errorf("failed to outbox: %w", err)
	}

	return tx.Commit()
}

// DeactivateAllActive deactivates every active, non-deleted strategy in a single
// transaction and writes one STRATEGY_DEACTIVATED outbox entry per strategy.
// Returns the number of strategies that were deactivated.
func (r *StrategyRepository) DeactivateAllActive(ctx context.Context) (int, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Bulk-deactivate all active strategies and capture their IDs/user/version.
	type deactivatedRow struct {
		StrategyID uuid.UUID `db:"strategy_id"`
		UserID     string    `db:"user_id"`
		Version    int64     `db:"version"`
	}

	updateQuery := `
		UPDATE strategies
		SET active = false, updated_at = CURRENT_TIMESTAMP, version = version + 1
		WHERE active = true AND deleted_at IS NULL
		RETURNING strategy_id, user_id, version`

	rows := []deactivatedRow{}
	if err := tx.SelectContext(ctx, &rows, updateQuery); err != nil {
		return 0, fmt.Errorf("failed to bulk-deactivate strategies: %w", err)
	}

	if len(rows) == 0 {
		return 0, tx.Commit()
	}

	// Insert one outbox event per deactivated strategy.
	outboxQuery := `INSERT INTO execution_outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`
	for _, row := range rows {
		payloadBytes, _ := json.Marshal(map[string]interface{}{
			"strategy_id": row.StrategyID,
			"user_id":     row.UserID,
			"version":     row.Version,
			"active":      false,
		})
		if _, err := tx.ExecContext(ctx, outboxQuery, row.StrategyID, "STRATEGY_DEACTIVATED", string(payloadBytes)); err != nil {
			return 0, fmt.Errorf("failed to insert outbox entry for strategy %s: %w", row.StrategyID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit bulk deactivation: %w", err)
	}

	return len(rows), nil
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
		condQuery := `SELECT condition_id, strategy_id, match_all_news, impact_score_min, impact_score_max, sentiments, news_categories, min_market_cap, max_market_cap, market_cap_types, min_price_change_pct, max_price_change_pct, exchanges, created_at FROM strategy_conditions WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, condition, condQuery, strategy.StrategyID)
		if err == nil {
			strategy.Conditions = condition
		}

		// Load trade config
		tradeConfig := &models.TradeConfig{}
		tradeQuery := `SELECT trade_config_id, strategy_id, order_type, product_type, validity, quantity, exchange, order_side, stop_loss_pct, take_profit_pct, trailing_sl_pct, stop_loss_type, limit_price, take_profit_type, trade_window_start, trade_window_end, created_at FROM trade_configs WHERE strategy_id = $1`
		err = r.db.GetContext(ctx, tradeConfig, tradeQuery, strategy.StrategyID)
		if err == nil {
			r.loadMultiLevelConfig(ctx, tradeConfig)
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

// ── Multi-Level Helpers ───────────────────────────────────────────────────────

// marshalMultiLevel converts a slice of MultiLevelExitLevel to a JSON string
// suitable for insertion into a JSONB column. Returns nil when levels is empty
// (which stores NULL in the DB). A *string is returned so pq sends the value
// as text rather than binary, which PostgreSQL accepts for JSONB columns.
func marshalMultiLevel(levels []models.MultiLevelExitLevel) (*string, error) {
	if len(levels) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(levels)
	if err != nil {
		return nil, err
	}
	s := string(b)
	return &s, nil
}

// loadMultiLevelConfig reads the multi_level_sl / multi_level_tp JSONB columns
// for a TradeConfig that was loaded via SELECT *. Because the model fields have
// db:"-" (sqlx skips them), we fetch the raw JSON separately and unmarshal.
func (r *StrategyRepository) loadMultiLevelConfig(ctx context.Context, tc *models.TradeConfig) {
	if tc == nil {
		return
	}

	var raw struct {
		MultiLevelSL []byte `db:"multi_level_sl"`
		MultiLevelTP []byte `db:"multi_level_tp"`
	}
	query := `SELECT multi_level_sl, multi_level_tp FROM trade_configs WHERE strategy_id = $1`
	if err := r.db.GetContext(ctx, &raw, query, tc.StrategyID); err != nil {
		return // non-fatal; columns may not exist on older DB instances
	}
	if len(raw.MultiLevelSL) > 0 {
		_ = json.Unmarshal(raw.MultiLevelSL, &tc.MultiLevelSL)
	}
	if len(raw.MultiLevelTP) > 0 {
		_ = json.Unmarshal(raw.MultiLevelTP, &tc.MultiLevelTP)
	}
}
