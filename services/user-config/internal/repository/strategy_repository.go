package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

// StrategyRepository handles database operations for strategies
type StrategyRepository struct {
	db *sqlx.DB
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
		Version:      1,
	}

	query := `
		INSERT INTO strategies (strategy_id, user_id, strategy_name, description, active, version)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at, updated_at`

	err = tx.QueryRowxContext(ctx, query,
		strategy.StrategyID, strategy.UserID, strategy.StrategyName,
		strategy.Description, strategy.Active, strategy.Version,
	).Scan(&strategy.CreatedAt, &strategy.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("failed to insert strategy: %w", err)
	}

	// Insert conditions
	if req.Conditions != nil {
		conditionID := uuid.New()
		condQuery := `
			INSERT INTO strategy_conditions (
				condition_id, strategy_id, impact_score_threshold, sentiments, categories,
				stock_codes, price_range_min, price_range_max, volume_threshold,
				pct_change_threshold, exchanges
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, condQuery,
			conditionID, strategy.StrategyID, req.Conditions.ImpactScoreThreshold,
			req.Conditions.Sentiments, req.Conditions.Categories, req.Conditions.StockCodes,
			req.Conditions.PriceRangeMin, req.Conditions.PriceRangeMax,
			req.Conditions.VolumeThreshold, req.Conditions.PctChangeThreshold,
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
		tradeQuery := `
			INSERT INTO trade_configs (
				trade_config_id, strategy_id, order_type, quantity, max_position_size,
				stop_loss_pct, take_profit_pct, exchange, order_side, limit_price, validity
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, tradeQuery,
			tradeConfigID, strategy.StrategyID, req.TradeConfig.OrderType,
			req.TradeConfig.Quantity, req.TradeConfig.MaxPositionSize,
			req.TradeConfig.StopLossPct, req.TradeConfig.TakeProfitPct,
			req.TradeConfig.Exchange, req.TradeConfig.OrderSide,
			req.TradeConfig.LimitPrice, req.TradeConfig.Validity,
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
				risk_limit_id, strategy_id, max_daily_trades, max_loss_per_day,
				position_sizing, max_portfolio_exposure_pct, max_per_trade_risk, enable_risk_checks
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING created_at`

		err = tx.QueryRowxContext(ctx, riskQuery,
			riskLimitID, strategy.StrategyID, req.RiskLimits.MaxDailyTrades,
			req.RiskLimits.MaxLossPerDay, req.RiskLimits.PositionSizing,
			req.RiskLimits.MaxPortfolioExposurePct, req.RiskLimits.MaxPerTradeRisk,
			req.RiskLimits.EnableRiskChecks,
		).Scan(&req.RiskLimits.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to insert risk limits: %w", err)
		}
		req.RiskLimits.RiskLimitID = riskLimitID
		req.RiskLimits.StrategyID = strategy.StrategyID
		strategy.RiskLimits = req.RiskLimits
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
	query := `SELECT * FROM strategies WHERE strategy_id = $1 AND user_id = $2`
	err := r.db.GetContext(ctx, strategy, query, strategyID, userID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("strategy not found")
		}
		return nil, fmt.Errorf("failed to get strategy: %w", err)
	}

	// Get conditions
	condition := &models.StrategyCondition{}
	condQuery := `SELECT condition_id, strategy_id, impact_score_threshold, sentiments, categories, 
		stock_codes, price_range_min, price_range_max, volume_threshold, pct_change_threshold, 
		exchanges, created_at FROM strategy_conditions WHERE strategy_id = $1`
	err = r.db.QueryRowContext(ctx, condQuery, strategyID).Scan(
		&condition.ConditionID,
		&condition.StrategyID,
		&condition.ImpactScoreThreshold,
		&condition.Sentiments,
		&condition.Categories,
		&condition.StockCodes,
		&condition.PriceRangeMin,
		&condition.PriceRangeMax,
		&condition.VolumeThreshold,
		&condition.PctChangeThreshold,
		&condition.Exchanges,
		&condition.CreatedAt,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get conditions: %w", err)
	}
	if err == nil {
		strategy.Conditions = condition
	}

	// Get trade config
	tradeConfig := &models.TradeConfig{}
	tradeQuery := `SELECT trade_config_id, strategy_id, order_type, quantity, max_position_size, 
		stop_loss_pct, take_profit_pct, exchange, order_side, limit_price, validity, created_at 
		FROM trade_configs WHERE strategy_id = $1`
	err = r.db.QueryRowContext(ctx, tradeQuery, strategyID).Scan(
		&tradeConfig.TradeConfigID,
		&tradeConfig.StrategyID,
		&tradeConfig.OrderType,
		&tradeConfig.Quantity,
		&tradeConfig.MaxPositionSize,
		&tradeConfig.StopLossPct,
		&tradeConfig.TakeProfitPct,
		&tradeConfig.Exchange,
		&tradeConfig.OrderSide,
		&tradeConfig.LimitPrice,
		&tradeConfig.Validity,
		&tradeConfig.CreatedAt,
	)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get trade config: %w", err)
	}
	if err == nil {
		strategy.TradeConfig = tradeConfig
	}

	// Get risk limits
	riskLimits := &models.RiskLimits{}
	riskQuery := `SELECT risk_limit_id, strategy_id, max_daily_trades, max_loss_per_day, 
		position_sizing, max_portfolio_exposure_pct, max_per_trade_risk, enable_risk_checks, created_at 
		FROM risk_limits WHERE strategy_id = $1`
	err = r.db.QueryRowContext(ctx, riskQuery, strategyID).Scan(
		&riskLimits.RiskLimitID,
		&riskLimits.StrategyID,
		&riskLimits.MaxDailyTrades,
		&riskLimits.MaxLossPerDay,
		&riskLimits.PositionSizing,
		&riskLimits.MaxPortfolioExposurePct,
		&riskLimits.MaxPerTradeRisk,
		&riskLimits.EnableRiskChecks,
		&riskLimits.CreatedAt,
	)
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

	query := `SELECT * FROM strategies WHERE user_id = $1`
	args := []interface{}{userID}

	if activeOnly {
		query += ` AND active = true`
	}

	// Get total count
	countQuery := `SELECT COUNT(*) FROM strategies WHERE user_id = $1`
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
	for _, strategy := range strategies {
		// Load conditions
		condition := &models.StrategyCondition{}
		condQuery := `SELECT condition_id, strategy_id, impact_score_threshold, sentiments, categories, 
			stock_codes, price_range_min, price_range_max, volume_threshold, pct_change_threshold, 
			exchanges, created_at FROM strategy_conditions WHERE strategy_id = $1`
		err = r.db.QueryRowContext(ctx, condQuery, strategy.StrategyID).Scan(
			&condition.ConditionID,
			&condition.StrategyID,
			&condition.ImpactScoreThreshold,
			&condition.Sentiments,
			&condition.Categories,
			&condition.StockCodes,
			&condition.PriceRangeMin,
			&condition.PriceRangeMax,
			&condition.VolumeThreshold,
			&condition.PctChangeThreshold,
			&condition.Exchanges,
			&condition.CreatedAt,
		)
		if err == nil {
			strategy.Conditions = condition
		}

		// Load trade config
		tradeConfig := &models.TradeConfig{}
		tradeQuery := `SELECT trade_config_id, strategy_id, order_type, quantity, max_position_size, 
			stop_loss_pct, take_profit_pct, exchange, order_side, limit_price, validity, created_at 
			FROM trade_configs WHERE strategy_id = $1`
		err = r.db.QueryRowContext(ctx, tradeQuery, strategy.StrategyID).Scan(
			&tradeConfig.TradeConfigID,
			&tradeConfig.StrategyID,
			&tradeConfig.OrderType,
			&tradeConfig.Quantity,
			&tradeConfig.MaxPositionSize,
			&tradeConfig.StopLossPct,
			&tradeConfig.TakeProfitPct,
			&tradeConfig.Exchange,
			&tradeConfig.OrderSide,
			&tradeConfig.LimitPrice,
			&tradeConfig.Validity,
			&tradeConfig.CreatedAt,
		)
		if err == nil {
			strategy.TradeConfig = tradeConfig
		}

		// Load risk limits
		riskLimits := &models.RiskLimits{}
		riskQuery := `SELECT risk_limit_id, strategy_id, max_daily_trades, max_loss_per_day, 
			position_sizing, max_portfolio_exposure_pct, max_per_trade_risk, enable_risk_checks, created_at 
			FROM risk_limits WHERE strategy_id = $1`
		err = r.db.QueryRowContext(ctx, riskQuery, strategy.StrategyID).Scan(
			&riskLimits.RiskLimitID,
			&riskLimits.StrategyID,
			&riskLimits.MaxDailyTrades,
			&riskLimits.MaxLossPerDay,
			&riskLimits.PositionSizing,
			&riskLimits.MaxPortfolioExposurePct,
			&riskLimits.MaxPerTradeRisk,
			&riskLimits.EnableRiskChecks,
			&riskLimits.CreatedAt,
		)
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
		    version = version + 1,
		    updated_at = CURRENT_TIMESTAMP
		WHERE strategy_id = $3 AND user_id = $4 AND version = $5
		RETURNING strategy_id, user_id, strategy_name, description, active, version, created_at, updated_at`

	strategy := &models.Strategy{}
	err = tx.QueryRowxContext(ctx, query,
		req.StrategyName, req.Description, req.StrategyID, req.UserID, req.Version,
	).StructScan(strategy)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("strategy not found or version mismatch")
		}
		return nil, fmt.Errorf("failed to update strategy: %w", err)
	}

	// Update conditions if provided
	if req.Conditions != nil {
		condQuery := `
			UPDATE strategy_conditions
			SET impact_score_threshold = $1, sentiments = $2, categories = $3,
			    stock_codes = $4, price_range_min = $5, price_range_max = $6,
			    volume_threshold = $7, pct_change_threshold = $8, exchanges = $9
			WHERE strategy_id = $10`

		_, err = tx.ExecContext(ctx, condQuery,
			req.Conditions.ImpactScoreThreshold, req.Conditions.Sentiments,
			req.Conditions.Categories, req.Conditions.StockCodes,
			req.Conditions.PriceRangeMin, req.Conditions.PriceRangeMax,
			req.Conditions.VolumeThreshold, req.Conditions.PctChangeThreshold,
			req.Conditions.Exchanges, req.StrategyID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update conditions: %w", err)
		}
	}

	// Update trade config if provided
	if req.TradeConfig != nil {
		tradeQuery := `
			UPDATE trade_configs
			SET order_type = $1, quantity = $2, max_position_size = $3,
			    stop_loss_pct = $4, take_profit_pct = $5, exchange = $6,
			    order_side = $7, limit_price = $8, validity = $9
			WHERE strategy_id = $10`

		_, err = tx.ExecContext(ctx, tradeQuery,
			req.TradeConfig.OrderType, req.TradeConfig.Quantity,
			req.TradeConfig.MaxPositionSize, req.TradeConfig.StopLossPct,
			req.TradeConfig.TakeProfitPct, req.TradeConfig.Exchange,
			req.TradeConfig.OrderSide, req.TradeConfig.LimitPrice,
			req.TradeConfig.Validity, req.StrategyID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update trade config: %w", err)
		}
	}

	// Update risk limits if provided
	if req.RiskLimits != nil {
		riskQuery := `
			UPDATE risk_limits
			SET max_daily_trades = $1, max_loss_per_day = $2, position_sizing = $3,
			    max_portfolio_exposure_pct = $4, max_per_trade_risk = $5, enable_risk_checks = $6
			WHERE strategy_id = $7`

		_, err = tx.ExecContext(ctx, riskQuery,
			req.RiskLimits.MaxDailyTrades, req.RiskLimits.MaxLossPerDay,
			req.RiskLimits.PositionSizing, req.RiskLimits.MaxPortfolioExposurePct,
			req.RiskLimits.MaxPerTradeRisk, req.RiskLimits.EnableRiskChecks,
			req.StrategyID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to update risk limits: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Reload full strategy
	return r.GetByID(ctx, req.StrategyID, req.UserID)
}

// Delete deletes a strategy
func (r *StrategyRepository) Delete(ctx context.Context, strategyID uuid.UUID, userID string) error {
	query := `DELETE FROM strategies WHERE strategy_id = $1 AND user_id = $2`
	result, err := r.db.ExecContext(ctx, query, strategyID, userID)
	if err != nil {
		return fmt.Errorf("failed to delete strategy: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("strategy not found")
	}

	return nil
}

// Activate activates a strategy
func (r *StrategyRepository) Activate(ctx context.Context, strategyID uuid.UUID, userID string) error {
	query := `UPDATE strategies SET active = true WHERE strategy_id = $1 AND user_id = $2`
	result, err := r.db.ExecContext(ctx, query, strategyID, userID)
	if err != nil {
		return fmt.Errorf("failed to activate strategy: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("strategy not found")
	}

	return nil
}

// Deactivate deactivates a strategy
func (r *StrategyRepository) Deactivate(ctx context.Context, strategyID uuid.UUID, userID string) error {
	query := `UPDATE strategies SET active = false WHERE strategy_id = $1 AND user_id = $2`
	result, err := r.db.ExecContext(ctx, query, strategyID, userID)
	if err != nil {
		return fmt.Errorf("failed to deactivate strategy: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("strategy not found")
	}

	return nil
}

// GetByIDs retrieves multiple strategies by their IDs
func (r *StrategyRepository) GetByIDs(ctx context.Context, strategyIDs []uuid.UUID) ([]*models.Strategy, error) {
	if len(strategyIDs) == 0 {
		return []*models.Strategy{}, nil
	}

	strategies := []*models.Strategy{}
	query := `SELECT * FROM strategies WHERE strategy_id = ANY($1)`

	err := r.db.SelectContext(ctx, &strategies, query, strategyIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to get strategies: %w", err)
	}

	// Load related data for each strategy
	for _, strategy := range strategies {
		// Load conditions
		condition := &models.StrategyCondition{}
		condQuery := `SELECT condition_id, strategy_id, impact_score_threshold, sentiments, categories, 
			stock_codes, price_range_min, price_range_max, volume_threshold, pct_change_threshold, 
			exchanges, created_at FROM strategy_conditions WHERE strategy_id = $1`
		err = r.db.QueryRowContext(ctx, condQuery, strategy.StrategyID).Scan(
			&condition.ConditionID,
			&condition.StrategyID,
			&condition.ImpactScoreThreshold,
			&condition.Sentiments,
			&condition.Categories,
			&condition.StockCodes,
			&condition.PriceRangeMin,
			&condition.PriceRangeMax,
			&condition.VolumeThreshold,
			&condition.PctChangeThreshold,
			&condition.Exchanges,
			&condition.CreatedAt,
		)
		if err == nil {
			strategy.Conditions = condition
		}

		// Load trade config
		tradeConfig := &models.TradeConfig{}
		tradeQuery := `SELECT trade_config_id, strategy_id, order_type, quantity, max_position_size, 
			stop_loss_pct, take_profit_pct, exchange, order_side, limit_price, validity, created_at 
			FROM trade_configs WHERE strategy_id = $1`
		err = r.db.QueryRowContext(ctx, tradeQuery, strategy.StrategyID).Scan(
			&tradeConfig.TradeConfigID,
			&tradeConfig.StrategyID,
			&tradeConfig.OrderType,
			&tradeConfig.Quantity,
			&tradeConfig.MaxPositionSize,
			&tradeConfig.StopLossPct,
			&tradeConfig.TakeProfitPct,
			&tradeConfig.Exchange,
			&tradeConfig.OrderSide,
			&tradeConfig.LimitPrice,
			&tradeConfig.Validity,
			&tradeConfig.CreatedAt,
		)
		if err == nil {
			strategy.TradeConfig = tradeConfig
		}

		// Load risk limits
		riskLimits := &models.RiskLimits{}
		riskQuery := `SELECT risk_limit_id, strategy_id, max_daily_trades, max_loss_per_day, 
			position_sizing, max_portfolio_exposure_pct, max_per_trade_risk, enable_risk_checks, created_at 
			FROM risk_limits WHERE strategy_id = $1`
		err = r.db.QueryRowContext(ctx, riskQuery, strategy.StrategyID).Scan(
			&riskLimits.RiskLimitID,
			&riskLimits.StrategyID,
			&riskLimits.MaxDailyTrades,
			&riskLimits.MaxLossPerDay,
			&riskLimits.PositionSizing,
			&riskLimits.MaxPortfolioExposurePct,
			&riskLimits.MaxPerTradeRisk,
			&riskLimits.EnableRiskChecks,
			&riskLimits.CreatedAt,
		)
		if err == nil {
			strategy.RiskLimits = riskLimits
		}
	}

	return strategies, nil
}

// ========================================================================
// Jobbing Strategy Configuration Repository Methods
// ========================================================================

// UpsertJobbingConfig inserts or updates a jobbing configuration for a user and token
func (r *StrategyRepository) UpsertJobbingConfig(ctx context.Context, cfg *models.JobbingConfig) error {
	if cfg.UserID == "" || cfg.Token == "" {
		return fmt.Errorf("invalid JobbingConfig: user_id and token are required")
	}

	// Set defaults if not provided
	if cfg.StrategyID == "" {
		cfg.StrategyID = "JOBBING"
	}
	if cfg.Exchange == "" {
		cfg.Exchange = "NSE"
	}
	if cfg.TradingMode == "" {
		cfg.TradingMode = "LIVE"
	}
	if cfg.ID == uuid.Nil {
		cfg.ID = uuid.New()
	}

	query := `
		INSERT INTO jobbing_configs (
			id, user_id, strategy_id, token, symbol, exchange,
			lower_range, higher_range,
			initial_buy_offset, distance_continue,
			quantity_per_order, max_quantity,
			trading_mode, enabled, enabled_at
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8,
			$9, $10,
			$11, $12,
			$13, $14, $15
		)
		ON CONFLICT (user_id, token)
		DO UPDATE SET
			symbol = EXCLUDED.symbol,
			exchange = EXCLUDED.exchange,
			lower_range = EXCLUDED.lower_range,
			higher_range = EXCLUDED.higher_range,
			initial_buy_offset = EXCLUDED.initial_buy_offset,
			distance_continue = EXCLUDED.distance_continue,
			quantity_per_order = EXCLUDED.quantity_per_order,
			max_quantity = EXCLUDED.max_quantity,
			trading_mode = EXCLUDED.trading_mode,
			enabled = EXCLUDED.enabled,
			updated_at = CURRENT_TIMESTAMP
		RETURNING created_at, updated_at`

	var enabledAt *time.Time
	if cfg.Enabled {
		now := time.Now()
		enabledAt = &now
	}

	err := r.db.QueryRowContext(ctx, query,
		cfg.ID, cfg.UserID, cfg.StrategyID, cfg.Token, cfg.Symbol, cfg.Exchange,
		cfg.LowerRange, cfg.HigherRange,
		cfg.InitialBuyOffset, cfg.DistanceContinue,
		cfg.QuantityPerOrder, cfg.MaxQuantity,
		cfg.TradingMode, cfg.Enabled, enabledAt,
	).Scan(&cfg.CreatedAt, &cfg.UpdatedAt)

	if err != nil {
		return fmt.Errorf("failed to upsert jobbing config: %w", err)
	}

	return nil
}

// GetJobbingConfig fetches a single jobbing configuration by user_id and token
func (r *StrategyRepository) GetJobbingConfig(ctx context.Context, userID, token string) (*models.JobbingConfig, error) {
	query := `
		SELECT 
			id, user_id, strategy_id, token, symbol, exchange,
			lower_range, higher_range,
			initial_buy_offset, distance_continue,
			quantity_per_order, max_quantity,
			trading_mode, enabled, enabled_at, disabled_at,
			created_at, updated_at
		FROM jobbing_configs
		WHERE user_id = $1 AND token = $2`

	var cfg models.JobbingConfig
	err := r.db.GetContext(ctx, &cfg, query, userID, token)
	if err == sql.ErrNoRows {
		return nil, nil // Not found, but not an error
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get jobbing config: %w", err)
	}

	return &cfg, nil
}

// ListJobbingConfigs fetches all jobbing configurations for a user
func (r *StrategyRepository) ListJobbingConfigs(ctx context.Context, userID string, enabledOnly bool) ([]models.JobbingConfig, error) {
	query := `
		SELECT 
			id, user_id, strategy_id, token, symbol, exchange,
			lower_range, higher_range,
			initial_buy_offset, distance_continue,
			quantity_per_order, max_quantity,
			trading_mode, enabled, enabled_at, disabled_at,
			created_at, updated_at
		FROM jobbing_configs
		WHERE user_id = $1`

	if enabledOnly {
		query += " AND enabled = true"
	}

	query += " ORDER BY created_at DESC"

	var configs []models.JobbingConfig
	err := r.db.SelectContext(ctx, &configs, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobbing configs: %w", err)
	}

	return configs, nil
}

// DeleteJobbingConfig deletes a jobbing configuration for a user and token
func (r *StrategyRepository) DeleteJobbingConfig(ctx context.Context, userID, token string) error {
	query := `DELETE FROM jobbing_configs WHERE user_id = $1 AND token = $2`

	result, err := r.db.ExecContext(ctx, query, userID, token)
	if err != nil {
		return fmt.Errorf("failed to delete jobbing config: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("jobbing config not found for user %s and token %s", userID, token)
	}

	return nil
}

// UpdateJobbingConfigStatus enables or disables a jobbing configuration
func (r *StrategyRepository) UpdateJobbingConfigStatus(ctx context.Context, userID, token string, enabled bool) error {
	query := `
		UPDATE jobbing_configs
		SET enabled = $1, updated_at = CURRENT_TIMESTAMP
		WHERE user_id = $2 AND token = $3`

	result, err := r.db.ExecContext(ctx, query, enabled, userID, token)
	if err != nil {
		return fmt.Errorf("failed to update jobbing config status: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("jobbing config not found for user %s and token %s", userID, token)
	}

	return nil
}

// ListAllEnabledJobbingConfigs fetches all enabled jobbing configurations across all users
// This is useful for the rules-engine to discover active jobbing strategies
func (r *StrategyRepository) ListAllEnabledJobbingConfigs(ctx context.Context) ([]models.JobbingConfig, error) {
	query := `
		SELECT 
			id, user_id, strategy_id, token, symbol, exchange,
			lower_range, higher_range,
			initial_buy_offset, distance_continue,
			quantity_per_order, max_quantity,
			trading_mode, enabled, enabled_at, disabled_at,
			created_at, updated_at
		FROM jobbing_configs
		WHERE enabled = true
		ORDER BY user_id, token`

	var configs []models.JobbingConfig
	err := r.db.SelectContext(ctx, &configs, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list all enabled jobbing configs: %w", err)
	}

	return configs, nil
}

// GetJobbingConfigsByToken fetches all enabled jobbing configurations for a specific token
// This allows the rules-engine to find all users interested in a particular token
func (r *StrategyRepository) GetJobbingConfigsByToken(ctx context.Context, token string) ([]models.JobbingConfig, error) {
	query := `
		SELECT 
			id, user_id, strategy_id, token, symbol, exchange,
			lower_range, higher_range,
			initial_buy_offset, distance_continue,
			quantity_per_order, max_quantity,
			trading_mode, enabled, enabled_at, disabled_at,
			created_at, updated_at
		FROM jobbing_configs
		WHERE token = $1 AND enabled = true
		ORDER BY user_id`

	var configs []models.JobbingConfig
	err := r.db.SelectContext(ctx, &configs, query, token)
	if err != nil {
		return nil, fmt.Errorf("failed to get jobbing configs by token: %w", err)
	}

	return configs, nil
}
