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

// UpsertCash52WConfig inserts or updates the ENHANCED Phase 1 52W configuration
// Supports: multi-level profit/SL, portfolio config, force exit controls
func (r *StrategyRepository) UpsertCash52WConfig(ctx context.Context, cfg *models.Cash52WConfig) error {
	if cfg == nil || cfg.UserID == "" {
		return fmt.Errorf("invalid Cash52WConfig: user_id is required")
	}

	// Validate config before saving
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	query := `
		INSERT INTO cash52w_configs (
			user_id, enabled, total_capital, capital_per_stock, max_stocks, auto_rebalance,
			stop_loss_levels, profit_levels, trading_mode,
			force_exit_all, force_exit_stocks, pause_new_entries,
			version, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (user_id)
		DO UPDATE SET
			enabled = EXCLUDED.enabled,
			total_capital = EXCLUDED.total_capital,
			capital_per_stock = EXCLUDED.capital_per_stock,
			max_stocks = EXCLUDED.max_stocks,
			auto_rebalance = EXCLUDED.auto_rebalance,
			stop_loss_levels = EXCLUDED.stop_loss_levels,
			profit_levels = EXCLUDED.profit_levels,
			trading_mode = EXCLUDED.trading_mode,
			force_exit_all = EXCLUDED.force_exit_all,
			force_exit_stocks = EXCLUDED.force_exit_stocks,
			pause_new_entries = EXCLUDED.pause_new_entries,
			version = cash52w_configs.version + 1,
			updated_at = EXCLUDED.updated_at
		RETURNING version
	`

	if cfg.UpdatedAt.IsZero() {
		cfg.UpdatedAt = time.Now()
	}

	var newVersion int
	err := r.db.QueryRowContext(ctx, query,
		cfg.UserID,
		cfg.Enabled,
		cfg.TotalCapital,
		cfg.CapitalPerStock,
		cfg.MaxStocks,
		cfg.AutoRebalance,
		cfg.StopLossLevels,
		cfg.ProfitLevels,
		cfg.TradingMode,
		cfg.ForceExitAll,
		cfg.ForceExitStocks,
		cfg.PauseNewEntries,
		cfg.Version,
		cfg.UpdatedAt,
	).Scan(&newVersion)

	if err != nil {
		return fmt.Errorf("failed to upsert cash52w_config: %w", err)
	}

	cfg.Version = newVersion
	return nil
}

// DeleteCash52WConfig removes the 52W configuration for a user from the
// dedicated table. Used when the managed 52W strategy is disabled.
func (r *StrategyRepository) DeleteCash52WConfig(ctx context.Context, userID string) error {
	if userID == "" {
		return fmt.Errorf("user_id is required")
	}

	query := `DELETE FROM cash52w_configs WHERE user_id = $1`
	_, err := r.db.ExecContext(ctx, query, userID)
	if err != nil {
		return fmt.Errorf("failed to delete cash52w_config: %w", err)
	}

	return nil
}

// GetCash52WConfig fetches the ENHANCED Phase 1 52W configuration for a user
// Returns (nil, nil) if no row exists
func (r *StrategyRepository) GetCash52WConfig(ctx context.Context, userID string) (*models.Cash52WConfig, error) {
	if userID == "" {
		return nil, fmt.Errorf("user_id is required")
	}

	var cfg models.Cash52WConfig
	query := `
		SELECT 
			user_id, enabled, total_capital, capital_per_stock, max_stocks, auto_rebalance,
			stop_loss_levels, profit_levels, trading_mode,
			force_exit_all, force_exit_stocks, pause_new_entries,
			version, updated_at
		FROM cash52w_configs 
		WHERE user_id = $1
	`
	err := r.db.GetContext(ctx, &cfg, query, userID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get cash52w_config: %w", err)
	}

	return &cfg, nil
}

// GetAllEnabledCash52WConfigs fetches all enabled 52W configurations
// Used by rules-engine for initial cache load
func (r *StrategyRepository) GetAllEnabledCash52WConfigs(ctx context.Context) ([]*models.Cash52WConfig, error) {
	var configs []*models.Cash52WConfig
	query := `
		SELECT 
			user_id, enabled, total_capital, capital_per_stock, max_stocks, auto_rebalance,
			stop_loss_levels, profit_levels, trading_mode,
			force_exit_all, force_exit_stocks, pause_new_entries,
			version, updated_at
		FROM cash52w_configs 
		WHERE enabled = TRUE
		ORDER BY updated_at DESC
	`
	err := r.db.SelectContext(ctx, &configs, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get enabled cash52w_configs: %w", err)
	}

	return configs, nil
}

// ConfigureCash52WeekStrategy creates or updates the managed "Cash 52W High"
// strategy for a user. It uses existing Create/Update/Activate/Deactivate
// methods rather than custom SQL so that all related tables stay consistent.
//
// Behaviour:
//   - If enabled=false: deactivate existing Cash 52W strategy (if any).
//   - If enabled=true: create or update a Cash 52W strategy with the provided
//     capital_per_stock and default SL/TP/risk settings.
func (r *StrategyRepository) ConfigureCash52WeekStrategy(
	ctx context.Context,
	userID string,
	capitalPerStock float64,
	maxPositions int,
	stopLossPct, takeProfitPct float64,
	riskProfile string,
	tradingMode string,
	enabled bool,
) (*models.Strategy, error) {
	const strategyName = "Cash 52W High"

	// Fetch existing strategies for this user and look for our managed one.
	strategies, _, err := r.ListByUserID(ctx, userID, false, 100, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to list strategies for ConfigureCash52WeekStrategy: %w", err)
	}

	var existing *models.Strategy
	for _, s := range strategies {
		if s.StrategyName == strategyName {
			existing = s
			break
		}
	}

	// If disabling, simply deactivate existing strategy if it exists.
	if !enabled {
		if existing == nil {
			return nil, nil
		}
		if err := r.Deactivate(ctx, existing.StrategyID, userID); err != nil {
			return nil, err
		}
		return r.GetByID(ctx, existing.StrategyID, userID)
	}

	// Common default values if the caller passed zero/invalid overrides.
	if maxPositions <= 0 {
		maxPositions = 25
	}
	if stopLossPct <= 0 {
		stopLossPct = 10
	}
	if takeProfitPct <= 0 {
		takeProfitPct = 20
	}

	// Helper to build TradeConfig and RiskLimits for this 52W strategy.
	buildTradeAndRisk := func() (*models.TradeConfig, *models.RiskLimits) {
		// TradeConfig: MARKET BUY, quantity is a dummy value required so that
		// generic strategy validation/indexing works. The Cash 52W engine
		// derives the actual per-trade quantity from CapitalPerStock/LTP at
		// runtime (e.g. ₹20,000 / LTP). We keep Quantity=1 here to avoid
		// confusing UIs while still satisfying validation rules.
		mc := &models.TradeConfig{
			OrderType:       "ORDER_TYPE_MARKET",
			Quantity:        1,
			MaxPositionSize: &capitalPerStock,
			StopLossPct:     &stopLossPct,
			TakeProfitPct:   &takeProfitPct,
			Exchange:        "EXCHANGE_NSE",
			OrderSide:       "ORDER_SIDE_BUY",
			LimitPrice:      nil,
			Validity:        "DAY",
		}

		// RiskLimits: reasonable defaults for this strategy; these are separate
		// from generic platform limits enforced in the risk-management service.
		maxDailyTrades := int32(50)
		maxLossPerDay := 50000.0
		maxPerTradeRisk := capitalPerStock * stopLossPct / 100.0
		positionSizing := "POSITION_SIZING_FIXED"
		maxPortfolioExposurePct := 0.0

		rl := &models.RiskLimits{
			MaxDailyTrades:          &maxDailyTrades,
			MaxLossPerDay:           &maxLossPerDay,
			PositionSizing:          positionSizing,
			MaxPortfolioExposurePct: &maxPortfolioExposurePct,
			MaxPerTradeRisk:         &maxPerTradeRisk,
			EnableRiskChecks:        true,
		}

		return mc, rl
	}

	if existing != nil {
		// Update existing strategy via generic Update method.
		tradeCfg, riskLimits := buildTradeAndRisk()
		upd := &models.UpdateStrategyRequest{
			StrategyID:  existing.StrategyID,
			UserID:      userID,
			Version:     existing.Version,
			TradeConfig: tradeCfg,
			RiskLimits:  riskLimits,
		}

		updated, err := r.Update(ctx, upd)
		if err != nil {
			return nil, err
		}

		// Persist per-strategy trading_mode for this user/strategy.
		if _, err := r.db.ExecContext(ctx,
			`UPDATE strategies SET trading_mode = $1 WHERE strategy_id = $2`,
			tradingMode, updated.StrategyID,
		); err != nil {
			return nil, fmt.Errorf("failed to update trading_mode for 52w strategy: %w", err)
		}
		updated.TradingMode = tradingMode

		// Ensure it is active.
		if err := r.Activate(ctx, updated.StrategyID, userID); err != nil {
			return nil, err
		}
		return r.GetByID(ctx, updated.StrategyID, userID)
	}

	// No existing strategy: create a new one using the generic Create path.
	tradeCfg, riskLimits := buildTradeAndRisk()
		cond := &models.StrategyCondition{
			ImpactScoreThreshold: 1, // minimal dummy condition; 52W engine does not use news filters
			// Use a non-empty sentiments array so that rules-engine's
			// Strategy.Validate() (which requires len(Sentiments) > 0)
			// accepts this strategy and indexes it into Elasticsearch.
			// The actual value ("ANY") is not used by the 52W engine.
			Sentiments: []string{"ANY"},
			Categories: nil,
			StockCodes: nil,
			Exchanges:  nil,
		}
	cr := &models.CreateStrategyRequest{
		UserID:              userID,
		StrategyName:        strategyName,
		Description:         "Managed Cash 52-Week High breakout strategy",
		Conditions:          cond,
		TradeConfig:         tradeCfg,
		RiskLimits:          riskLimits,
		ActivateImmediately: true,
	}

	created, err := r.Create(ctx, cr)
	if err != nil {
		return nil, err
	}

	// Set trading_mode on the newly created strategy row.
	if _, err := r.db.ExecContext(ctx,
		`UPDATE strategies SET trading_mode = $1 WHERE strategy_id = $2`,
		tradingMode, created.StrategyID,
	); err != nil {
		return nil, fmt.Errorf("failed to set trading_mode for new 52w strategy: %w", err)
	}
	created.TradingMode = tradingMode
	return created, nil
}
