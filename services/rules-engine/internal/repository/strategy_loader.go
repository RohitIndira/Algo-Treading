package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

// StrategyLoader loads strategies from PostgreSQL
type StrategyLoader struct {
	db     *sql.DB
	logger *zap.Logger
}

// NewStrategyLoader creates a new strategy loader
func NewStrategyLoader(dbHost, dbPort, dbUser, dbPassword, dbName, dbSSLMode string, logger *zap.Logger) (*StrategyLoader, error) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		dbHost, dbPort, dbUser, dbPassword, dbName, dbSSLMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	logger.Info("PostgreSQL connection established",
		zap.String("host", dbHost),
		zap.String("database", dbName))

	return &StrategyLoader{
		db:     db,
		logger: logger,
	}, nil
}

// LoadAllStrategies loads all strategies from the database
func (sl *StrategyLoader) LoadAllStrategies(ctx context.Context) ([]*models.Strategy, error) {
	query := `
		SELECT 
			s.strategy_id,
			s.user_id,
			s.strategy_name,
			s.description,
			s.active,
			s.created_at,
			s.updated_at,
			s.version,
			c.match_all_news,
			c.impact_score_threshold,
			c.sentiments,
			c.categories,
			c.stock_codes,
			c.price_range_min,
			c.price_range_max,
			c.volume_threshold,
			c.pct_change_threshold,
			c.exchanges,
			t.order_type,
			t.quantity,
			t.max_position_size,
			t.stop_loss_pct,
			t.take_profit_pct,
			t.exchange as trade_exchange,
			t.order_side,
			t.limit_price,
			t.validity,
			r.max_daily_trades,
			r.max_loss_per_day,
			r.position_sizing,
			r.max_portfolio_exposure_pct,
			r.max_per_trade_risk,
			r.enable_risk_checks
		FROM strategies s
		LEFT JOIN strategy_conditions c ON s.strategy_id = c.strategy_id
		LEFT JOIN trade_configs t ON s.strategy_id = t.strategy_id
		LEFT JOIN risk_limits r ON s.strategy_id = r.strategy_id
		ORDER BY s.created_at DESC
	`

	rows, err := sl.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query strategies: %w", err)
	}
	defer rows.Close()

	strategies := make([]*models.Strategy, 0)

	for rows.Next() {
		var (
			strategyID           string
			userID               string
			strategyName         string
			description          sql.NullString
			active               bool
			createdAt            time.Time
			updatedAt            time.Time
			version              int
			matchAllNews         sql.NullBool
			impactScoreThreshold sql.NullInt64
			sentimentsJSON       sql.NullString
			categoriesJSON       sql.NullString
			stockCodesJSON       sql.NullString
			priceRangeMin        sql.NullFloat64
			priceRangeMax        sql.NullFloat64
			volumeThreshold      sql.NullInt64
			pctChangeThreshold   sql.NullFloat64
			exchangesJSON        sql.NullString
			orderType            sql.NullString
			quantity             sql.NullInt64
			maxPositionSize      sql.NullFloat64
			stopLossPct          sql.NullFloat64
			takeProfitPct        sql.NullFloat64
			tradeExchange        sql.NullString
			orderSide            sql.NullString
			limitPrice           sql.NullFloat64
			validity             sql.NullString
			maxDailyTrades       sql.NullInt64
			maxLossPerDay        sql.NullFloat64
			positionSizing       sql.NullString
			maxPortfolioExposure sql.NullFloat64
			maxPerTradeRisk      sql.NullFloat64
			enableRiskChecks     sql.NullBool
		)

		err := rows.Scan(
			&strategyID, &userID, &strategyName, &description, &active,
			&createdAt, &updatedAt, &version,
			&matchAllNews, &impactScoreThreshold, &sentimentsJSON, &categoriesJSON,
			&stockCodesJSON, &priceRangeMin, &priceRangeMax, &volumeThreshold,
			&pctChangeThreshold, &exchangesJSON,
			&orderType, &quantity, &maxPositionSize, &stopLossPct, &takeProfitPct,
			&tradeExchange, &orderSide, &limitPrice, &validity,
			&maxDailyTrades, &maxLossPerDay, &positionSizing, &maxPortfolioExposure,
			&maxPerTradeRisk, &enableRiskChecks,
		)
		if err != nil {
			sl.logger.Warn("Failed to scan strategy row", zap.Error(err))
			continue
		}

		strategy := &models.Strategy{
			StrategyID:   strategyID,
			UserID:       userID,
			StrategyName: strategyName,
			Active:       active,
			CreatedAt:    createdAt,
			UpdatedAt:    updatedAt,
			Conditions: models.Conditions{
				MatchAllNews:         matchAllNews.Bool,
				ImpactScoreThreshold: int32(impactScoreThreshold.Int64),
				Sentiments:           parseJSONArray(sentimentsJSON.String),
				Categories:           parseJSONArray(categoriesJSON.String),
				Stocks:               parseJSONInt64Array(stockCodesJSON.String),
				PriceRange: models.PriceRange{
					MinPrice: priceRangeMin.Float64,
					MaxPrice: priceRangeMax.Float64,
				},
				VolumeThreshold:    volumeThreshold.Int64,
				PctChangeThreshold: pctChangeThreshold.Float64,
			},
			TradeConfig: models.TradeConfig{
				OrderType:       orderType.String,
				Quantity:        int32(quantity.Int64),
				MaxPositionSize: maxPositionSize.Float64,
				StopLossPct:     stopLossPct.Float64,
				TakeProfitPct:   takeProfitPct.Float64,
				Exchange:        tradeExchange.String,
			},
			RiskLimits: models.RiskLimits{
				MaxDailyTrades: int32(maxDailyTrades.Int64),
				MaxLossPerDay:  maxLossPerDay.Float64,
				PositionSizing: positionSizing.String,
			},
		}

		strategies = append(strategies, strategy)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	sl.logger.Info("Loaded strategies from PostgreSQL", zap.Int("count", len(strategies)))
	return strategies, nil
}

// LoadActiveStrategies loads only active strategies
func (sl *StrategyLoader) LoadActiveStrategies(ctx context.Context) ([]*models.Strategy, error) {
	allStrategies, err := sl.LoadAllStrategies(ctx)
	if err != nil {
		return nil, err
	}

	activeStrategies := make([]*models.Strategy, 0)
	for _, strategy := range allStrategies {
		if strategy.Active {
			activeStrategies = append(activeStrategies, strategy)
		}
	}

	sl.logger.Info("Filtered active strategies", zap.Int("active_count", len(activeStrategies)))
	return activeStrategies, nil
}

// Close closes the database connection
func (sl *StrategyLoader) Close() error {
	return sl.db.Close()
}

// Helper functions to parse JSON arrays from PostgreSQL

func parseJSONArray(jsonStr string) []string {
	if jsonStr == "" || jsonStr == "null" || jsonStr == "[]" {
		return []string{}
	}

	// Simple JSON array parsing (PostgreSQL stores as ["item1","item2"])
	// Remove brackets and quotes, split by comma
	jsonStr = jsonStr[1 : len(jsonStr)-1] // Remove [ ]
	if jsonStr == "" {
		return []string{}
	}

	result := make([]string, 0)
	current := ""
	inQuote := false

	for _, char := range jsonStr {
		if char == '"' {
			inQuote = !inQuote
		} else if char == ',' && !inQuote {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else if inQuote {
			current += string(char)
		}
	}

	if current != "" {
		result = append(result, current)
	}

	return result
}

func parseJSONInt64Array(jsonStr string) []int64 {
	if jsonStr == "" || jsonStr == "null" || jsonStr == "[]" {
		return []int64{}
	}

	// Simple JSON array parsing for integers
	jsonStr = jsonStr[1 : len(jsonStr)-1] // Remove [ ]
	if jsonStr == "" {
		return []int64{}
	}

	result := make([]int64, 0)
	current := ""

	for _, char := range jsonStr + "," {
		if char == ',' {
			if current != "" {
				var val int64
				fmt.Sscanf(current, "%d", &val)
				result = append(result, val)
				current = ""
			}
		} else if char >= '0' && char <= '9' {
			current += string(char)
		}
	}

	return result
}
