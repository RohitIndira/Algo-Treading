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

// TradeSignalRepository handles trade signal persistence
type TradeSignalRepository struct {
	db     *sql.DB
	logger *zap.Logger
}

// NewTradeSignalRepository creates a new trade signal repository
func NewTradeSignalRepository(dbHost, dbPort, dbUser, dbPassword, dbName, dbSSLMode string, logger *zap.Logger) (*TradeSignalRepository, error) {
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

	logger.Info("PostgreSQL connection established for trade signals",
		zap.String("host", dbHost),
		zap.String("database", dbName))

	return &TradeSignalRepository{
		db:     db,
		logger: logger,
	}, nil
}

// SaveTradeSignal saves a trade signal to the database
func (r *TradeSignalRepository) SaveTradeSignal(ctx context.Context, orderReq *models.OrderRequest) error {
	query := `
		INSERT INTO trade_signals (
			order_id, user_id, strategy_id, strategy_name, event_id,
			stock_code, symbol, exchange,
			order_type, order_side, quantity, price, stop_loss, take_profit,
			match_score, impact_score, sentiment, news_category,
			status, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8,
			$9, $10, $11, $12, $13, $14,
			$15, $16, $17, $18,
			'PENDING', $19, $19
		)
	`

	_, err := r.db.ExecContext(ctx, query,
		orderReq.OrderID,      // $1
		orderReq.UserID,       // $2
		orderReq.StrategyID,   // $3
		orderReq.StrategyName, // $4
		orderReq.EventID,      // $5
		orderReq.StockCode,    // $6
		orderReq.Symbol,       // $7
		orderReq.Exchange,     // $8
		orderReq.OrderType,    // $9
		orderReq.OrderSide,    // $10
		orderReq.Quantity,     // $11
		orderReq.Price,        // $12
		orderReq.StopLoss,     // $13
		orderReq.TakeProfit,   // $14
		orderReq.MatchScore,   // $15
		nil,                   // $16 - impact_score (not available in OrderRequest)
		nil,                   // $17 - sentiment (not available in OrderRequest)
		nil,                   // $18 - news_category (not available in OrderRequest)
		orderReq.Timestamp,    // $19
	)

	if err != nil {
		return fmt.Errorf("failed to save trade signal: %w", err)
	}

	r.logger.Debug("Trade signal saved to database",
		zap.String("order_id", orderReq.OrderID),
		zap.String("user_id", orderReq.UserID),
		zap.String("status", "PENDING"))

	return nil
}

// UpdateSignalStatus updates the execution status of a trade signal
func (r *TradeSignalRepository) UpdateSignalStatus(ctx context.Context, orderID string, status string, executionPrice float64, brokerOrderID string, errorMsg string) error {
	query := `
		UPDATE trade_signals
		SET status = $1,
		    execution_price = $2,
		    execution_time = $3,
		    broker_order_id = $4,
		    error_message = $5,
		    updated_at = $6
		WHERE order_id = $7
	`

	now := time.Now()
	var execTime *time.Time
	if status == "EXECUTED" || status == "FAILED" {
		execTime = &now
	}

	var execPrice *float64
	if executionPrice > 0 {
		execPrice = &executionPrice
	}

	_, err := r.db.ExecContext(ctx, query,
		status,
		execPrice,
		execTime,
		brokerOrderID,
		errorMsg,
		now,
		orderID,
	)

	if err != nil {
		return fmt.Errorf("failed to update signal status: %w", err)
	}

	r.logger.Info("Trade signal status updated",
		zap.String("order_id", orderID),
		zap.String("status", status))

	return nil
}

// GetPendingSignals retrieves all pending trade signals
func (r *TradeSignalRepository) GetPendingSignals(ctx context.Context, limit int) ([]*models.OrderRequest, error) {
	query := `
		SELECT order_id, user_id, strategy_id, strategy_name, event_id,
		       stock_code, symbol, exchange,
		       order_type, quantity, price, stop_loss, take_profit,
		       match_score, impact_score, sentiment, news_category,
		       created_at
		FROM trade_signals
		WHERE status = 'PENDING'
		ORDER BY created_at ASC
		LIMIT $1
	`

	rows, err := r.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query pending signals: %w", err)
	}
	defer rows.Close()

	signals := make([]*models.OrderRequest, 0)
	for rows.Next() {
		var orderReq models.OrderRequest
		err := rows.Scan(
			&orderReq.OrderID,
			&orderReq.UserID,
			&orderReq.StrategyID,
			&orderReq.StrategyName,
			&orderReq.EventID,
			&orderReq.StockCode,
			&orderReq.Symbol,
			&orderReq.Exchange,
			&orderReq.OrderType,
			&orderReq.Quantity,
			&orderReq.Price,
			&orderReq.StopLoss,
			&orderReq.TakeProfit,
			&orderReq.MatchScore,
			&orderReq.Timestamp,
		)
		if err != nil {
			r.logger.Warn("Failed to scan signal row", zap.Error(err))
			continue
		}
		signals = append(signals, &orderReq)
	}

	return signals, rows.Err()
}

// GetUserSignals retrieves trade signals for a specific user
func (r *TradeSignalRepository) GetUserSignals(ctx context.Context, userID string, limit int, offset int) ([]*models.OrderRequest, error) {
	query := `
		SELECT order_id, user_id, strategy_id, strategy_name, event_id,
		       stock_code, symbol, exchange,
		       order_type, quantity, price, stop_loss, take_profit,
		       match_score, impact_score, sentiment, news_category,
		       created_at
		FROM trade_signals
		WHERE user_id = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`

	rows, err := r.db.QueryContext(ctx, query, userID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to query user signals: %w", err)
	}
	defer rows.Close()

	signals := make([]*models.OrderRequest, 0)
	for rows.Next() {
		var orderReq models.OrderRequest
		err := rows.Scan(
			&orderReq.OrderID,
			&orderReq.UserID,
			&orderReq.StrategyID,
			&orderReq.StrategyName,
			&orderReq.EventID,
			&orderReq.StockCode,
			&orderReq.Symbol,
			&orderReq.Exchange,
			&orderReq.OrderType,
			&orderReq.Quantity,
			&orderReq.Price,
			&orderReq.StopLoss,
			&orderReq.TakeProfit,
			&orderReq.MatchScore,
			&orderReq.Timestamp,
		)
		if err != nil {
			r.logger.Warn("Failed to scan signal row", zap.Error(err))
			continue
		}
		signals = append(signals, &orderReq)
	}

	return signals, rows.Err()
}

// Close closes the database connection
func (r *TradeSignalRepository) Close() error {
	return r.db.Close()
}
