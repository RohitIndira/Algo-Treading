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

// DB exposes the underlying connection pool so sibling components that write to
// the same database (e.g. the signal-decisions recorder) can reuse it instead of
// opening a second pool against the same server. Returns nil on a nil receiver.
func (r *TradeSignalRepository) DB() *sql.DB {
	if r == nil {
		return nil
	}
	return r.db
}

// SaveTradeSignal saves a trade signal to the database
func (r *TradeSignalRepository) SaveTradeSignal(ctx context.Context, orderReq *models.OrderRequest) error {
	query := `
		INSERT INTO trade_signals (
			order_id, user_id, strategy_id, strategy_name, event_id,
			stock_code, symbol, exchange,
			order_type, order_side, quantity, price, stop_loss, take_profit,
			match_score, impact_score, sentiment, news_category,
			status, signal_kind, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8,
			$9, 'BUY', $10, $11, $12, $13,
			$14, $15, $16, $17,
			'PENDING', $19, $18, $18
		)
	`

	_, err := r.db.ExecContext(ctx, query,
		orderReq.OrderID,
		orderReq.UserID,
		orderReq.StrategyID,
		orderReq.StrategyName,
		orderReq.EventID,
		orderReq.StockCode,
		orderReq.Symbol,
		orderReq.Exchange,
		orderReq.OrderType,
		orderReq.Quantity,
		orderReq.Price,
		orderReq.StopLoss,
		orderReq.TakeProfit,
		orderReq.MatchScore,
		orderReq.ImpactScore,
		orderReq.Sentiment,
		orderReq.NewsCategory,
		orderReq.Timestamp,
		orderReq.SignalKind(),
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

// HasSignalToday returns true if the strategy already has any LIVE-or-PENDING
// signal for this stock on today's IST date AND after sinceTime (the strategy's
// last activation). Signals that ended in FAILED or CANCELLED do not consume the
// per-stock daily slot — the strategy may retry the same stock that day.
func (r *TradeSignalRepository) HasSignalToday(ctx context.Context, strategyID string, stockCode int64, sinceTime time.Time) (bool, error) {
	query := `
		SELECT EXISTS(
			SELECT 1 FROM trade_signals
			WHERE strategy_id = $1
			  AND stock_code  = $2
			  AND (created_at AT TIME ZONE 'Asia/Kolkata')::date
			      = (NOW()    AT TIME ZONE 'Asia/Kolkata')::date
			  AND created_at >= $3
			  AND status NOT IN ('FAILED', 'CANCELLED')
		)
	`
	var exists bool
	if err := r.db.QueryRowContext(ctx, query, strategyID, stockCode, sinceTime).Scan(&exists); err != nil {
		return false, fmt.Errorf("failed to check daily signal: %w", err)
	}
	return exists, nil
}

// (GetStrategyTradesToday was removed: the per-strategy daily cap is now enforced
// by an atomic Redis reservation — pkg/tradecap — that counts only *actual
// trades*, not price-monitor watches. The durable committed-trade count used to
// reseed that counter lives in CountCommittedTradesToday below.)

// CountCommittedTradesToday returns how many *actual trades* the strategy has
// committed today (IST) since sinceTime — used to reseed the Redis trade counter
// (pkg/tradecap) after a Redis flush so a hard cap is never silently re-opened.
//
// A committed trade is an IMMEDIATE signal (placed at signal time) or a
// MONITORING watch whose status has advanced to EXECUTED/TRIGGERED (its target
// fired). Waiting watches, FAILED, and CANCELLED rows never count.
func (r *TradeSignalRepository) CountCommittedTradesToday(ctx context.Context, strategyID string, sinceTime time.Time) (int64, error) {
	query := `
		SELECT COUNT(*) FROM trade_signals
		WHERE strategy_id = $1
		  AND (created_at AT TIME ZONE 'Asia/Kolkata')::date
		      = (NOW()    AT TIME ZONE 'Asia/Kolkata')::date
		  AND created_at >= $2
		  AND status NOT IN ('FAILED', 'CANCELLED')
		  AND (signal_kind = 'IMMEDIATE' OR status IN ('EXECUTED', 'TRIGGERED', 'FILLED', 'SENT'))
	`
	var count int64
	if err := r.db.QueryRowContext(ctx, query, strategyID, sinceTime).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to count committed strategy trades: %w", err)
	}
	return count, nil
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
			&orderReq.ImpactScore,
			&orderReq.Sentiment,
			&orderReq.NewsCategory,
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
			&orderReq.ImpactScore,
			&orderReq.Sentiment,
			&orderReq.NewsCategory,
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
