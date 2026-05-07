package v2

import (
	"context"
	"time"

	models "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type MetricsRepository struct {
	db *gorm.DB
}

func NewMetricsRepository(db *gorm.DB) *MetricsRepository {
	return &MetricsRepository{db: db}
}

// CreateFromSignal initializes a metrics row when a signal is consumed.
func (r *MetricsRepository) CreateFromSignal(ctx context.Context, signalID uuid.UUID, userID, strategyID, symbol, exchange, tradingMode string, signalGeneratedAt time.Time) (*models.SignalMetrics, error) {
	now := time.Now()
	m := &models.SignalMetrics{
		SignalID:          signalID,
		UserID:            userID,
		StrategyID:        &strategyID,
		Symbol:            &symbol,
		Exchange:          &exchange,
		TradingMode:       &tradingMode,
		SignalGeneratedAt: &signalGeneratedAt,
		SignalConsumedAt:  &now,
		CreatedAt:         now,
	}
	if err := r.db.WithContext(ctx).Create(m).Error; err != nil {
		return nil, err
	}
	return m, nil
}

// UpdateOrderCreated records when the order was created in DB.
func (r *MetricsRepository) UpdateOrderCreated(ctx context.Context, signalID, orderID uuid.UUID) error {
	now := time.Now()
	return r.db.WithContext(ctx).Model(&models.SignalMetrics{}).
		Where("signal_id = ?", signalID).
		Updates(map[string]interface{}{
			"order_id":         orderID,
			"order_created_at": now,
		}).Error
}

// UpdateBrokerRequest records when the Codifi API request was sent.
func (r *MetricsRepository) UpdateBrokerRequest(ctx context.Context, signalID uuid.UUID) error {
	now := time.Now()
	return r.db.WithContext(ctx).Model(&models.SignalMetrics{}).
		Where("signal_id = ?", signalID).
		Updates(map[string]interface{}{
			"broker_request_sent_at": now,
		}).Error
}

// UpdateBrokerResponse records when the Codifi API response came back.
func (r *MetricsRepository) UpdateBrokerResponse(ctx context.Context, signalID uuid.UUID, retries int) error {
	now := time.Now()
	return r.db.WithContext(ctx).Model(&models.SignalMetrics{}).
		Where("signal_id = ?", signalID).
		Updates(map[string]interface{}{
			"broker_response_received_at": now,
			"broker_api_retries":          retries,
		}).Error
}

// UpdateBrokerWSUpdate records the first WS update from broker.
func (r *MetricsRepository) UpdateBrokerWSUpdate(ctx context.Context, signalID uuid.UUID) error {
	now := time.Now()
	// Only set if not already set (first update)
	return r.db.WithContext(ctx).Model(&models.SignalMetrics{}).
		Where("signal_id = ? AND broker_ws_first_update_at IS NULL", signalID).
		Update("broker_ws_first_update_at", now).Error
}

// UpdateFilled records when the order was filled and computes all latencies.
func (r *MetricsRepository) UpdateFilled(ctx context.Context, signalID uuid.UUID, finalStatus string) error {
	now := time.Now()

	// First set the filled timestamp
	err := r.db.WithContext(ctx).Model(&models.SignalMetrics{}).
		Where("signal_id = ?", signalID).
		Updates(map[string]interface{}{
			"broker_ws_filled_at": now,
			"final_status":       finalStatus,
		}).Error
	if err != nil {
		return err
	}

	// Fetch and compute latencies
	var m models.SignalMetrics
	if err := r.db.WithContext(ctx).Where("signal_id = ?", signalID).First(&m).Error; err != nil {
		return err
	}

	m.ComputeLatencies()

	return r.db.WithContext(ctx).Model(&models.SignalMetrics{}).
		Where("signal_id = ?", signalID).
		Updates(map[string]interface{}{
			"kafka_latency_ms":      m.KafkaLatencyMs,
			"processing_latency_ms": m.ProcessingLatencyMs,
			"broker_api_latency_ms": m.BrokerAPILatencyMs,
			"broker_fill_latency_ms": m.BrokerFillLatencyMs,
			"total_e2e_ms":          m.TotalE2EMs,
		}).Error
}

// UpdateError records an error in the metrics.
func (r *MetricsRepository) UpdateError(ctx context.Context, signalID uuid.UUID, errorType, errorMsg, finalStatus string) error {
	return r.db.WithContext(ctx).Model(&models.SignalMetrics{}).
		Where("signal_id = ?", signalID).
		Updates(map[string]interface{}{
			"error_type":    errorType,
			"error_message": errorMsg,
			"final_status":  finalStatus,
		}).Error
}

// ────────────────────────────────────────────────────────────────────────────
// Analytics Queries
// ────────────────────────────────────────────────────────────────────────────

type LatencyStats struct {
	AvgE2EMs     float64 `json:"avg_e2e_ms"`
	P50E2EMs     float64 `json:"p50_e2e_ms"`
	P95E2EMs     float64 `json:"p95_e2e_ms"`
	P99E2EMs     float64 `json:"p99_e2e_ms"`
	AvgKafkaMs   float64 `json:"avg_kafka_ms"`
	AvgBrokerMs  float64 `json:"avg_broker_ms"`
	TotalSignals int     `json:"total_signals"`
	FailedCount  int     `json:"failed_count"`
}

func (r *MetricsRepository) GetLatencyStats(ctx context.Context, userID string, since time.Time) (*LatencyStats, error) {
	var stats LatencyStats

	err := r.db.WithContext(ctx).Model(&models.SignalMetrics{}).
		Select(`
			COALESCE(AVG(total_e2e_ms), 0) AS avg_e2e_ms,
			COALESCE(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY total_e2e_ms), 0) AS p50_e2e_ms,
			COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_e2e_ms), 0) AS p95_e2e_ms,
			COALESCE(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY total_e2e_ms), 0) AS p99_e2e_ms,
			COALESCE(AVG(kafka_latency_ms), 0) AS avg_kafka_ms,
			COALESCE(AVG(broker_api_latency_ms), 0) AS avg_broker_ms,
			COUNT(*) AS total_signals,
			COUNT(*) FILTER (WHERE error_type IS NOT NULL) AS failed_count
		`).
		Where("user_id = ? AND created_at >= ?", userID, since).
		Scan(&stats).Error

	return &stats, err
}

// ────────────────────────────────────────────────────────────────────────────
// DailyPnLRepository
// ────────────────────────────────────────────────────────────────────────────

type DailyPnLRepository struct {
	db *gorm.DB
}

func NewDailyPnLRepository(db *gorm.DB) *DailyPnLRepository {
	return &DailyPnLRepository{db: db}
}

func (r *DailyPnLRepository) Upsert(ctx context.Context, summary *models.DailyPnLSummary) error {
	return r.db.WithContext(ctx).
		Where("user_id = ? AND COALESCE(strategy_id, '') = COALESCE(?, '') AND trade_date = ? AND trading_mode = ?",
			summary.UserID, summary.StrategyID, summary.TradeDate, summary.TradingMode).
		Assign(*summary).
		FirstOrCreate(summary).Error
}

func (r *DailyPnLRepository) GetByUser(ctx context.Context, userID, tradingMode string, limit int) ([]models.DailyPnLSummary, error) {
	var summaries []models.DailyPnLSummary
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND trading_mode = ?", userID, tradingMode).
		Order("trade_date DESC").
		Limit(limit).
		Find(&summaries).Error
	return summaries, err
}
