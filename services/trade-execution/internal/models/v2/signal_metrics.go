package v2

import (
	"time"

	"github.com/google/uuid"
)

type SignalMetrics struct {
	ID        int64     `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	SignalID  uuid.UUID `gorm:"column:signal_id;type:uuid;not null" json:"signal_id"`
	OrderID   *uuid.UUID `gorm:"column:order_id;type:uuid" json:"order_id,omitempty"`
	UserID    string    `gorm:"column:user_id;type:varchar(50);not null" json:"user_id"`
	StrategyID *string  `gorm:"column:strategy_id;type:varchar(50)" json:"strategy_id,omitempty"`
	Symbol    *string   `gorm:"column:symbol;type:varchar(50)" json:"symbol,omitempty"`
	Exchange  *string   `gorm:"column:exchange;type:varchar(10)" json:"exchange,omitempty"`
	TradingMode *string `gorm:"column:trading_mode;type:varchar(10)" json:"trading_mode,omitempty"`
	FinalStatus *string `gorm:"column:final_status;type:varchar(20)" json:"final_status,omitempty"`

	// Timestamps at each pipeline stage
	SignalGeneratedAt       *time.Time `gorm:"column:signal_generated_at" json:"signal_generated_at,omitempty"`
	SignalPublishedAt       *time.Time `gorm:"column:signal_published_at" json:"signal_published_at,omitempty"`
	SignalConsumedAt        *time.Time `gorm:"column:signal_consumed_at" json:"signal_consumed_at,omitempty"`
	RiskCheckStartedAt      *time.Time `gorm:"column:risk_check_started_at" json:"risk_check_started_at,omitempty"`
	RiskCheckCompletedAt    *time.Time `gorm:"column:risk_check_completed_at" json:"risk_check_completed_at,omitempty"`
	OrderCreatedAt          *time.Time `gorm:"column:order_created_at" json:"order_created_at,omitempty"`
	BrokerRequestSentAt     *time.Time `gorm:"column:broker_request_sent_at" json:"broker_request_sent_at,omitempty"`
	BrokerResponseReceivedAt *time.Time `gorm:"column:broker_response_received_at" json:"broker_response_received_at,omitempty"`
	BrokerWSFirstUpdateAt   *time.Time `gorm:"column:broker_ws_first_update_at" json:"broker_ws_first_update_at,omitempty"`
	BrokerWSFilledAt        *time.Time `gorm:"column:broker_ws_filled_at" json:"broker_ws_filled_at,omitempty"`
	PositionOpenedAt        *time.Time `gorm:"column:position_opened_at" json:"position_opened_at,omitempty"`

	// Computed latencies (milliseconds)
	KafkaLatencyMs      *int `gorm:"column:kafka_latency_ms" json:"kafka_latency_ms,omitempty"`
	ProcessingLatencyMs *int `gorm:"column:processing_latency_ms" json:"processing_latency_ms,omitempty"`
	BrokerAPILatencyMs  *int `gorm:"column:broker_api_latency_ms" json:"broker_api_latency_ms,omitempty"`
	BrokerFillLatencyMs *int `gorm:"column:broker_fill_latency_ms" json:"broker_fill_latency_ms,omitempty"`
	TotalE2EMs          *int `gorm:"column:total_e2e_ms" json:"total_e2e_ms,omitempty"`

	// Metadata
	BrokerAPIRetries int     `gorm:"column:broker_api_retries;not null;default:0" json:"broker_api_retries"`
	ErrorType        *string `gorm:"column:error_type;type:varchar(20)" json:"error_type,omitempty"`
	ErrorMessage     *string `gorm:"column:error_message;type:text" json:"error_message,omitempty"`
	CreatedAt        time.Time `gorm:"column:created_at;not null;default:now()" json:"created_at"`
}

func (SignalMetrics) TableName() string { return "signal_metrics" }

// ComputeLatencies calculates all latency fields from timestamps.
func (m *SignalMetrics) ComputeLatencies() {
	m.KafkaLatencyMs = diffMs(m.SignalPublishedAt, m.SignalConsumedAt)
	m.ProcessingLatencyMs = diffMs(m.SignalConsumedAt, m.BrokerRequestSentAt)
	m.BrokerAPILatencyMs = diffMs(m.BrokerRequestSentAt, m.BrokerResponseReceivedAt)
	m.BrokerFillLatencyMs = diffMs(m.BrokerResponseReceivedAt, m.BrokerWSFilledAt)
	m.TotalE2EMs = diffMs(m.SignalGeneratedAt, m.BrokerWSFilledAt)
}

func diffMs(from, to *time.Time) *int {
	if from == nil || to == nil {
		return nil
	}
	ms := int(to.Sub(*from).Milliseconds())
	return &ms
}
