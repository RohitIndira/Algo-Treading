package v2

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Source constants for status changes
const (
	SourceSystem    = "SYSTEM"
	SourceBroker    = "BROKER"
	SourceUser      = "USER"
	SourceScheduler = "SCHEDULER"
	SourceStrategy  = "STRATEGY"
)

type StatusHistory struct {
	ID            int64            `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	OrderID       uuid.UUID        `gorm:"column:order_id;type:uuid;not null;index:idx_osh_order_time" json:"order_id"`
	FromStatus    *string          `gorm:"column:from_status;type:varchar(20)" json:"from_status,omitempty"`
	ToStatus      string           `gorm:"column:to_status;type:varchar(20);not null" json:"to_status"`
	Source        string           `gorm:"column:source;type:varchar(20);not null" json:"source"`
	Reason        *string          `gorm:"column:reason;type:text" json:"reason,omitempty"`
	BrokerRawData json.RawMessage  `gorm:"column:broker_raw_data;type:jsonb" json:"broker_raw_data,omitempty"`
	Actor         *string          `gorm:"column:actor;type:varchar(50)" json:"actor,omitempty"`
	DedupKey      *string          `gorm:"column:dedup_key;type:varchar(128)" json:"dedup_key,omitempty"`
	CreatedAt     time.Time        `gorm:"column:created_at;not null;default:now();index:idx_osh_order_time" json:"created_at"`
}

func (StatusHistory) TableName() string { return "order_status_history" }

func NewStatusTransition(orderID uuid.UUID, from *string, to, source, reason string) StatusHistory {
	return StatusHistory{
		OrderID:    orderID,
		FromStatus: from,
		ToStatus:   to,
		Source:     source,
		Reason:     &reason,
		CreatedAt:  time.Now(),
	}
}

func NewBrokerStatusTransition(orderID uuid.UUID, from *string, to, reason string, rawData json.RawMessage) StatusHistory {
	return StatusHistory{
		OrderID:       orderID,
		FromStatus:    from,
		ToStatus:      to,
		Source:        SourceBroker,
		Reason:        &reason,
		BrokerRawData: rawData,
		CreatedAt:     time.Now(),
	}
}
