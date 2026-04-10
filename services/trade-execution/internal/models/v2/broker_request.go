package v2

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Action constants
const (
	BrokerActionPlace  = "PLACE"
	BrokerActionModify = "MODIFY"
	BrokerActionCancel = "CANCEL"
)

// ErrorType constants
const (
	ErrorTypeTimeout  = "TIMEOUT"
	ErrorTypeAuth     = "AUTH"
	ErrorTypeBusiness = "BUSINESS"
	ErrorTypeNetwork  = "NETWORK"
)

type BrokerRequest struct {
	ID            int64           `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	OrderID       uuid.UUID       `gorm:"column:order_id;type:uuid;not null;index:idx_br_order" json:"order_id"`
	Action        string          `gorm:"column:action;type:varchar(10);not null" json:"action"`
	BrokerName    string          `gorm:"column:broker_name;type:varchar(20);not null;default:'INDIRA'" json:"broker_name"`
	Attempt       int             `gorm:"column:attempt;not null;default:0" json:"attempt"`
	RequestURL    *string         `gorm:"column:request_url;type:varchar(255)" json:"request_url,omitempty"`
	RequestBody   json.RawMessage `gorm:"column:request_body;type:jsonb" json:"request_body,omitempty"`
	HTTPStatus    *int            `gorm:"column:http_status" json:"http_status,omitempty"`
	ResponseBody  json.RawMessage `gorm:"column:response_body;type:jsonb" json:"response_body,omitempty"`
	BrokerOrderID *string         `gorm:"column:broker_order_id;type:varchar(50)" json:"broker_order_id,omitempty"`
	LatencyMs     *int            `gorm:"column:latency_ms" json:"latency_ms,omitempty"`
	ErrorType     *string         `gorm:"column:error_type;type:varchar(20)" json:"error_type,omitempty"`
	ErrorMessage  *string         `gorm:"column:error_message;type:text" json:"error_message,omitempty"`
	CreatedAt     time.Time       `gorm:"column:created_at;not null;default:now();index:idx_br_order" json:"created_at"`
}

func (BrokerRequest) TableName() string { return "broker_requests" }
