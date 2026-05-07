package v2

import "time"

type BrokerAccount struct {
	AccountID      int        `gorm:"column:account_id;primaryKey;autoIncrement" json:"account_id"`
	UserID         string     `gorm:"column:user_id;type:varchar(50);not null;uniqueIndex:uq_broker_accounts_user_broker" json:"user_id"`
	BrokerName     string     `gorm:"column:broker_name;type:varchar(20);not null;default:'INDIRA';uniqueIndex:uq_broker_accounts_user_broker" json:"broker_name"`
	BrokerUserID   *string    `gorm:"column:broker_user_id;type:varchar(100)" json:"broker_user_id,omitempty"`
	AppID          *string    `gorm:"column:app_id;type:varchar(100)" json:"app_id,omitempty"`
	Source         string     `gorm:"column:source;type:varchar(20);not null;default:'WEB'" json:"source"`
	BearerToken    *string    `gorm:"column:bearer_token;type:text" json:"-"` // never expose in JSON
	IsActive       bool       `gorm:"column:is_active;not null;default:true" json:"is_active"`
	TokenUpdatedAt *time.Time `gorm:"column:token_updated_at" json:"token_updated_at,omitempty"`
	CreatedAt      time.Time  `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt      time.Time  `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
}

func (BrokerAccount) TableName() string { return "broker_accounts" }
