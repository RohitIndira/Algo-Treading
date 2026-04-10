package v2

import (
	"time"

	"github.com/google/uuid"
)

// Group type constants
const (
	GroupTypeOCO       = "OCO"
	GroupTypeBracket   = "BRACKET"
	GroupTypeSquareOff = "SQUARE_OFF"
)

// Group status constants
const (
	GroupStatusActive    = "ACTIVE"
	GroupStatusCompleted = "COMPLETED"
	GroupStatusCancelled = "CANCELLED"
)

// Leg role constants
const (
	LegRoleEntry = "ENTRY"
	LegRoleSL    = "SL_LEG"
	LegRoleTP    = "TP_LEG"
)

type OrderGroup struct {
	GroupID   uuid.UUID       `gorm:"column:group_id;type:uuid;primaryKey;default:gen_random_uuid()" json:"group_id"`
	GroupType string          `gorm:"column:group_type;type:varchar(20);not null" json:"group_type"`
	UserID    string          `gorm:"column:user_id;type:varchar(50);not null" json:"user_id"`
	Status    string          `gorm:"column:status;type:varchar(20);not null;default:'ACTIVE'" json:"status"`
	CreatedAt time.Time       `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt time.Time       `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	Legs      []OrderGroupLeg `gorm:"foreignKey:GroupID" json:"legs,omitempty"`
}

func (OrderGroup) TableName() string { return "order_groups" }

type OrderGroupLeg struct {
	ID        int       `gorm:"column:id;primaryKey;autoIncrement" json:"id"`
	GroupID   uuid.UUID `gorm:"column:group_id;type:uuid;not null;uniqueIndex:uq_ogl_group_order" json:"group_id"`
	OrderID   uuid.UUID `gorm:"column:order_id;type:uuid;not null;uniqueIndex:uq_ogl_group_order" json:"order_id"`
	LegRole   string    `gorm:"column:leg_role;type:varchar(20);not null" json:"leg_role"`
	Sequence  int       `gorm:"column:sequence;not null;default:0" json:"sequence"`
	CreatedAt time.Time `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	Order     *Order    `gorm:"foreignKey:OrderID" json:"order,omitempty"`
}

func (OrderGroupLeg) TableName() string { return "order_group_legs" }
