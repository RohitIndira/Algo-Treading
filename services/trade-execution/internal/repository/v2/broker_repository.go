package v2

import (
	"context"

	models "github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models/v2"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

// ────────────────────────────────────────────────────────────────────────────
// BrokerAccountRepository
// ────────────────────────────────────────────────────────────────────────────

type BrokerAccountRepository struct {
	db *gorm.DB
}

func NewBrokerAccountRepository(db *gorm.DB) *BrokerAccountRepository {
	return &BrokerAccountRepository{db: db}
}

func (r *BrokerAccountRepository) Upsert(ctx context.Context, account *models.BrokerAccount) error {
	return r.db.WithContext(ctx).
		Where("user_id = ? AND broker_name = ?", account.UserID, account.BrokerName).
		Assign(*account).
		FirstOrCreate(account).Error
}

func (r *BrokerAccountRepository) GetByUserID(ctx context.Context, userID string) (*models.BrokerAccount, error) {
	var account models.BrokerAccount
	err := r.db.WithContext(ctx).
		Where("user_id = ? AND broker_name = 'INDIRA' AND is_active = true", userID).
		First(&account).Error
	if err != nil {
		return nil, err
	}
	return &account, nil
}

func (r *BrokerAccountRepository) UpdateToken(ctx context.Context, userID, token string) error {
	return r.db.WithContext(ctx).
		Model(&models.BrokerAccount{}).
		Where("user_id = ? AND broker_name = 'INDIRA'", userID).
		Updates(map[string]interface{}{
			"bearer_token":     token,
			"token_updated_at": gorm.Expr("NOW()"),
		}).Error
}

// ────────────────────────────────────────────────────────────────────────────
// BrokerRequestRepository
// ────────────────────────────────────────────────────────────────────────────

type BrokerRequestRepository struct {
	db *gorm.DB
}

func NewBrokerRequestRepository(db *gorm.DB) *BrokerRequestRepository {
	return &BrokerRequestRepository{db: db}
}

func (r *BrokerRequestRepository) Create(ctx context.Context, req *models.BrokerRequest) error {
	return r.db.WithContext(ctx).Create(req).Error
}

func (r *BrokerRequestRepository) GetByOrderID(ctx context.Context, orderID uuid.UUID) ([]models.BrokerRequest, error) {
	var reqs []models.BrokerRequest
	err := r.db.WithContext(ctx).
		Where("order_id = ?", orderID).
		Order("created_at ASC").
		Find(&reqs).Error
	return reqs, err
}

func (r *BrokerRequestRepository) GetByBrokerOrderID(ctx context.Context, brokerOrderID string) (*models.BrokerRequest, error) {
	var req models.BrokerRequest
	err := r.db.WithContext(ctx).
		Where("broker_order_id = ?", brokerOrderID).
		Order("created_at DESC").
		First(&req).Error
	if err != nil {
		return nil, err
	}
	return &req, nil
}

// ────────────────────────────────────────────────────────────────────────────
// InstrumentRepository
// ────────────────────────────────────────────────────────────────────────────

type InstrumentRepository struct {
	db *gorm.DB
}

func NewInstrumentRepository(db *gorm.DB) *InstrumentRepository {
	return &InstrumentRepository{db: db}
}

func (r *InstrumentRepository) Upsert(ctx context.Context, inst *models.Instrument) error {
	return r.db.WithContext(ctx).
		Where("stock_code = ? AND exchange = ?", inst.StockCode, inst.Exchange).
		Assign(*inst).
		FirstOrCreate(inst).Error
}

func (r *InstrumentRepository) GetByStockCode(ctx context.Context, stockCode int64, exchange string) (*models.Instrument, error) {
	var inst models.Instrument
	err := r.db.WithContext(ctx).
		Where("stock_code = ? AND exchange = ?", stockCode, exchange).
		First(&inst).Error
	if err != nil {
		return nil, err
	}
	return &inst, nil
}

func (r *InstrumentRepository) GetOrCreate(ctx context.Context, stockCode int64, symbol, exchange string) (*models.Instrument, error) {
	var inst models.Instrument
	err := r.db.WithContext(ctx).
		Where("stock_code = ? AND exchange = ?", stockCode, exchange).
		Attrs(models.Instrument{
			StockCode:      stockCode,
			Symbol:         symbol,
			Exchange:       exchange,
			InstrumentType: "STK",
			LotSize:        1,
			IsActive:       true,
		}).
		FirstOrCreate(&inst).Error
	if err != nil {
		return nil, err
	}
	return &inst, nil
}
