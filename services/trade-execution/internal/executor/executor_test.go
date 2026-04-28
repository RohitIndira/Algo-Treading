package executor

import (
	"context"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// minimal mock repo implementing needed methods
type mockRepo struct{}

func (m *mockRepo) Create(ctx context.Context, order *models.Order) error { return nil }
func (m *mockRepo) Update(ctx context.Context, order *models.Order) error { return nil }
func (m *mockRepo) GetByID(ctx context.Context, orderID uuid.UUID) (*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetByOdinOrderID(ctx context.Context, odinOrderID string) (*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetUserOrders(ctx context.Context, userID string, limit, offset int) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetOrdersByStatus(ctx context.Context, status models.OrderStatus, limit int) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) UpdateStatus(ctx context.Context, orderID uuid.UUID, status models.OrderStatus) error {
	return nil
}
func (m *mockRepo) RecordExecutionEvent(ctx context.Context, orderID uuid.UUID, eventType string, eventData map[string]interface{}) error {
	return nil
}
func (m *mockRepo) GetOpenOrders(ctx context.Context) ([]*models.Order, error) { return nil, nil }
func (m *mockRepo) GetTrailingStopLossOrders(ctx context.Context) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetAllActivePaperOrders(ctx context.Context) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetFilledPaperOrdersBySymbol(ctx context.Context, symbol string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetFilledPaperOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) UpdatePaperTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error {
	return nil
}
func (m *mockRepo) GetClosedPaperOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetClosedLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) UpdateLiveTradeExit(ctx context.Context, orderID uuid.UUID, exitPrice, pnl float64) error {
	return nil
}
func (m *mockRepo) CancelAllLiveOrdersByUser(ctx context.Context, userID string) error {
	return nil
}
func (m *mockRepo) GetActiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetExitableLiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetExitablePaperOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetFilledLiveOrdersByStrategy(ctx context.Context, strategyID, userID string) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) CancelAllOrdersByStrategy(ctx context.Context, strategyID, userID string) error {
	return nil
}
func (m *mockRepo) GetPendingMonitorOrders(ctx context.Context) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) ExistsByID(ctx context.Context, orderID uuid.UUID) (bool, error) {
	return false, nil
}
func (m *mockRepo) GetByIndiraOrderID(ctx context.Context, indiraOrderID string) (*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetDashboardStats(ctx context.Context, userID string, isPaper bool) (*repository.DashboardStats, error) {
	return nil, nil
}
func (m *mockRepo) GetDistinctActiveUserIDs(ctx context.Context) ([]string, error) {
	return nil, nil
}
func (m *mockRepo) GetActiveOCOOrders(ctx context.Context) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetOCOGroupOrders(ctx context.Context, groupID uuid.UUID) ([]*models.Order, error) {
	return nil, nil
}
func (m *mockRepo) GetAllTodayLiveOrdersByUser(ctx context.Context, userID string) ([]*models.Order, error) {
	return nil, nil
}

// credentials repo mock (not used in these tests)
type mockCredsRepo struct{}

func (m *mockCredsRepo) StoreIndiraCredentials(ctx context.Context, userID, userId, appId, source, bearerToken string) error {
	return nil
}

func (m *mockCredsRepo) GetIndiraCredentials(ctx context.Context, userID string) (string, string, string, string, error) {
	return "", "", "", "", nil
}

func TestExecuteOrder_MissingAuthFails(t *testing.T) {
	repo := &mockRepo{}
	creds := &mockCredsRepo{}

	exec := NewOrderExecutor(repo, creds, nil, nil, nil, zap.NewNop(), 0, 0)

	order := &models.Order{
		OrderID:      uuid.New(),
		UserID:       "", // missing
		StockCode:    11536,
		Exchange:     models.ExchangeNSE,
		Symbol:       "TCS",
		OrderType:    models.OrderTypeMarket,
		OrderSide:    models.OrderSideBuy,
		Quantity:     1,
		RiskApproved: true,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err := exec.ExecuteOrder(context.Background(), order)
	if err == nil {
		t.Fatalf("expected error when auth data missing, got nil")
	}
	if order.Status != models.StatusFailed {
		t.Fatalf("expected order status to be FAILED, got %s", order.Status)
	}
}

func TestExecuteOrder_NotApprovedIsRejected(t *testing.T) {
	repo := &mockRepo{}
	creds := &mockCredsRepo{}

	exec := NewOrderExecutor(repo, creds, nil, nil, nil, zap.NewNop(), 0, 0)

	order := &models.Order{
		OrderID:      uuid.New(),
		UserID:       "ISPL001",
		StockCode:    11536,
		Exchange:     models.ExchangeNSE,
		Symbol:       "TCS",
		OrderType:    models.OrderTypeMarket,
		OrderSide:    models.OrderSideBuy,
		Quantity:     1,
		RiskApproved: false, // not approved
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	err := exec.ExecuteOrder(context.Background(), order)
	if err == nil {
		t.Fatalf("expected error when risk not approved, got nil")
	}
	if order.Status != models.StatusRejected {
		t.Fatalf("expected order status to be REJECTED, got %s", order.Status)
	}
}
