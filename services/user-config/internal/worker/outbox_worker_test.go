package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/events"
	"github.com/RohitIndira/Algo-Treading/services/user-config/internal/models"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/segmentio/kafka-go"
)

type mockKafkaWriter struct {
	messages []kafka.Message
	err      error
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.err != nil {
		return m.err
	}
	m.messages = append(m.messages, msgs...)
	return nil
}

func (m *mockKafkaWriter) Close() error { return nil }

func TestProcessEvent_Created_PublishesCorrectly(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)

	strategyID := uuid.New()
	s := models.Strategy{
		StrategyID:   strategyID,
		UserID:       "user-1",
		StrategyName: "s1",
		Active:       true,
		TradingMode:  models.TradingModeLive,
		Version:      1,
		CreatedAt:    time.Now().Add(-1 * time.Hour),
		UpdatedAt:    time.Now(),
		Conditions: &models.StrategyCondition{
			ImpactScoreMin: 1,
			ImpactScoreMax: 2,
			Sentiments:     pq.StringArray{"POSITIVE"},
			Categories:     pq.StringArray{"EARNINGS"},
			StockCodes:     pq.Int64Array{500325},
			Exchanges:      pq.StringArray{"NSE"},
		},
		TradeConfig: &models.TradeConfig{OrderType: "MARKET", ProductType: "INTRADAY", Validity: "DAY", Quantity: 1, Exchange: "NSE", OrderSide: "BUY", StopLossType: "FIXED"},
		RiskLimits:  &models.RiskLimits{EnableRiskChecks: true, EnableAutoSquareOff: false, AutoSquareOffTime: "15:15"},
	}

	payload, _ := json.Marshal(s)
	outbox := &models.ExecutionOutbox{ID: 10, EventType: "STRATEGY_CREATED", Payload: payload}

	published, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !published {
		t.Fatalf("expected published=true")
	}
	if len(w.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.messages))
	}
	msg := w.messages[0]
	if string(msg.Key) != "user-1" {
		t.Fatalf("expected key user-1, got %s", string(msg.Key))
	}

	var ce events.ConfigEvent
	if err := json.Unmarshal(msg.Value, &ce); err != nil {
		t.Fatalf("unmarshal published event: %v", err)
	}
	if ce.Type != events.ConfigCreated {
		t.Fatalf("expected %s got %s", events.ConfigCreated, ce.Type)
	}
	if ce.Config == nil {
		t.Fatalf("expected full config")
	}
	if ce.UserID != "user-1" || ce.StrategyID != strategyID.String() {
		t.Fatalf("id mismatch")
	}
	assertHeader(t, msg.Headers, "event_type", string(events.ConfigCreated))
	assertHeader(t, msg.Headers, "user_id", "user-1")
}

func TestProcessEvent_Deleted_PublishesCorrectly(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)

	payload := []byte(`{"strategy_id":"strat-1","user_id":"user-1","version":3,"active":false,"deleted":true}`)
	outbox := &models.ExecutionOutbox{ID: 11, EventType: "STRATEGY_DELETED", Payload: payload}

	published, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !published {
		t.Fatalf("expected published=true")
	}
	if len(w.messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(w.messages))
	}
	msg := w.messages[0]
	if string(msg.Key) != "user-1" {
		t.Fatalf("expected key user-1, got %s", string(msg.Key))
	}
	var ce events.ConfigEvent
	_ = json.Unmarshal(msg.Value, &ce)
	if ce.Type != events.ConfigDeleted {
		t.Fatalf("expected %s got %s", events.ConfigDeleted, ce.Type)
	}
	if ce.Config != nil {
		t.Fatalf("expected thin event")
	}
	if ce.Version != 3 {
		t.Fatalf("expected version=3 got %d", ce.Version)
	}
}

func TestProcessEvent_Activated_PublishesResumed(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)
	payload := []byte(`{"strategy_id":"strat-1","user_id":"user-1","version":2,"active":true}`)
	outbox := &models.ExecutionOutbox{ID: 12, EventType: "STRATEGY_ACTIVATED", Payload: payload}

	_, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(w.messages) != 1 {
		t.Fatalf("expected 1 message")
	}
	var ce events.ConfigEvent
	_ = json.Unmarshal(w.messages[0].Value, &ce)
	if ce.Type != events.ConfigResumed {
		t.Fatalf("expected %s got %s", events.ConfigResumed, ce.Type)
	}
	if ce.Config != nil {
		t.Fatalf("expected thin resumed event")
	}
}

func TestProcessEvent_Deactivated_PublishesPaused(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)
	payload := []byte(`{"strategy_id":"strat-1","user_id":"user-1","version":2,"active":false}`)
	outbox := &models.ExecutionOutbox{ID: 13, EventType: "STRATEGY_DEACTIVATED", Payload: payload}

	_, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var ce events.ConfigEvent
	_ = json.Unmarshal(w.messages[0].Value, &ce)
	if ce.Type != events.ConfigPaused {
		t.Fatalf("expected %s got %s", events.ConfigPaused, ce.Type)
	}
	if ce.Config != nil {
		t.Fatalf("expected thin paused event")
	}
}

func TestProcessEvent_UnknownEventType_SkipsGracefully(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)
	outbox := &models.ExecutionOutbox{ID: 14, EventType: "SOMETHING_UNKNOWN", Payload: []byte(`{}`)}

	published, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err != nil {
		t.Fatalf("expected nil error")
	}
	if !published {
		t.Fatalf("expected published=true so it can be marked processed")
	}
	if len(w.messages) != 0 {
		t.Fatalf("expected 0 messages")
	}
}

func TestProcessEvent_EmptyPayload_SkipsGracefully(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)
	outbox := &models.ExecutionOutbox{ID: 15, EventType: "STRATEGY_CREATED", Payload: []byte{}}

	published, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err != nil {
		t.Fatalf("expected nil error")
	}
	if !published {
		t.Fatalf("expected published=true")
	}
	if len(w.messages) != 0 {
		t.Fatalf("expected 0 messages")
	}
}

func TestProcessEvent_MalformedJSON_ReturnsError(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)
	outbox := &models.ExecutionOutbox{ID: 16, EventType: "STRATEGY_CREATED", Payload: []byte(`{this is not json`)}

	_, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err == nil {
		t.Fatalf("expected error")
	}
	if len(w.messages) != 0 {
		t.Fatalf("expected 0 messages")
	}
}

func TestProcessEvent_EmptyUserID_SkipsAndDoesNotPublish(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)
	strategyID := uuid.New()
	s := models.Strategy{StrategyID: strategyID, UserID: "", StrategyName: "s1", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	payload, _ := json.Marshal(s)
	outbox := &models.ExecutionOutbox{ID: 17, EventType: "STRATEGY_CREATED", Payload: payload}

	published, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err != nil {
		t.Fatalf("expected nil error")
	}
	if !published {
		t.Fatalf("expected published=true so it can be marked processed")
	}
	if len(w.messages) != 0 {
		t.Fatalf("expected 0 messages")
	}
}

func TestProcessEvent_KafkaFailure_ReturnsError(t *testing.T) {
	w := &mockKafkaWriter{err: errors.New("kafka down")}
	ow := NewOutboxWorker(nil, w, 0)
	strategyID := uuid.New()
	s := models.Strategy{StrategyID: strategyID, UserID: "u1", StrategyName: "s1", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	payload, _ := json.Marshal(s)
	outbox := &models.ExecutionOutbox{ID: 18, EventType: "STRATEGY_CREATED", Payload: payload}

	_, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestProcessEvent_VersionZero_StillPublishes(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)
	strategyID := uuid.New()
	s := models.Strategy{StrategyID: strategyID, UserID: "u1", StrategyName: "s1", Version: 0, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	payload, _ := json.Marshal(s)
	outbox := &models.ExecutionOutbox{ID: 19, EventType: "STRATEGY_CREATED", Payload: payload}

	_, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var ce events.ConfigEvent
	_ = json.Unmarshal(w.messages[0].Value, &ce)
	if ce.Version != 0 {
		t.Fatalf("expected version=0 got %d", ce.Version)
	}
}

func TestOutboxWorker_KafkaPartitionKey_IsUserID_NotStrategyID(t *testing.T) {
	w := &mockKafkaWriter{}
	ow := NewOutboxWorker(nil, w, 0)
	userID := "user-99"

	for i := 0; i < 3; i++ {
		strategyID := uuid.New()
		s := models.Strategy{StrategyID: strategyID, UserID: userID, StrategyName: "s", CreatedAt: time.Now(), UpdatedAt: time.Now()}
		payload, _ := json.Marshal(s)
		outbox := &models.ExecutionOutbox{ID: int64(100 + i), EventType: "STRATEGY_CREATED", Payload: payload}
		_, err := ow.processOneOutboxEvent(context.Background(), outbox)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(w.messages[i].Key) != userID {
			t.Fatalf("expected key=%s got %s", userID, string(w.messages[i].Key))
		}
		if string(w.messages[i].Key) == strategyID.String() {
			t.Fatalf("key must not be strategyID")
		}
	}
}

func assertHeader(t *testing.T, headers []kafka.Header, key string, want string) {
	t.Helper()
	for _, h := range headers {
		if h.Key == key {
			if string(h.Value) != want {
				t.Fatalf("header %s expected %s got %s", key, want, string(h.Value))
			}
			return
		}
	}
	t.Fatalf("missing header %s", key)
}

func TestProcessEvent_KafkaPublishErrorDoesNotLoseMessages(t *testing.T) {
	// Not a strict requirement, but ensures error bubbles up.
	w := &mockKafkaWriter{err: errors.New("write failed")}
	ow := NewOutboxWorker(nil, w, 0)
	strategyID := uuid.New()
	s := models.Strategy{StrategyID: strategyID, UserID: "u1", StrategyName: "s1", CreatedAt: time.Now(), UpdatedAt: time.Now()}
	payload, _ := json.Marshal(s)
	outbox := &models.ExecutionOutbox{ID: 20, EventType: "STRATEGY_CREATED", Payload: payload}

	_, err := ow.processOneOutboxEvent(context.Background(), outbox)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, w.err) {
		t.Fatalf("expected wrapped kafka error")
	}
}
