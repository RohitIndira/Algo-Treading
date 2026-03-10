package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configsync"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/segmentio/kafka-go"
)

type fakeReader struct {
	msgs   []kafka.Message
	idx    int
	closed bool
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	_ = ctx
	if f.closed {
		return kafka.Message{}, errors.New("closed")
	}
	if f.idx >= len(f.msgs) {
		// block-ish
		time.Sleep(10 * time.Millisecond)
		return kafka.Message{}, context.Canceled
	}
	m := f.msgs[f.idx]
	f.idx++
	return m, nil
}

func (f *fakeReader) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	_ = ctx
	_ = msgs
	return nil
}

func (f *fakeReader) Close() error {
	f.closed = true
	return nil
}

func TestConfigConsumer_UnknownType_SkipsGracefully(t *testing.T) {
	store := configstore.New()
	r := &fakeReader{msgs: []kafka.Message{{Value: []byte(`{"type":"WAT","user_id":"u","strategy_id":"s","version":1}`)}}}
	c := NewConfigConsumer(r, store)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = c.Start(ctx)
}

func TestConfigConsumer_ParseError_ContinuesNotCrashes(t *testing.T) {
	store := configstore.New()
	r := &fakeReader{msgs: []kafka.Message{{Value: []byte(`not-json`)}}}
	c := NewConfigConsumer(r, store)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	_ = c.Start(ctx)
}

func TestConfigConsumer_Created_Upserts(t *testing.T) {
	store := configstore.New()
	// create minimal config payload compatible with configsync mapper
	msg := kafka.Message{Value: []byte(`{"type":"CONFIG_CREATED","user_id":"u1","strategy_id":"s1","version":1,"config":{"strategy_id":"s1","user_id":"u1","strategy_name":"n","active":true,"trading_mode":"PAPER","conditions":{"match_all_news":true,"impact_score_min":1,"impact_score_max":2,"sentiments":[],"categories":[],"stock_codes":[]},"trade_config":{"order_type":"MARKET","quantity":1,"exchange":"NSE","order_side":"BUY"},"risk_limits":{"max_daily_trades":1}}}`)}
	r := &fakeReader{msgs: []kafka.Message{msg}}
	c := NewConfigConsumer(r, store)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_ = c.Start(ctx)

	if _, ok := store.GetStrategy("u1", "s1"); !ok {
		t.Fatalf("expected strategy loaded")
	}
}

func TestConfigConsumer_StaleVersion_Skipped(t *testing.T) {
	store := configstore.New()
	store.BulkLoad([]*models.StrategyConfig{{StrategyID: "s1", UserID: "u1", Version: 5, Active: true, TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1}, RiskLimits: models.RiskLimits{MaxDailyTrades: 1}}})

	msg := kafka.Message{Value: []byte(`{"type":"CONFIG_UPDATED","user_id":"u1","strategy_id":"s1","version":4,"config":{"strategy_id":"s1","user_id":"u1","strategy_name":"n","active":true,"trading_mode":"PAPER","conditions":{"match_all_news":true,"impact_score_min":1,"impact_score_max":2,"sentiments":[],"categories":[],"stock_codes":[]},"trade_config":{"order_type":"MARKET","quantity":1,"exchange":"NSE","order_side":"BUY"},"risk_limits":{"max_daily_trades":1}}}`)}
	r := &fakeReader{msgs: []kafka.Message{msg}}
	c := NewConfigConsumer(r, store)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_ = c.Start(ctx)
	st, _ := store.GetStrategy("u1", "s1")
	if st.Version != 5 {
		t.Fatalf("expected version 5, got %d", st.Version)
	}
}

func TestConfigConsumer_Paused_RemovesFromActiveKeepsInByUser(t *testing.T) {
	store := configstore.New()
	store.BulkLoad([]*models.StrategyConfig{{StrategyID: "s1", UserID: "u1", Version: 1, Active: true, TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1}, RiskLimits: models.RiskLimits{MaxDailyTrades: 1}}})

	msg := kafka.Message{Value: []byte(`{"type":"CONFIG_PAUSED","user_id":"u1","strategy_id":"s1","version":2}`)}
	r := &fakeReader{msgs: []kafka.Message{msg}}
	c := NewConfigConsumer(r, store)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_ = c.Start(ctx)

	snap := store.GetSnapshot()
	if len(snap.AllActive) != 0 {
		t.Fatalf("expected no active strategies")
	}
	if _, ok := store.GetStrategy("u1", "s1"); !ok {
		t.Fatalf("expected strategy still present (paused)")
	}
}

func TestConfigConsumer_Resumed_ReAddsToActive(t *testing.T) {
	store := configstore.New()
	store.BulkLoad([]*models.StrategyConfig{{StrategyID: "s1", UserID: "u1", Version: 1, Active: true, TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1}, RiskLimits: models.RiskLimits{MaxDailyTrades: 1}}})

	// pause then resume
	r := &fakeReader{msgs: []kafka.Message{{Value: []byte(`{"type":"CONFIG_PAUSED","user_id":"u1","strategy_id":"s1","version":2}`)}, {Value: []byte(`{"type":"CONFIG_RESUMED","user_id":"u1","strategy_id":"s1","version":3}`)}}}
	c := NewConfigConsumer(r, store)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	_ = c.Start(ctx)

	snap := store.GetSnapshot()
	if len(snap.AllActive) != 1 {
		t.Fatalf("expected 1 active strategy")
	}
}

func TestConfigConsumer_Deleted_RemovesFromBoth(t *testing.T) {
	store := configstore.New()
	store.BulkLoad([]*models.StrategyConfig{{StrategyID: "s1", UserID: "u1", Version: 1, Active: true, TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1}, RiskLimits: models.RiskLimits{MaxDailyTrades: 1}}})

	msg := kafka.Message{Value: []byte(`{"type":"CONFIG_DELETED","user_id":"u1","strategy_id":"s1","version":2}`)}
	r := &fakeReader{msgs: []kafka.Message{msg}}
	c := NewConfigConsumer(r, store)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_ = c.Start(ctx)

	snap := store.GetSnapshot()
	if len(snap.ByUser["u1"].Active)+len(snap.ByUser["u1"].Paused) != 0 {
		t.Fatalf("expected removed from byUser")
	}
	if len(snap.AllActive) != 0 {
		t.Fatalf("expected removed from AllActive")
	}
}

// ensure configsync is linked
var _ = configsync.ConfigEvent{}
