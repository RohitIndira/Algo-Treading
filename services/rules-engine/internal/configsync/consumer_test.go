package configsync

import (
	"context"
	"testing"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

type fakeStore struct {
	upserts []uint64
	pauses  []uint64
	resumes []uint64
	deletes []uint64

	lastUserID     string
	lastStrategyID string
}

func (f *fakeStore) Upsert(cfg *models.StrategyConfig) error {
	if cfg != nil {
		f.lastUserID, f.lastStrategyID = cfg.UserID, cfg.StrategyID
		f.upserts = append(f.upserts, cfg.Version)
	}
	return nil
}

func (f *fakeStore) Pause(userID, strategyID string, version uint64) error {
	f.lastUserID, f.lastStrategyID = userID, strategyID
	f.pauses = append(f.pauses, version)
	return nil
}

func (f *fakeStore) Resume(userID, strategyID string, version uint64) error {
	f.lastUserID, f.lastStrategyID = userID, strategyID
	f.resumes = append(f.resumes, version)
	return nil
}

func (f *fakeStore) Remove(userID, strategyID string, version uint64) error {
	f.lastUserID, f.lastStrategyID = userID, strategyID
	f.deletes = append(f.deletes, version)
	return nil
}

func (f *fakeStore) GetStrategy(userID, strategyID string) (*models.StrategyConfig, bool) {
	_ = userID
	_ = strategyID
	// return none so stale-check doesn't interfere with tests
	return nil, false
}

func TestConfigSync_Created_CallsUpsert(t *testing.T) {
	fs := &fakeStore{}
	c := NewConsumer(fs)

	msg := []byte(`{"type":"CONFIG_CREATED","user_id":"u1","strategy_id":"s1","version":1,"config":{"strategy_id":"s1","user_id":"u1","strategy_name":"n","active":true,"trading_mode":"PAPER","conditions":{"match_all_news":true,"impact_score_min":1,"impact_score_max":2,"sentiments":[],"categories":[],"stock_codes":[]},"trade_config":{"order_type":"MARKET","quantity":1,"exchange":"NSE","order_side":"BUY"},"risk_limits":{"max_daily_trades":1}}}`)
	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(fs.upserts) != 1 || fs.upserts[0] != 1 {
		t.Fatalf("expected one upsert version 1")
	}
}

func TestConfigSync_Paused_CallsPause(t *testing.T) {
	fs := &fakeStore{}
	c := NewConsumer(fs)
	msg := []byte(`{"type":"CONFIG_PAUSED","user_id":"u1","strategy_id":"s1","version":2}`)
	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(fs.pauses) != 1 || fs.pauses[0] != 2 {
		t.Fatalf("expected pause")
	}
}

func TestConfigSync_Resumed_CallsResume(t *testing.T) {
	fs := &fakeStore{}
	c := NewConsumer(fs)
	msg := []byte(`{"type":"CONFIG_RESUMED","user_id":"u1","strategy_id":"s1","version":3}`)
	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(fs.resumes) != 1 || fs.resumes[0] != 3 {
		t.Fatalf("expected resume")
	}
}

func TestConfigSync_Deleted_CallsDelete(t *testing.T) {
	fs := &fakeStore{}
	c := NewConsumer(fs)
	msg := []byte(`{"type":"CONFIG_DELETED","user_id":"u1","strategy_id":"s1","version":4}`)
	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(fs.deletes) != 1 || fs.deletes[0] != 4 {
		t.Fatalf("expected delete")
	}
}

func TestConfigSync_UnknownType_SkipsGracefully(t *testing.T) {
	fs := &fakeStore{}
	c := NewConsumer(fs)
	msg := []byte(`{"type":"WAT","user_id":"u1","strategy_id":"s1","version":1}`)
	if err := c.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestConfigSync_ParseError_ContinuesNotCrashes(t *testing.T) {
	fs := &fakeStore{}
	c := NewConsumer(fs)
	if err := c.ProcessMessage(context.Background(), []byte(`not-json`)); err == nil {
		t.Fatalf("expected error")
	}
}
