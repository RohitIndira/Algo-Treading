package configstore

import (
	"fmt"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// ConfigStore is the public facade used by startup + Kafka consumers + engine.
//
// Internally it holds an atomic Snapshot for lock-free reads and a single-writer
// copy-on-write Writer for updates.
//
// IMPORTANT: writes must be performed by a single goroutine (enforced by
// convention). ConfigStore methods are guarded by a small mutex to avoid
// accidental concurrent writes.
type ConfigStore struct {
	store  *Store
	writer *Writer
	mu     sync.Mutex
}

func New() *ConfigStore {
	s := NewStore()
	return &ConfigStore{store: s, writer: NewWriter(s)}
}

func (cs *ConfigStore) GetSnapshot() *Snapshot {
	return cs.store.Snapshot()
}

type Metrics struct {
	GeneratedAt     time.Time
	TotalUsers      int
	TotalStrategies int
	TotalActive     int
	TotalPaused     int
}

func (cs *ConfigStore) Metrics() Metrics {
	s := cs.store.Snapshot()
	return Metrics{
		GeneratedAt:     s.GeneratedAt,
		TotalUsers:      s.TotalUsers,
		TotalStrategies: s.TotalStrategies,
		TotalActive:     s.TotalActive,
		TotalPaused:     s.TotalPaused,
	}
}

// BulkLoad replaces the entire snapshot with the provided active strategies.
// This is called once at startup after fetching from User Config gRPC.
func (cs *ConfigStore) BulkLoad(strategies []*models.StrategyConfig) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	snap := &Snapshot{ByUser: make(map[string]UserView)}
	for _, st := range strategies {
		if st == nil {
			continue
		}
		if !st.Active {
			continue
		}
		uv := snap.ByUser[st.UserID]
		if uv.Active == nil {
			uv.Active = make(map[string]*models.Strategy)
		}
		if uv.Paused == nil {
			uv.Paused = make(map[string]*models.Strategy)
		}
		// StrategyConfig is an alias of Strategy.
		uv.Active[st.StrategyID] = (*models.Strategy)(st)
		snap.ByUser[st.UserID] = uv
	}
	Finalize(snap)
	cs.store.ReplaceSnapshot(snap)
	cs.writer.resetFromStore()
}

func (cs *ConfigStore) Upsert(cfg *models.StrategyConfig) error {
	if cfg == nil {
		return fmt.Errorf("strategy config is nil")
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	_, err := cs.writer.ApplyUpsert((*models.Strategy)(cfg), cfg.Version)
	return err
}

func (cs *ConfigStore) Pause(userID, strategyID string, version uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	_, err := cs.writer.ApplyPause(userID, strategyID, version)
	return err
}

func (cs *ConfigStore) Resume(userID, strategyID string, version uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	_, err := cs.writer.ApplyResume(userID, strategyID, version)
	return err
}

func (cs *ConfigStore) Remove(userID, strategyID string, version uint64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	_, err := cs.writer.ApplyDelete(userID, strategyID, version)
	return err
}

// GetStrategy returns a strategy from the current snapshot, searching both
// active and paused sets.
func (cs *ConfigStore) GetStrategy(userID, strategyID string) (*models.StrategyConfig, bool) {
	s := cs.store.Snapshot()
	uv, ok := s.ByUser[userID]
	if !ok {
		return nil, false
	}
	if st, ok := uv.Active[strategyID]; ok {
		return (*models.StrategyConfig)(st), true
	}
	if st, ok := uv.Paused[strategyID]; ok {
		return (*models.StrategyConfig)(st), true
	}
	return nil, false
}
