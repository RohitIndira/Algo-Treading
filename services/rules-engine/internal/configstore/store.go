package configstore

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
)

// Snapshot is an immutable view of all strategies currently known to the Rules Engine.
//
// It is safe for concurrent reads without locks.
// Writers must build a NEW Snapshot (copy-on-write) and publish it atomically.
//
// Views:
//  1. ByUser: user_id -> strategies for that user (active + paused)
//  2. AllActive: flat list of all ACTIVE strategies across users
//
// Note: We intentionally store pointers to *models.Strategy. Writers MUST treat
// these as immutable once placed in a Snapshot.
type Snapshot struct {
	GeneratedAt time.Time

	// userID -> state
	ByUser map[string]UserView

	// All active strategies across the system.
	// This is the hot-path iteration list.
	AllActive []*models.Strategy

	// Stats
	TotalUsers          int
	TotalStrategies     int // active + paused
	TotalActive         int
	TotalPaused         int
	TotalDeletedTombsto int
}

// Finalize recomputes derived views (AllActive + stats) for the snapshot.
// Call this after building/updating ByUser manually.
func Finalize(s *Snapshot) {
	if s == nil {
		return
	}
	if s.ByUser == nil {
		s.ByUser = make(map[string]UserView)
	}
	rebuildAllActive(s)
	s.GeneratedAt = time.Now()
}

type UserView struct {
	// Active strategies for this user
	Active map[string]*models.Strategy // strategyID -> strategy
	// Paused strategies for this user
	Paused map[string]*models.Strategy // strategyID -> strategy
}

func newEmptySnapshot() *Snapshot {
	return &Snapshot{
		GeneratedAt: time.Now(),
		ByUser:      make(map[string]UserView),
		AllActive:   []*models.Strategy{},
	}
}

// Store holds an atomically published Snapshot.
// All writes must go through a single goroutine (recommended) via Writer.
type Store struct {
	snap atomic.Pointer[Snapshot]
}

func NewStore() *Store {
	s := &Store{}
	s.snap.Store(newEmptySnapshot())
	return s
}

func (s *Store) Snapshot() *Snapshot {
	return s.snap.Load()
}

// ReplaceSnapshot replaces current snapshot atomically.
// This is mainly used after bootstrap bulk-load.
func (s *Store) ReplaceSnapshot(next *Snapshot) {
	if next == nil {
		// defensive: never publish nil
		next = newEmptySnapshot()
	}
	next.GeneratedAt = time.Now()
	s.snap.Store(next)
}

// Writer applies copy-on-write updates to the Store.
//
// IMPORTANT: Writer is NOT concurrency-safe. Use it from a single goroutine.
type Writer struct {
	store   *Store
	current *Snapshot

	// tombstones hold strategy versions for deleted strategies.
	// This prevents out-of-order delivery resurrecting deleted strategies.
	// Key: userID|strategyID -> version
	tombstones map[string]uint64

	// versions tracks the last seen version for active/paused strategies.
	// Key: userID|strategyID -> version
	versions map[string]uint64
}

func NewWriter(store *Store) *Writer {
	if store == nil {
		store = NewStore()
	}
	w := &Writer{store: store}
	w.resetFromStore()
	return w
}

func (w *Writer) resetFromStore() {
	w.current = w.store.Snapshot()
	// initialize tombstones from scratch (best-effort)
	if w.tombstones == nil {
		w.tombstones = make(map[string]uint64)
	}
	if w.versions == nil {
		w.versions = make(map[string]uint64)
	}
}

func (w *Writer) Commit(next *Snapshot) {
	w.store.ReplaceSnapshot(next)
	w.current = next
}

func (w *Writer) key(userID, strategyID string) string {
	return userID + "|" + strategyID
}

// ApplyUpsert inserts or updates a strategy in the ACTIVE set.
// It performs version monotonicity checks and tombstone checks.
func (w *Writer) ApplyUpsert(strategy *models.Strategy, version uint64) (applied bool, err error) {
	if strategy == nil {
		return false, fmt.Errorf("strategy is nil")
	}
	if strategy.UserID == "" || strategy.StrategyID == "" {
		return false, fmt.Errorf("missing user_id/strategy_id")
	}
	// Drop any upsert with inactive flag. Upstream contract: bulk-load only active.
	// But for safety, keep this guard.
	if !strategy.Active {
		return false, nil
	}

	// Tombstone check
	if tv, ok := w.tombstones[w.key(strategy.UserID, strategy.StrategyID)]; ok {
		if version <= tv {
			return false, nil
		}
	}

	// Monotonic version check
	k := w.key(strategy.UserID, strategy.StrategyID)
	if last, ok := w.versions[k]; ok {
		if version <= last {
			return false, nil
		}
	}

	cur := w.current
	next := cloneSnapshotShallow(cur)

	uv := next.ByUser[strategy.UserID]
	// clone user view maps as needed (copy-on-write)
	uv = cloneUserView(uv)
	// If exists in paused, move to active
	if _, ok := uv.Paused[strategy.StrategyID]; ok {
		delete(uv.Paused, strategy.StrategyID)
	}
	// Set/replace
	strategy.Version = version
	// Set/replace
	uv.Active[strategy.StrategyID] = strategy
	next.ByUser[strategy.UserID] = uv
	w.versions[k] = version

	Finalize(next)
	w.Commit(next)
	return true, nil
}

// ApplyPause moves a strategy from Active -> Paused.
// If the strategy isn't present in Active, it's a no-op.
func (w *Writer) ApplyPause(userID, strategyID string, version uint64) (bool, error) {
	if userID == "" || strategyID == "" {
		return false, fmt.Errorf("missing user_id/strategy_id")
	}
	// Tombstone check
	if tv, ok := w.tombstones[w.key(userID, strategyID)]; ok {
		if version <= tv {
			return false, nil
		}
	}
	k := w.key(userID, strategyID)
	if last, ok := w.versions[k]; ok {
		if version <= last {
			return false, nil
		}
	}

	cur := w.current
	uv, ok := cur.ByUser[userID]
	if !ok {
		return false, nil
	}
	if _, ok := uv.Active[strategyID]; !ok {
		return false, nil
	}

	next := cloneSnapshotShallow(cur)
	nuv := cloneUserView(uv)

	strat := nuv.Active[strategyID]
	delete(nuv.Active, strategyID)
	nuv.Paused[strategyID] = strat
	next.ByUser[userID] = nuv
	w.versions[k] = version

	Finalize(next)
	w.Commit(next)
	return true, nil
}

// ApplyResume moves a strategy from Paused -> Active.
// If the strategy isn't present in Paused (e.g. restart between pause/resume), no-op.
func (w *Writer) ApplyResume(userID, strategyID string, version uint64) (bool, error) {
	if userID == "" || strategyID == "" {
		return false, fmt.Errorf("missing user_id/strategy_id")
	}
	if tv, ok := w.tombstones[w.key(userID, strategyID)]; ok {
		if version <= tv {
			return false, nil
		}
	}
	k := w.key(userID, strategyID)
	if last, ok := w.versions[k]; ok {
		if version <= last {
			return false, nil
		}
	}

	cur := w.current
	uv, ok := cur.ByUser[userID]
	if !ok {
		return false, nil
	}
	if _, ok := uv.Paused[strategyID]; !ok {
		return false, nil
	}

	next := cloneSnapshotShallow(cur)
	nuv := cloneUserView(uv)

	strat := nuv.Paused[strategyID]
	delete(nuv.Paused, strategyID)
	// Ensure active=true on resume
	stratCopy := *strat
	stratCopy.Active = true
	stratCopy.Version = version
	nuv.Active[strategyID] = &stratCopy
	next.ByUser[userID] = nuv
	w.versions[k] = version

	Finalize(next)
	w.Commit(next)
	return true, nil
}

// ApplyDelete removes a strategy from both Active and Paused and records a tombstone.
func (w *Writer) ApplyDelete(userID, strategyID string, version uint64) (bool, error) {
	if userID == "" || strategyID == "" {
		return false, fmt.Errorf("missing user_id/strategy_id")
	}

	tk := w.key(userID, strategyID)
	if tv, ok := w.tombstones[tk]; ok {
		if version <= tv {
			return false, nil
		}
	}
	w.tombstones[tk] = version
	// version record also advances
	w.versions[tk] = version

	cur := w.current
	uv, ok := cur.ByUser[userID]
	if !ok {
		return false, nil
	}

	// If not present, still consider delete applied because tombstone updated.
	_, inA := uv.Active[strategyID]
	_, inP := uv.Paused[strategyID]

	next := cloneSnapshotShallow(cur)
	nuv := cloneUserView(uv)
	delete(nuv.Active, strategyID)
	delete(nuv.Paused, strategyID)
	next.ByUser[userID] = nuv

	Finalize(next)
	w.Commit(next)
	return inA || inP, nil
}

// cloneSnapshotShallow clones the snapshot maps (but not strategies).
func cloneSnapshotShallow(cur *Snapshot) *Snapshot {
	next := &Snapshot{
		GeneratedAt: cur.GeneratedAt,
		ByUser:      make(map[string]UserView, len(cur.ByUser)),
		AllActive:   nil, // rebuilt
	}
	for uid, uv := range cur.ByUser {
		next.ByUser[uid] = uv // user maps cloned lazily in cloneUserView
	}
	return next
}

func cloneUserView(uv UserView) UserView {
	// When uv maps are nil (not yet present), create them.
	out := UserView{}
	if uv.Active == nil {
		out.Active = make(map[string]*models.Strategy)
	} else {
		out.Active = make(map[string]*models.Strategy, len(uv.Active))
		for k, v := range uv.Active {
			out.Active[k] = v
		}
	}
	if uv.Paused == nil {
		out.Paused = make(map[string]*models.Strategy)
	} else {
		out.Paused = make(map[string]*models.Strategy, len(uv.Paused))
		for k, v := range uv.Paused {
			out.Paused[k] = v
		}
	}
	return out
}

func rebuildAllActive(s *Snapshot) {
	// Flatten all active strategies into deterministic order.
	// Deterministic ordering provides strict fail-fast ordering and stable tests.
	userIDs := make([]string, 0, len(s.ByUser))
	totalStrategies := 0
	totalActive := 0
	totalPaused := 0
	for uid, uv := range s.ByUser {
		userIDs = append(userIDs, uid)
		totalStrategies += len(uv.Active) + len(uv.Paused)
		totalActive += len(uv.Active)
		totalPaused += len(uv.Paused)
	}
	sort.Strings(userIDs)
	all := make([]*models.Strategy, 0, totalActive)
	for _, uid := range userIDs {
		uv := s.ByUser[uid]
		sids := make([]string, 0, len(uv.Active))
		for sid := range uv.Active {
			sids = append(sids, sid)
		}
		sort.Strings(sids)
		for _, sid := range sids {
			all = append(all, uv.Active[sid])
		}
	}

	s.AllActive = all
	s.TotalUsers = len(s.ByUser)
	s.TotalStrategies = totalStrategies
	s.TotalActive = totalActive
	s.TotalPaused = totalPaused
}
