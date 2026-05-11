package executor

import (
	"context"
	"sync"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
)

// cacheTTL bounds the staleness window when the Kafka-based invalidation
// path drops an event. Even if every USER_CREDENTIALS_UPDATED event were
// lost forever, a user's cache entry refreshes from DB at most this many
// seconds after the broker JWT changed.
//
// Why 60s: the JWT itself is valid for ~12h, so freshness within a minute
// is well inside the broker's session window. Shorter means more DB load
// (~2k–20k users × 1 reload/min); longer means a worse worst-case UX after
// a missed event. 60s is the sweet spot for the current 1k–10k scale.
const cacheTTL = 60 * time.Second

type cachedCreds struct {
	userId, appId, source, bearerToken string
	loadedAt                           time.Time
}

// CredentialsCache keeps Indira credentials in memory for active users,
// eliminating a DB + decrypt round-trip on every order execution.
//
// Consistency model: cache is invalidated three independent ways so a
// missed Kafka event never wedges a user in a stale-JWT loop:
//
//   1. Kafka USER_CREDENTIALS_UPDATED event → Invalidate(userID)
//      (zero-latency happy path; works when the publish succeeds)
//
//   2. ErrAuthExpired observed on any broker call → Invalidate(userID)
//      (zero-latency self-heal; wired via JWTNotifier.OnSessionExpired
//      in main.go — see the SESSION_EXPIRED fan-out)
//
//   3. cacheTTL elapsed since last load → fall through to DB on next Get
//      (worst-case bound; closes the dual-write gap when both #1 and #2
//      somehow drop, e.g. the very first call after a silent JWT swap)
//
// Thread-safe for concurrent reads and writes.
type CredentialsCache struct {
	mu    sync.RWMutex
	cache map[string]*cachedCreds
	repo  repository.CredentialsRepository
}

// NewCredentialsCache wraps repo with an in-memory cache.
func NewCredentialsCache(repo repository.CredentialsRepository) *CredentialsCache {
	return &CredentialsCache{
		cache: make(map[string]*cachedCreds),
		repo:  repo,
	}
}

// Get returns credentials for userID. Cache hit avoids DB; on miss OR on
// an entry older than cacheTTL it reloads from the repository, stores the
// fresh result, and returns it.
func (c *CredentialsCache) Get(ctx context.Context, userID string) (userId, appId, source, bearerToken string, err error) {
	c.mu.RLock()
	creds, ok := c.cache[userID]
	c.mu.RUnlock()
	if ok && time.Since(creds.loadedAt) < cacheTTL {
		return creds.userId, creds.appId, creds.source, creds.bearerToken, nil
	}

	// Miss OR expired — load from DB (decrypt happens inside repo).
	userId, appId, source, bearerToken, err = c.repo.GetIndiraCredentials(ctx, userID)
	if err != nil {
		return
	}

	c.mu.Lock()
	c.cache[userID] = &cachedCreds{
		userId:      userId,
		appId:       appId,
		source:      source,
		bearerToken: bearerToken,
		loadedAt:    time.Now(),
	}
	c.mu.Unlock()
	return
}

// Warm pre-loads credentials for userIDs concurrently using one goroutine per
// user. Call this on startup with the list of users who have active strategies
// so the first order for each user hits the cache rather than the DB.
// Errors per user are silently ignored — they will fall back to DB on demand.
func (c *CredentialsCache) Warm(ctx context.Context, userIDs []string) {
	var wg sync.WaitGroup
	for _, uid := range userIDs {
		uid := uid
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Get(ctx, uid) //nolint:errcheck — miss is non-fatal, will retry per-order
		}()
	}
	wg.Wait()
}

// Invalidate removes userID from the cache. Triggered by:
//
//   - The Kafka USER_CREDENTIALS_UPDATED consumer when SSO refreshes a JWT
//     (the cache-invalidator interface in consumer/strategy_events_consumer.go)
//
//   - The JWTExpiryNotifier's OnSessionExpired callback wired in main.go,
//     so the FIRST AU004 from any broker call also force-evicts the entry
//     instead of waiting for cacheTTL.
//
// The next Get for that user reloads from the repository.
func (c *CredentialsCache) Invalidate(userID string) {
	c.mu.Lock()
	delete(c.cache, userID)
	c.mu.Unlock()
}
