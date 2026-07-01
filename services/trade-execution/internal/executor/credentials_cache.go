package executor

import (
	"context"
	"log"
	"runtime/debug"
	"sync"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
)

type cachedCreds struct {
	userId, appId, source, bearerToken string
}

// CredentialsCache keeps Indira credentials in memory for active users,
// eliminating a DB + decrypt round-trip on every order execution.
//
// Miss behaviour: load from repository, populate cache, return result.
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

// Get returns credentials for userID. Cache hit avoids DB; on miss it loads
// from the repository, stores the result, and returns it.
func (c *CredentialsCache) Get(ctx context.Context, userID string) (userId, appId, source, bearerToken string, err error) {
	c.mu.RLock()
	if creds, ok := c.cache[userID]; ok {
		c.mu.RUnlock()
		return creds.userId, creds.appId, creds.source, creds.bearerToken, nil
	}
	c.mu.RUnlock()

	// Cache miss — load from DB (decrypt happens inside repo).
	userId, appId, source, bearerToken, err = c.repo.GetIndiraCredentials(ctx, userID)
	if err != nil {
		return
	}

	c.mu.Lock()
	c.cache[userID] = &cachedCreds{userId, appId, source, bearerToken}
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
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[credentials] PANIC RECOVERED warming user %s: %v\n%s", uid, r, debug.Stack())
				}
			}()
			c.Get(ctx, uid) //nolint:errcheck — miss is non-fatal, will retry per-order
		}()
	}
	wg.Wait()
}

// Invalidate removes userID from the cache (e.g. on bearer-token rotation or logout).
// The next Get will reload from the repository.
func (c *CredentialsCache) Invalidate(userID string) {
	c.mu.Lock()
	delete(c.cache, userID)
	c.mu.Unlock()
}
