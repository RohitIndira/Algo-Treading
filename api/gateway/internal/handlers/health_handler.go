package handlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	commonpb "github.com/RohitIndira/Algo-Treading/api/proto/common"
	"github.com/RohitIndira/Algo-Treading/api/gateway/internal/grpc_clients"
)

// HealthHandler implements the standard three-tier health probe split used
// by production systems (k8s liveness/readiness, AWS ALB, Datadog synthetics):
//
//   /livez   — IS THE PROCESS ALIVE?
//              Always 200 if the HTTP server is up. No dependency calls.
//              Used by k8s liveness probe; failures restart the container.
//
//   /readyz  — CAN I SERVE TRAFFIC?
//              Reads-only probe of every critical dependency (3 Postgres
//              DBs, local Redis, external Redis, user-config gRPC).
//              Used by k8s readiness probe / load-balancer health checks
//              to gate traffic in/out without killing the process.
//
//   /health  — DEEP / WRITE PROBE.
//              Everything readyz does, PLUS a single-row UPSERT against
//              `health_probes` on each Postgres DB to verify the connection
//              isn't read-only (replica failover) and writes actually
//              commit. Returns per-component status + latency JSON.
//              Used by external synthetic monitoring (Datadog, Grafana
//              Synthetics) and humans during incidents.
//
// All probes run in parallel and have a 2-second hard timeout per component
// — a slow dependency can't stall the health endpoint past ~2s.
type HealthHandler struct {
	signalsDB     *sql.DB // market_data
	positionsDB   *sql.DB // trading_db
	ordersDB      *sql.DB // trading_execution
	redisClient   *redis.Client
	extRedis      *redis.Client
	userConfigGRP *grpc_clients.UserConfigClient
	logger        *zap.Logger

	startedAt time.Time
}

// NewHealthHandler wires every dependency the gateway holds. Any of them
// may be nil — the handler skips nil components in the readiness/deep
// reports rather than failing the whole probe.
func NewHealthHandler(
	signalsDB, positionsDB, ordersDB *sql.DB,
	redisClient, extRedis *redis.Client,
	userConfigGRP *grpc_clients.UserConfigClient,
	logger *zap.Logger,
) *HealthHandler {
	h := &HealthHandler{
		signalsDB:     signalsDB,
		positionsDB:   positionsDB,
		ordersDB:      ordersDB,
		redisClient:   redisClient,
		extRedis:      extRedis,
		userConfigGRP: userConfigGRP,
		logger:        logger,
		startedAt:     time.Now(),
	}
	// Bootstrap the probe table on each DB we have a handle for. Idempotent
	// — CREATE TABLE IF NOT EXISTS. Failing here doesn't block startup; the
	// write probe will simply report the table-missing error.
	bootCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for name, db := range h.eachDB() {
		if db == nil {
			continue
		}
		if _, err := db.ExecContext(bootCtx, healthProbeTableDDL); err != nil {
			if logger != nil {
				logger.Warn("health_probes table bootstrap failed",
					zap.String("db", name), zap.Error(err))
			}
		}
	}
	return h
}

// healthProbeTableDDL is the single-row probe table. PRIMARY KEY on service
// + replica_id keeps the table at ≤ N rows forever; each probe call is an
// UPSERT updating last_probe_at and incrementing probe_count.
const healthProbeTableDDL = `
CREATE TABLE IF NOT EXISTS health_probes (
    service       TEXT        NOT NULL,
    replica_id    TEXT        NOT NULL DEFAULT 'default',
    last_probe_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    probe_count   BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (service, replica_id)
)`

// componentResult is the per-dependency report block returned by readyz / health.
type componentResult struct {
	Status     string `json:"status"`               // "ok" | "fail" | "skipped"
	LatencyMs  int64  `json:"latency_ms,omitempty"` // round-trip in milliseconds
	Error      string `json:"error,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

// Livez handles GET /livez. Always 200 as long as the HTTP server can
// respond. No dependency checks — that's readyz's job. Restarting the
// pod won't help if a DB is down, so liveness must NEVER fail on a
// dependency outage.
func (h *HealthHandler) Livez(w http.ResponseWriter, r *http.Request) {
	respondWithJSON(w, http.StatusOK, map[string]any{
		"status":     "ok",
		"service":    "api-gateway",
		"uptime_sec": int(time.Since(h.startedAt).Seconds()),
		"now":        time.Now().UTC().Format(time.RFC3339),
	})
}

// Readyz handles GET /readyz. Read-only probes of every dependency, in
// parallel. Returns 200 if all reachable components are healthy, 503 if
// any required component is failing. Nil components are reported as
// "skipped" (not a failure — gateway can boot without optional deps).
func (h *HealthHandler) Readyz(w http.ResponseWriter, r *http.Request) {
	results := h.probeAll(r.Context(), false /* writes=false */)
	status, code := overall(results)
	respondWithJSON(w, code, map[string]any{
		"status":     status,
		"service":    "api-gateway",
		"uptime_sec": int(time.Since(h.startedAt).Seconds()),
		"now":        time.Now().UTC().Format(time.RFC3339),
		"components": results,
	})
}

// Health handles GET /health. The deep probe — readyz + DB write probes.
// Writes a single-row UPSERT to health_probes on each Postgres DB and
// measures the round-trip. Useful for catching read-only replicas, locked
// tables, or slow disk that wouldn't show in a pure SELECT 1 readiness.
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	results := h.probeAll(r.Context(), true /* writes=true */)
	status, code := overall(results)
	respondWithJSON(w, code, map[string]any{
		"status":     status,
		"service":    "api-gateway",
		"uptime_sec": int(time.Since(h.startedAt).Seconds()),
		"now":        time.Now().UTC().Format(time.RFC3339),
		"components": results,
		"probe_kind": "deep",
	})
}

// ─── implementation ──────────────────────────────────────────────────────

// eachDB returns the three Postgres handles by logical name, in a fixed
// iteration order, so the JSON output is stable across calls.
func (h *HealthHandler) eachDB() map[string]*sql.DB {
	return map[string]*sql.DB{
		"postgres_market_data":      h.signalsDB,
		"postgres_trading_db":       h.positionsDB,
		"postgres_trading_execution": h.ordersDB,
	}
}

// probeAll runs every dependency check in parallel with a per-probe
// timeout. write=true triggers the UPSERT on each Postgres DB on top of
// the SELECT 1 ping.
func (h *HealthHandler) probeAll(ctx context.Context, write bool) map[string]componentResult {
	out := make(map[string]componentResult)
	var mu sync.Mutex
	var wg sync.WaitGroup

	set := func(name string, res componentResult) {
		mu.Lock()
		out[name] = res
		mu.Unlock()
	}

	// Postgres — one goroutine per DB
	for name, db := range h.eachDB() {
		name, db := name, db
		wg.Add(1)
		go func() {
			defer wg.Done()
			set(name, h.probePostgres(ctx, db, name, write))
		}()
	}

	// Local Redis (WS pub/sub)
	wg.Add(1)
	go func() {
		defer wg.Done()
		set("redis_local", h.probeRedis(ctx, h.redisClient, "wspubsub"))
	}()

	// External Redis (live LTP)
	wg.Add(1)
	go func() {
		defer wg.Done()
		set("redis_external", h.probeRedis(ctx, h.extRedis, "live-ltp"))
	}()

	// user-config gRPC
	wg.Add(1)
	go func() {
		defer wg.Done()
		set("grpc_user_config", h.probeUserConfig(ctx))
	}()

	wg.Wait()
	return out
}

func (h *HealthHandler) probePostgres(ctx context.Context, db *sql.DB, logicalName string, write bool) componentResult {
	if db == nil {
		return componentResult{Status: "skipped", Detail: "DB handle not configured"}
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	t0 := time.Now()
	// Read probe — SELECT 1 confirms session + network round-trip.
	if err := db.PingContext(cctx); err != nil {
		return componentResult{
			Status:    "fail",
			LatencyMs: time.Since(t0).Milliseconds(),
			Error:     "ping: " + err.Error(),
		}
	}
	res := componentResult{
		Status:    "ok",
		LatencyMs: time.Since(t0).Milliseconds(),
	}
	if !write {
		return res
	}

	// Write probe — single-row UPSERT. Confirms:
	//   1. Connection isn't read-only (replica failover catches this)
	//   2. WAL accepts writes (full disk / locked tablespace catches this)
	//   3. Trigger execution path is healthy
	t1 := time.Now()
	_, err := db.ExecContext(cctx,
		`INSERT INTO health_probes (service, replica_id, last_probe_at, probe_count)
         VALUES ($1, $2, now(), 1)
         ON CONFLICT (service, replica_id)
         DO UPDATE SET last_probe_at = EXCLUDED.last_probe_at,
                       probe_count   = health_probes.probe_count + 1`,
		"api-gateway", "default")
	if err != nil {
		return componentResult{
			Status:    "fail",
			LatencyMs: res.LatencyMs,
			Error:     "write probe: " + err.Error(),
			Detail:    "read OK; write failed — possible read-only replica or missing health_probes table",
		}
	}
	res.LatencyMs = time.Since(t0).Milliseconds() // total (read + write)
	res.Detail = fmt.Sprintf("write probe %dms", time.Since(t1).Milliseconds())
	return res
}

func (h *HealthHandler) probeRedis(ctx context.Context, c *redis.Client, label string) componentResult {
	if c == nil {
		return componentResult{Status: "skipped", Detail: label + ": not configured"}
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	t0 := time.Now()
	if err := c.Ping(cctx).Err(); err != nil {
		return componentResult{
			Status:    "fail",
			LatencyMs: time.Since(t0).Milliseconds(),
			Error:     err.Error(),
		}
	}
	return componentResult{
		Status:    "ok",
		LatencyMs: time.Since(t0).Milliseconds(),
		Detail:    label,
	}
}

func (h *HealthHandler) probeUserConfig(ctx context.Context) componentResult {
	if h.userConfigGRP == nil {
		return componentResult{Status: "skipped", Detail: "user-config client not configured"}
	}
	cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	t0 := time.Now()
	_, err := h.userConfigGRP.HealthCheck(cctx, &commonpb.HealthCheckRequest{})
	if err != nil {
		return componentResult{
			Status:    "fail",
			LatencyMs: time.Since(t0).Milliseconds(),
			Error:     err.Error(),
			Detail:    "downstream user-config service unreachable — strategy CRUD will fail",
		}
	}
	return componentResult{
		Status:    "ok",
		LatencyMs: time.Since(t0).Milliseconds(),
	}
}

// overall summarises the per-component map into a single status + HTTP
// code. Any "fail" → 503. All-"ok"-or-"skipped" → 200.
func overall(results map[string]componentResult) (string, int) {
	for _, r := range results {
		if r.Status == "fail" {
			return "degraded", http.StatusServiceUnavailable
		}
	}
	return "ok", http.StatusOK
}
