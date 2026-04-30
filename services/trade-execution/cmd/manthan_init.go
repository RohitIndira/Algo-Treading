package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	indiraPkg "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/manthan"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/statusservice"
)

// ManthanModule holds all Manthan order execution components.
type ManthanModule struct {
	SignalConsumer    *manthan.SignalConsumer
	SafetyMonitor     *manthan.SafetyMonitor
	Reconciler        *manthan.Reconciler
	Recovery          *manthan.Recovery
	WSSBridge         *manthan.WSSBridge
	EntryHandler      *manthan.EntryHandler
	SLHandler         *manthan.SLHandler
	BrokerAdapter     *manthan.BrokerAdapter
	ExternalDetector  *manthan.ExternalActivityDetector // nil when disabled
	ProtectiveReplay  *manthan.ProtectiveReplay         // custom-GTC AMO replayer
	JWTNotifier       *manthan.JWTExpiryNotifier        // pre-open JWT-expiry alerts
}

// InitManthan initializes all Manthan order execution components.
// Returns nil if external Redis is not configured (manthan disabled).
func InitManthan(
	ctx context.Context,
	db *sql.DB,
	indiraClient *indiraPkg.Client,
	statusSvc *statusservice.OrderStatusService,
	credsCache *executor.CredentialsCache,
	kafkaBrokers []string,
	logger *zap.Logger,
) *ManthanModule {
	// External Redis for live LTP
	extRedisAddr := os.Getenv("EXT_REDIS_ADDR")
	extRedisPass := os.Getenv("EXT_REDIS_PASSWORD")
	if extRedisAddr == "" {
		log.Println("[manthan] EXT_REDIS_ADDR not set — Manthan order execution disabled")
		return nil
	}

	extRedis := redis.NewClient(&redis.Options{
		Addr:         extRedisAddr,
		Password:     extRedisPass,
		PoolSize:     20,
		MinIdleConns: 5,
		ReadTimeout:  2 * time.Second,
	})
	if err := extRedis.Ping(ctx).Err(); err != nil {
		log.Printf("[manthan] External Redis not available (%v) — Manthan disabled", err)
		return nil
	}
	log.Println("[manthan] ✓ External Redis connected")

	// Auth helper — reads from the same credentials cache used by existing order executor.
	getAuth := func(userID string) *manthan.BrokerAuth {
		if credsCache == nil {
			return nil
		}
		uid, appID, source, token, err := credsCache.Get(ctx, userID)
		if err != nil || token == "" {
			log.Printf("[manthan] auth lookup failed for %s: %v", userID, err)
			return nil
		}
		return &manthan.BrokerAuth{
			UserID:      uid,
			BearerToken: token,
			AppID:       appID,
			Source:      source,
		}
	}

	// Refresh helper — invalidates the cache and re-fetches from DB. Called
	// on broker auth errors (AU004/401) to pick up any fresher token the
	// user-config service wrote after the last cache hit.
	refreshAuth := func(userID string) *manthan.BrokerAuth {
		if credsCache == nil {
			return nil
		}
		credsCache.Invalidate(userID)
		return getAuth(userID)
	}

	// Init components
	broker := manthan.NewBrokerAdapter(indiraClient, extRedis, logger)
	repo := manthan.NewRepository(db)
	wssBridge := manthan.NewWSSBridge(logger)
	preCheck := manthan.NewPreChecker(repo, broker, logger)
	slHandler := manthan.NewSLHandler(broker, repo, logger)
	slHandler.SetAuthProvider(getAuth)
	slHandler.SetRefreshAuth(refreshAuth)
	entryHandler := manthan.NewEntryHandler(broker, repo, preCheck, slHandler, logger)
	entryHandler.SetWSSBridge(wssBridge)
	entryHandler.SetAuthProvider(getAuth)
	entryHandler.SetRefreshAuth(refreshAuth)

	// Centralized event publisher — single point that emits ENTRY_FILLED,
	// ENTRY_REJECTED, ENTRY_PARTIAL_FILL, SL_*, EXIT_*, RECONCILER_DRIFT_FIX
	// to manthan.execution.events with idempotent (signal_id, event_seq).
	// Also emits the legacy FILL_CONFIRMED/FILL_UPDATE so existing rules-engine
	// consumers keep working until the new FillConsumer takes over.
	eventPub := manthan.NewManthanEventPublisher(kafkaBrokers, logger)
	entryHandler.SetEventPublisher(eventPub)
	slHandler.SetEventPublisher(eventPub)

	// Wire WSS subscription so order updates come via real-time WebSocket
	if statusSvc != nil {
		entryHandler.SetWSSSubscriber(func(userID, bearerToken, appID, source string) error {
			return statusSvc.StartSubscription(context.Background(), userID, &indiraPkg.AuthContext{
				UserId:      userID,
				BearerToken: bearerToken,
				AppId:       appID,
				Source:      source,
			})
		})
	}

	// Wire WSS bridge into status service — route Manthan order updates
	if statusSvc != nil {
		statusSvc.SetManthanBridge(func(brokerOrderID, status string, filledQty int, avgPrice, triggerPrice float64, reason string) bool {
			return wssBridge.HandleUpdate(brokerOrderID, status, filledQty, avgPrice, triggerPrice, reason)
		})
		log.Println("[manthan] ✓ WSS bridge wired to status service")

		// Real-time manual-exit publisher: when the WSS sees an EXECUTED
		// order we didn't place, look up our matching entry signal_id and
		// publish MANUAL_EXIT_DETECTED so rules-engine can flip state +
		// notify the user (no 30-min wait for the API poller).
		statusSvc.SetManthanManualExitPub(func(userID, symbol, brokerOrderID, reason string) {
			lookupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			signalID, strategyID, err := repo.GetLiveEntryForSymbol(lookupCtx, userID, symbol)
			if err != nil {
				log.Printf("[manthan] manual-exit-pub: lookup failed user=%s symbol=%s err=%v", userID, symbol, err)
				return
			}
			if signalID == "" {
				// User sold something we never owned via the algo — nothing
				// to project. Common case if account has both algo and manual
				// positions. Silent skip.
				return
			}
			eventPub.PublishManualExitDetected(lookupCtx, signalID, strategyID, userID, symbol, brokerOrderID, reason, 0)
		})
		log.Println("[manthan] ✓ manual-exit publisher wired (WSS → manthan.execution.events)")
	}

	// Signal consumer (reads from trade-signals topic)
	signalConsumer := manthan.NewSignalConsumer(
		manthan.SignalConsumerConfig{KafkaBrokers: kafkaBrokers},
		entryHandler, slHandler, repo, logger,
	)

	// Safety monitor
	safetyMonitor := manthan.NewSafetyMonitor(
		broker, repo, slHandler,
		repo.GetActiveSLOrders,
		getAuth, logger,
		manthan.SafetyMonitorConfig{PollInterval: 15 * time.Second},
	)
	safetyMonitor.SetEventPublisher(eventPub)

	// Reconciler — enforces Postgres = source of truth for order state every 5min.
	// Fetches each active user's broker order-book and fixes any drift between
	// DB beliefs and broker reality (e.g. KINGFA 2026-04-23 "PLACED but actually
	// filled" bug would have been auto-detected within 5 minutes).
	reconciler := manthan.NewReconciler(
		broker, repo,
		func() []string {
			ids, err := repo.ListUserIDsWithLiveOrders(ctx)
			if err != nil {
				log.Printf("[manthan] reconciler: user list query failed: %v", err)
				return nil
			}
			return ids
		},
		getAuth, logger,
		manthan.ReconcilerConfig{PollInterval: 5 * time.Minute},
	)
	reconciler.SetEventPublisher(eventPub)

	// Recovery
	recovery := manthan.NewRecovery(broker, repo, wssBridge, slHandler, getAuth, logger)

	// External activity detector — watches for manual user interference
	// (manual sell, manual SL cancel, manual buy) outside our system.
	// Off by default; enable with MANTHAN_EXTERNAL_DETECTOR_ENABLED=true.
	var externalDetector *manthan.ExternalActivityDetector
	if os.Getenv("MANTHAN_EXTERNAL_DETECTOR_ENABLED") == "true" {
		externalDetector = manthan.NewExternalActivityDetector(
			broker, repo, eventPub,
			func() []string {
				ids, err := repo.ListUserIDsWithLiveOrders(ctx)
				if err != nil {
					log.Printf("[manthan] external-detector: user list query failed: %v", err)
					return nil
				}
				return ids
			},
			getAuth, logger,
			manthan.ExternalActivityDetectorConfig{PollInterval: 30 * time.Minute},
		)
		log.Println("[manthan] ExternalActivityDetector ENABLED (30 min poll)")
	}

	// Protective replayer — server-side trigger engine. Single 09:14 IST cron
	// builds plans → fires SL/MARKET at 09:15:00.1 → reconciles at 09:15:30.
	// No AMO. Off by default; enable with MANTHAN_PROTECTIVE_REPLAY_ENABLED=true
	// once migrations 011 + 007_manthan_protective_audit are applied.
	var protectiveReplay *manthan.ProtectiveReplay
	if os.Getenv("MANTHAN_PROTECTIVE_REPLAY_ENABLED") == "true" {
		protectiveReplay = manthan.NewProtectiveReplay(broker, repo, getAuth, logger)
		protectiveReplay.SetEventPublisher(eventPub)
		log.Println("[manthan] Protective replayer ENABLED (server-side trigger: 09:14 plan → 09:15:00.1 fire → 09:15:30 reconcile)")
	}

	// JWT expiry notifier serves two roles on the same Kafka topic:
	//   1. PROACTIVE — JWT_EXPIRING alerts 8h before token exp (poll loop).
	//      Opt-in via MANTHAN_JWT_NOTIFIER_ENABLED=true; the poll loop only
	//      starts when that flag is set.
	//   2. REACTIVE — SESSION_EXPIRED alerts on every AU004 seen by the live
	//      polling loops (reconciler, entry-poll, external-activity, replay).
	//      This path is ALWAYS on whenever Kafka is reachable, since it's the
	//      signal the frontend renders as a "re-login required" flash.
	var jwtNotifier *manthan.JWTExpiryNotifier
	if len(kafkaBrokers) > 0 {
		jwtNotifier = manthan.NewJWTExpiryNotifier(
			kafkaBrokers,
			func() []string {
				ids, err := repo.ListUserIDsWithLiveOrders(ctx)
				if err != nil {
					log.Printf("[manthan] jwt-notifier: user list query failed: %v", err)
					return nil
				}
				return ids
			},
			getAuth, logger,
			manthan.JWTExpiryNotifierConfig{}, // defaults: 8h alert, 30m poll, 2h dedup
		)
		// Wire SESSION_EXPIRED publisher into every loop that calls the broker.
		// Each loop short-circuits + emits one event per user per ~5min on AU004.
		reconciler.SetAuthExpiryNotifier(jwtNotifier)
		entryHandler.SetAuthExpiryNotifier(jwtNotifier)
		safetyMonitor.SetAuthExpiryNotifier(jwtNotifier)
		if externalDetector != nil {
			externalDetector.SetAuthExpiryNotifier(jwtNotifier)
		}
		if protectiveReplay != nil {
			protectiveReplay.SetAuthExpiryNotifier(jwtNotifier)
		}
		log.Println("[manthan] SESSION_EXPIRED notifier ENABLED (publishes to manthan.notifications on AU004)")
		if os.Getenv("MANTHAN_JWT_NOTIFIER_ENABLED") == "true" {
			log.Println("[manthan] JWT_EXPIRING poll loop ENABLED (8h pre-emptive warning)")
		}
	}

	log.Println("[manthan] ✓ All components initialized")

	return &ManthanModule{
		SignalConsumer:   signalConsumer,
		SafetyMonitor:    safetyMonitor,
		Reconciler:       reconciler,
		Recovery:         recovery,
		WSSBridge:        wssBridge,
		EntryHandler:     entryHandler,
		SLHandler:        slHandler,
		BrokerAdapter:    broker,
		ExternalDetector: externalDetector,
		ProtectiveReplay: protectiveReplay,
		JWTNotifier:      jwtNotifier,
	}
}

// Start runs recovery then starts consumers + monitor.
func (m *ManthanModule) Start(ctx context.Context) {
	if m == nil {
		return
	}

	// Recovery first — reconcile with broker before consuming new signals
	if err := m.Recovery.Run(ctx); err != nil {
		log.Printf("[manthan] Recovery failed (non-fatal): %v", err)
	}

	// Start safety monitor
	go func() {
		log.Println("[manthan] Starting safety monitor...")
		m.SafetyMonitor.Start(ctx)
	}()

	// Start broker↔DB reconciler (every 5 min — enforces DB=truth)
	if m.Reconciler != nil {
		go func() {
			log.Println("[manthan] Starting reconciler (broker ↔ DB truth sync)...")
			m.Reconciler.Start(ctx)
		}()
	}

	// Start external-activity detector (every 30 min — watches for manual
	// user activity outside our system). Only present when env flag is on.
	if m.ExternalDetector != nil {
		go func() {
			log.Println("[manthan] Starting external-activity detector...")
			m.ExternalDetector.Start(ctx)
		}()
	}

	// Start signal consumer
	go func() {
		log.Println("[manthan] Starting signal consumer (trade-signals)...")
		m.SignalConsumer.Start(ctx)
	}()

	// Start protective replayer (custom GTC) if enabled.
	if m.ProtectiveReplay != nil {
		go func() {
			log.Println("[manthan] Starting protective replayer (Phase A/B/C cron)...")
			m.ProtectiveReplay.Start(ctx)
		}()
	}

	// Start JWT expiry POLL loop only when explicitly enabled. The reactive
	// SESSION_EXPIRED publisher (used by the AU004 short-circuits) is wired
	// into the notifier object itself and works without the poll loop.
	if m.JWTNotifier != nil && os.Getenv("MANTHAN_JWT_NOTIFIER_ENABLED") == "true" {
		go func() {
			log.Println("[manthan] Starting JWT expiry notifier (poll loop)...")
			m.JWTNotifier.Start(ctx)
		}()
	}

	log.Println("[manthan] ✓ Manthan order execution ACTIVE")
}

// Stop gracefully shuts down manthan components.
func (m *ManthanModule) Stop() {
	if m == nil {
		return
	}
	if m.SignalConsumer != nil {
		_ = m.SignalConsumer.Stop()
	}
	if m.JWTNotifier != nil {
		_ = m.JWTNotifier.Close()
	}
	log.Println("[manthan] Stopped")
}
