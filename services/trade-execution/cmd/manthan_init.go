package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"

	indiraPkg "github.com/RohitIndira/Algo-Treading/pkg/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/manthan"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/orderstatus"
)

// ManthanModule holds all Manthan order execution components.
type ManthanModule struct {
	SignalConsumer      *manthan.SignalConsumer
	SafetyMonitor       *manthan.SafetyMonitor
	Reconciler          *manthan.Reconciler
	Recovery            *manthan.Recovery
	WSSBridge           *manthan.WSSBridge
	EntryHandler        *manthan.EntryHandler
	SLHandler           *manthan.SLHandler
	BrokerAdapter       *manthan.BrokerAdapter
	ExternalDetector    *manthan.ExternalActivityDetector // nil when disabled
	ProtectiveReplay    *manthan.ProtectiveReplay         // custom-GTC AMO replayer
	JWTNotifier         *manthan.JWTExpiryNotifier        // pre-open JWT-expiry alerts
	InboxWorker         *manthan.InboxWorker              // drains signal_inbox (transactional inbox)
	ArmRetryWorker      *manthan.ArmRetryWorker           // Layer 4: drains manthan_arm_retries on re-login
	OrderEventsConsumer *manthan.OrderEventsConsumer      // Kafka reader for order.events from orderstatus svc (Chunk E)
	ManualExitLedger    *manthan.ManualExitLedgerConsumer // position.events → manthan_orders manual-exit projection (formation fix)

	// AuthProvider — the same per-user credentials closure used internally
	// by all handlers. Exposed so main.go can compose it with BrokerAdapter
	// for the gRPC GetBrokerHoldings RPC (positions svc reconciler consumer).
	AuthProvider func(userID string) *manthan.BrokerAuth
}

// InitManthan initializes all Manthan order execution components.
// Returns nil if external Redis is not configured (manthan disabled).
func InitManthan(
	ctx context.Context,
	db *sql.DB,
	indiraClient *indiraPkg.Client,
	statusSvc *orderstatus.OrderStatusService,
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

	// Boot recovery: repopulate the WSSBridge `known` set from manthan_orders.
	// Without this, a trade-execution restart drops the pending map (in-
	// memory) and every WSS event for a broker_order_id placed by the
	// PREVIOUS process falls into HandleUpdate's "not Manthan" branch,
	// silencing WSSKafkaBridge.PublishFill. See wss_bridge.go's docstring
	// on the pending/known split. Non-fatal on error — degrades to the
	// old race behavior but the service still boots.
	{
		recCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		bids, err := repo.ListActiveManthanBrokerOrderIDs(recCtx)
		cancel()
		if err != nil {
			log.Printf("[manthan] WARNING: WSS bridge boot recovery failed: %v (WSS→Kafka fanout may miss orders from prior process lifetime)", err)
		} else {
			for _, bid := range bids {
				wssBridge.MarkKnown(bid)
			}
			log.Printf("[manthan] ✓ WSS bridge recovered %d known broker orders (last 7 days, non-terminal)", wssBridge.KnownCount())
		}
	}

	// wssKafkaBridge closes the fill-price race documented in the
	// 2026-07-17 postmortem. When a broker WSS fill lands, wssBridge
	// routes it in-process (existing behavior) AND wssKafkaBridge
	// publishes an `order.events` message with source=WSS_MANTHAN and
	// the real avg fill price. Positions svc gets the SSOT traded_price
	// directly, no gRPC race against manthan_orders.avg_fill_price.
	wssKafkaBridge := manthan.NewWSSKafkaBridge(kafkaBrokers, repo, logger)
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

	// Chunk E: parallel Kafka consumer for order.events (from orderstatus svc).
	// Dual-path with the in-process WSS listener below during rollout — same
	// bridge, same downstream handlers, ok-flag in HandleUpdate makes the
	// second-arriving path silently no-op after the first has been consumed.
	// Once orderstatus svc is trusted the in-process WSS wiring can go.
	orderEventsConsumer := manthan.NewOrderEventsConsumer(
		manthan.OrderEventsConsumerConfig{KafkaBrokers: kafkaBrokers},
		wssBridge,
		logger,
	)

	// Wire WSS bridge into status service — route Manthan order updates
	if statusSvc != nil {
		statusSvc.SetManthanBridge(func(brokerOrderID, status string, filledQty int, avgPrice, triggerPrice float64, reason string) bool {
			handled := wssBridge.HandleUpdate(brokerOrderID, status, filledQty, avgPrice, triggerPrice, reason)
			// Fire-and-forget publish to Kafka order.events with the real
			// broker fill price. Async writer inside the bridge means this
			// doesn't block the WSS goroutine even under load. See
			// wss_kafka_bridge.go PublishFill for the guard (only publishes
			// when avgPrice>0 AND filledQty>0).
			if handled {
				go wssKafkaBridge.PublishFill(context.Background(), brokerOrderID, status, filledQty, avgPrice, triggerPrice, reason)
			}
			return handled
		})
		log.Println("[manthan] ✓ WSS bridge wired to status service (in-process + order.events fanout)")

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

	// Signal consumer reads from trade-signals topic and persists each
	// MANTHAN_* message into signal_inbox. The actual broker work happens
	// in InboxWorker (below). See migration 012 for the design rationale.
	signalConsumer := manthan.NewSignalConsumer(
		manthan.SignalConsumerConfig{KafkaBrokers: kafkaBrokers},
		repo, logger,
	)

	// Inbox worker pool — drains signal_inbox with bounded backoff + DLQ.
	// 4 workers, 2s poll, 50 attempts before DLQ. The pool is also poked
	// by the consumer on every INSERT (worker.Notify) for 0-latency processing.
	inboxWorker := manthan.NewInboxWorker(
		repo, entryHandler, slHandler, nil /* authNotif wired below */, logger,
		manthan.InboxWorkerConfig{},
	)
	signalConsumer.SetInboxWorker(inboxWorker)

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

	// External activity detector — RETIRED PATH, do not enable (2026-09-03
	// ghost investigation): it publishes through ManthanEventPublisher, a
	// no-op since manthan.execution.events was retired 2026-07-10 with its
	// consumers, so enabling it burns broker API polls for zero effect.
	// Manual exits are covered by the live canonical chain instead:
	// orderstatus (whole-book) → order.events → positions svc manual-FIFO →
	// position.events → rules-engine slot release + the manual-exit ledger
	// consumer below. The env check stays only to warn loudly if someone
	// sets the old flag expecting the old behavior.
	var externalDetector *manthan.ExternalActivityDetector
	if os.Getenv("MANTHAN_EXTERNAL_DETECTOR_ENABLED") == "true" {
		log.Println("[manthan] ⚠ MANTHAN_EXTERNAL_DETECTOR_ENABLED is set but the detector's event path " +
			"(manthan.execution.events) was retired 2026-07-10 — REFUSING to start it. Manual exits are " +
			"handled by the order.events → position.events chain; unset this variable.")
	}

	// Manual-exit ledger projector (formation fix, 2026-09-03): consumes
	// position.events and records a synthetic FILLED SELL in manthan_orders
	// when positions svc confirms a full manual exit of a Manthan lot —
	// releasing the position from ListPositionsNeedingProtection so EOD
	// stops arming AMO SLs for shares the user already sold.
	// ON by default (it is the fix); MANTHAN_MANUAL_EXIT_LEDGER=off disables.
	var manualExitLedger *manthan.ManualExitLedgerConsumer
	if os.Getenv("MANTHAN_MANUAL_EXIT_LEDGER") == "off" {
		log.Println("[manthan] manual-exit ledger consumer DISABLED via MANTHAN_MANUAL_EXIT_LEDGER=off")
	} else if len(kafkaBrokers) > 0 {
		manualExitLedger = manthan.NewManualExitLedgerConsumer(
			manthan.ManualExitLedgerConfig{KafkaBrokers: kafkaBrokers}, repo, logger)
	}

	// Protective replayer — server-side trigger engine. Single 09:14 IST cron
	// builds plans → fires SL/MARKET at 09:15:00.1 → reconciles at 09:15:30.
	// No AMO. Off by default; enable with MANTHAN_PROTECTIVE_REPLAY_ENABLED=true
	// once migrations 011 + 007_manthan_protective_audit are applied.
	var protectiveReplay *manthan.ProtectiveReplay
	if os.Getenv("MANTHAN_PROTECTIVE_REPLAY_ENABLED") == "true" {
		protectiveReplay = manthan.NewProtectiveReplay(broker, repo, getAuth, logger)
		// Trail source of truth: rules-engine's book in trading_db (same
		// Postgres, different database). Read-only; nil-safe fallback to the
		// order-history trigger if the DB is unreachable.
		if tdb, terr := openTradingDBReadOnly(); terr != nil {
			logger.Warn("trading_db (rules-engine stops) unavailable — replayer falls back to order-history triggers", zap.Error(terr))
		} else {
			protectiveReplay.SetStopSource(func(ctx context.Context, strategyID, symbol string) (float64, bool) {
				var sl float64
				qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				defer cancel()
				err := tdb.QueryRowContext(qctx, `
					SELECT current_sl FROM manthan_positions
					WHERE strategy_id = $1 AND symbol = $2 AND status = 'ACTIVE'
					ORDER BY updated_at DESC LIMIT 1`, strategyID, symbol).Scan(&sl)
				if err != nil || sl <= 0 {
					return 0, false
				}
				return sl, true
			})
			logger.Info("Replayer stop source wired: rules-engine trading_db.manthan_positions.current_sl")
		}
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
		inboxWorker.SetAuthExpiryNotifier(jwtNotifier)
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

	// Layer 4 retry worker — drains manthan_arm_retries on USER_CREDENTIALS_UPDATED
	// + every 5 minutes. Only meaningful when ProtectiveReplay is enabled (it owns
	// the EOD Phase A logic the worker re-invokes). The actual Kafka-side wake hook
	// is wired in main.go via strategyEventsConsumer.SetCredentialsObserver.
	var armRetryWorker *manthan.ArmRetryWorker
	if protectiveReplay != nil {
		armRetryWorker = manthan.NewArmRetryWorker(protectiveReplay, repo, logger)
		log.Println("[manthan] ArmRetryWorker ENABLED (Layer 4: 5-min poll + on-login wake on manthan_arm_retries)")
	}

	log.Println("[manthan] ✓ All components initialized")

	return &ManthanModule{
		SignalConsumer:      signalConsumer,
		SafetyMonitor:       safetyMonitor,
		Reconciler:          reconciler,
		Recovery:            recovery,
		WSSBridge:           wssBridge,
		EntryHandler:        entryHandler,
		SLHandler:           slHandler,
		BrokerAdapter:       broker,
		ExternalDetector:    externalDetector,
		ProtectiveReplay:    protectiveReplay,
		InboxWorker:         inboxWorker,
		JWTNotifier:         jwtNotifier,
		ArmRetryWorker:      armRetryWorker,
		OrderEventsConsumer: orderEventsConsumer,
		ManualExitLedger:    manualExitLedger,
		AuthProvider:        getAuth,
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

	// Chunk E: Kafka consumer for order.events (from orderstatus svc).
	// Runs in parallel with the in-process WSS listener during rollout.
	// Both feed the same wssBridge; whichever arrives first wins, second no-ops.
	if m.OrderEventsConsumer != nil {
		go func() {
			log.Println("[manthan] Starting order.events consumer (from orderstatus svc)...")
			m.OrderEventsConsumer.Start(ctx)
		}()
	}

	// Manual-exit ledger projector: position.events → manthan_orders
	// synthetic SELL on confirmed manual exits (formation fix 2026-09-03).
	if m.ManualExitLedger != nil {
		go func() {
			log.Println("[manthan] Starting manual-exit ledger consumer (position.events)...")
			m.ManualExitLedger.Start(ctx)
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

	// Start inbox worker BEFORE the signal consumer — orphaned RUNNING rows
	// from a previous crash get reaped + re-queued by the worker, and any
	// rows already PENDING from a prior run start draining immediately.
	if m.InboxWorker != nil {
		go func() {
			log.Println("[manthan] Starting inbox worker (drains signal_inbox)...")
			m.InboxWorker.Start(ctx)
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

	// Start Layer-4 arm-retry worker. Constructor-gated on ProtectiveReplay
	// presence so this is a no-op when the replayer is disabled.
	if m.ArmRetryWorker != nil {
		log.Println("[manthan] Starting arm-retry worker (Layer 4: drains manthan_arm_retries)...")
		m.ArmRetryWorker.Start(ctx)
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

// authGateWaker is the AuthGateClearer the strategy-events consumer uses
// when a USER_CREDENTIALS_UPDATED Kafka event arrives. It both clears the
// JWT notifier's per-user gate (so safety monitor + reconciler resume) and
// pokes the inbox worker (so any AUTH_EXPIRED-deferred rows process on the
// very next worker tick instead of waiting up to 30s for the backoff).
//
// Lives in the same package as main.go so we don't need to add a manthan
// import there just for this 8-line wrapper.
type authGateWaker struct {
	*manthan.JWTExpiryNotifier
	worker *manthan.InboxWorker
}

func (a authGateWaker) ClearSessionExpired(userID string) {
	if a.JWTExpiryNotifier != nil {
		a.JWTExpiryNotifier.ClearSessionExpired(userID)
	}
	if a.worker != nil {
		a.worker.Notify()
	}
}

// openTradingDBReadOnly opens rules-engine's trading_db on the same Postgres
// as execution_db (env RULES_ENGINE_DB, default "trading_db").
func openTradingDBReadOnly() (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSTGRES_USER", "postgres"),
		getEnv("POSTGRES_PASSWORD", "postgres"),
		getEnv("RULES_ENGINE_DB", "trading_db"),
		getEnv("POSTGRES_SSL_MODE", "disable"),
	)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
