package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"sync"

	"github.com/google/uuid"
	indiraPkg "github.com/RohitIndira/Algo-Treading/pkg/indira"
	pkglogger "github.com/RohitIndira/Algo-Treading/pkg/logger"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/consumer"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/marketws"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/executor"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/indira"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/lifecycle"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/metrics"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/models"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/oco"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/multilevel"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/paper"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/publisher"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/repository"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/scheduler"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/server"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/statusservice"
	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/tickstore"
)

func main() {
	// Load .env file
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found, using environment variables")
	}

	log.Println("========================================")
	log.Println("Starting Trade Execution Service...")
	log.Println("========================================")

	// Load configuration
	cfg := loadConfig()

	// Initialize PostgreSQL
	log.Println("Connecting to PostgreSQL...")
	db, err := initPostgres(cfg)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	log.Println("✓ Connected to PostgreSQL")

	// Initialize repositories
	orderRepo := repository.NewOrderRepository(db)
	credsRepo := repository.NewCredentialsRepository(db, cfg.EncryptionKey)
	log.Println("✓ Repository layer initialized")

	// Initialize Indira client (stateless, supports multiple users)
	indiraClient := indira.NewExecutionClient()
	log.Println("✓ Indira API client initialized")

	// Initialize Kafka publisher for trade-executions and order-updates topics
	log.Println("Initializing Kafka publisher...")
	logger, _ := initLogger()
	kafkaPub := publisher.NewKafkaPublisher(cfg.KafkaBrokers, logger)
	log.Println("✓ Kafka publisher initialized")

	// Initialize Order Status Service (WebSocket-based real-time order updates)
	// The backend opens one WS connection per user to Indira after placing their first order.
	log.Println("Initializing WebSocket Order Status Service...")
	statusService := statusservice.NewOrderStatusService(indiraClient, orderRepo, credsRepo, kafkaPub, logger)
	log.Println("✓ Order Status Service initialized")

	// Initialize executor with credentials repository, Kafka publisher, and status service.
	// The executor owns: retries, WS subscription start, and Kafka order-update publishing.
	orderExecutor := executor.NewOrderExecutor(
		orderRepo,
		credsRepo,
		indiraClient,
		kafkaPub,
		statusService,
		logger,
		cfg.MaxRetries,
		cfg.RetryDelay,
	)
	log.Println("✓ Order executor initialized")

	// Position-book checker: used by auto-square-off and strategy deactivation
	// to skip reverse orders for symbols already flat (NetQty == 0) at the
	// broker. Without it, a redundant square-off would open a fresh short.
	positionChecker := scheduler.NewPositionChecker(indiraClient, orderExecutor.CredentialsCache())

	// Pre-warm credentials cache with active live-trading users so the first
	// order per user hits memory instead of DB + decrypt.
	go func() {
		warmCtx, warmCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer warmCancel()
		userIDs, err := orderRepo.GetDistinctActiveUserIDs(warmCtx)
		if err != nil {
			log.Printf("[credentials] Cache warm-up skipped: %v", err)
			return
		}
		if len(userIDs) > 0 {
			orderExecutor.CredentialsCache().Warm(warmCtx, userIDs)
			log.Printf("✓ Credentials cache warmed for %d active users", len(userIDs))
		}
	}()

	// Initialize Kafka signal consumer (trade-signals topic → SignalProcessor)
	log.Println("Initializing Kafka consumer for trade-signals...")
	signalProcessor := executor.NewSignalProcessor(orderExecutor, orderRepo, kafkaPub, statusService, logger)
	kafkaConsumer := consumer.NewKafkaConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID, signalProcessor, logger, cfg.WorkerCount)
	log.Println("✓ Kafka consumer initialized")


	// Initialize strategy events consumer (user-config-events → close positions on deactivate/delete)
	log.Println("Initializing Kafka consumer for user-config-events...")
	strategyEventsConsumer := consumer.NewStrategyEventsConsumer(cfg.KafkaBrokers, orderRepo, orderExecutor, logger)
	log.Println("✓ Strategy events consumer initialized")

	// Initialize gRPC server
	grpcServer := server.NewServer(orderRepo, orderExecutor, cfg.GRPCPort)
	log.Println("✓ gRPC server initialized")

	// ── Paper Trading Layer ────────────────────────────────────────────────────
	log.Println("Initializing Paper Trading layer...")
	paperWSServer := paper.NewPaperWSServer(orderRepo)

	// Wire Indira positions fetcher — used by the /ws/live-orders/indira-positions endpoint.
	// Converts pkg/indira.Position → paper.BrokerPosition so the paper package stays decoupled.
	paperWSServer.SetPositionsFetcher(func(ctx context.Context, bearerToken, appId, userId, source string) ([]paper.BrokerPosition, error) {
		auth := &indiraPkg.AuthContext{
			UserId:      userId,
			AppId:       appId,
			Source:      source,
			BearerToken: bearerToken,
		}
		positions, err := indiraClient.GetPositions(ctx, auth)
		if err != nil {
			return nil, err
		}
		// Deduplicate: Indira returns both DAILY and EXPIRY rows per symbol.
		// Keep only DAILY to avoid showing the same position twice.
		seen := make(map[string]bool, len(positions))
		result := make([]paper.BrokerPosition, 0, len(positions))
		for _, p := range positions {
			if p.Type != "DAILY" {
				continue // skip EXPIRY rows — DAILY has the same data
			}
			// Use dispSym for display; fall back to baseSym or full symbol string.
			sym := p.Symbol.DispSym
			if sym == "" {
				sym = p.Symbol.BaseSym
			}
			if sym == "" {
				sym = p.Symbol.Symbol
			}
			key := sym + "|" + p.Symbol.Exc + "|" + p.PrdType
			if seen[key] {
				continue
			}
			seen[key] = true

			// Calculate P&L when broker returns 0.
			pnl := p.NetPnL
			if pnl == 0 && p.BuyQty > 0 && p.SellQty > 0 {
				// Closed position: realized P&L = (sellAvg - buyAvg) * traded qty
				tradedQty := p.SellQty
				if p.BuyQty < tradedQty {
					tradedQty = p.BuyQty
				}
				pnl = (p.SellAvgPrice - p.BuyAvgPrice) * float64(tradedQty)
			} else if pnl == 0 && p.NetQty != 0 && p.LTP > 0 {
				// Open position: unrealized P&L using LTP
				if p.NetQty > 0 {
					pnl = (p.LTP - p.BuyAvgPrice) * float64(p.NetQty)
				} else {
					pnl = (p.SellAvgPrice - p.LTP) * float64(-p.NetQty)
				}
			}
			pnlPct := p.PnLPerc
			if pnlPct == 0 && pnl != 0 && p.BuyAvgPrice > 0 {
				tradedQty := p.BuyQty
				if p.SellQty > 0 && p.SellQty < tradedQty {
					tradedQty = p.SellQty
				}
				pnlPct = (pnl / (p.BuyAvgPrice * float64(tradedQty))) * 100
			}

			result = append(result, paper.BrokerPosition{
				Symbol:        sym,
				Exchange:      p.Symbol.Exc,
				ProductType:   p.PrdType,
				NetQty:        p.NetQty,
				BuyQty:        p.BuyQty,
				SellQty:       p.SellQty,
				BuyAvgPrice:   p.BuyAvgPrice,
				SellAvgPrice:  p.SellAvgPrice,
				CurrentPrice:  p.LTP,
				PnL:           pnl,
				PnLPercentage: pnlPct,
				ExcTkn:        p.Symbol.ExcTkn,
			})
		}
		return result, nil
	})

	// Link OrderExecutor → live orders WS so the frontend gets real-time order events.
	// This is called only for LIVE (non-paper) orders after broker placement.
	orderExecutor.SetWSBroadcaster(func(userID string, eventType string, order *models.Order) {
		paperWSServer.BroadcastLiveOrder(userID, paper.LiveOrderUpdate{
			Type:   eventType,
			UserID: userID,
			Order:  order,
		})
	})

	// Wire ExitOrderPlacer — used by force-exit endpoints to place reverse limit orders
	// at LTP ± 1% (compliance: no market orders). Creates a new order in DB and executes it.
	paperWSServer.SetExitOrderPlacer(func(ctx context.Context, originalOrder *models.Order, limitPrice float64, bearerToken, appId, source string) error {
		reverseSide := models.OrderSideSell
		if originalOrder.OrderSide == models.OrderSideSell {
			reverseSide = models.OrderSideBuy
		}

		exitOrder := &models.Order{
			OrderID:          uuid.New(),
			UserID:           originalOrder.UserID,
			StrategyID:       originalOrder.StrategyID,
			StrategyName:     originalOrder.StrategyName,
			StockCode:        originalOrder.StockCode,
			Exchange:         originalOrder.Exchange,
			Symbol:           originalOrder.Symbol,
			OrderType:        models.OrderTypeLimit,
			OrderSide:        reverseSide,
			Quantity:         originalOrder.FilledQuantity,
			Price:            &limitPrice,
			Validity:         "IOC",
			ProductType:      originalOrder.ProductType,
			Status:           models.StatusReceived,
			BearerToken:      &bearerToken,
			AppId:            &appId,
			Source:           &source,
			RiskApproved:     true,
			IsSquareOffOrder: true,
			TradingMode:      "LIVE",
			CreatedAt:        time.Now(),
			UpdatedAt:        time.Now(),
		}

		if err := orderRepo.Create(ctx, exitOrder); err != nil {
			return fmt.Errorf("failed to create exit order: %w", err)
		}

		log.Printf("[exit-order] Placing %s exit for %s @ %.2f (strategy=%s, original=%s)",
			reverseSide, originalOrder.Symbol, limitPrice, originalOrder.StrategyID, originalOrder.OrderID)

		return orderExecutor.ExecuteOrder(ctx, exitOrder)
	})

	// Wire LiveBrokerCanceller — used by force-exit-all to cancel submitted orders at the
	// broker before bulk-cancelling in DB. Delegates to OrderExecutor which handles the
	// broker API call and local status update in one step.
	paperWSServer.SetLiveBrokerCanceller(func(ctx context.Context, order *models.Order, reason string) error {
		return orderExecutor.CancelOrder(ctx, order, reason)
	})

	// Link OrderStatusService → live orders WS so every status change received from
	// the Indira broker WebSocket (SUBMITTED→FILLED, PARTIALLY_FILLED, REJECTED, etc.)
	// is pushed to the frontend immediately without requiring a page refresh.
	statusService.SetWSBroadcaster(func(userID string, order *models.Order) {
		paperWSServer.BroadcastLiveOrder(userID, paper.LiveOrderUpdate{
			Type:   "order_update",
			UserID: userID,
			Order:  order,
		})
	})

	// Wire StatusService.StartSubscription into PaperWSServer so the
	// POST /ws/live-orders/subscribe-broker-ws endpoint can start the Indira
	// WS subscription when a strategy is created/activated — before the first order fires.
	paperWSServer.SetStatusService(func(ctx context.Context, userID, bearerToken, appID, source string) error {
		return statusService.StartSubscription(ctx, userID, &indiraPkg.AuthContext{
			UserId:      userID,
			AppId:       appID,
			Source:      source,
			BearerToken: bearerToken,
		})
	})

	// When the frontend sends a resume_token message on /ws/live-orders after re-login,
	// forward the new credentials to statusService so the Indira WS can reconnect.
	paperWSServer.SetResumeService(func(ctx context.Context, userID, bearerToken, appID, source string) error {
		statusService.ResumeUserSubscription(userID, &indiraPkg.AuthContext{
			UserId:      userID,
			AppId:       appID,
			Source:      source,
			BearerToken: bearerToken,
		})
		return nil
	})

	// When the Indira WS token is confirmed expired (30s first retry also failed),
	// push a token_expired event to the frontend via /ws/live-orders.
	statusService.SetTokenExpiredNotifier(func(userID string) {
		paperWSServer.BroadcastLiveOrder(userID, paper.LiveOrderUpdate{
			Type:   "token_expired",
			UserID: userID,
		})
	})

	// After each order fill, push updated positions to the frontend via /ws/live-orders.
	// Debounced per-user (300ms): rapid fills (e.g. OCO entry→SL→TP) collapse into one
	// broker API call, always using the latest auth at fire time.
	type positionDebounce struct {
		mu    sync.Mutex
		timer *time.Timer
		auth  *indiraPkg.AuthContext
	}
	var positionDebouncers sync.Map // userID → *positionDebounce
	statusService.SetOnOrderFilled(func(userID string, auth *indiraPkg.AuthContext) {
		// Cache credentials so PushPositions can be triggered on the next WS connect
		// without waiting for another fill event.
		paperWSServer.StoreAuth(userID, auth.BearerToken, auth.AppId, auth.Source)

		val, _ := positionDebouncers.LoadOrStore(userID, &positionDebounce{})
		d := val.(*positionDebounce)
		d.mu.Lock()
		d.auth = auth // always keep the latest auth
		if d.timer != nil {
			d.timer.Stop()
		}
		d.timer = time.AfterFunc(300*time.Millisecond, func() {
			d.mu.Lock()
			latestAuth := d.auth
			d.timer = nil
			d.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			paperWSServer.PushPositions(ctx, userID, latestAuth.BearerToken, latestAuth.AppId, latestAuth.Source)
		})
		d.mu.Unlock()
	})

	// Initialize Redis price client — used for accurate order fill prices, PnL fallback,
	// and dynamic tick size lookup for limit order price rounding.
	// Non-fatal: if Redis is unavailable the service still runs with hardcoded tick sizes.
	redisPrices, redisErr := paper.NewRedisPriceClient(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	var priceLookup executor.PriceLookup
	if redisErr != nil {
		log.Printf("[paper] Redis price client unavailable (non-fatal): %v", redisErr)
		log.Println("[paper] Order fills will use signal price; PnL shown only when WSS is live")
		log.Println("[paper] Tick size will use hardcoded NSE defaults (0.05/0.01)")
	} else {
		log.Printf("✓ Redis price client connected (%s)", cfg.RedisAddr)
		priceLookup = redisPrices
		// Wire Redis tick size lookup into the Indira execution client
		indiraClient.SetTickSizeLookup(redisPrices)
		indiraClient.SetDPRLookup(redisPrices)
		indiraClient.SetLTPLookup(redisPrices)
		log.Println("✓ Dynamic tick size + DPR + LTP lookup enabled (Redis market data)")
		// Wire Redis price lookup into strategy events consumer for accurate paper exit PnL
		strategyEventsConsumer.SetPriceClient(redisPrices)
	}

	paperExec := executor.NewPaperOrderExecutor(orderRepo, kafkaPub, priceLookup)
	orderExecutor.SetPaperExecutor(paperExec)

	var paperMonitorRef *paper.PaperTradeMonitor
	
	paperExec.OnPaperFilled = func(order *models.Order) {
		if paperMonitorRef != nil {
			paperMonitorRef.AddOrder(order)
		}
	}

	paperMarketClient := paper.NewPaperMarketClient(
		cfg.PaperMarketWSURL,
		func(symbol string, ltp float64) {
			if paperMonitorRef != nil {
				paperMonitorRef.OnPriceUpdate(symbol, ltp)
			}
		},
	)
	paperMonitor := paper.NewPaperTradeMonitor(orderRepo, paperExec, paperWSServer, paperMarketClient, redisPrices)
	paperMonitorRef = paperMonitor
	paperWSServer.SetMonitor(paperMonitor)
	// paperMarketClient tick writer wired below after tickStoreWriter is created (line ~415).
	log.Println("✓ Paper trading layer initialized")
	// ─────────────────────────────────────────────────────────────────────────

	// ── Live position monitor ──────────────────────────────────────────────
	// Brings /ws/live-orders to parity with /ws/paper-trades: it broadcasts
	// per-position live P&L ticks (pnl_update), new_order, and position_exit
	// events for LIVE positions. It uses its OWN market WSS connection so its
	// symbol subscriptions never interfere with the paper monitor's shared
	// subscription set.
	var liveMonitorRef *paper.LiveOrderMonitor
	liveMarketClient := paper.NewPaperMarketClient(
		cfg.PaperMarketWSURL,
		func(symbol string, ltp float64) {
			if liveMonitorRef != nil {
				liveMonitorRef.OnPriceUpdate(symbol, ltp)
			}
		},
	)
	liveMonitor := paper.NewLiveOrderMonitor(orderRepo, paperWSServer, liveMarketClient, redisPrices)
	liveMonitorRef = liveMonitor
	paperWSServer.SetLiveMonitor(liveMonitor)
	log.Println("✓ Live order monitor initialized")
	// ─────────────────────────────────────────────────────────────────────────

	// Initialize PriceMonitor for below_min orders (Case 2).
	// Primary: WebSocket (enhanced-stream) for real-time push prices.
	// Fallback: Redis MGET polling for any tokens not covered by WSS.
	var priceMonitorRef *scheduler.PriceMonitor
	var priceMonitorWSClient *marketws.Client
	var tickStoreWriter *tickstore.TickWriter
	{
		// RoutingExecutor: live orders → orderExecutor, paper orders → paperExec.
		// This lets the PriceMonitor handle below_min paper orders without broker calls.
		routingExec := executor.NewRoutingExecutor(orderExecutor, paperExec)

		priceMonitorRef = scheduler.NewPriceMonitor(
			redisPrices,   // Redis fallback (nil-safe)
			orderRepo,
			kafkaPub,
			routingExec, // routes paper/live based on IsPaperTrade
			100*time.Millisecond, // check interval for evaluating WSS-cached prices
		)

		// Wire WebSocket market data client as primary price source (started later with ctx)
		priceMonitorWSClient = marketws.New(cfg.PaperMarketWSURL)
		priceMonitorRef.SetWSClient(priceMonitorWSClient)
		// Event-driven: WSS tick → immediate evaluation (no polling delay)
		priceMonitorWSClient.SetOnPriceUpdate(priceMonitorRef.OnPriceUpdate)

		// Tick store: persist every socket tick to localhost Redis DB=1 for testing.
		// Wired to both WebSocket clients (paper-market + price-monitor).
		// Non-fatal — if localhost Redis is unavailable the algo runs unchanged.
		// Goroutine is started below after ctx is defined.
		if tw, err := tickstore.NewTickWriter("localhost:6379", ""); err != nil {
			log.Printf("[tickstore] unavailable, tick history disabled (non-fatal): %v", err)
		} else {
			paperMarketClient.SetTickWriter(tw.InCh())   // paper SL/TP orders
			priceMonitorWSClient.SetTickWriter(tw.InCh()) // below_min price monitor orders
			tickStoreWriter = tw
			log.Println("✓ Tick writer connected — all socket ticks will be stored to localhost Redis DB=1")
		}

		signalProcessor.SetPriceMonitor(priceMonitorRef)
		strategyEventsConsumer.SetPriceMonitor(priceMonitorRef)
		strategyEventsConsumer.SetPaperMonitor(paperMonitor)
		strategyEventsConsumer.SetCredentialsCache(orderExecutor.CredentialsCache())
		strategyEventsConsumer.SetPositionsLookup(positionsLookupAdapter{pc: positionChecker})
		paperWSServer.SetPriceMonitor(priceMonitorRef)
		priceMonitorRef.SetOnTickDone(paperWSServer.BroadcastPriceWatches)
		log.Println("✓ Price Monitor initialized (WSS primary, Redis fallback, 100ms check interval)")
	}

	// ── OCO (One-Cancels-the-Other) Layer ─────────────────────────────────
	log.Println("Initializing OCO order management layer...")
	ocoManager := oco.NewOCOManager(orderRepo, indiraClient)
	ocoManager.SetCredentialsCache(orderExecutor.CredentialsCache())

	// Configure partial fill timeout from env (default 50s).
	// When an entry order partially fills, SL/TP legs are placed immediately for the
	// filled qty. This timeout controls how long to wait for the remaining qty before
	// cancelling the unfilled portion.
	if pfTimeout := getEnvInt("OCO_PARTIAL_FILL_TIMEOUT", 50); pfTimeout > 0 {
		ocoManager.SetPartialFillTimeout(time.Duration(pfTimeout) * time.Second)
	}

	// Wire OCO manager → frontend WS so OCO events appear in real-time.
	// Some OCO events (oco_legs_confirmed, oco_completed) have no order attached —
	// broadcast them anyway so the UI can update OCO group state.
	ocoManager.SetWSBroadcaster(func(userID string, eventType string, order *models.Order) {
		paperWSServer.BroadcastLiveOrder(userID, paper.LiveOrderUpdate{
			Type:   eventType,
			UserID: userID,
			Order:  order,
		})
	})

	// Wire OCO handler into StatusService — every broker WS event is checked
	// for OCO group membership (entry fill → place legs, leg fill → cancel other)
	statusService.SetOCOHandler(ocoManager)

	// Wire OCO canceller into WS server — force-exit-all cancels OCO legs too
	paperWSServer.SetOCOCanceller(ocoManager)

	// Wire OCO manager into SignalProcessor — trailing SL signals are routed
	// to the custom OCO system instead of broker's native bracket order.
	signalProcessor.SetOCOManager(ocoManager)

	// Wire OCO manager into PriceMonitor — required so reloadFromDB can
	// rebuild the onAfterFill closure for below_min orders after restart.
	// Without this, a below_min order whose entry triggers post-restart fills
	// with no SL/TP protection (the original signal-time closure is gone).
	priceMonitorRef.SetOCOAdopter(ocoManager)

	// OCO market data WSS client (enhanced-stream binary, primary price source for trailing SL)
	ocoMarketClient := oco.NewOCOMarketClient(cfg.PaperMarketWSURL, nil) // callback wired below

	// OCO Trailing SL Monitor: WSS primary, Redis fallback
	var ocoRedisProvider oco.RedisLTPProvider
	if redisPrices != nil {
		ocoRedisProvider = redisPrices
	}
	ocoTrailingMonitor := oco.NewTrailingMonitor(ocoManager, ocoMarketClient, ocoRedisProvider, 500*time.Millisecond)

	// Wire WSS price callback → trailing monitor
	ocoMarketClient = oco.NewOCOMarketClient(cfg.PaperMarketWSURL, func(symbol string, ltp float64) {
		ocoTrailingMonitor.OnPriceUpdate(symbol, ltp)
	})
	// Re-create trailing monitor with the properly wired market client
	ocoTrailingMonitor = oco.NewTrailingMonitor(ocoManager, ocoMarketClient, ocoRedisProvider, 500*time.Millisecond)

	// Reload active OCO groups from DB (restart recovery).
	// Run synchronously: the trailing SL monitor's first refreshGroups() must
	// see the reconstructed groups, otherwise there is a window (≤5s, until
	// the next refresh tick) where new highs go un-trailed after every restart.
	{
		reloadCtx, reloadCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := ocoManager.Reload(reloadCtx); err != nil {
			log.Printf("[oco] Reload failed (non-fatal): %v", err)
		} else {
			log.Printf("✓ OCO manager initialized (%d active groups)", ocoManager.ActiveCount())
		}
		reloadCancel()
	}

	log.Println("✓ OCO layer initialized")
	// ──────────────────────────────────────────────────────────────────────

	// ── Multi-Level SL/TP Layer ────────────────────────────────────────────
	log.Println("Initializing multi-level SL/TP management layer...")
	var mlPriceLookup multilevel.PriceLookup
	if redisPrices != nil {
		mlPriceLookup = redisPrices
	}
	mlManager := multilevel.NewManager(orderRepo, mlPriceLookup, indiraClient, logger)

	// Wire ML handler into StatusService — broker WS events are forwarded here
	// so entry fills and TP limit order fills are detected.
	statusService.SetMLHandler(mlManager)

	// Wire ML manager into SignalProcessor — multi-level signals (Route 4) are
	// registered and executed through the ML manager.
	signalProcessor.SetMultiLevelManager(mlManager)

	// Wire ML paper completion → WS broadcast so the frontend receives a
	// position_exit event when all levels of a paper ML position have triggered.
	mlManager.OnPaperGroupCompleted = func(userID, orderID string, finalPnL, avgExitPrice float64) {
		// Remove from paper monitor cache so it stops receiving price updates.
		// Without this the order stays in cache with FilledQuantity=0 and triggers
		// warn-spam on every tick after all ML levels have exited.
		if oid, err := uuid.Parse(orderID); err == nil {
			paperMonitor.RemoveOrder(oid)
		}
		paperWSServer.Broadcast(userID, paper.PaperUpdate{
			Type:      "position_exit",
			OrderID:   orderID,
			UserID:    userID,
			LTP:       avgExitPrice,
			FinalPnL:  finalPnL,
			Reason:    "MULTI_LEVEL_COMPLETE",
			ExitPrice: avgExitPrice,
		})
	}

	// Wire ML canceller into the paper monitor so force-exit stops ML goroutines.
	paperMonitor.SetMLCanceller(mlManager)

	// Wire ML level trigger → WS broadcast so frontend refreshes level chips in real time.
	mlManager.OnPaperLevelTriggered = func(userID, orderID, exitType string, levelNum int, exitPrice float64, remainingQty int32, cancelledExitType string, cancelledLevelNum int) {
		paperWSServer.Broadcast(userID, paper.PaperUpdate{
			Type:              "ml_level_triggered",
			OrderID:           orderID,
			UserID:            userID,
			ExitType:          exitType,
			LevelNum:          levelNum,
			ExitPrice:         exitPrice,
			RemainingQty:      remainingQty,
			CancelledExitType: cancelledExitType,
			CancelledLevelNum: cancelledLevelNum,
		})
	}

	// Wire partial-exit qty update → monitor cache so PnL broadcasts use remaining qty.
	mlManager.OnPaperQtyUpdated = func(entryOrderID uuid.UUID, remainingQty int32) {
		paperMonitor.UpdateCachedOrderQty(entryOrderID, remainingQty)
	}

	// Wire SL breakeven move → monitor cache so the regular SL price-check in the
	// paper monitor uses the updated (tighter) stop after each TP level fires.
	// After TP L1: SL moves to entry price (breakeven). After TP L2+: SL moves to
	// the previous TP trigger price, locking in that level's profit.
	mlManager.OnPaperSLMoved = func(entryOrderID uuid.UUID, newSL float64) {
		paperMonitor.UpdateCachedOrderSL(entryOrderID, newSL)
	}

	// Wire ML manager into strategy events consumer so paper partial exits are
	// recorded for remaining ML qty when a strategy is paused or deleted.
	strategyEventsConsumer.SetMLManager(mlManager)

	// Wire the broker WS starter: a CONFIG_CREATED / CONFIG_UPDATED event opens
	// the user's per-user order-status socket immediately — loading their broker
	// credentials and calling StartSubscription — so order updates stream from
	// the moment the strategy is active, not only after the first order.
	strategyEventsConsumer.SetBrokerWSStarter(func(ctx context.Context, userID string) error {
		brokerUserId, appId, source, bearerToken, err := credsRepo.GetIndiraCredentials(ctx, userID)
		if err != nil {
			return fmt.Errorf("load broker credentials: %w", err)
		}
		return statusService.StartSubscription(ctx, userID, &indiraPkg.AuthContext{
			UserId:      brokerUserId,
			AppId:       appId,
			Source:      source,
			BearerToken: bearerToken,
		})
	})

	// Wire paper market WSS as the price feed for the ML manager.
	// paperMarketClient fires ticks with (symbol, ltp); the ML manager needs
	// "exchange:token" key format so groups can be found by stockCode.
	// SetTickUpdateCallback delivers exchange+token on each tick for this purpose.
	paperMarketClient.SetTickUpdateCallback(func(exchange, token string, ltp float64) {
		mlManager.OnPriceUpdate(exchange+":"+token, ltp)
	})
	// Use the same WSS connection for ML group subscription management.
	mlManager.SetWSClient(paperMarketClient)

	log.Println("✓ Multi-level SL/TP layer initialized")
	// ──────────────────────────────────────────────────────────────────────

	// ── Auto Square-Off Scheduler ─────────────────────────────────────────
	// Runs a cron-like check every minute. At the configured time (default 15:05 IST)
	// it closes all open INTRADAY positions placed through our algo strategies.
	// Positions opened manually on other platforms are NOT touched.
	autoSquareOff := scheduler.NewAutoSquareOffScheduler(
		orderRepo,
		credsRepo,
		orderExecutor,
		cfg.AutoSquareOffTime,
	)
	// Wire paper monitor so paper positions are closed at market close alongside live positions.
	autoSquareOff.SetPaperSquareOff(paperMonitor.SquareOffAll)
	// Wire per-user paper exit: closes a specific user's paper positions at their custom time.
	autoSquareOff.SetPaperForceExitUser(paperMonitor.ForceExitAll)
	// Wire broker position-book check so flat positions (NetQty == 0) aren't squared off again.
	autoSquareOff.SetPositionChecker(positionChecker)
	log.Printf("✓ Auto Square-Off Scheduler initialized (time: %s, paper square-off: enabled, per-user: enabled, broker netQty check: enabled)", cfg.AutoSquareOffTime)

	// Backfill user_square_off_config from today's orders on every startup.
	// Covers orders placed before the per-user custom square-off fix was deployed,
	// so users who sent signals with auto_square_off_time earlier today still fire correctly.
	go func() {
		bfCtx, bfCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer bfCancel()
		n, err := orderRepo.BackfillTodaySquareOffConfig(bfCtx)
		if err != nil {
			log.Printf("[auto-square-off] Backfill warning: %v", err)
		} else if n > 0 {
			log.Printf("[auto-square-off] Backfilled square-off config for %d user(s) from today's orders", n)
		}
	}()
	// ──────────────────────────────────────────────────────────────────────

	// ── Startup Recovery: broker WS pre-warm ─────────────────────────────
	// Eagerly re-subscribe the Indira order-status WS for every user with
	// live exposure (in-flight orders or open positions). Without this, fills,
	// cancels and rejections that occur during a restart are silently dropped
	// until the user takes a new action — leaving the DB out of sync with the
	// broker. Idempotent: StartSubscription tolerates already-subscribed users.
	go func() {
		subsystem := "broker_ws"
		metrics.StartupRecoveryReady.WithLabelValues(subsystem).Set(0)
		start := time.Now()
		defer func() {
			metrics.StartupRecoveryDuration.WithLabelValues(subsystem).Observe(time.Since(start).Seconds())
			metrics.StartupRecoveryReady.WithLabelValues(subsystem).Set(1)
		}()

		warmCtx, warmCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer warmCancel()

		userIDs, err := orderRepo.GetUsersWithLiveExposure(warmCtx)
		if err != nil {
			log.Printf("[startup-recovery] broker-ws: list users failed (non-fatal): %v", err)
			metrics.StartupRecoveryItems.WithLabelValues(subsystem, "error").Inc()
			return
		}
		if len(userIDs) == 0 {
			log.Println("[startup-recovery] broker-ws: no users with live exposure — skipping pre-warm")
			return
		}

		log.Printf("[startup-recovery] broker-ws: pre-warming subscriptions for %d user(s)", len(userIDs))
		for _, userID := range userIDs {
			userCtx, userCancel := context.WithTimeout(warmCtx, 5*time.Second)
			brokerUserId, appId, source, bearerToken, err := credsRepo.GetIndiraCredentials(userCtx, userID)
			if err != nil {
				log.Printf("[startup-recovery] broker-ws: skip user=%s creds_error=%v", userID, err)
				metrics.StartupRecoveryItems.WithLabelValues(subsystem, "skip").Inc()
				userCancel()
				continue
			}
			auth := &indiraPkg.AuthContext{
				UserId:      brokerUserId,
				AppId:       appId,
				Source:      source,
				BearerToken: bearerToken,
			}
			if err := statusService.StartSubscription(userCtx, userID, auth); err != nil {
				log.Printf("[startup-recovery] broker-ws: subscribe failed user=%s err=%v", userID, err)
				metrics.StartupRecoveryItems.WithLabelValues(subsystem, "error").Inc()
				userCancel()
				continue
			}
			// Cache the auth on paperWSServer so PushPositions can fire without
			// waiting for the next fill — matches the lazy path's behavior.
			paperWSServer.StoreAuth(userID, bearerToken, appId, source)
			metrics.StartupRecoveryItems.WithLabelValues(subsystem, "ok").Inc()
			userCancel()
		}
		log.Printf("[startup-recovery] broker-ws: pre-warm complete")
	}()
	// ──────────────────────────────────────────────────────────────────────

	// ── Startup Recovery: in-flight order reconciliation ─────────────────
	// For each non-terminal order with an indira_order_id, fetch the broker's
	// authoritative state via GetOrderBook and apply the snapshot if it differs
	// from the DB. Covers fills/cancels/rejects that landed while we were down.
	// Gated by ENABLE_STARTUP_RECONCILE — opt-in until we've validated it on
	// staging. Read-only against the broker; only writes the DB when a change
	// is observed, and only via the same handleStatusUpdate path used for
	// real-time WS events.
	if cfg.EnableStartupReconcile {
		go func() {
			subsystem := "reconcile"
			metrics.StartupRecoveryReady.WithLabelValues(subsystem).Set(0)
			start := time.Now()
			defer func() {
				metrics.StartupRecoveryDuration.WithLabelValues(subsystem).Observe(time.Since(start).Seconds())
				metrics.StartupRecoveryReady.WithLabelValues(subsystem).Set(1)
			}()

			rcCtx, rcCancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer rcCancel()

			candidates, err := orderRepo.GetReconciliationCandidates(rcCtx, 24, 30)
			if err != nil {
				log.Printf("[startup-recovery] reconcile: list candidates failed: %v", err)
				metrics.StartupRecoveryItems.WithLabelValues(subsystem, "error").Inc()
				return
			}
			if len(candidates) == 0 {
				log.Println("[startup-recovery] reconcile: no in-flight orders to reconcile")
				return
			}

			// Group orders by user so we make at most one GetOrderBook call per user.
			byUser := make(map[string][]*models.Order, 8)
			for _, o := range candidates {
				byUser[o.UserID] = append(byUser[o.UserID], o)
			}
			log.Printf("[startup-recovery] reconcile: %d order(s) across %d user(s)", len(candidates), len(byUser))

			for userID, orders := range byUser {
				userCtx, userCancel := context.WithTimeout(rcCtx, 15*time.Second)
				brokerUserId, appId, source, bearerToken, err := credsRepo.GetIndiraCredentials(userCtx, userID)
				if err != nil {
					log.Printf("[startup-recovery] reconcile: skip user=%s creds_error=%v", userID, err)
					metrics.StartupRecoveryItems.WithLabelValues(subsystem, "skip").Add(float64(len(orders)))
					userCancel()
					continue
				}
				auth := &indiraPkg.AuthContext{
					UserId:      brokerUserId,
					AppId:       appId,
					Source:      source,
					BearerToken: bearerToken,
				}
				book, err := indiraClient.GetOrderBook(userCtx, auth)
				if err != nil {
					log.Printf("[startup-recovery] reconcile: order book fetch failed user=%s err=%v", userID, err)
					metrics.StartupRecoveryItems.WithLabelValues(subsystem, "error").Add(float64(len(orders)))
					userCancel()
					continue
				}
				// Index broker book by OrdId for O(1) lookup.
				byOrdID := make(map[string]*indiraPkg.OrderBook, len(book))
				for i := range book {
					byOrdID[book[i].OrdId] = &book[i]
				}
				for _, o := range orders {
					if o.IndiraOrderID == nil || *o.IndiraOrderID == "" {
						metrics.StartupRecoveryItems.WithLabelValues(subsystem, "skip").Inc()
						continue
					}
					bk, ok := byOrdID[*o.IndiraOrderID]
					if !ok {
						// Order not in today's book — could be older than the book's window.
						// Skip rather than assume terminal state.
						metrics.StartupRecoveryItems.WithLabelValues(subsystem, "skip").Inc()
						continue
					}
					// Build a minimal WSOrderStatus snapshot from the order-book row.
					// Only fields handleStatusUpdate consults need to be populated;
					// others can remain zero values.
					snapshot := &indiraPkg.WSOrderStatus{
						UniqueCode:   *o.IndiraOrderID,
						OrderStatus:  bk.Status,
						Symbol:       bk.Symbol,
						OrderNumber:  bk.OrdId,
						TradedQTY:    indiraPkg.FlexInt(fmt.Sprintf("%d", bk.TradedQty)),
						OrderPrice:   fmt.Sprintf("%f", bk.LimitPrice),
						TriggerPrice: bk.TriggerPrice,
						Product:      bk.PrdType,
						OrderType:    bk.OrdType,
						BuySell:      bk.OrdAction,
					}
					changed, rerr := statusService.ReconcileOrderFromBrokerBook(userCtx, o, snapshot)
					if rerr != nil {
						log.Printf("[startup-recovery] reconcile: apply failed order=%s err=%v", o.OrderID, rerr)
						metrics.StartupRecoveryItems.WithLabelValues(subsystem, "error").Inc()
						continue
					}
					if changed {
						metrics.StartupRecoveryItems.WithLabelValues(subsystem, "ok").Inc()
					} else {
						metrics.StartupRecoveryItems.WithLabelValues(subsystem, "skip").Inc()
					}
				}
				userCancel()
			}
			log.Printf("[startup-recovery] reconcile: complete")
		}()
	} else {
		log.Println("[startup-recovery] reconcile: disabled (ENABLE_STARTUP_RECONCILE=false)")
	}
	// ──────────────────────────────────────────────────────────────────────

	// ── Startup Recovery: multi-level exit group reload ─────────────────
	// Rebuild in-memory state for entry orders whose ML SL/TP groups are
	// still in progress, so price ticks and broker WS events keep routing
	// correctly after a restart. Read-only against the DB; the manager places
	// no broker orders during reload. Gated by ENABLE_ML_RELOAD until validated.
	if cfg.EnableMLReload {
		go func() {
			subsystem := "ml_reload"
			metrics.StartupRecoveryReady.WithLabelValues(subsystem).Set(0)
			start := time.Now()
			defer func() {
				metrics.StartupRecoveryDuration.WithLabelValues(subsystem).Observe(time.Since(start).Seconds())
				metrics.StartupRecoveryReady.WithLabelValues(subsystem).Set(1)
			}()
			// Wait briefly so mlManager.Start (worker pool) and the price WSS
			// have had a chance to come up before we register groups.
			time.Sleep(2 * time.Second)
			rlCtx, rlCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer rlCancel()
			stats, err := mlManager.Reload(rlCtx)
			if err != nil {
				log.Printf("[startup-recovery] ml_reload: error: %v", err)
				metrics.StartupRecoveryItems.WithLabelValues(subsystem, "error").Inc()
				return
			}
			metrics.StartupRecoveryItems.WithLabelValues(subsystem, "ok").Add(float64(stats.GroupsRestored))
			metrics.StartupRecoveryItems.WithLabelValues(subsystem, "skip").Add(float64(stats.GroupsSkipped))
			log.Printf("[startup-recovery] ml_reload: restored=%d skipped=%d levels=%d sl_lost=%d",
				stats.GroupsRestored, stats.GroupsSkipped, stats.LevelsRestored, stats.WarnSingleSLLost)
		}()
	} else {
		log.Println("[startup-recovery] ml_reload: disabled (ENABLE_ML_RELOAD=false)")
	}
	// ──────────────────────────────────────────────────────────────────────

	// ── Startup Recovery: halted-strategy set hydration ──────────────────
	// The in-memory haltedStrategies map starts empty on each restart. The
	// primary defense against signals for paused strategies is upstream
	// (rules-engine itself stops publishing for paused strategies), but a
	// strategy paused while trade-execution was offline is briefly eligible
	// for signals after restart until rules-engine re-emits the pause.
	//
	// LoadHaltedFromSource accepts a HaltedStrategiesLoader implementation;
	// supplying a user-config gRPC client here would close the gap fully.
	// For now we log readiness with a nil loader so the extension point is
	// visible and the metric/log surface exists for future wiring.
	go func() {
		subsystem := "halted_strategies"
		metrics.StartupRecoveryReady.WithLabelValues(subsystem).Set(0)
		start := time.Now()
		defer func() {
			metrics.StartupRecoveryDuration.WithLabelValues(subsystem).Observe(time.Since(start).Seconds())
			metrics.StartupRecoveryReady.WithLabelValues(subsystem).Set(1)
		}()
		// No loader is wired yet — this is the extension point. Once a
		// user-config client is available, construct it here and pass to
		// LoadHaltedFromSource.
		var loader executor.HaltedStrategiesLoader = nil
		if loader == nil {
			log.Println("[startup-recovery] halted_strategies: no loader configured — set empty (relies on rules-engine to not publish for paused strategies)")
			metrics.StartupRecoveryItems.WithLabelValues(subsystem, "skip").Inc()
			return
		}
		hCtx, hCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer hCancel()
		n, err := signalProcessor.LoadHaltedFromSource(hCtx, loader)
		if err != nil {
			log.Printf("[startup-recovery] halted_strategies: load failed (non-fatal): %v", err)
			metrics.StartupRecoveryItems.WithLabelValues(subsystem, "error").Inc()
			return
		}
		metrics.StartupRecoveryItems.WithLabelValues(subsystem, "ok").Add(float64(n))
		log.Printf("[startup-recovery] halted_strategies: hydrated %d strategy ID(s)", n)
	}()
	// ──────────────────────────────────────────────────────────────────────

	// Start services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start tick store drain loop now that ctx is available.
	if tickStoreWriter != nil {
		go tickStoreWriter.Start(ctx)
		log.Println("✓ Tick writer drain loop started (localhost Redis DB=1, ticks:{exchange}:{token}, TTL=12h)")
	}

	// Idle broker-WS sweep: periodically close per-user broker WebSocket
	// connections for users with no live exposure so connections don't
	// accumulate over the trading day.
	statusService.StartIdleSweep(ctx, 15*time.Minute)
	log.Println("✓ Broker WS idle-connection sweep started (15m interval)")

	// ── Lifecycle: ordered graceful shutdown ──────────────────────────────────
	lc := lifecycle.New(cancel, 15*time.Second)

	// Register components in shutdown order:
	// 1. Stop accepting new signals (Kafka consumers)
	lc.Register(lifecycle.Component{
		Name:    "Kafka consumer (trade-signals)",
		StopFn:  kafkaConsumer.Stop,
		CloseFn: kafkaConsumer.Close,
	})
	lc.RegisterCloseable("Kafka consumer (user-config-events)", strategyEventsConsumer)
	// 2. gRPC server (drain RPCs)
	lc.RegisterStoppable("gRPC server", grpcServer)
	// 3. Kafka publisher (flush writes)
	lc.RegisterCloseable("Kafka publisher", kafkaPub)
	// 4. Auto Square-Off Scheduler
	lc.RegisterStoppable("Auto Square-Off Scheduler", autoSquareOff)
	// 4b. Per-user broker WebSocket connections (stop receiving order updates)
	lc.Register(lifecycle.Component{
		Name:   "Broker WS connections",
		StopFn: statusService.CloseAll,
	})
	// 5. PostgreSQL (release connections)
	lc.Register(lifecycle.Component{
		Name:    "PostgreSQL",
		CloseFn: db.Close,
	})

	// ── Prometheus metrics HTTP server ───────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Liveness only — process is up. Keep semantics unchanged so existing
		// orchestrators / probes that hit this endpoint behave identically.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	// /readyz reports whether the per-subsystem startup recovery has completed.
	// Orchestrators that want to delay traffic until recovery is done can probe
	// this instead of /healthz. Subsystems are listed only if their goroutine
	// actually runs in this build/config; unstarted subsystems are reported
	// as "not started" rather than blocking ready forever.
	metricsMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		// We expose readiness for the subsystems that ALWAYS run on startup.
		// Optional ones (reconcile, ml_reload) are gated by env flags; if they
		// didn't start, their gauge stays at 0 and they shouldn't gate readiness.
		required := []string{"broker_ws"}
		notReady := make([]string, 0, len(required))
		for _, name := range required {
			m := &dto.Metric{}
			if err := metrics.StartupRecoveryReady.WithLabelValues(name).Write(m); err == nil {
				if g := m.GetGauge(); g != nil && g.GetValue() >= 1 {
					continue
				}
			}
			notReady = append(notReady, name)
		}
		if len(notReady) > 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "not ready: %v\n", notReady)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	})
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: metricsMux,
	}
	go func() {
		log.Printf("Starting Prometheus metrics server on :%d/metrics", cfg.MetricsPort)
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()
	lc.Register(lifecycle.Component{
		Name: "Metrics HTTP server",
		StopFn: func() {
			shutCtx, c := context.WithTimeout(context.Background(), 3*time.Second)
			defer c()
			metricsServer.Shutdown(shutCtx)
		},
	})

	// Start Auto Square-Off Scheduler
	go func() {
		log.Println("Starting Auto Square-Off Scheduler...")
		if err := autoSquareOff.Start(ctx); err != nil {
			log.Printf("Auto Square-Off Scheduler error: %v", err)
		}
	}()

	// Start market data WSS client (paper trading price feed)
	go func() {
		log.Println("Starting paper market WSS client...")
		paperMarketClient.Start(ctx)
	}()

	// Load active paper orders and subscribe symbols
	go func() {
		time.Sleep(2 * time.Second) // wait for WSS to connect
		if err := paperMonitor.Initialize(ctx); err != nil {
			log.Printf("[paper] Monitor init error (non-fatal): %v", err)
		}
	}()

	// Start live market WSS client (dedicated feed for the live order monitor)
	go func() {
		log.Println("Starting live market WSS client...")
		liveMarketClient.Start(ctx)
	}()

	// Load open live positions and subscribe symbols for live P&L broadcasting
	go func() {
		time.Sleep(2 * time.Second) // wait for WSS to connect
		if err := liveMonitor.Initialize(ctx); err != nil {
			log.Printf("[live] Monitor init error (non-fatal): %v", err)
		}
	}()

	// Start paper trading WebSocket server for frontend
	go func() {
		paperWSAddr := fmt.Sprintf(":%d", cfg.PaperWSPort)
		log.Printf("Starting paper trading WS server on %s", paperWSAddr)
		if err := paperWSServer.StartHTTPServer(ctx, paperWSAddr); err != nil {
			log.Printf("Paper WS server stopped: %v", err)
		}
	}()

	// Start PriceMonitor WSS client + price monitor for below_min orders
	if priceMonitorWSClient != nil {
		go func() {
			log.Println("Starting Price Monitor WSS client...")
			priceMonitorWSClient.Start(ctx)
		}()
	}
	if priceMonitorRef != nil {
		go func() {
			log.Println("Starting Price Monitor...")
			if err := priceMonitorRef.Start(ctx); err != nil {
				log.Printf("Price Monitor error: %v", err)
			}
		}()
	}

	// Start OCO market data WSS client (price feed for trailing SL)
	go func() {
		log.Println("Starting OCO market WSS client...")
		ocoMarketClient.Start(ctx)
	}()

	// Start OCO trailing SL monitor
	go func() {
		log.Println("Starting OCO trailing SL monitor...")
		ocoTrailingMonitor.Start(ctx)
	}()

	// Start multi-level SL/TP manager worker pool.
	// Must be called before Kafka consumer starts so eval workers are ready
	// when the first multi-level paper order fills and enqueues price jobs.
	mlManager.Start(ctx)
	log.Println("✓ Multi-level SL/TP manager worker pool started")

	// Start Kafka consumer — primary intake path (rules-engine trade-signals)
	go func() {
		log.Println("Starting Kafka consumer (trade-signals)...")
		if err := kafkaConsumer.Start(ctx); err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()

	// Start strategy events consumer — closes positions on strategy deactivate/delete
	go func() {
		log.Println("Starting Kafka consumer (user-config-events)...")
		if err := strategyEventsConsumer.Start(ctx); err != nil {
			log.Printf("Strategy events consumer error: %v", err)
		}
	}()

	// Give consumer time to start
	time.Sleep(1 * time.Second)

	// Start gRPC server
	go func() {
		log.Println("Starting gRPC server...")
		if err := grpcServer.Start(); err != nil {
			log.Fatalf("gRPC server error: %v", err)
		}
	}()

	log.Println("========================================")
	log.Println("✓ Trade Execution Service Started")
	log.Printf("  - gRPC Server: localhost:%d", cfg.GRPCPort)
	log.Printf("  - Metrics:     localhost:%d/metrics", cfg.MetricsPort)
	log.Printf("  - Kafka Topic: %s (Group: %s)", cfg.KafkaTopic, cfg.KafkaGroupID)
	log.Printf("  - Workers: %d (bounded pool)", cfg.WorkerCount)
	log.Println("========================================")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan

	log.Printf("\nReceived signal: %v", sig)

	// Ordered graceful shutdown via lifecycle manager
	lc.Shutdown()

	log.Println("========================================")
	log.Println("Trade Execution Service stopped")
	log.Println("========================================")
}

// Config holds service configuration
type Config struct {
	GRPCPort         int
	WorkerCount      int
	KafkaBrokers     []string
	KafkaGroupID     string
	KafkaTopic       string
	MaxRetries       int
	RetryDelay       time.Duration
	PostgresURL      string
	EncryptionKey    string
	// Paper Trading
	PaperWSPort      int
	PaperMarketWSURL string
	// Redis (market price feed)
	RedisAddr     string
	RedisPassword string
	RedisDB       int
	// Observability
	MetricsPort int
	// Auto square-off
	AutoSquareOffTime string // "HH:MM" format, default "15:05"
	// Startup recovery toggles. Both are read-only / additive recovery passes
	// gated by env flags so they can be rolled out independently and rolled
	// back without redeploy.
	EnableStartupReconcile bool // poll broker for in-flight orders on startup
	EnableMLReload         bool // rebuild multi-level exit groups on startup
}

func loadConfig() Config {
	kafkaBrokersStr := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaBrokers := []string{}
	for _, broker := range splitAndTrim(kafkaBrokersStr, ",") {
		if broker != "" {
			kafkaBrokers = append(kafkaBrokers, broker)
		}
	}

	return Config{
		GRPCPort:         getEnvInt("SERVICE_PORT", 9004),
		WorkerCount:      getEnvInt("WORKER_COUNT", 100),
		KafkaBrokers:     kafkaBrokers,
		KafkaGroupID:     getEnv("KAFKA_GROUP_ID", "trade-execution-service"),
		KafkaTopic:       getEnv("KAFKA_TOPIC", "trade-signals"),
		MaxRetries:       getEnvInt("MAX_RETRIES", 3),
		RetryDelay:       time.Duration(getEnvInt("RETRY_DELAY_SEC", 1)) * time.Second,
		PostgresURL:      buildPostgresURL(),
		EncryptionKey:    getEnv("ENCRYPTION_KEY", "0123456789abcdef0123456789abcdef"),
		PaperWSPort:      getEnvInt("PAPER_WS_PORT", 8081),
		PaperMarketWSURL: getEnv("PAPER_MARKET_WS_URL", ""),
		RedisAddr:        getEnv("REDIS_HOST", "localhost") + ":" + getEnv("REDIS_PORT", "6379"),
		RedisPassword:    getEnv("REDIS_PASSWORD", ""),
		RedisDB:          getEnvInt("REDIS_DB", 0),
		MetricsPort:      getEnvInt("METRICS_PORT", 9090),
		AutoSquareOffTime: getEnv("AUTO_SQUARE_OFF_TIME", "15:05"),
		EnableStartupReconcile: getEnvBool("ENABLE_STARTUP_RECONCILE", false),
		EnableMLReload:         getEnvBool("ENABLE_ML_RELOAD", false),
	}
}

func initPostgres(cfg Config) (*sqlx.DB, error) {
	db, err := sqlx.Connect("postgres", cfg.PostgresURL)
	if err != nil {
		return nil, err
	}

	// Configure connection pool — defaults should be >= WORKER_COUNT to avoid
	// worker threads blocking on DB connection acquisition.
	db.SetMaxOpenConns(getEnvInt("MAX_OPEN_CONNS", 120))
	db.SetMaxIdleConns(getEnvInt("MAX_IDLE_CONNS", 60))
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Verify required tables exist and give actionable errors if migrations haven't been applied
	if err := checkRequiredTables(db); err != nil {
		return nil, err
	}

	return db, nil
}

func buildPostgresURL() string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		getEnv("POSTGRES_HOST", "localhost"),
		getEnv("POSTGRES_PORT", "5432"),
		getEnv("POSTGRES_USER", "postgres"),
		getEnv("POSTGRES_PASSWORD", "postgres"),
		getEnv("POSTGRES_DB", "trading_execution"),
		getEnv("POSTGRES_SSL_MODE", "disable"),
	)
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var intVal int
		if _, err := fmt.Sscanf(value, "%d", &intVal); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return defaultValue
}

func splitAndTrim(s, sep string) []string {
	if s == "" {
		return []string{}
	}
	parts := []string{}
	for _, part := range split(s, sep) {
		trimmed := trim(part)
		if trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}

func split(s, sep string) []string {
	result := []string{}
	current := ""
	for _, char := range s {
		if string(char) == sep {
			result = append(result, current)
			current = ""
		} else {
			current += string(char)
		}
	}
	result = append(result, current)
	return result
}

func trim(s string) string {
	start := 0
	end := len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

func initLogger() (*zap.Logger, error) {
	env := os.Getenv("ENVIRONMENT")
	if env == "" {
		env = "development"
	}
	logLevel := os.Getenv("LOG_LEVEL")
	if logLevel == "" {
		logLevel = "info"
	}
	logDir := os.Getenv("LOG_DIR")
	if logDir == "" {
		logDir = "logs"
	}
	lgr, err := pkglogger.New(pkglogger.Config{
		Environment: env,
		Level:       logLevel,
		ServiceName: "trade-execution",
		LogDir:      logDir,
	})
	if err != nil {
		return nil, err
	}
	return lgr.Logger, nil
}

// checkRequiredTables ensures critical tables for this service exist.
// If they don't, return a helpful error message explaining how to run migrations.
func checkRequiredTables(db *sqlx.DB) error {
	var exists bool
	query := `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'orders'
	)`

	if err := db.Get(&exists, query); err != nil {
		return fmt.Errorf("failed to check database schema: %w", err)
	}

	if !exists {
		// Give an actionable error pointing to migration SQL and setup script
		return fmt.Errorf("required table 'orders' does not exist in the database. " +
			"Run the migration: `psql -h <host> -U <user> -d <db> -f services/trade-execution/migrations/001_init.sql` " +
			"or run `scripts/setup_all_databases.sh` to create databases and run migrations.")
	}

	return nil
}

// positionsLookupAdapter bridges *scheduler.PositionChecker (which returns the
// concrete *scheduler.OpenPositions) to consumer.OpenPositionsLookup (which
// expects the interface consumer.OpenPositionsSnapshot). On error it explicitly
// returns a nil interface so the consumer's nil-check fail-open path triggers.
type positionsLookupAdapter struct {
	pc *scheduler.PositionChecker
}

func (a positionsLookupAdapter) FetchOpenPositions(ctx context.Context, userID string) (consumer.OpenPositionsSnapshot, error) {
	snap, err := a.pc.FetchOpenPositions(ctx, userID)
	if err != nil {
		return nil, err
	}
	return snap, nil
}
