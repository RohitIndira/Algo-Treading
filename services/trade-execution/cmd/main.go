package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
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

	// Reload active OCO groups from DB (restart recovery)
	go func() {
		reloadCtx, reloadCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer reloadCancel()
		if err := ocoManager.Reload(reloadCtx); err != nil {
			log.Printf("[oco] Reload failed (non-fatal): %v", err)
		} else {
			log.Printf("✓ OCO manager initialized (%d active groups)", ocoManager.ActiveCount())
		}
	}()

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
	log.Printf("✓ Auto Square-Off Scheduler initialized (time: %s, paper square-off: enabled, per-user: enabled)", cfg.AutoSquareOffTime)

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

	// Start services
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start tick store drain loop now that ctx is available.
	if tickStoreWriter != nil {
		go tickStoreWriter.Start(ctx)
		log.Println("✓ Tick writer drain loop started (localhost Redis DB=1, ticks:{exchange}:{token}, TTL=12h)")
	}

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
	// 5. PostgreSQL (release connections)
	lc.Register(lifecycle.Component{
		Name:    "PostgreSQL",
		CloseFn: db.Close,
	})

	// ── Prometheus metrics HTTP server ───────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
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
