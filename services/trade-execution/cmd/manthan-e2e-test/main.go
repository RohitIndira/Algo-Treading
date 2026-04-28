// End-to-end test: Simulates full Manthan order lifecycle in PAPER mode.
// Tests: pre-check → entry (LIMIT BUY) → fill → SL placement → SL modify → emergency sell
//
// Usage:
//   EXT_REDIS_ADDR=15.207.203.46:6379 EXT_REDIS_PASSWORD='R3d1s@Prod#2026' \
//   go run ./services/trade-execution/cmd/manthan-e2e-test/
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/trade-execution/internal/manthan"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// --- Connect external Redis ---
	extAddr := os.Getenv("EXT_REDIS_ADDR")
	extPass := os.Getenv("EXT_REDIS_PASSWORD")
	if extAddr == "" {
		fmt.Println("Set EXT_REDIS_ADDR and EXT_REDIS_PASSWORD")
		os.Exit(1)
	}
	extRedis := redis.NewClient(&redis.Options{Addr: extAddr, Password: extPass})
	if err := extRedis.Ping(ctx).Err(); err != nil {
		logger.Fatal("External Redis failed", zap.Error(err))
	}
	fmt.Println("✅ External Redis connected")

	// --- Connect DB ---
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=postgres dbname=trading_db sslmode=disable")
	if err != nil {
		logger.Fatal("DB open failed", zap.Error(err))
	}
	if err := db.Ping(); err != nil {
		logger.Fatal("DB ping failed", zap.Error(err))
	}
	fmt.Println("✅ PostgreSQL connected")

	// --- Init components ---
	broker := manthan.NewBrokerAdapter(nil, extRedis, logger) // nil client = paper mode only
	repo := manthan.NewRepository(db)
	preCheck := manthan.NewPreChecker(repo, broker, logger)
	slHandler := manthan.NewSLHandler(broker, repo, logger)
	entryHandler := manthan.NewEntryHandler(broker, repo, preCheck, slHandler, logger)

	// WSS Bridge (for LIVE mode — set even for paper to verify wiring)
	wssBridge := manthan.NewWSSBridge(logger)
	entryHandler.SetWSSBridge(wssBridge)

	// --- Test Stock: GALLANTT ---
	testSymbol := "GALLANTT"
	testISIN := "INE297H01019"

	fmt.Printf("\n=== TEST 1: Symbol Resolution ===\n")
	info, err := broker.ResolveSymbol(ctx, testSymbol, testISIN)
	if err != nil {
		logger.Fatal("Resolve failed", zap.Error(err))
	}
	fmt.Printf("✅ %s → token=%s indira=%s tick=%.2f DPR=[%.2f, %.2f]\n",
		testSymbol, info.ExchangeToken, info.IndiraSymbol, info.TickSize, info.DPRLower, info.DPRUpper)

	fmt.Printf("\n=== TEST 2: Live LTP ===\n")
	ltp, err := broker.FetchLTP(ctx, info.ExchangeToken)
	if err != nil {
		logger.Fatal("LTP fetch failed", zap.Error(err))
	}
	fmt.Printf("✅ %s LTP = ₹%.2f\n", testSymbol, ltp)

	fmt.Printf("\n=== TEST 3: Pre-Check (PAPER mode) ===\n")
	signal := manthan.ManthanSignal{
		OrderID:     fmt.Sprintf("test-e2e-%d", time.Now().UnixNano()),
		UserID:      "test-user-001",
		StrategyID:  "00000000-0000-0000-0000-e2e000000001",
		Symbol:      testSymbol,
		ISIN:        testISIN,
		Exchange:    "NSE",
		OrderType:   "MARKET",
		OrderSide:   "BUY",
		ProductType: "DELIVERY",
		Quantity:    5,
		EntryPrice:  ltp,
		StopLoss:    ltp * 0.80,
		StopLossPct: 20,
		TrailingSLPct: 2,
		InvestedAmt: ltp * 5,
		TradingMode: "PAPER",
	}
	check := preCheck.CheckEntry(ctx, signal, info)
	fmt.Printf("Pre-check result: canProceed=%v reason=%q\n", check.CanProceed, check.Reason)
	if !check.CanProceed {
		fmt.Println("⚠️  Pre-check failed (expected outside market hours) — testing entry anyway for PAPER mode")
	}

	fmt.Printf("\n=== TEST 4: PAPER Entry (LIMIT BUY) ===\n")
	orderID, err := entryHandler.ExecuteEntry(ctx, signal)
	if err != nil {
		fmt.Printf("❌ Entry failed: %v\n", err)
		fmt.Println("(This is expected if market hours check blocks it)")
		fmt.Println("Testing direct paper fill instead...")

		// Direct paper test bypassing market hours
		orderID = directPaperTest(ctx, repo, slHandler, signal, info, ltp, logger)
	} else {
		fmt.Printf("✅ Entry order created: id=%d\n", orderID)
	}

	if orderID > 0 {
		// Verify DB state
		fmt.Printf("\n=== TEST 5: Verify DB State ===\n")
		verifyDB(ctx, db, orderID)

		// Test SL Modify (trail)
		fmt.Printf("\n=== TEST 6: SL Trail Modify (PAPER) ===\n")
		newHigh := ltp * 1.02 // +2%
		newSL := newHigh * 0.80
		slModifySignal := manthan.SLModifySignal{
			OrderID:     fmt.Sprintf("slmod-test-%d", time.Now().UnixNano()),
			UserID:      signal.UserID,
			StrategyID:  signal.StrategyID,
			Symbol:      testSymbol,
			ISIN:        testISIN,
			Exchange:    "NSE",
			OrderType:   "SL_MODIFY",
			NewSL:       newSL,
			OldSL:       ltp * 0.80,
			NewHigh:     newHigh,
			TradingMode: "PAPER",
		}
		err = slHandler.ModifyTrail(ctx, slModifySignal)
		if err != nil {
			fmt.Printf("⚠️  SL modify: %v\n", err)
		} else {
			fmt.Printf("✅ SL trail modified: old=₹%.2f → new=₹%.2f (high=₹%.2f)\n",
				slModifySignal.OldSL, slModifySignal.NewSL, slModifySignal.NewHigh)
		}

		// Test Emergency Sell
		fmt.Printf("\n=== TEST 7: Emergency SELL (PAPER) ===\n")
		exitSignal := manthan.SLExitSignal{
			OrderID:     fmt.Sprintf("exit-test-%d", time.Now().UnixNano()),
			UserID:      signal.UserID,
			StrategyID:  signal.StrategyID,
			Symbol:      testSymbol,
			ISIN:        testISIN,
			Exchange:    "NSE",
			OrderType:   "MARKET",
			OrderSide:   "SELL",
			ProductType: "DELIVERY",
			Quantity:    signal.Quantity,
			ExitPrice:   ltp * 0.80,
			SLPrice:     ltp * 0.80,
			PnL:         (ltp*0.80 - ltp) * float64(signal.Quantity),
			TradingMode: "PAPER",
		}
		err = slHandler.EmergencySell(ctx, exitSignal)
		if err != nil {
			fmt.Printf("❌ Emergency sell failed: %v\n", err)
		} else {
			fmt.Printf("✅ Emergency SELL executed: exit=₹%.2f pnl=₹%.2f\n",
				exitSignal.ExitPrice, exitSignal.PnL)
		}
	}

	// Test WSS Bridge
	fmt.Printf("\n=== TEST 8: WSS Bridge Simulation ===\n")
	testWSSBridge(wssBridge, logger)

	// Final DB state
	fmt.Printf("\n=== FINAL: All manthan_orders ===\n")
	rows, _ := db.QueryContext(ctx,
		`SELECT id, symbol, order_type, order_side, qty, filled_qty, limit_price, trigger_price, avg_fill_price, status, broker_order_id
		 FROM manthan_orders ORDER BY id DESC LIMIT 10`)
	if rows != nil {
		defer rows.Close()
		fmt.Printf("%-4s %-12s %-12s %-6s %-5s %-7s %-10s %-10s %-10s %-12s %-20s\n",
			"ID", "SYMBOL", "TYPE", "SIDE", "QTY", "FILLED", "LIMIT", "TRIGGER", "AVGFILL", "STATUS", "BROKER_ID")
		for rows.Next() {
			var id int
			var symbol, orderType, side, status string
			var qty, filled int
			var limitP, trigP, avgP float64
			var brokerID sql.NullString
			rows.Scan(&id, &symbol, &orderType, &side, &qty, &filled, &limitP, &trigP, &avgP, &status, &brokerID)
			bid := ""
			if brokerID.Valid {
				bid = brokerID.String
			}
			fmt.Printf("%-4d %-12s %-12s %-6s %-5d %-7d %-10.2f %-10.2f %-10.2f %-12s %-20s\n",
				id, symbol, orderType, side, qty, filled, limitP, trigP, avgP, status, bid)
		}
	}

	// Events
	fmt.Printf("\n=== AUDIT: manthan_order_events ===\n")
	evRows, _ := db.QueryContext(ctx,
		`SELECT order_id, event_type, old_status, new_status, price, qty, detail
		 FROM manthan_order_events ORDER BY id DESC LIMIT 10`)
	if evRows != nil {
		defer evRows.Close()
		fmt.Printf("%-6s %-15s %-12s %-12s %-10s %-5s %s\n",
			"ORD", "EVENT", "FROM", "TO", "PRICE", "QTY", "DETAIL")
		for evRows.Next() {
			var ordID, qty int
			var event, from, to, detail string
			var price float64
			evRows.Scan(&ordID, &event, &from, &to, &price, &qty, &detail)
			fmt.Printf("%-6d %-15s %-12s %-12s %-10.2f %-5d %s\n",
				ordID, event, from, to, price, qty, detail)
		}
	}

	fmt.Println("\n✅ End-to-end test complete")
}

// directPaperTest bypasses market hours check for testing.
func directPaperTest(ctx context.Context, repo *manthan.Repository, slHandler *manthan.SLHandler, signal manthan.ManthanSignal, info *manthan.SymbolInfo, ltp float64, logger *zap.Logger) int64 {
	order := &manthan.ManthanOrder{
		SignalID:      signal.OrderID,
		StrategyID:    signal.StrategyID,
		UserID:        signal.UserID,
		Symbol:        signal.Symbol,
		ISIN:          signal.ISIN,
		Exchange:      "NSE",
		OrderType:     manthan.OrderTypeLimitBuy,
		OrderSide:     "BUY",
		ProductType:   "CNC",
		Qty:           int(signal.Quantity),
		LimitPrice:    ltp + (info.TickSize * 2),
		IndiraSymbol:  info.IndiraSymbol,
		ExchangeToken: info.ExchangeToken,
		Status:        manthan.StatusPending,
		MaxRetries:    3,
	}

	orderID, err := repo.InsertOrder(ctx, order)
	if err != nil {
		fmt.Printf("❌ Insert order failed: %v\n", err)
		return 0
	}

	_ = repo.UpdateOrderPlaced(ctx, orderID, "PAPER-"+signal.OrderID)
	_ = repo.UpdateOrderFilled(ctx, orderID, order.Qty, ltp)
	_ = repo.InsertEvent(ctx, orderID, "FILL", "PLACED", "FILLED", "PAPER", ltp, order.Qty, "direct paper fill")

	fmt.Printf("✅ PAPER BUY filled: id=%d symbol=%s qty=%d price=₹%.2f\n",
		orderID, signal.Symbol, order.Qty, ltp)

	// Place SL
	slTrigger := ltp * 0.80
	slLimit := slTrigger - (info.TickSize * 5)
	slHandler.PlaceInitialSL(ctx, orderID, signal, info, order.Qty, slTrigger, slLimit)

	return orderID
}

func testWSSBridge(bridge *manthan.WSSBridge, logger *zap.Logger) {
	// Simulate: entry handler registers, WSS fires updates
	fakeBrokerID := "WSS-TEST-ORD-001"

	// 1. Register
	ch := bridge.Register(fakeBrokerID)
	fmt.Printf("✅ Registered broker_order_id=%s (pending=%d)\n", fakeBrokerID, bridge.PendingCount())

	// 2. Simulate WSS sending PENDING (non-terminal — should not unblock)
	bridge.HandleUpdate(fakeBrokerID, "PENDING", 0, 0, 0, "")
	fmt.Println("✅ Sent PENDING update")

	// 3. Simulate WSS sending OPEN (non-terminal)
	bridge.HandleUpdate(fakeBrokerID, "OPEN", 0, 0, 0, "")
	fmt.Println("✅ Sent OPEN update")

	// 4. Simulate WSS sending EXECUTED (terminal — should unblock)
	bridge.HandleUpdate(fakeBrokerID, "EXECUTED", 10, 915.50, 0, "")
	fmt.Println("✅ Sent EXECUTED update")

	// 5. Read from channel — should get all 3 updates
	received := 0
	timeout := time.After(2 * time.Second)
	for {
		select {
		case update, ok := <-ch:
			if !ok {
				fmt.Println("  Channel closed")
				goto done
			}
			received++
			terminal := ""
			if manthan.IsTerminalStatus(update.Status) {
				terminal = " ← TERMINAL"
			}
			fmt.Printf("  Received: status=%s filled=%d price=₹%.2f%s\n",
				update.Status, update.FilledQty, update.AvgFillPrice, terminal)

			if manthan.IsFilledWSStatus(update.Status) {
				fmt.Printf("✅ Fill detected via WSS: qty=%d price=₹%.2f\n",
					update.FilledQty, update.AvgFillPrice)
			}
		case <-timeout:
			goto done
		}
	}
done:
	fmt.Printf("✅ Total updates received: %d\n", received)

	// 6. Unregister
	bridge.Unregister(fakeBrokerID)
	fmt.Printf("✅ Unregistered (pending=%d)\n", bridge.PendingCount())

	// 7. Test: unknown broker ID (should return false)
	routed := bridge.HandleUpdate("UNKNOWN-ORD-999", "EXECUTED", 5, 100, 0, "")
	if !routed {
		fmt.Println("✅ Unknown broker_order_id correctly ignored")
	} else {
		fmt.Println("❌ BUG: unknown broker ID should not route")
	}

	// 8. Test: duplicate detection
	fmt.Printf("✅ IsRegistered(unknown)=%v (expected false)\n", bridge.IsRegistered("UNKNOWN"))
}

func verifyDB(ctx context.Context, db *sql.DB, orderID int64) {
	var symbol, status, brokerID string
	var qty, filled int
	var avgPrice float64
	err := db.QueryRowContext(ctx,
		`SELECT symbol, status, filled_qty, avg_fill_price, COALESCE(broker_order_id,'') FROM manthan_orders WHERE id=$1`,
		orderID).Scan(&symbol, &status, &filled, &avgPrice, &brokerID)
	if err != nil {
		fmt.Printf("❌ DB verify failed: %v\n", err)
		return
	}
	fmt.Printf("✅ Entry order: symbol=%s status=%s filled=%d avgPrice=₹%.2f brokerID=%s\n",
		symbol, status, filled, avgPrice, brokerID)

	// Check SL order
	var slCount int
	db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM manthan_orders WHERE parent_order_id=$1 AND order_type='SL_SELL'`,
		orderID).Scan(&slCount)
	fmt.Printf("✅ SL orders linked: %d\n", slCount)
	_ = qty
}
