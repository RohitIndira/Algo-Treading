package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/configstore"
	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"go.uber.org/zap"
)

func TestShutdown_WorkerPoolDrainsBeforeExit(t *testing.T) {
	logger := zap.NewNop()
	store := configstore.New()
	// load 1 dummy strategy so EvaluateEvent submits 1 job per event
	store.BulkLoad([]*models.StrategyConfig{{
		StrategyID: "s1", UserID: "u1", Active: true, Version: 1,
		Conditions:  models.Conditions{MatchAllNews: true, ImpactScoreMin: 1, ImpactScoreMax: 2},
		TradeConfig: models.TradeConfig{OrderType: "MARKET", Quantity: 1, Exchange: "NSE"},
		RiskLimits:  models.RiskLimits{MaxDailyTrades: 1},
	}})

	eng := New(store, Config{Workers: 4}, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eng.Start(ctx)

	for i := 0; i < 1000; i++ {
		evt := &models.MarketEvent{EventID: fmt.Sprintf("e-%d", i), EventType: "news", Timestamp: time.Now()}
		evt.StockData.StockCode = 1
		evt.StockData.Symbol = "X"
		evt.Analysis.ImpactScore = 5
		go func() {
			_, _ = eng.EvaluateEvent(context.Background(), evt)
		}()
	}

	// give some time for submissions
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	eng.Shutdown()
	if time.Since(start) > 5*time.Second {
		t.Fatalf("shutdown took too long")
	}
}
