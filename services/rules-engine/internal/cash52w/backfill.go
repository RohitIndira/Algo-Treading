package cash52w

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// BackfillFromBreakouts replays today's breakout messages from Kafka and
// processes them for a single user until the engine reaches MaxPositions.
//
// This solves the operational gap where a user enables CASH_52W_HIGH after the
// data-ingestion service has already published today's breakouts.
func BackfillFromBreakouts(
	ctx context.Context,
	logger *zap.Logger,
	brokers []string,
	topic string,
	engine *Engine,
	userID string,
	timeBudget time.Duration,
) error {
	if engine == nil {
		return fmt.Errorf("nil engine")
	}
	if userID == "" {
		return fmt.Errorf("user_id is required")
	}
	if topic == "" {
		topic = "market.data.52w_breakouts"
	}
	if timeBudget <= 0 {
		timeBudget = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeBudget)
	defer cancel()

	logger.Info("Starting Cash52W backfill from breakouts",
		zap.String("user_id", userID),
		zap.String("topic", topic),
		zap.Duration("time_budget", timeBudget))

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer r.Close()

	processed := 0
	opened := 0
	for {
		select {
		case <-ctx.Done():
			logger.Info("Cash52W backfill finished",
				zap.String("user_id", userID),
				zap.Int("processed", processed),
				zap.Int("opened", opened),
				zap.Error(ctx.Err()))
			return nil
		default:
		}

		msg, err := r.ReadMessage(ctx)
		if err != nil {
			// timeout/cancel is fine
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("backfill read message: %w", err)
		}

		var ev models.Breakout52WEvent
		if err := json.Unmarshal(msg.Value, &ev); err != nil {
			continue
		}
		processed++

		// HandleBreakout already filters non-today messages.
		didOpen, _ := engine.HandleBreakoutForUser(ctx, userID, &ev)
		if didOpen {
			opened++
		}

		// Stop early if we have filled the basket.
		st := engine.getUserState(userID)
		st.mu.Lock()
		n := len(st.Positions)
		st.mu.Unlock()
		if n >= engine.cfg.MaxPositions {
			logger.Info("Cash52W backfill reached max positions",
				zap.String("user_id", userID),
				zap.Int("positions", n),
				zap.Int("opened", opened),
				zap.Int("processed", processed))
			return nil
		}
	}
}
