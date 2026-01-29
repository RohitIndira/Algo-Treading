package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/paper-execution/internal/models"
)

// SignalHandler consumes validated PAPER trade-signals.
type SignalHandler interface {
	OnTradeSignal(ctx context.Context, sig *models.TradeSignal) error
}

type TradeSignalConsumer struct {
	reader  *kafka.Reader
	handler SignalHandler
	logger  *zap.Logger
}

func NewTradeSignalConsumer(brokers []string, topic string, groupID string, handler SignalHandler, logger *zap.Logger) *TradeSignalConsumer {
	if groupID == "" {
		groupID = "paper-execution-service"
	}
	if topic == "" {
		topic = "trade-signals"
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        1 * time.Second,
	})

	logger.Info("Paper trade-signals consumer created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group_id", groupID))

	return &TradeSignalConsumer{reader: r, handler: handler, logger: logger}
}

func (c *TradeSignalConsumer) Close() error {
	if c.reader != nil {
		return c.reader.Close()
	}
	return nil
}

func (c *TradeSignalConsumer) Start(ctx context.Context) error {
	c.logger.Info("Starting paper trade-signals consumer")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				c.logger.Error("Failed to fetch trade-signal message", zap.Error(err))
				time.Sleep(500 * time.Millisecond)
				continue
			}

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process trade-signal message", zap.Error(err))
				// Do not commit; retry.
				continue
			}

			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Warn("Failed to commit trade-signal message", zap.Error(err))
			}
		}
	}
}

func (c *TradeSignalConsumer) processMessage(ctx context.Context, msg kafka.Message) error {
	var sig models.TradeSignal
	if err := json.Unmarshal(msg.Value, &sig); err != nil {
		return fmt.Errorf("unmarshal trade-signal: %w", err)
	}

	// Filter strictly to PAPER.
	if strings.ToUpper(strings.TrimSpace(sig.TradingMode)) != "PAPER" {
		return nil
	}
	if strings.ToUpper(strings.TrimSpace(sig.OrderSide)) != "BUY" {
		// paper-execution currently only consumes BUY signals; SELLs are
		// generated internally by the simulator.
		return nil
	}

	if c.handler != nil {
		return c.handler.OnTradeSignal(ctx, &sig)
	}
	return nil
}
