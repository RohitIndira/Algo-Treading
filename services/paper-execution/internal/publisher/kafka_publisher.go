package publisher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/RohitIndira/Algo-Treading/services/paper-execution/internal/models"
)

type KafkaPublisher struct {
	execWriter *kafka.Writer
	pnlWriter  *kafka.Writer
	logger     *zap.Logger

	execTopic string
	pnlTopic  string
}

func NewKafkaPublisher(brokers []string, execTopic string, pnlTopic string, logger *zap.Logger) *KafkaPublisher {
	execWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        execTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}

	pnlWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        pnlTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
	}

	logger.Info("Paper Kafka publisher initialized",
		zap.Strings("brokers", brokers),
		zap.String("exec_topic", execTopic),
		zap.String("pnl_topic", pnlTopic))

	return &KafkaPublisher{execWriter: execWriter, pnlWriter: pnlWriter, logger: logger, execTopic: execTopic, pnlTopic: pnlTopic}
}

func (p *KafkaPublisher) Close() error {
	var err1, err2 error
	if p.execWriter != nil {
		err1 = p.execWriter.Close()
	}
	if p.pnlWriter != nil {
		err2 = p.pnlWriter.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *KafkaPublisher) PublishExecution(ctx context.Context, ev *models.PaperExecutionEvent) error {
	if ev == nil {
		return fmt.Errorf("nil execution event")
	}
	if p.execWriter == nil {
		return fmt.Errorf("exec writer not initialized")
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal execution event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(ev.UserID),
		Value: payload,
	}
	if err := p.execWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write to topic %s: %w", p.execTopic, err)
	}
	p.logger.Info("paper execution event published",
		zap.String("topic", p.execTopic),
		zap.String("user_id", ev.UserID),
		zap.Int64("token", ev.Token),
		zap.String("leg", ev.Leg),
		zap.String("side", ev.OrderSide),
		zap.Int32("qty", ev.Quantity),
		zap.Float64("price", ev.Price),
		zap.Float64("pnl", ev.PnL))
	return nil
}

func (p *KafkaPublisher) PublishPnLSnapshot(ctx context.Context, snap *models.PaperPnLSnapshot) error {
	if snap == nil {
		return fmt.Errorf("nil pnl snapshot")
	}
	if p.pnlWriter == nil {
		return fmt.Errorf("pnl writer not initialized")
	}
	payload, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal pnl snapshot: %w", err)
	}
	msg := kafka.Message{Key: []byte(snap.UserID), Value: payload}
	if err := p.pnlWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write to topic %s: %w", p.pnlTopic, err)
	}
	p.logger.Debug("paper pnl snapshot published",
		zap.String("topic", p.pnlTopic),
		zap.String("user_id", snap.UserID),
		zap.Float64("closed_pnl", snap.ClosedPnL),
		zap.Int("open_positions", snap.OpenPositions))
	return nil
}

// PublishPortfolioSummary publishes a comprehensive portfolio P&L summary with
// open position details, market values, unrealized P&L, and closed P&L.
func (p *KafkaPublisher) PublishPortfolioSummary(ctx context.Context, summary *models.PortfolioPnLSummary) error {
	if summary == nil {
		return fmt.Errorf("nil portfolio summary")
	}
	if p.pnlWriter == nil {
		return fmt.Errorf("pnl writer not initialized")
	}
	payload, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("marshal portfolio summary: %w", err)
	}
	msg := kafka.Message{Key: []byte(summary.UserID), Value: payload}
	if err := p.pnlWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write to topic %s: %w", p.pnlTopic, err)
	}
	p.logger.Info("portfolio summary published",
		zap.String("topic", p.pnlTopic),
		zap.String("user_id", summary.UserID),
		zap.Float64("portfolio_value", summary.PortfolioValue),
		zap.Float64("market_value", summary.TotalMarketValue),
		zap.Float64("unrealized_pnl", summary.TotalUnrealizedPnL),
		zap.Float64("closed_pnl", summary.TotalClosedPnL),
		zap.Int("open_positions", summary.OpenPositionsCount))
	return nil
}

// PublishReinvestmentSignal publishes a reinvestment signal when a position
// fully closes and capital becomes available for buying a new 52W breakout.
func (p *KafkaPublisher) PublishReinvestmentSignal(ctx context.Context, signal *models.ReinvestmentSignal) error {
	if signal == nil {
		return fmt.Errorf("nil reinvestment signal")
	}
	if p.execWriter == nil {
		return fmt.Errorf("exec writer not initialized")
	}
	payload, err := json.Marshal(signal)
	if err != nil {
		return fmt.Errorf("marshal reinvestment signal: %w", err)
	}
	msg := kafka.Message{Key: []byte(signal.UserID), Value: payload}
	if err := p.execWriter.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write to topic %s: %w", p.execTopic, err)
	}
	p.logger.Info("reinvestment signal published",
		zap.String("topic", p.execTopic),
		zap.String("user_id", signal.UserID),
		zap.Float64("available_capital", signal.AvailableCapital),
		zap.Int64("closed_token", signal.ClosedToken),
		zap.String("closed_symbol", signal.ClosedSymbol))
	return nil
}
