package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/nats-io/nats.go"
)

// NATSPublisher publishes TradeSignals to NATS.
//
// Subject format: trade.signals.{userID}
//
// Rule Engine does NOT wait for any response — fire and forget.
// Publish errors must be logged by the caller and must NOT crash the engine.
type NATSPublisher struct {
	conn *nats.Conn
}

// NewNATSPublisher connects to NATS with automatic reconnect.
// Returns error if initial connection fails.
func NewNATSPublisher(address string) (*NATSPublisher, error) {
	opts := []nats.Option{
		nats.MaxReconnects(20),
		nats.ReconnectWait(2 * time.Second),
		nats.Timeout(5 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			if err != nil {
				log.Printf("[NATS] disconnected: %v", err)
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] reconnected to %s", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Printf("[NATS] connection closed")
		}),
	}

	conn, err := nats.Connect(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("NATS connect to %s failed: %w", address, err)
	}
	log.Printf("[NATS] connected to %s", conn.ConnectedUrl())
	return &NATSPublisher{conn: conn}, nil
}

// PublishSignal publishes a TradeSignal to trade.signals.{userID}.
func (p *NATSPublisher) PublishSignal(_ context.Context, signal *models.TradeSignal) error {
	if signal == nil {
		return fmt.Errorf("signal is nil")
	}
	if signal.UserID == "" {
		return fmt.Errorf("signal has empty user_id — cannot route")
	}
	if p.conn == nil {
		return fmt.Errorf("nats connection is nil")
	}

	payload, err := json.Marshal(signal)
	if err != nil {
		return fmt.Errorf("marshal TradeSignal: %w", err)
	}

	subject := fmt.Sprintf("trade.signals.%s", signal.UserID)
	if err := p.conn.Publish(subject, payload); err != nil {
		return fmt.Errorf("NATS publish to %s failed: %w", subject, err)
	}
	return nil
}

// Close drains in-flight messages then closes connection.
func (p *NATSPublisher) Close() {
	if p.conn == nil || p.conn.IsClosed() {
		return
	}
	if err := p.conn.Drain(); err != nil {
		log.Printf("[NATS] drain error: %v", err)
	}
}

func (p *NATSPublisher) IsConnected() bool {
	return p.conn != nil && p.conn.IsConnected()
}
