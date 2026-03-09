package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/models"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func runTestNATSServer(t *testing.T) (*server.Server, string) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	opts := &server.Options{Host: "127.0.0.1", Port: port}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(2 * time.Second) {
		s.Shutdown()
		t.Fatalf("nats server not ready")
	}
	return s, fmt.Sprintf("nats://127.0.0.1:%d", port)
}

func TestNATSPublisher_PublishSignal_CorrectSubject(t *testing.T) {
	s, addr := runTestNATSServer(t)
	defer s.Shutdown()

	pub, err := NewNATSPublisher(addr)
	if err != nil {
		t.Fatalf("NewNATSPublisher: %v", err)
	}
	defer pub.Close()

	// Subscribe
	ch := make(chan *models.TradeSignal, 1)
	subj := "trade.signals.user-test-123"
	_, err = pub.conn.Subscribe(subj, func(m *nats.Msg) {
		var got models.TradeSignal
		if err := json.Unmarshal(m.Data, &got); err == nil {
			ch <- &got
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	pub.conn.Flush()

	signal := &models.TradeSignal{UserID: "user-test-123", StrategyID: "s1", NewsID: "n1", TradingMode: "PAPER", StockCode: 1, GeneratedAt: time.Now().UnixNano()}
	if err := pub.PublishSignal(context.Background(), signal); err != nil {
		t.Fatalf("PublishSignal: %v", err)
	}
	pub.conn.Flush()

	select {
	case got := <-ch:
		if got.UserID != "user-test-123" {
			t.Fatalf("unexpected user_id: %s", got.UserID)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for message")
	}
}

func TestNATSPublisher_PublishSignal_EmptyUserID_ReturnsError(t *testing.T) {
	s, addr := runTestNATSServer(t)
	defer s.Shutdown()

	pub, err := NewNATSPublisher(addr)
	if err != nil {
		t.Fatalf("NewNATSPublisher: %v", err)
	}
	defer pub.Close()

	err = pub.PublishSignal(context.Background(), &models.TradeSignal{UserID: ""})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestNATSPublisher_PublishSignal_NilSignal_ReturnsError(t *testing.T) {
	s, addr := runTestNATSServer(t)
	defer s.Shutdown()

	pub, err := NewNATSPublisher(addr)
	if err != nil {
		t.Fatalf("NewNATSPublisher: %v", err)
	}
	defer pub.Close()

	err = pub.PublishSignal(context.Background(), nil)
	if err == nil {
		t.Fatalf("expected error")
	}
}
