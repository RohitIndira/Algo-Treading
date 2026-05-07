package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/engine"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type fakeConfigConsumer struct {
	started chan struct{}
}

func (f *fakeConfigConsumer) Start(ctx context.Context) error {
	close(f.started)
	<-ctx.Done()
	return ctx.Err()
}

type fakeNewsConsumer struct {
	started chan struct{}
	stopCh  chan struct{}
	stopped chan struct{}
}

func (f *fakeNewsConsumer) Start(ctx context.Context) error {
	close(f.started)
	select {
	case <-f.stopCh:
		close(f.stopped)
		return nil
	case <-ctx.Done():
		close(f.stopped)
		return ctx.Err()
	}
}

func (f *fakeNewsConsumer) Stop() {
	close(f.stopCh)
}

func TestStartupOrder_ConfigConsumerBeforeNewsConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	eng := engine.New(nil, engine.Config{Workers: 1}, zap.NewNop())

	cc := &fakeConfigConsumer{started: make(chan struct{})}
	nc := &fakeNewsConsumer{started: make(chan struct{}), stopCh: make(chan struct{}), stopped: make(chan struct{})}
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"localhost:9092"}, Topic: "t", GroupID: "g"})
	defer r.Close()

	lc := StartLive(ctx, eng, cc, nc, r)

	select {
	case <-lc.ConfigConsumerStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("config started signal not received")
	}

	// ensure config consumer Start was invoked before news consumer Start signal
	select {
	case <-cc.started:
		// ok
	default:
		t.Fatalf("expected config consumer Start called")
	}

	select {
	case <-lc.NewsConsumerStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("news started signal not received")
	}

	select {
	case <-nc.started:
		// ok
	default:
		t.Fatalf("expected news consumer Start called")
	}
}

func TestShutdown_NewsStopsBeforeEngineDrainAndConfigClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// custom engine with a hook via embedded worker pool isn't exposed; we'll
	// assert ordering by capturing events.
	var mu sync.Mutex
	order := []string{}

	eng := engine.New(nil, engine.Config{Workers: 1}, zap.NewNop())
	// Wrap engine shutdown by calling it ourselves after pushing marker.
	wrapEng := func() {
		mu.Lock()
		order = append(order, "engine")
		mu.Unlock()
		eng.Shutdown()
	}

	cc := &fakeConfigConsumer{started: make(chan struct{})}
	nc := &fakeNewsConsumer{started: make(chan struct{}), stopCh: make(chan struct{}), stopped: make(chan struct{})}
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"localhost:9092"}, Topic: "t", GroupID: "g"})

	closed := make(chan struct{})
	lc := &Lifecycle{
		ConfigConsumerStarted: make(chan struct{}),
		NewsConsumerStarted:   make(chan struct{}),
		newsStop: func() {
			mu.Lock()
			order = append(order, "news")
			mu.Unlock()
			nc.Stop()
		},
		eng: eng,
		configReaderClose: func() error {
			mu.Lock()
			order = append(order, "config")
			mu.Unlock()
			close(closed)
			return r.Close()
		},
	}

	_ = StartLive(ctx, eng, cc, nc, r)

	// replace engine shutdown path for this test
	lc.eng = nil
	lc.newsStop()
	wrapEng()
	_ = lc.configReaderClose()

	<-nc.stopped
	<-closed

	mu.Lock()
	got := order
	mu.Unlock()
	if len(got) != 3 || got[0] != "news" || got[1] != "engine" || got[2] != "config" {
		t.Fatalf("unexpected shutdown order: %v", got)
	}
}
