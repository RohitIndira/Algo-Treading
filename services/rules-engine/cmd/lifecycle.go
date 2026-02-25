package main

import (
	"context"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/engine"
	"github.com/segmentio/kafka-go"
)

// Lifecycle is a small wrapper returned by StartLive that allows tests to
// assert shutdown ordering without depending on OS signals.
type Lifecycle struct {
	ConfigConsumerStarted chan struct{}
	NewsConsumerStarted   chan struct{}

	newsStop func()
	eng      *engine.Engine

	configReaderClose func() error
}

func (l *Lifecycle) StopNewsFirstThenDrainEngineThenStopConfig() {
	if l.newsStop != nil {
		l.newsStop()
	}
	if l.eng != nil {
		l.eng.Shutdown()
	}
	if l.configReaderClose != nil {
		_ = l.configReaderClose()
	}
}

type configConsumerRunner interface {
	Start(ctx context.Context) error
}

type newsConsumerRunner interface {
	Start(ctx context.Context) error
	Stop()
}

// StartLive starts the config consumer BEFORE the news consumer and signals
// the order via channels.
func StartLive(ctx context.Context, eng *engine.Engine, cfgConsumer configConsumerRunner, newsConsumer newsConsumerRunner, configReader *kafka.Reader) *Lifecycle {
	l := &Lifecycle{
		ConfigConsumerStarted: make(chan struct{}),
		NewsConsumerStarted:   make(chan struct{}),
		newsStop:              newsConsumer.Stop,
		eng:                   eng,
		configReaderClose:     configReader.Close,
	}

	go func() {
		close(l.ConfigConsumerStarted)
		_ = cfgConsumer.Start(ctx)
	}()

	go func() {
		// tiny yield so order is deterministic even if goroutines schedule oddly
		time.Sleep(1 * time.Millisecond)
		close(l.NewsConsumerStarted)
		_ = newsConsumer.Start(ctx)
	}()

	return l
}
