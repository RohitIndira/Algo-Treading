package main

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/RohitIndira/Algo-Treading/services/rules-engine/internal/engine"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
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
func StartLive(ctx context.Context, eng *engine.Engine, cfgConsumer configConsumerRunner, newsConsumer newsConsumerRunner, configReader *kafka.Reader, logger ...*zap.Logger) *Lifecycle {
	var log *zap.Logger
	if len(logger) > 0 && logger[0] != nil {
		log = logger[0]
	} else {
		log, _ = zap.NewProduction()
	}

	l := &Lifecycle{
		ConfigConsumerStarted: make(chan struct{}),
		NewsConsumerStarted:   make(chan struct{}),
		newsStop:              newsConsumer.Stop,
		eng:                   eng,
		configReaderClose:     configReader.Close,
	}

	go func() {
		close(l.ConfigConsumerStarted)
		// Backstop only: both consumer implementations already recover their
		// own per-message panics internally and keep looping. This catches
		// anything unexpected that slips past that, instead of silently
		// killing config sync for the rest of the process's life.
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("PANIC RECOVERED in config consumer — config sync is now non-functional",
					zap.Any("panic", rec),
					zap.String("stacktrace", string(debug.Stack())))
			}
		}()
		if err := cfgConsumer.Start(ctx); err != nil {
			log.Error("Config consumer exited with error", zap.Error(err))
		}
	}()

	go func() {
		// tiny yield so order is deterministic even if goroutines schedule oddly
		time.Sleep(1 * time.Millisecond)
		close(l.NewsConsumerStarted)
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("PANIC RECOVERED in news consumer — news processing is now non-functional",
					zap.Any("panic", rec),
					zap.String("stacktrace", string(debug.Stack())))
			}
		}()
		if err := newsConsumer.Start(ctx); err != nil {
			log.Error("News consumer exited with error", zap.Error(err))
		}
	}()

	return l
}
