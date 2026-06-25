package main

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// Lifecycle is a small wrapper returned by StartLive that allows tests to
// assert shutdown ordering without depending on OS signals.
//
// Legacy news consumer + engine fields removed 2026-06-25 — the news-event
// path (engine + matcher + news consumer) has been deleted entirely. Manthan
// is the only signal path now; ConfigConsumer drives strategy events.
type Lifecycle struct {
	ConfigConsumerStarted chan struct{}

	configReaderClose func() error
}

// StopConfigConsumer closes the config Kafka reader. Engine + news consumer
// shutdown logic gone with the news-path removal.
func (l *Lifecycle) StopConfigConsumer() {
	if l.configReaderClose != nil {
		_ = l.configReaderClose()
	}
}

type configConsumerRunner interface {
	Start(ctx context.Context) error
}

// StartLive starts the config consumer (strategy events from user-config).
// Returns a Lifecycle whose channel signals readiness for the rest of main.go
// (e.g. it can now warm the Redis cache and start the Manthan consumer).
func StartLive(ctx context.Context, cfgConsumer configConsumerRunner, configReader *kafka.Reader, logger ...*zap.Logger) *Lifecycle {
	var log *zap.Logger
	if len(logger) > 0 && logger[0] != nil {
		log = logger[0]
	} else {
		log, _ = zap.NewProduction()
	}

	l := &Lifecycle{
		ConfigConsumerStarted: make(chan struct{}),
		configReaderClose:     configReader.Close,
	}

	go func() {
		close(l.ConfigConsumerStarted)
		if err := cfgConsumer.Start(ctx); err != nil {
			log.Error("Config consumer exited with error", zap.Error(err))
		}
	}()

	return l
}
