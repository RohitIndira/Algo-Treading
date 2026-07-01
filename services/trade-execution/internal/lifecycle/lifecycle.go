// Package lifecycle provides ordered graceful shutdown for the trade-execution service.
//
// Shutdown order:
//  1. Stop accepting new signals  — close Kafka consumers
//  2. Drain in-flight work        — wait for worker pool to finish
//  3. Stop price monitor          — no new triggers
//  4. Stop WebSocket servers      — close client connections
//  5. Stop gRPC server            — graceful stop (drains RPCs)
//  6. Close publishers            — flush Kafka writes
//  7. Close database              — release connections
//
// This mirrors the rules-engine lifecycle.go pattern but is adapted for
// trade-execution's component set.
package lifecycle

import (
	"context"
	"log"
	"runtime/debug"
	"time"
)

// Stoppable is any component that can be stopped or closed.
type Stoppable interface {
	Stop()
}

// Closeable is any component with a Close method.
type Closeable interface {
	Close() error
}

// Component represents a named service component for shutdown logging.
type Component struct {
	Name    string
	StopFn  func()       // called if non-nil
	CloseFn func() error // called if non-nil (after StopFn)
}

// Lifecycle orchestrates ordered startup signalling and shutdown.
type Lifecycle struct {
	components []Component
	cancel     context.CancelFunc
	timeout    time.Duration
}

// New creates a lifecycle with the given shutdown timeout.
func New(cancel context.CancelFunc, timeout time.Duration) *Lifecycle {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	return &Lifecycle{
		cancel:  cancel,
		timeout: timeout,
	}
}

// Register adds a component to the shutdown sequence.
// Components are shut down in the order they are registered.
func (l *Lifecycle) Register(c Component) {
	l.components = append(l.components, c)
}

// RegisterStoppable is a convenience for components implementing Stop().
func (l *Lifecycle) RegisterStoppable(name string, s Stoppable) {
	l.components = append(l.components, Component{
		Name:   name,
		StopFn: s.Stop,
	})
}

// RegisterCloseable is a convenience for components implementing Close().
func (l *Lifecycle) RegisterCloseable(name string, c Closeable) {
	l.components = append(l.components, Component{
		Name:    name,
		CloseFn: c.Close,
	})
}

// Shutdown executes the ordered shutdown sequence.
// It first cancels the root context, then stops each component in order.
func (l *Lifecycle) Shutdown() {
	log.Println("[lifecycle] ── Graceful shutdown initiated ──")
	start := time.Now()

	// 1. Cancel root context — signals all goroutines to stop
	if l.cancel != nil {
		l.cancel()
	}

	// 2. Stop each component in registration order with per-component timeout
	perComponent := l.timeout / time.Duration(len(l.components)+1)
	if perComponent < 2*time.Second {
		perComponent = 2 * time.Second
	}

	for i, c := range l.components {
		log.Printf("[lifecycle]   [%d/%d] Stopping %s...", i+1, len(l.components), c.Name)
		done := make(chan struct{})

		go func(comp Component) {
			defer close(done)
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[lifecycle] PANIC RECOVERED stopping %s: %v\n%s", comp.Name, r, debug.Stack())
				}
			}()
			if comp.StopFn != nil {
				comp.StopFn()
			}
			if comp.CloseFn != nil {
				if err := comp.CloseFn(); err != nil {
					log.Printf("[lifecycle]   [%d/%d] %s close error: %v", i+1, len(l.components), comp.Name, err)
				}
			}
		}(c)

		select {
		case <-done:
			log.Printf("[lifecycle]   [%d/%d] %s stopped", i+1, len(l.components), c.Name)
		case <-time.After(perComponent):
			log.Printf("[lifecycle]   [%d/%d] %s shutdown timed out after %v", i+1, len(l.components), c.Name, perComponent)
		}
	}

	log.Printf("[lifecycle] ── Shutdown complete (%v) ──", time.Since(start).Round(time.Millisecond))
}
