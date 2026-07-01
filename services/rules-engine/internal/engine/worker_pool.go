package engine

import (
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// WorkerPool executes submitted jobs using a fixed number of workers.
//
// Shutdown() closes the jobs channel and blocks until all submitted jobs finish.
// This provides the hard invariant required for graceful shutdown: no queued
// evaluations are dropped.
type WorkerPool struct {
	jobs   chan func()
	wg     sync.WaitGroup
	logger *zap.Logger

	closeOnce sync.Once
	closed    atomic.Bool
}

func NewWorkerPool(workers int, buffer int, logger *zap.Logger) *WorkerPool {
	if workers <= 0 {
		workers = 1
	}
	if buffer <= 0 {
		buffer = workers * 4
	}
	wp := &WorkerPool{jobs: make(chan func(), buffer), logger: logger}
	for i := 0; i < workers; i++ {
		go func() {
			for job := range wp.jobs {
				if job == nil {
					wp.wg.Done()
					continue
				}
				wp.runJob(job)
				wp.wg.Done()
			}
		}()
	}
	return wp
}

// runJob executes a single job with panic recovery. One strategy's bad data
// (or an evaluator edge case) must not crash the worker and take every other
// in-flight evaluation — across all strategies and all workers — down with it.
func (wp *WorkerPool) runJob(job func()) {
	defer func() {
		if rec := recover(); rec != nil {
			if wp.logger != nil {
				wp.logger.Error("PANIC RECOVERED in worker pool job",
					zap.Any("panic", rec),
					zap.String("stacktrace", string(debug.Stack())),
				)
			}
		}
	}()
	job()
}

var ErrWorkerPoolClosed = errors.New("worker pool closed")

// Submit enqueues a job. It returns ErrWorkerPoolClosed if Shutdown() was called.
func (wp *WorkerPool) Submit(job func()) error {
	if wp.closed.Load() {
		return ErrWorkerPoolClosed
	}

	wp.wg.Add(1)
	defer func() {
		// If send panics due to closed channel, revert wg and return error.
		if r := recover(); r != nil {
			wp.wg.Done()
		}
	}()

	// Send may panic if Shutdown closes the channel concurrently.
	wp.jobs <- job
	return nil
}

// Shutdown waits for all pending jobs to complete then stops workers.
func (wp *WorkerPool) Shutdown() {
	wp.closeOnce.Do(func() {
		wp.closed.Store(true)
		close(wp.jobs)
	})
	wp.wg.Wait()
}
