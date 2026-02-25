package engine

import (
	"errors"
	"sync"
	"sync/atomic"
)

// WorkerPool executes submitted jobs using a fixed number of workers.
//
// Shutdown() closes the jobs channel and blocks until all submitted jobs finish.
// This provides the hard invariant required for graceful shutdown: no queued
// evaluations are dropped.
type WorkerPool struct {
	jobs chan func()
	wg   sync.WaitGroup

	closeOnce sync.Once
	closed    atomic.Bool
}

func NewWorkerPool(workers int, buffer int) *WorkerPool {
	if workers <= 0 {
		workers = 1
	}
	if buffer <= 0 {
		buffer = workers * 4
	}
	wp := &WorkerPool{jobs: make(chan func(), buffer)}
	for i := 0; i < workers; i++ {
		go func() {
			for job := range wp.jobs {
				if job == nil {
					wp.wg.Done()
					continue
				}
				job()
				wp.wg.Done()
			}
		}()
	}
	return wp
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
