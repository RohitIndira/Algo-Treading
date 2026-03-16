// Package workerpool provides a bounded goroutine pool with a buffered task queue.
//
// It replaces the unbounded semaphore+goroutine pattern in the Kafka consumer
// with a fixed pool that caps both active goroutines and pending tasks.
package workerpool

import (
	"sync"
	"sync/atomic"
)

// Pool is a bounded worker pool. Tasks are submitted to a buffered channel
// and consumed by a fixed number of persistent goroutines.
type Pool struct {
	tasks  chan func()
	wg     sync.WaitGroup
	active int64 // atomic: currently executing tasks
	closed int32 // atomic: 1 after Stop() called
}

// New creates a pool with `workers` goroutines and a task buffer of `queueSize`.
// Workers start immediately and block on the task channel.
func New(workers, queueSize int) *Pool {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = workers * 4
	}

	p := &Pool{
		tasks: make(chan func(), queueSize),
	}

	p.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go p.worker()
	}

	return p
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for task := range p.tasks {
		atomic.AddInt64(&p.active, 1)
		task()
		atomic.AddInt64(&p.active, -1)
	}
}

// Submit enqueues a task. It blocks if the queue is full.
// Returns false if the pool is stopped.
func (p *Pool) Submit(task func()) bool {
	if atomic.LoadInt32(&p.closed) != 0 {
		return false
	}
	p.tasks <- task
	return true
}

// TrySubmit enqueues a task without blocking.
// Returns false if the queue is full or the pool is stopped.
func (p *Pool) TrySubmit(task func()) bool {
	if atomic.LoadInt32(&p.closed) != 0 {
		return false
	}
	select {
	case p.tasks <- task:
		return true
	default:
		return false
	}
}

// Active returns the number of goroutines currently executing a task.
func (p *Pool) Active() int {
	return int(atomic.LoadInt64(&p.active))
}

// Queued returns the number of tasks waiting in the buffer.
func (p *Pool) Queued() int {
	return len(p.tasks)
}

// Stop closes the task channel and waits for all in-flight tasks to complete.
// It is safe to call Stop multiple times.
func (p *Pool) Stop() {
	if atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		close(p.tasks)
	}
	p.wg.Wait()
}
