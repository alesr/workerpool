package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// Task defines the interface for a task to be executed by a worker pool.
type Task interface {
	Do(context.Context)
}

// Option configures a Pool.
type Option[T Task] func(*Pool[T])

// WithBuffer sets the task channel buffer size.
func WithBuffer[T Task](size int) Option[T] {
	return func(p *Pool[T]) {
		p.buffer = size
	}
}

// Pool maintains fixed worker goroutines processing tasks from a channel.
type Pool[T Task] struct {
	tasks  chan T         // channel for tasks waiting to be processed
	buffer int            // size of the task channel
	wg     sync.WaitGroup // wait group for worker goroutines

	// immediate termination
	ctx            context.Context
	cancel         context.CancelFunc
	ungracefulStop atomic.Bool

	// graceful shutdown
	stop         chan struct{}
	shutdownOnce sync.Once
}

// New creates a pool with numOfWorkers workers.
// The context can be used to stop the pool immediately, skipping any buffered
// tasks. In-flight tasks will still run to completion.
func New[T Task](ctx context.Context, numOfWorkers int, opts ...Option[T]) *Pool[T] {
	if numOfWorkers <= 0 {
		numOfWorkers = 1
	}

	ctx, cancel := context.WithCancel(ctx)

	p := &Pool[T]{
		ctx:    ctx,
		cancel: cancel,
		stop:   make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.tasks = make(chan T, p.buffer)

	p.wg.Add(numOfWorkers)
	for range numOfWorkers {
		go p.worker()
	}
	return p
}

func (p *Pool[T]) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			// exit without draining buffered tasks
			p.ungracefulStop.Store(true)
			return
		case <-p.stop:
			// drain remaining buffered tasks before exiting
			for {
				select {
				case task := <-p.tasks:
					task.Do(p.ctx)
				default:
					return
				}
			}
		case task := <-p.tasks:
			task.Do(p.ctx)
		}
	}
}

// Submit sends a task to the pool. Blocks if the task channel is full.
// Returns false if the pool is shutting down or the context was cancelled.
func (p *Pool[T]) Submit(task T) bool {
	select {
	case <-p.ctx.Done(): // forcefully terminate via ctx
		return false
	case <-p.stop: // terminated via graceful shutdown
		return false
	case p.tasks <- task:
		return true
	}
}

// GracefulShutdown stops accepting new tasks, drains all buffered tasks,
// and waits for in-flight tasks to complete before returning.
// Returns an error if the ctx was cancelled before shutdown completed.
func (p *Pool[T]) GracefulShutdown() error {
	p.shutdownOnce.Do(func() {
		close(p.stop)
	})

	p.wg.Wait()
	p.cancel()

	if p.ungracefulStop.Load() {
		return errors.New("pool was forcefully terminated before shutdown")
	}
	return nil
}
