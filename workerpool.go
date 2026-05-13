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

type entry[T Task] struct {
	ctx context.Context
	job T
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
	entries chan entry[T]  // channel for jobs waiting to be processed
	buffer  int            // size of the task channel
	wg      sync.WaitGroup // wait group for worker goroutines

	// immediate termination
	ctx            context.Context
	cancel         context.CancelFunc
	ungracefulStop atomic.Bool

	// graceful shutdown
	stop         chan struct{}
	shutdownOnce sync.Once
}

// New creates a pool with number of available workers.
// The context can be used to stop the pool immediately, skipping any buffered
// tasks. In-flight tasks will still run to completion.
func New[T Task](ctx context.Context, workers int, opts ...Option[T]) *Pool[T] {
	if workers <= 0 {
		workers = 1
	}

	poolCtx, cancel := context.WithCancel(ctx)

	p := &Pool[T]{
		ctx:    poolCtx,
		cancel: cancel,
		stop:   make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.entries = make(chan entry[T], p.buffer)

	p.wg.Add(workers)
	for range workers {
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
				case entry := <-p.entries:
					entry.job.Do(entry.ctx)
				default:
					// channel is empty. since p.stop is closed,
					// no more tasks can be submitted
					return
				}
			}
		case entry := <-p.entries:
			entry.job.Do(entry.ctx)
		}
	}
}

var (
	ErrPoolClosed    = errors.New("pool is closed")
	ErrTaskCancelled = errors.New("task context cancelled")
)

// Submit sends a task to the pool. Blocks if the task channel is full.
// Returns false if the pool is shutting down or the context was cancelled.
func (p *Pool[T]) Submit(ctx context.Context, task T) error {
	select {
	case <-ctx.Done():
		return ErrTaskCancelled
	case <-p.ctx.Done(): // forcefully terminate via ctx
		return ErrPoolClosed
	case <-p.stop: // terminated via graceful shutdown
		return ErrPoolClosed
	case p.entries <- entry[T]{ctx: ctx, job: task}:
		return nil
	}
}

// GracefulShutdown stops accepting new tasks, drains all buffered tasks,
// and waits for in-flight tasks to complete before returning.
// Returns an error if the ctx was cancelled before shutdown completed.
func (p *Pool[T]) GracefulShutdown() error {
	if p.ungracefulStop.Load() {
		return errors.New("pool was forcefully terminated before shutdown")
	}

	p.shutdownOnce.Do(func() {
		close(p.stop)
		p.wg.Wait()
		p.cancel()

		// only close(p.entries) with a lock here and
		// a read lock in Submit otherwise senders will panic =]
		// but it's just a good to have, since p.stop is closed
		// and submit already checks for that
	})
	return nil
}
