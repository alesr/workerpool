package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrPoolClosed    = errors.New("pool is closed")
	ErrTaskCancelled = errors.New("task context cancelled")
)

// Task defines the interface for a task to be executed by a worker pool.
type Task interface {
	Do(context.Context)
}

type entry[T Task] struct {
	ctx context.Context
	job T
}

// Pool is the interface that wraps the basic worker pool operations.
type Pool[T Task] interface {
	Submit(ctx context.Context, task T) error
	GracefulShutdown() error
}

type config struct {
	bufferSize int
	unbounded  bool
}

// Option configures a Pool.
type Option[T Task] func(*config)

// WithBuffer sets the task channel buffer size for a bounded pool.
func WithBuffer[T Task](size int) Option[T] {
	return func(cfg *config) {
		cfg.bufferSize = size
		cfg.unbounded = false
	}
}

// WithUnboundedQueue configures the pool to use an unbounded in-memory queue.
// Submit never blocks on capacity — tasks are buffered in a dynamic slice.
func WithUnboundedQueue[T Task]() Option[T] {
	return func(cfg *config) {
		cfg.unbounded = true
	}
}

// poolCore holds shared lifecycle state for bounded and unbounded pool implementations.
type poolCore[T Task] struct {
	ctx    context.Context
	cancel context.CancelFunc

	stop     chan struct{}
	dispatch chan entry[T]

	wg             sync.WaitGroup
	shutdownOnce   sync.Once
	ungracefulStop atomic.Bool
}

// New creates a worker pool. Returns a bounded channel-backed pool by default.
// Use WithUnboundedQueue to get a memory-backed pool that never blocks on Submit.
func New[T Task](ctx context.Context, workers int, opts ...Option[T]) Pool[T] {
	if workers <= 0 {
		workers = 1
	}

	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.unbounded {
		return newUnboundedPool[T](ctx, workers)
	}
	return newBoundedPool[T](ctx, workers, cfg.bufferSize)
}

// GracefulShutdown stops accepting new tasks, drains all buffered tasks,
// and waits for in-flight tasks to complete before returning.
func (p *poolCore[T]) GracefulShutdown() error {
	if p.ungracefulStop.Load() {
		return errors.New("pool was forcefully terminated before shutdown")
	}

	p.shutdownOnce.Do(func() {
		close(p.stop)
		p.wg.Wait()
		p.cancel()
	})
	return nil
}
