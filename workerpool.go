package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var (
	ErrPoolClosed    = errors.New("workerpool: pool is closed")
	ErrTaskCancelled = errors.New("workerpool: task context cancelled")
	ErrTaskTimeout   = errors.New("workerpool: task deadline exceeded")
	ErrQueueFull     = errors.New("workerpool: queue is full")
	ErrTaskPanic     = errors.New("workerpool: task panicked")
)

// IsTaskTimeout returns true if the error is ErrTaskTimeout.
func IsTaskTimeout(err error) bool { return errors.Is(err, ErrTaskTimeout) }

// IsTaskCancelled returns true if the error is ErrTaskCancelled.
func IsTaskCancelled(err error) bool { return errors.Is(err, ErrTaskCancelled) }

// IsPoolClosed returns true if the error is ErrPoolClosed.
func IsPoolClosed(err error) bool { return errors.Is(err, ErrPoolClosed) }

// IsQueueFull returns true if the error is ErrQueueFull.
func IsQueueFull(err error) bool { return errors.Is(err, ErrQueueFull) }

// IsTaskPanic returns true if the error is ErrTaskPanic.
func IsTaskPanic(err error) bool { return errors.Is(err, ErrTaskPanic) }

// Task is the unit of work executed by a Pool. The task's Do method receives
// a context that controls the task's execution lifetime.
type Task[T any] interface {
	// Do executes the task with the provided context. The context should be
	// respected by the task implementation. Return the result or an error.
	Do(context.Context) (T, error)
}

// Pool executes Tasks on a fixed number of workers.
type Pool[T any] interface {
	// Do enqueues the task and waits for its result. The task is executed with
	// the provided context, which controls the task's lifetime. On a full bounded
	// queue it blocks until capacity is available (backpressure).
	Do(ctx context.Context, task Task[T]) (T, error)

	// TryDo enqueues the task without blocking on capacity. The task is executed
	// with the provided context. On a full bounded queue it returns ErrQueueFull;
	// on an unbounded pool it may return ErrQueueFull if the ingress buffer is full.
	TryDo(ctx context.Context, task Task[T]) (T, error)

	// GracefulShutdown rejects new tasks, drains queued tasks, and waits for
	// in-flight tasks to finish. Note: Tasks already running will complete even
	// if their context is still active.
	GracefulShutdown() error

	// IsClosed returns true if the pool has been shut down via GracefulShutdown
	// or if the pool context has been cancelled.
	IsClosed() bool
}

type entry[T any] struct {
	ctx     context.Context
	task    Task[T]
	result  chan result[T]
	started atomic.Bool // set by the worker when it picks up the entry
}

type result[T any] struct {
	value T
	err   error
}

type config struct {
	bufferSize    int
	bufferSizeSet bool
	unbounded     bool
	ingressBuffer int // For unbounded pool's ingress channel
}

type Option func(*config)

// WithBuffer sets the task channel buffer size for a bounded pool.
// Pass 0 for unbuffered (direct handoff) behavior.
// Negative sizes are clamped to 0.
// Note: Using this after WithUnboundedQueue will override and create a bounded pool.
func WithBuffer(size int) Option {
	return func(cfg *config) {
		if size < 0 {
			size = 0
		}
		cfg.bufferSize = size
		cfg.bufferSizeSet = true
		cfg.unbounded = false
	}
}

// WithUnboundedQueue configures the pool to use an unbounded in-memory queue.
// Do blocks only when the ingress buffer is full. TryDo returns ErrQueueFull
// when the ingress buffer is full. Use WithIngressBuffer to configure the size.
// Note: If WithBuffer is used after this, it will override and create a bounded pool.
func WithUnboundedQueue() Option {
	return func(cfg *config) {
		cfg.unbounded = true
	}
}

// WithIngressBuffer sets the ingress channel buffer size for an unbounded pool.
// Larger values reduce the chance of TryDo returning ErrQueueFull but use more memory.
// Default: workers * 50. Pass a very large value (e.g., math.MaxInt) for effectively
// unbounded submission, at the cost of potentially unbounded memory usage.
// Note: Passing 0 or not calling this function uses the default size (workers * 50).
func WithIngressBuffer(size int) Option {
	return func(cfg *config) {
		if size < 0 {
			size = 0
		}
		cfg.ingressBuffer = size
	}
}

type poolCore[T any] struct {
	ctx    context.Context
	cancel context.CancelFunc

	stop     chan struct{}
	submitMu sync.RWMutex // guards the send path against shutdown draining

	closeQueue func() // set by the concrete pool: stops intake + flushes backlog

	wg            sync.WaitGroup
	shutdownOnce  sync.Once
	shutdownErr   error      // persists error from GracefulShutdown
	shutdownErrMu sync.Mutex // guards shutdownErr
}

// New creates a worker pool with a default buffer size equal to the number
// of workers. The pool context (ctx) controls the pool's lifetime, while
// individual task contexts control task execution. Use WithBuffer(N) for a
// custom buffer size or WithBuffer(0) for unbuffered (direct handoff) behavior.
// Use WithUnboundedQueue for an unbounded queue.
func New[T any](ctx context.Context, workers int, opts ...Option) Pool[T] {
	if workers <= 0 {
		panic("workerpool: workers must be positive")
	}

	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	// Default buffer size = workers for reasonable backpressure,
	// but only if WithBuffer was not explicitly called (including WithBuffer(0)).
	if !cfg.unbounded && !cfg.bufferSizeSet && cfg.bufferSize == 0 {
		cfg.bufferSize = workers
	}

	if cfg.unbounded {
		return newUnboundedPool[T](ctx, workers, cfg)
	}
	return newBoundedPool[T](ctx, workers, cfg.bufferSize)
}

// submit sends e to ch, guarded against shutdown draining. try=true performs a
// non-blocking send returning ErrQueueFull when ch is full.
func (p *poolCore[T]) submit(ctx context.Context, ch chan<- *entry[T], e *entry[T], try bool) error {
	p.submitMu.RLock()
	defer p.submitMu.RUnlock()

	// Bail fast once shutdown has begun. This guarantees we never reach a send
	// below on a closed channel: closeQueue runs under the write lock, which
	// cannot be acquired while we hold the read lock.
	select {
	case <-p.stop:
		return ErrPoolClosed
	default:
	}

	// Check for context cancellation in deterministic priority order.
	// Priority: task context (highest) -> pool context -> pool stop (lowest).
	// Using ctx.Err() avoids select's pseudo-random choice when multiple cases are ready.
	if err := ctx.Err(); err != nil {
		if err == context.DeadlineExceeded {
			return ErrTaskTimeout
		}
		return ErrTaskCancelled
	}

	// Re-check pool context and stop for race protection.
	// Priority: pool context -> pool stop.
	// If p.stop was closed between the first check and now, we catch it here.
	select {
	case <-p.ctx.Done():
		return ErrPoolClosed
	case <-p.stop:
		return ErrPoolClosed
	default:
	}

	if try {
		select {
		case ch <- e:
			return nil
		default:
			return ErrQueueFull
		}
	}

	// For non-try (Do), perform blocking send but still respect cancellation.
	// Priority: task context -> pool context/stop -> send.
	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrTaskTimeout
		}
		return ErrTaskCancelled
	case <-p.ctx.Done():
		return ErrPoolClosed
	case <-p.stop:
		return ErrPoolClosed
	case ch <- e:
		return nil
	}
}

func (p *poolCore[T]) run(e *entry[T]) {
	defer func() {
		if r := recover(); r != nil {
			// Recover from task panic and convert to error
			e.result <- result[T]{
				err: fmt.Errorf("%w: recovered panic: %v", ErrTaskPanic, r),
			}
		}
	}()

	r, err := e.task.Do(e.ctx)
	e.result <- result[T]{value: r, err: err}
}

// waitResult waits for and returns a task result from ch. The started flag
// reports whether a worker has already picked up the entry, allowing us to
// distinguish in-flight tasks from queued ones when the pool shuts down.
//
// It uses a two-phase select to prevent a race condition:
// If the result is already buffered in ch when we enter this function,
// a single select might incorrectly choose a ready context case over the
// ready channel case (due to select's pseudo-random selection), causing
// us to discard a valid result. The first non-blocking select ensures
// we prioritize the channel if data is immediately available.
//
// Pool context handling: if the pool's context is cancelled (immediate
// shutdown), workers exit and queued entries are never dispatched. If the
// task already started (started=true), its worker is still running it and
// will deliver the result into the buffered channel — we wait for it. If the
// task never started, it will never run, so we return ErrPoolClosed instead
// of hanging until the task context expires.
//
// Caveat: In phase 2, if both ch and a context case become ready simultaneously,
// select may choose the context case and discard the result. This is a narrow
// race that doesn't break the "nil => ran once" invariant but may discard a
// valid result.
func (p *poolCore[T]) waitResult(ctx context.Context, ch chan result[T], started *atomic.Bool) (T, error) {
	// Phase 1: Non-blocking check for already-available result
	select {
	case r := <-ch:
		return r.value, r.err
	default:
	}

	// Phase 2: Blocking wait for result, task context cancellation, or pool shutdown
	select {
	case r := <-ch:
		return r.value, r.err
	case <-ctx.Done():
		// The result may have arrived simultaneously with the context
		// cancellation — prefer it if so.
		select {
		case r := <-ch:
			return r.value, r.err
		default:
		}
		var zero T
		// Distinguish between deadline exceeded and explicit cancellation
		if ctx.Err() == context.DeadlineExceeded {
			return zero, ErrTaskTimeout
		}
		return zero, ErrTaskCancelled
	case <-p.ctx.Done():
		// Pool is shutting down. If the task already started, its worker
		// will complete it and deliver the result into the buffered channel.
		if started.Load() {
			select {
			case r := <-ch:
				return r.value, r.err
			case <-ctx.Done():
				select {
				case r := <-ch:
					return r.value, r.err
				default:
				}
				var zero T
				if ctx.Err() == context.DeadlineExceeded {
					return zero, ErrTaskTimeout
				}
				return zero, ErrTaskCancelled
			}
		}
		// Task was queued but never picked up — it will never run.
		var zero T
		return zero, ErrPoolClosed
	}
}

func (p *poolCore[T]) GracefulShutdown() error {
	p.shutdownOnce.Do(func() {
		// Check if the pool was already forcefully terminated
		// (context cancelled before GracefulShutdown was called).
		if err := p.ctx.Err(); err != nil {
			p.shutdownErrMu.Lock()
			p.shutdownErr = fmt.Errorf("workerpool: pool was forcefully terminated before shutdown: %w", err)
			p.shutdownErrMu.Unlock()
			// Still wait for in-flight tasks to complete, maintaining the API contract
			// that GracefulShutdown waits for in-flight tasks to finish.
			p.wg.Wait()
			return
		}

		close(p.stop)
		p.submitMu.Lock()
		p.closeQueue() // inside the write lock: no submit can be mid-send
		p.submitMu.Unlock()
		p.wg.Wait()
		p.cancel()
	})

	p.shutdownErrMu.Lock()
	defer p.shutdownErrMu.Unlock()
	return p.shutdownErr
}

// IsClosed returns true if the pool has been shut down via GracefulShutdown
// or if the pool context has been cancelled.
func (p *poolCore[T]) IsClosed() bool {
	select {
	case <-p.stop:
		return true
	case <-p.ctx.Done():
		return true
	default:
		return false
	}
}
