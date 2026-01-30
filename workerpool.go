package workerpool

import (
	"context"
	"sync"
)

// Input wraps a task's execution.
type Input interface {
	Do(context.Context)
}

// Task bundles a function with its input.
type Task[T Input] struct {
	Fn    func(T)
	Input T
}

// Option configures a Pool.
type Option[T Input] func(*Pool[T])

// WithBuffer sets the task channel buffer size.
func WithBuffer[T Input](size int) Option[T] {
	return func(p *Pool[T]) {
		p.buffer = size
	}
}

// Pool maintains fixed worker goroutines processing tasks from a channel.
type Pool[T Input] struct {
	tasks        chan Task[T]
	wg           sync.WaitGroup
	stop         chan struct{}
	shutdownOnce sync.Once
	buffer       int
}

// New creates a pool with size workers.
func New[T Input](size int, opts ...Option[T]) *Pool[T] {
	if size <= 0 {
		size = 1
	}

	p := &Pool[T]{
		stop: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.tasks = make(chan Task[T], p.buffer)

	p.wg.Add(size)
	for range size {
		go p.worker()
	}
	return p
}

func (p *Pool[T]) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			// drain remaining buffered tasks
			for {
				select {
				case task := <-p.tasks:
					task.Fn(task.Input)
				default:
					return
				}
			}
		case task := <-p.tasks:
			task.Fn(task.Input)
		}
	}
}

// Submit sends a task to the pool. Blocks if all workers are busy.
// Returns false if pool is shut down.
func (p *Pool[T]) Submit(task Task[T]) bool {
	select {
	case <-p.stop:
		return false
	case p.tasks <- task:
		return true
	}
}

// Shutdown stops accepting tasks and waits for active tasks to complete.
func (p *Pool[T]) Shutdown() {
	p.shutdownOnce.Do(func() {
		close(p.stop)
	})
	p.wg.Wait()
}
