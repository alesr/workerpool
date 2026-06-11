package workerpool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type testTask struct {
	value   int
	counter *atomic.Int32
	fn      func(context.Context)
}

func (t testTask) Do(ctx context.Context) {
	if t.fn != nil {
		t.fn(ctx)
	}
	t.counter.Add(int32(t.value))
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("defaults to a single worker when size is zero", func(t *testing.T) {
		t.Parallel()

		pool := New[testTask](context.TODO(), 0) // should default to 1 worker

		var counter atomic.Int32

		task := testTask{
			value:   42,
			counter: &counter,
		}

		if err := pool.Submit(context.TODO(), task); err != nil {
			t.Error("failed to submit task")
		}

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("unexpected shutdown error: %v", err)
		}

		if got := counter.Load(); got != 42 {
			t.Errorf("expected counter = 42, got %d", got)
		}
	})
}

func TestPool_GracefulShutdown(t *testing.T) {
	t.Parallel()

	t.Run("Submit task after graceful shutdown", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[testTask](context.TODO(), 2)

			var counter atomic.Int32
			blocker := make(chan struct{})

			task1 := testTask{
				value:   1,
				counter: &counter,
				fn: func(context.Context) {
					<-blocker
				},
			}

			if err := pool.Submit(context.TODO(), task1); err != nil {
				t.Error("expected Submit to return true")
			}

			// wait for worker to pick up the task and block
			synctest.Wait()

			// immediately shutdown the pool
			// will close(p.stop) but block on p.wg.Wait()
			shutdownDone := make(chan error, 1)
			go func() {
				shutdownDone <- pool.GracefulShutdown()
			}()

			synctest.Wait() // wait shutdown to propagate

			task2 := testTask{
				value:   1,
				counter: &counter,
			}

			err := pool.Submit(context.TODO(), task2)
			if err == nil {
				t.Error("expected Submit to return error after shutdown")
			}

			if !errors.Is(err, ErrPoolClosed) {
				t.Errorf("expected error to be %v, got %v", ErrPoolClosed, err)
			}

			close(blocker)

			if err := <-shutdownDone; err != nil {
				t.Errorf("shutdown error: %v", err)
			}
		})
	})

	t.Run("Graceful shutdown waits for all queued tasks to be complete", func(t *testing.T) {
		t.Parallel()

		// use a single worker and a buffered channel
		//
		// block the worker with a task that won't complete until
		// we say so, then fill the buffer with additional tasks,
		// call GracefulShutdown while the worker is still blocked,
		// then unblock it and verify that GracefulShutdown only returns
		// after all buffered tasks have been processed (not just the in-flight one)

		synctest.Test(t, func(t *testing.T) {
			const buffered, workers = 5, 1

			pool := New(context.TODO(), workers, WithBuffer[testTask](buffered))

			var counter atomic.Int32
			blocker := make(chan struct{})

			pool.Submit(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value:   1,
				counter: &counter,
			})

			synctest.Wait()

			// fill the buffer while the worker is blocked
			for range buffered {
				pool.Submit(context.TODO(), testTask{
					value: 1, counter: &counter,
				})
			}

			// call GracefulShutdown before unblocking
			// it must not return until the buffer is fully drained

			shutdownDone := make(chan error, 1)
			go func() {
				shutdownDone <- pool.GracefulShutdown()
			}()

			close(blocker) // release the worker to start draining

			if err := <-shutdownDone; err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}

			// buffered+1 (started+buffered)
			if got := counter.Load(); got != buffered+1 {
				t.Errorf("expected counter = %d, got %d", buffered+1, got)
			}
		})
	})

	t.Run("task observes context cancellation", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			workers := 1
			pool := New[testTask](t.Context(), workers)

			var counter atomic.Int32

			observed := make(chan struct{})

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			pool.Submit(ctx, testTask{
				fn: func(ctx context.Context) {
					<-ctx.Done()
					close(observed)
				},
				value:   1,
				counter: &counter,
			})

			cancel()
			synctest.Wait()

			select {
			case <-observed:
				// task observed its cancellation
			default:
				t.Error("task should have observed cancelled ctx")
			}

			if err := pool.GracefulShutdown(); err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}
		})
	})
}

func TestPool_ImmediateShutdown(t *testing.T) {
	t.Parallel()

	t.Run("pool stops when context is cancelled", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			pool := New[testTask](ctx, 2)

			var counter atomic.Int32
			blocker := make(chan struct{})

			pool.Submit(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value: 1, counter: &counter,
			})

			synctest.Wait()

			cancel() // immediate shutdown
			synctest.Wait()

			close(blocker) // release the task — but pool is already stopped
		})
	})

	t.Run("GracefulShutdown returns error after immediate stop", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			pool := New[testTask](ctx, 2)

			cancel()
			synctest.Wait()

			err := pool.GracefulShutdown()
			if err == nil {
				t.Error("expected error when calling GracefulShutdown after immediate stop")
			}
		})
	})

	t.Run("Submit returns ErrPoolClosed after immediate stop", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		pool := New[testTask](ctx, 2)

		cancel()

		err := pool.Submit(context.TODO(), testTask{value: 1})
		if err == nil {
			t.Error("expected error after immediate stop")
		}
		if !errors.Is(err, ErrPoolClosed) {
			t.Errorf("expected ErrPoolClosed, got %v", err)
		}
	})
}
