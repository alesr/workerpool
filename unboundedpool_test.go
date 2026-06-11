package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func TestPool_Unbounded(t *testing.T) {
	t.Parallel()

	t.Run("queues and executes tasks", func(t *testing.T) {
		t.Parallel()

		pool := New[testTask](context.TODO(), 3, WithUnboundedQueue[testTask]())

		var counter atomic.Int32
		for i := 1; i <= 10; i++ {
			task := testTask{
				value:   i,
				counter: &counter,
			}
			if err := pool.Submit(context.TODO(), task); err != nil {
				t.Error("failed to submit task")
			}
		}

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("shutdown error: %v", err)
		}

		if got := counter.Load(); got != 55 {
			t.Errorf("expected counter = 55, got %d", got)
		}
	})

	t.Run("handles concurrent submissions", func(t *testing.T) {
		t.Parallel()

		const workers, tasks = 5, 100

		pool := New[testTask](context.TODO(), workers, WithUnboundedQueue[testTask]())

		var (
			counter atomic.Int32
			wg      sync.WaitGroup
		)

		wg.Add(tasks)

		for i := range tasks {
			go func(val int) {
				defer wg.Done()
				task := testTask{
					value:   1,
					counter: &counter,
				}
				pool.Submit(context.TODO(), task)
			}(i)
		}

		wg.Wait()

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("unexpected shutdown error: %v", err)
		}

		if got := counter.Load(); got != tasks {
			t.Errorf("expected counter = %d, got %d", tasks, got)
		}
	})

	t.Run("Submit after shutdown returns ErrPoolClosed", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[testTask](t.Context(), 2, WithUnboundedQueue[testTask]())

			if err := pool.GracefulShutdown(); err != nil {
				t.Errorf("shutdown error: %v", err)
			}

			err := pool.Submit(context.TODO(), testTask{value: 1})
			if err == nil {
				t.Error("expected error after shutdown")
			}
			if !errors.Is(err, ErrPoolClosed) {
				t.Errorf("expected ErrPoolClosed, got %v", err)
			}
		})
	})

	t.Run("graceful shutdown drains all queued tasks", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[testTask](t.Context(), 1, WithUnboundedQueue[testTask]())

			var counter atomic.Int32
			blocker := make(chan struct{})

			pool.Submit(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value: 1, counter: &counter,
			})

			synctest.Wait()

			for range 5 {
				pool.Submit(context.TODO(), testTask{
					value: 1, counter: &counter,
				})
			}

			shutdownDone := make(chan error, 1)
			go func() {
				shutdownDone <- pool.GracefulShutdown()
			}()

			close(blocker)

			if err := <-shutdownDone; err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}

			if got := counter.Load(); got != 6 {
				t.Errorf("expected counter = 6, got %d", got)
			}
		})
	})
}
