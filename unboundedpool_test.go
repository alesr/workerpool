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

		pool := New[int](context.TODO(), 3, WithUnboundedQueue())

		var counter atomic.Int32
		for i := 1; i <= 10; i++ {
			task := testTask{
				value:   i,
				counter: &counter,
			}
			result, err := pool.Do(context.TODO(), task)
			if err != nil {
				t.Error("failed to execute task")
			}
			if result != i {
				t.Errorf("expected result %d, got %d", i, result)
			}
		}

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("shutdown error: %v", err)
		}

		if got := counter.Load(); got != 55 {
			t.Errorf("expected counter = 55, got %d", got)
		}
	})

	t.Run("TryDo is equivalent to Do", func(t *testing.T) {
		t.Parallel()

		pool := New[int](context.TODO(), 3, WithUnboundedQueue())

		var counter atomic.Int32
		for i := 1; i <= 10; i++ {
			task := testTask{
				value:   i,
				counter: &counter,
			}
			result, err := pool.TryDo(context.TODO(), task)
			if err != nil {
				t.Fatalf("TryDo returned error: %v", err)
			}
			if result != i {
				t.Errorf("expected result %d, got %d", i, result)
			}
		}

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("unexpected shutdown error: %v", err)
		}

		if got := counter.Load(); got != 55 {
			t.Errorf("expected counter = 55, got %d", got)
		}
	})

	t.Run("handles concurrent submissions", func(t *testing.T) {
		t.Parallel()

		const workers, tasks = 5, 100

		pool := New[int](context.TODO(), workers, WithUnboundedQueue())

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
				pool.Do(context.TODO(), task)
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

	t.Run("Do after shutdown returns ErrPoolClosed", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[int](context.TODO(), 2, WithUnboundedQueue())

			if err := pool.GracefulShutdown(); err != nil {
				t.Errorf("shutdown error: %v", err)
			}

			_, err := pool.Do(context.TODO(), testTask{value: 1})
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
			pool := New[int](context.TODO(), 1, WithUnboundedQueue())

			var counter atomic.Int32
			blocker := make(chan struct{})

			go pool.Do(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value: 1, counter: &counter,
			})

			synctest.Wait()

			for range 5 {
				go pool.Do(context.TODO(), testTask{
					value: 1, counter: &counter,
				})
			}
			synctest.Wait()

			shutdownDone := make(chan error, 1)
			go func() {
				shutdownDone <- pool.GracefulShutdown()
			}()

			close(blocker)
			synctest.Wait()

			if err := <-shutdownDone; err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}

			if got := counter.Load(); got != 6 {
				t.Errorf("expected counter = 6, got %d", got)
			}
		})
	})
}

// TestPool_Unbounded_TryDoIngressFull verifies that TryDo on an unbounded pool
// returns ErrQueueFull when the ingress buffer is full. Many TryDo calls are
// released simultaneously on a start barrier, with ingress buffer 1 and a
// single broker draining it, at least one non-blocking send must observe the
// full buffer.
func TestPool_Unbounded_TryDoIngressFull(t *testing.T) {
	t.Parallel()

	pool := New[int](context.Background(), 4, WithUnboundedQueue(), WithIngressBuffer(1))

	var queueFull atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 500 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := pool.TryDo(context.Background(), testTask{value: 1}); errors.Is(err, ErrQueueFull) {
				queueFull.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if err := pool.GracefulShutdown(); err != nil {
		t.Errorf("shutdown: %v", err)
	}

	if queueFull.Load() == 0 {
		t.Error("expected at least one ErrQueueFull from concurrent TryDo on full ingress")
	}
}

func TestUnboundedPool_BrokerDrainOnShutdown(t *testing.T) {
	t.Parallel()

	t.Run("drains all queued tasks during graceful shutdown", func(t *testing.T) {
		t.Parallel()

		pool := New[int](context.TODO(), 1, WithUnboundedQueue(), WithIngressBuffer(5))

		var counter atomic.Int32

		// submit tasks to fill the pool
		for range 5 {
			_, err := pool.Do(context.TODO(), testTask{
				value:   1,
				counter: &counter,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		}

		// shutdown should drain all queued tasks
		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("shutdown error: %v", err)
		}

		// all tasks should have completed
		if got := counter.Load(); got != 5 {
			t.Errorf("expected counter = 5, got %d", got)
		}
	})
}
