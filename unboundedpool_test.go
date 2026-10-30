package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
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
		wg.Go(func() {
			<-start
			if _, err := pool.TryDo(context.Background(), testTask{value: 1}); errors.Is(err, ErrQueueFull) {
				queueFull.Add(1)
			}
		})
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

// TestUnboundedPool_DrainVsContextCancel verifies that cancelling the pool
// context while GracefulShutdown is draining the broker queue does not hang:
// the broker must abandon the remaining queue instead of blocking on dispatch
// once workers have exited. Abandoned entries never start, so their callers
// get ErrPoolClosed; the in-flight task still completes.
func TestUnboundedPool_DrainVsContextCancel(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		pool := New[int](ctx, 1, WithUnboundedQueue(), WithIngressBuffer(60))

		blocker := make(chan struct{})

		// task1 occupies the only worker and blocks inside the task.
		task1Err := make(chan error, 1)
		go func() {
			_, err := pool.Do(context.Background(), testTask{
				fn:    func(_ context.Context) { <-blocker },
				value: 1,
			})
			task1Err <- err
		}()
		synctest.Wait()

		// queue many tasks behind the blocked worker; they land in ingress
		// and the broker's slice. with dispatch capacity 1, the pre-fix drain
		// loop blocks on the second send forever once the context is cancelled.
		const queued = 50
		errs := make(chan error, queued)
		for range queued {
			go func() {
				_, err := pool.Do(context.Background(), testTask{value: 1})
				errs <- err
			}()
		}
		synctest.Wait()

		// begin graceful shutdown: closes ingress, broker starts draining
		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- pool.GracefulShutdown()
		}()
		synctest.Wait()

		// cancel the pool context mid-drain while the worker is still busy,
		// so no queued entry can start: all queued callers must get ErrPoolClosed
		cancel()
		synctest.Wait()

		for range queued {
			if err := <-errs; !errors.Is(err, ErrPoolClosed) {
				t.Errorf("queued task: expected ErrPoolClosed, got %v", err)
			}
		}

		// release the worker; shutdown must now complete instead of hanging
		close(blocker)
		synctest.Wait()

		select {
		case err := <-shutdownDone:
			if err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("GracefulShutdown hung after pool context cancelled mid-drain")
		}

		if err := <-task1Err; err != nil {
			t.Errorf("in-flight task1: expected nil error, got %v", err)
		}
	})
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
