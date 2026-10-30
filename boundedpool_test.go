package workerpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func TestPool_WithBuffer(t *testing.T) {
	t.Parallel()

	pool := New[int](context.TODO(), 3, WithBuffer(10))

	var counter atomic.Int32

	for i := 1; i <= 20; i++ {
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
		t.Errorf("unexpected shutdown error: %v", err)
	}

	if got := counter.Load(); got != 210 {
		t.Errorf("expected counter = 210, got %d", got)
	}
}

func TestPool_Do(t *testing.T) {
	t.Parallel()

	t.Run("queues and executes tasks", func(t *testing.T) {
		t.Parallel()

		pool := New[int](context.TODO(), 3)

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

	t.Run("second Do blocks when the worker is busy", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[int](t.Context(), 1)

			counter := &atomic.Int32{}
			blocker := make(chan struct{})

			go pool.Do(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value:   1,
				counter: counter,
			})

			synctest.Wait()

			task2Done := make(chan struct{})
			go func() {
				pool.Do(context.TODO(), testTask{
					value:   2,
					counter: counter,
				})
				close(task2Done)
			}()

			synctest.Wait()

			select {
			case <-task2Done:
				t.Error("Do should be blocked while worker is busy")
			default:
			}

			close(blocker)

			synctest.Wait()

			select {
			case <-task2Done:
			default:
				t.Error("Do should have completed after worker was unblocked")
			}

			if err := pool.GracefulShutdown(); err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}

			if got := counter.Load(); got != 3 {
				t.Errorf("expected counter = 3, got %d", got)
			}
		})
	})

	t.Run("task observes its own context cancellation via Do", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			taskCtx, cancelTask := context.WithCancel(t.Context())

			pool := New[int](t.Context(), 1)

			var counter atomic.Int32
			taskDone := make(chan struct{})

			go pool.Do(taskCtx, testTask{
				fn: func(ctx context.Context) {
					<-ctx.Done()
					close(taskDone)
				},
				value:   1,
				counter: &counter,
			})

			synctest.Wait()
			cancelTask()

			synctest.Wait()

			select {
			case <-taskDone:
			default:
				t.Error("task should have observed context cancellation")
			}

			if err := pool.GracefulShutdown(); err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}
		})
	})

	t.Run("handles concurrent submissions", func(t *testing.T) {
		t.Parallel()

		const workers, tasks = 5, 100

		pool := New[int](context.TODO(), workers)

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

	t.Run("blocks caller until capacity is available", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[int](context.TODO(), 1)

			var counter atomic.Int32
			blocker := make(chan struct{})

			task1 := testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value:   1,
				counter: &counter,
			}

			go pool.Do(context.TODO(), task1)

			synctest.Wait()

			task2Done := make(chan struct{})

			go func() {
				pool.Do(context.TODO(), testTask{
					value:   2,
					counter: &counter,
				})
				close(task2Done)
			}()

			synctest.Wait()

			select {
			case <-task2Done:
				t.Error("Do should be blocked while worker is busy")
			default:
			}

			close(blocker)
			synctest.Wait()

			select {
			case <-task2Done:
			default:
				t.Error("Do should have completed after worker was unblocked")
			}

			if err := pool.GracefulShutdown(); err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}

			if got := counter.Load(); got != 3 {
				t.Errorf("expected counter = 3, got %d", got)
			}
		})
	})
}

func TestPool_TryDo(t *testing.T) {
	t.Parallel()

	t.Run("returns ErrQueueFull when the bounded queue is full", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[int](context.TODO(), 1, WithBuffer(1))

			var counter atomic.Int32
			blocker := make(chan struct{})

			// Worker picks up task1 and blocks.
			go pool.Do(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value:   1,
				counter: &counter,
			})

			synctest.Wait()

			// task2 fills the dispatch buffer (size 1).
			go pool.Do(context.TODO(), testTask{
				value:   2,
				counter: &counter,
			})

			synctest.Wait()

			// buffer is full: TryDo must fail immediately without blocking
			_, err := pool.TryDo(context.TODO(), testTask{
				value:   3,
				counter: &counter,
			})
			if !errors.Is(err, ErrQueueFull) {
				t.Fatalf("expected ErrQueueFull, got %v", err)
			}

			close(blocker)
			synctest.Wait()

			if err := pool.GracefulShutdown(); err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}

			// task3 was rejected, so only task1 (1) and task2 (2) ran
			if got := counter.Load(); got != 3 {
				t.Errorf("expected counter = 3, got %d", got)
			}
		})
	})
}

// TestPool_WithBufferZero verifies that WithBuffer(0) creates an unbuffered pool
// (direct handoff), not one with a default buffer size.
func TestPool_WithBufferZero(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		// create pool with explicit WithBuffer(0) - should be unbuffered
		pool := New[int](context.TODO(), 1, WithBuffer(0))

		var counter atomic.Int32
		blocker := make(chan struct{})

		// first task blocks the worker
		go pool.Do(context.TODO(), testTask{
			fn: func(_ context.Context) {
				<-blocker
			},
			value:   1,
			counter: &counter,
		})

		synctest.Wait()

		// second Do should block because there's no buffer and worker is busy
		task2Done := make(chan struct{})
		go func() {
			pool.Do(context.TODO(), testTask{
				value:   2,
				counter: &counter,
			})
			close(task2Done)
		}()

		synctest.Wait()

		// task2 should be blocked (no buffer, worker busy)
		select {
		case <-task2Done:
			t.Error("Do should be blocked when worker is busy with unbuffered pool")
		default:
		}

		// unblock worker
		close(blocker)

		synctest.Wait()

		// now task2 should complete
		select {
		case <-task2Done:
		default:
			t.Error("Do should have completed after worker was unblocked")
		}

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("unexpected shutdown error: %v", err)
		}

		if got := counter.Load(); got != 3 {
			t.Errorf("expected counter = 3, got %d", got)
		}
	})
}
