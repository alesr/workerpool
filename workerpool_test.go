package workerpool

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type testTask struct {
	value   int
	counter *atomic.Int32
	fn      func(context.Context)
}

func (t testTask) Do(ctx context.Context) (int, error) {
	if t.fn != nil {
		t.fn(ctx)
	}
	if t.counter != nil {
		t.counter.Add(int32(t.value))
	}
	return t.value, nil
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("panics on zero workers", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for zero workers")
			}
		}()

		_ = New[int](context.TODO(), 0)
	})

	t.Run("panics on negative workers", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for negative workers")
			}
		}()

		_ = New[int](context.TODO(), -1)
	})
}

func TestPool_GracefulShutdown(t *testing.T) {
	t.Parallel()

	t.Run("Do task after graceful shutdown", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[int](context.TODO(), 2)

			var counter atomic.Int32
			blocker := make(chan struct{})

			task1 := testTask{
				value:   1,
				counter: &counter,
				fn: func(context.Context) {
					<-blocker
				},
			}

			go pool.Do(context.TODO(), task1)

			synctest.Wait()

			shutdownDone := make(chan error, 1)
			go func() {
				shutdownDone <- pool.GracefulShutdown()
			}()

			synctest.Wait()

			_, err := pool.Do(context.TODO(), testTask{
				value:   1,
				counter: &counter,
			})
			if err == nil {
				t.Error("expected Do to return error after shutdown")
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

		synctest.Test(t, func(t *testing.T) {
			const buffered, workers = 5, 1

			pool := New[int](context.TODO(), workers, WithBuffer(buffered))

			var counter atomic.Int32
			blocker := make(chan struct{})

			go pool.Do(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value:   1,
				counter: &counter,
			})

			synctest.Wait()

			for range buffered {
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

			if got := counter.Load(); got != buffered+1 {
				t.Errorf("expected counter = %d, got %d", buffered+1, got)
			}
		})
	})

	t.Run("task observes context cancellation", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			workers := 1
			pool := New[int](t.Context(), workers)

			var counter atomic.Int32

			observed := make(chan struct{})

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go pool.Do(ctx, testTask{
				fn: func(ctx context.Context) {
					<-ctx.Done()
					close(observed)
				},
				value:   1,
				counter: &counter,
			})

			synctest.Wait()

			cancel()
			synctest.Wait()

			select {
			case <-observed:
			default:
				t.Error("task should have observed cancelled ctx")
			}

			if err := pool.GracefulShutdown(); err != nil {
				t.Errorf("unexpected shutdown error: %v", err)
			}
		})
	})

	t.Run("task returns result even if pool context is cancelled after submission", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			// create a pool with a cancellable context
			poolCtx, poolCancel := context.WithCancel(t.Context())
			pool := New[int](poolCtx, 1)

			var counter atomic.Int32

			// Start a task with its own context (never cancels)
			taskDone := make(chan struct{})

			go func() {
				_, err := pool.Do(context.Background(), testTask{
					fn: func(ctx context.Context) {
						counter.Add(1)
						close(taskDone)
					},
					value: 42,
				})
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}()

			synctest.Wait()

			// cancel the pool context (immediate shutdown)
			poolCancel()

			// Wait for the task to complete
			select {
			case <-taskDone:
				// Task completed successfully
			case <-time.After(time.Second):
				t.Fatal("task did not complete in time")
			}

			// verify the task ran
			if got := counter.Load(); got != 1 {
				t.Errorf("expected counter = 1, got %d", got)
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
			pool := New[int](ctx, 2)

			var counter atomic.Int32
			blocker := make(chan struct{})

			go pool.Do(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value: 1, counter: &counter,
			})

			synctest.Wait()

			cancel()
			synctest.Wait()

			close(blocker)
		})
	})

	t.Run("GracefulShutdown returns error after immediate stop", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			pool := New[int](ctx, 2)

			cancel()
			synctest.Wait()

			err := pool.GracefulShutdown()
			if err == nil {
				t.Error("expected error when calling GracefulShutdown after immediate stop")
			}
		})
	})

	t.Run("Do returns ErrPoolClosed after immediate stop", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		pool := New[int](ctx, 2)

		cancel()

		_, err := pool.Do(context.TODO(), testTask{value: 1})
		if err == nil {
			t.Error("expected error after immediate stop")
		}
		if !errors.Is(err, ErrPoolClosed) {
			t.Errorf("expected ErrPoolClosed, got %v", err)
		}
	})
}

// TestPool_ImmediateShutdown_QueuedVsInflight verifies that on immediate
// (context) shutdown, callers of queued-but-never-started tasks receive
// ErrPoolClosed instead of hanging, while callers of in-flight tasks still
// receive their results.
func TestPool_ImmediateShutdown_QueuedVsInflight(t *testing.T) {
	t.Parallel()

	t.Run("bounded", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			pool := New[int](ctx, 1, WithBuffer(2))

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

			// task2 and task3 fill the dispatch buffer (size 2) and stay queued
			task2Err := make(chan error, 1)
			go func() {
				_, err := pool.Do(context.Background(), testTask{value: 2})
				task2Err <- err
			}()
			task3Err := make(chan error, 1)
			go func() {
				_, err := pool.Do(context.Background(), testTask{value: 3})
				task3Err <- err
			}()
			synctest.Wait()

			// immediate shutdown: cancel the pool context
			cancel()
			synctest.Wait()

			// Queued tasks never started — callers must get ErrPoolClosed, not hang
			if err := <-task2Err; !errors.Is(err, ErrPoolClosed) {
				t.Errorf("queued task2: expected ErrPoolClosed, got %v", err)
			}
			if err := <-task3Err; !errors.Is(err, ErrPoolClosed) {
				t.Errorf("queued task3: expected ErrPoolClosed, got %v", err)
			}

			// unblock the worker so the in-flight task can complete
			close(blocker)
			synctest.Wait()

			// the in-flight task ran to completion and its caller got the result
			if err := <-task1Err; err != nil {
				t.Errorf("in-flight task1: expected nil error, got %v", err)
			}
		})
	})

	t.Run("unbounded", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			pool := New[int](ctx, 1, WithUnboundedQueue(), WithIngressBuffer(10))

			blocker := make(chan struct{})

			task1Err := make(chan error, 1)
			go func() {
				_, err := pool.Do(context.Background(), testTask{
					fn:    func(_ context.Context) { <-blocker },
					value: 1,
				})
				task1Err <- err
			}()
			synctest.Wait()

			// queue several tasks behind the blocked worker
			errs := make(chan error, 3)
			for range 3 {
				go func() {
					_, err := pool.Do(context.Background(), testTask{value: 1})
					errs <- err
				}()
			}
			synctest.Wait()

			cancel()
			synctest.Wait()

			for range 3 {
				if err := <-errs; !errors.Is(err, ErrPoolClosed) {
					t.Errorf("queued task: expected ErrPoolClosed, got %v", err)
				}
			}

			close(blocker)
			synctest.Wait()

			if err := <-task1Err; err != nil {
				t.Errorf("in-flight task1: expected nil error, got %v", err)
			}
		})
	})
}

// TestPool_ShutdownRace_NoTaskLost stresses concurrent Do submissions racing a
// GracefulShutdown. It asserts the caller-observable invariant that the
// submitMu barrier guarantees: every task that Do reports as successful ran
// exactly once, and no task runs without being reported. The deterministic
// synctest drain tests cover the "graceful shutdown drains everything queued"
// contract; this test guards against double-execution and deadlock under
// genuinely concurrent scheduling.
func TestPool_ShutdownRace_NoTaskLost(t *testing.T) {
	t.Parallel()

	t.Run("bounded", func(t *testing.T) {
		t.Parallel()
		testShutdownRace(t, New[int](context.TODO(), 4, WithBuffer(8)))
	})

	t.Run("unbounded", func(t *testing.T) {
		t.Parallel()
		testShutdownRace(t, New[int](context.TODO(), 4, WithUnboundedQueue()))
	})
}

func testShutdownRace(t *testing.T, pool Pool[int]) {
	t.Helper()

	const tasks = 500

	var (
		ran     atomic.Int32 // incremented inside testTask.Do
		success atomic.Int32 // Do returned nil
		reject  atomic.Int32 // Do returned an error
		wg      sync.WaitGroup
	)

	wg.Add(tasks)
	for range tasks {
		go func() {
			defer wg.Done()
			task := testTask{value: 1, counter: &ran}
			if _, err := pool.Do(context.TODO(), task); err != nil {
				reject.Add(1)
			} else {
				success.Add(1)
			}
		}()
	}

	shutdownDone := make(chan struct{})
	go func() {
		_ = pool.GracefulShutdown()
		close(shutdownDone)
	}()

	wg.Wait()
	<-shutdownDone

	if got, want := ran.Load(), success.Load(); got != want {
		t.Errorf("ran=%d but success=%d: a task ran without success, or reported success without running", got, want)
	}

	if got, want := success.Load()+reject.Load(), int32(tasks); got != want {
		t.Errorf("success+reject=%d but submitted=%d: unaccounted tasks", got, want)
	}
}

func TestPool_PanicHandling(t *testing.T) {
	t.Parallel()

	t.Run("recovers from panicking task", func(t *testing.T) {
		t.Parallel()

		pool := New[int](context.TODO(), 2)

		var counter atomic.Int32

		// create a panicking task
		panickingTask := testTask{
			value:   42,
			counter: &counter,
			fn: func(context.Context) {
				panic("intentional panic")
			},
		}

		// submit the panicking task
		_, err := pool.Do(context.TODO(), panickingTask)
		if err == nil {
			t.Fatal("expected error from panicking task")
		}
		if !errors.Is(err, ErrTaskPanic) {
			t.Errorf("expected ErrTaskPanic, got %v", err)
		}

		// verify the pool is still functional
		normalTask := testTask{
			value:   1,
			counter: &counter,
		}

		result, err := pool.Do(context.TODO(), normalTask)
		if err != nil {
			t.Fatalf("pool should still work after panic: %v", err)
		}
		if result != 1 {
			t.Errorf("expected result 1, got %d", result)
		}

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("unexpected shutdown error: %v", err)
		}
	})

	t.Run("worker continues after panic", func(t *testing.T) {
		t.Parallel()

		pool := New[int](context.TODO(), 1)

		var counter atomic.Int32

		// submit panicking task
		_, err := pool.Do(context.TODO(), testTask{
			value:   1,
			counter: &counter,
			fn: func(context.Context) {
				panic("test panic")
			},
		})
		if err == nil {
			t.Fatal("expected error from panicking task")
		}
		if !errors.Is(err, ErrTaskPanic) {
			t.Errorf("expected ErrTaskPanic, got %v", err)
		}

		// submit another task - should still work with the same worker
		// (the worker recovered from the panic)
		normalTask := testTask{
			value:   1,
			counter: &counter,
		}

		result, err := pool.Do(context.TODO(), normalTask)
		if err != nil {
			t.Fatalf("worker should have recovered: %v", err)
		}
		if result != 1 {
			t.Errorf("expected result 1, got %d", result)
		}

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("unexpected shutdown error: %v", err)
		}
	})
}

func TestPool_TimeoutHandling(t *testing.T) {
	t.Parallel()

	t.Run("returns ErrTaskTimeout on deadline exceeded", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		pool := New[int](context.Background(), 1) // use background context for pool

		// create a task that runs longer than the deadline
		blocker := make(chan struct{})
		task := testTask{
			value: 42,
			fn: func(context.Context) {
				<-blocker // Block indefinitely
			},
		}

		_, err := pool.Do(ctx, task)
		if err == nil {
			t.Fatal("expected error from timed out task")
		}
		if !errors.Is(err, ErrTaskTimeout) {
			t.Errorf("expected ErrTaskTimeout, got %v", err)
		}

		// unblock the task so the pool can shut down cleanly
		close(blocker)

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("unexpected shutdown error: %v", err)
		}
	})

	t.Run("returns ErrTaskCancelled on explicit cancellation", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		pool := New[int](context.Background(), 1)

		blocker := make(chan struct{})
		task := testTask{
			value: 42,
			fn: func(context.Context) {
				<-blocker
			},
		}

		// start task in bg
		var wg sync.WaitGroup
		wg.Add(1)
		var taskErr error
		go func() {
			defer wg.Done()
			_, taskErr = pool.Do(ctx, task)
		}()

		// cancel the task context
		cancel()

		wg.Wait()

		if taskErr == nil {
			t.Fatal("expected error from cancelled task")
		}
		if !errors.Is(taskErr, ErrTaskCancelled) {
			t.Errorf("expected ErrTaskCancelled, got %v", taskErr)
		}

		close(blocker)

		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("unexpected shutdown error: %v", err)
		}
	})

	t.Run("helper functions", func(t *testing.T) {
		t.Parallel()

		if !IsTaskTimeout(ErrTaskTimeout) {
			t.Error("IsTaskTimeout should return true for ErrTaskTimeout")
		}
		if IsTaskTimeout(ErrTaskCancelled) {
			t.Error("IsTaskTimeout should return false for ErrTaskCancelled")
		}
		if !IsTaskCancelled(ErrTaskCancelled) {
			t.Error("IsTaskCancelled should return true for ErrTaskCancelled")
		}
		if IsTaskCancelled(ErrTaskTimeout) {
			t.Error("IsTaskCancelled should return false for ErrTaskTimeout")
		}
	})
}

func TestPool_GracefulShutdownAfterForcefulStop(t *testing.T) {
	t.Parallel()

	t.Run("waits for in-flight tasks after context cancellation", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			pool := New[int](ctx, 2)

			var counter atomic.Int32
			blocker := make(chan struct{})

			go pool.Do(context.TODO(), testTask{
				fn: func(_ context.Context) {
					<-blocker
				},
				value:   1,
				counter: &counter,
			})
			synctest.Wait()

			cancel()
			synctest.Wait()

			shutdownDone := make(chan error, 1)
			go func() {
				shutdownDone <- pool.GracefulShutdown()
			}()
			synctest.Wait()

			close(blocker)
			synctest.Wait()

			err := <-shutdownDone
			if err == nil {
				t.Error("expected error from GracefulShutdown after forceful stop")
			}
			if !strings.Contains(err.Error(), "forcefully terminated") {
				t.Errorf("expected forceful termination error, got: %v", err)
			}

			if got := counter.Load(); got != 1 {
				t.Errorf("expected counter = 1, got %d", got)
			}
		})
	})
}

func TestErrorHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		checker  func(error) bool
		expected bool
	}{
		{"IsPoolClosed/true", ErrPoolClosed, IsPoolClosed, true},
		{"IsPoolClosed/false", ErrQueueFull, IsPoolClosed, false},
		{"IsQueueFull/true", ErrQueueFull, IsQueueFull, true},
		{"IsQueueFull/false", ErrPoolClosed, IsQueueFull, false},
		{"IsTaskPanic/true", ErrTaskPanic, IsTaskPanic, true},
		{"IsTaskPanic/false", ErrQueueFull, IsTaskPanic, false},
		{"IsTaskTimeout/true", ErrTaskTimeout, IsTaskTimeout, true},
		{"IsTaskTimeout/false", ErrQueueFull, IsTaskTimeout, false},
		{"IsTaskCancelled/true", ErrTaskCancelled, IsTaskCancelled, true},
		{"IsTaskCancelled/false", ErrQueueFull, IsTaskCancelled, false},
		{"nil error", nil, IsPoolClosed, false},
		{"wrapped ErrPoolClosed", fmt.Errorf("wrapped: %w", ErrPoolClosed), IsPoolClosed, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.checker(tt.err); got != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestPool_IsClosed(t *testing.T) {
	t.Parallel()

	t.Run("returns false for new pool", func(t *testing.T) {
		t.Parallel()
		pool := New[int](context.TODO(), 1)
		defer pool.GracefulShutdown()

		if got := pool.IsClosed(); got {
			t.Error("expected pool to be open")
		}
	})

	t.Run("returns true after GracefulShutdown", func(t *testing.T) {
		t.Parallel()
		pool := New[int](context.TODO(), 1)
		pool.GracefulShutdown()

		if got := pool.IsClosed(); !got {
			t.Error("expected pool to be closed")
		}
	})

	t.Run("returns true after context cancellation", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			pool := New[int](ctx, 1)

			cancel()
			synctest.Wait()

			if got := pool.IsClosed(); !got {
				t.Error("expected pool to be closed after context cancellation")
			}
		})
	})
}
