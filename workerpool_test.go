package workerpool

import (
	"context"
	"errors"
	"sync"
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

func TestPool_WithBuffer(t *testing.T) {
	t.Parallel()

	pool := New(context.TODO(), 3, WithBuffer[testTask](10))

	var counter atomic.Int32

	// submit 20 tasks
	for i := 1; i <= 20; i++ {
		task := testTask{
			value:   i,
			counter: &counter,
		}

		if err := pool.Submit(context.TODO(), task); err != nil {
			t.Error("failed to submit task")
		}
	}

	if err := pool.GracefulShutdown(); err != nil {
		t.Errorf("unexpected shutdown error: %v", err)
	}

	// sum of 1..20 = 210
	if got := counter.Load(); got != 210 {
		t.Errorf("expected counter = 210, got %d", got)
	}
}

func TestPool_Submit(t *testing.T) {
	t.Parallel()

	t.Run("queues and executes tasks", func(t *testing.T) {
		t.Parallel()

		pool := New[testTask](context.TODO(), 3)

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

		// sum of 1..10 = 55
		if got := counter.Load(); got != 55 {
			t.Errorf("expected counter = 55, got %d", got)
		}
	})

	t.Run("second submit blocks when the worker is busy”", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[testTask](t.Context(), 1)

			counter := &atomic.Int32{}
			blocker := make(chan struct{})

			go func() {
				pool.Submit(context.TODO(), testTask{
					fn: func(_ context.Context) {
						<-blocker
					},
					value:   1,
					counter: counter,
				})
			}()

			synctest.Wait()

			// second Submit must block since channel is unbuffered and worker is busy
			task2Submitted := make(chan struct{})
			go func() {
				pool.Submit(context.TODO(), testTask{
					value:   2,
					counter: counter,
				})
				close(task2Submitted)
			}()

			// task2 goroutine is durably blocked on the channel send
			synctest.Wait()

			select {
			case <-task2Submitted:
				t.Error("submit should be blocked on unbuffered channel")
			default:
				// task2Submitted is not yet closed. Submit is still blocked
			}

			close(blocker) // release the worker

			synctest.Wait() // worker processes both tasks and task2 goroutine exits

			select {
			case <-task2Submitted:
				// task2Submitted is closed and Submit completed after worker was unblocked, as expected
			default:
				t.Error("Submit should have completed after worker was unblocked")
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

			pool := New[testTask](t.Context(), 1)

			var counter atomic.Int32
			taskDone := make(chan struct{})

			pool.Submit(taskCtx, testTask{
				fn: func(ctx context.Context) {
					<-ctx.Done() // block
					close(taskDone)
				},
				value:   1,
				counter: &counter,
			})

			synctest.Wait() // worker is now blocked
			cancelTask()

			// wait until the <-ctx.Done() is complete (so it can close taskDone)
			synctest.Wait()

			select {
			case <-taskDone:
				// task observed its ctx cancellation
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

		pool := New[testTask](context.TODO(), workers)

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

	t.Run("blocks caller until capacity is available", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			pool := New[testTask](context.TODO(), 1)

			var counter atomic.Int32
			blocker := make(chan struct{})

			// submit blocking task
			task1 := testTask{
				fn: func(_ context.Context) {
					<-blocker // block here
				},
				value:   1,
				counter: &counter,
			}

			go pool.Submit(context.TODO(), task1)

			synctest.Wait() // let worker pick up task1

			// try to submit second task should block since channel is unbuffered
			task2Submitted := make(chan struct{})

			go func() {
				pool.Submit(context.TODO(), testTask{
					value:   2,
					counter: &counter,
				})
				close(task2Submitted)
			}()

			synctest.Wait() // worker still blocked

			select {
			case <-task2Submitted:
				t.Error("Submit should be blocked while worker is busy")
			default:
				// expected
			}

			close(blocker)  // release the worker
			synctest.Wait() // worker processes both tasks; task2 goroutine exits

			select {
			case <-task2Submitted:
				// expected: Submit completed
			default:
				t.Error("Submit should have completed after worker was unblocked")
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
