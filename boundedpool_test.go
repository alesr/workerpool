package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

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

	t.Run("second submit blocks when the worker is busy", func(t *testing.T) {
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
