package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

type testTask struct {
	value   int
	counter *atomic.Int32
	fn      func()
}

func (t testTask) Do(ctx context.Context) {
	if t.fn != nil {
		t.fn()
	}
	t.counter.Add(int32(t.value))
}

func TestPool_Submit(t *testing.T) {
	t.Parallel()

	pool := New[testTask](context.TODO(), 3)

	var counter atomic.Int32

	// submit tasks
	for i := 1; i <= 10; i++ {
		task := testTask{
			value:   i,
			counter: &counter,
		}

		if !pool.Submit(task) {
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
}

func TestPool_GracefulShutdown(t *testing.T) {
	t.Parallel()

	t.Run("Submit task after graceful shutdown", func(t *testing.T) {
		t.Parallel()

		pool := New[testTask](context.TODO(), 2)

		// immediately shutdown the pool
		if err := pool.GracefulShutdown(); err != nil {
			t.Errorf("shutdown error: %v", err)
		}

		var counter atomic.Int32

		task := testTask{
			value:   1,
			counter: &counter,
		}

		if pool.Submit(task) {
			t.Error("expected Submit to return false after shutdown")
		}
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

		const buffered, workers = 5, 1

		pool := New(context.TODO(), workers, WithBuffer[testTask](buffered))

		var counter atomic.Int32
		blocker := make(chan struct{})
		started := make(chan struct{})

		// submit a task that signals when it starts and then blocks,
		// keeping the worker occupied while we fill the buffer
		pool.Submit(testTask{
			fn: func() {
				close(started)
				<-blocker
			},
			value:   1,
			counter: &counter,
		})

		<-started // worker is now blocked

		// fill the buffer while the worker is blocked
		for range buffered {
			pool.Submit(testTask{
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

		// buffered+1 (started+buffered tasks)
		if got := counter.Load(); got != buffered+1 {
			t.Errorf("expected counter = %d, got %d", buffered+1, got)
		}
	})

	t.Run("Context cancellation terminates pool without draining buffered tasks", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())

			pool := New[testTask](ctx, 1)

			var counter atomic.Int32
			blocker := make(chan struct{})
			started := make(chan struct{})

			pool.Submit(testTask{
				fn: func() {
					close(started)
					<-blocker
				},
				value:   1,
				counter: &counter,
			})

			<-started      // worker is blocked
			cancel()       // cancel the caller's context
			close(blocker) // unblock the worker

			synctest.Wait()

			if err := pool.GracefulShutdown(); err == nil {
				t.Error("expected GracefulShutdown to return an error after context cancellation")
			}
		})
	})
}

func TestPool_Concurrency(t *testing.T) {
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
			pool.Submit(task)
		}(i)
	}

	wg.Wait()

	if err := pool.GracefulShutdown(); err != nil {
		t.Errorf("unexpected shutdown error: %v", err)
	}

	if got := counter.Load(); got != tasks {
		t.Errorf("expected counter = %d, got %d", tasks, got)
	}
}

func TestPool_ZeroSize(t *testing.T) {
	pool := New[testTask](context.TODO(), 0) // should default to 1 worker

	var counter atomic.Int32

	task := testTask{
		value:   42,
		counter: &counter,
	}

	if !pool.Submit(task) {
		t.Error("failed to submit task")
	}

	if err := pool.GracefulShutdown(); err != nil {
		t.Errorf("unexpected shutdown error: %v", err)
	}

	if got := counter.Load(); got != 42 {
		t.Errorf("expected counter = 42, got %d", got)
	}
}

func TestPool_Backpressure(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := New[testTask](context.TODO(), 1)

		var counter atomic.Int32
		blocker := make(chan struct{})

		// submit blocking task
		task1 := testTask{
			fn: func() {
				<-blocker // block here
			},
			value:   1,
			counter: &counter,
		}

		go pool.Submit(task1)

		synctest.Wait() // let worker pick up task1

		// try to submit second task should block since channel is unbuffered
		task2Submitted := make(chan struct{})

		go func() {
			pool.Submit(testTask{
				value:   2,
				counter: &counter,
			})
			close(task2Submitted)
		}()

		// worker still blocked on <-blocker
		// task2 goroutine durably blocked on channel send
		synctest.Wait()
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

		if !pool.Submit(task) {
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

func TestPool_UnbufferedDefault(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		pool := New[testTask](t.Context(), 1)

		counter := &atomic.Int32{}
		blocker := make(chan struct{})
		started := make(chan struct{})

		go func() {
			pool.Submit(testTask{
				fn: func() {
					close(started)
					<-blocker
				},
				value:   1,
				counter: counter,
			})
		}()

		<-started // worker has picked up task1 and is blocked

		// second Submit must block since channel is unbuffered and worker is busy
		task2Submitted := make(chan struct{})
		go func() {
			pool.Submit(testTask{
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
}
