package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testInput struct {
	value   int
	counter *atomic.Int32
}

func (t testInput) Do(ctx context.Context) {
	t.counter.Add(int32(t.value))
}

func TestPool_Submit(t *testing.T) {
	t.Parallel()

	pool := New[testInput](3)
	defer pool.Shutdown()

	var counter atomic.Int32

	// submit tasks
	for i := 1; i <= 10; i++ {
		task := Task[testInput]{
			Fn: func(input testInput) {
				input.Do(context.TODO())
			},
			Input: testInput{value: i, counter: &counter},
		}
		if !pool.Submit(task) {
			t.Error("failed to submit task")
		}
	}

	pool.Shutdown()

	// sum of 1..10 = 55
	if got := counter.Load(); got != 55 {
		t.Errorf("expected counter = 55, got %d", got)
	}
}

func TestPool_SubmitAfterShutdown(t *testing.T) {
	t.Parallel()

	pool := New[testInput](2)
	pool.Shutdown()

	var counter atomic.Int32

	task := Task[testInput]{
		Fn: func(input testInput) {
			input.Do(context.TODO())
		},
		Input: testInput{value: 1, counter: &counter},
	}

	if pool.Submit(task) {
		t.Error("expected Submit to return false after shutdown")
	}
}

func TestPool_Concurrency(t *testing.T) {
	const (
		workers = 5
		tasks   = 100
	)

	pool := New[testInput](workers)
	defer pool.Shutdown()

	var (
		counter atomic.Int32
		wg      sync.WaitGroup
	)

	wg.Add(tasks)
	for i := range tasks {
		go func(val int) {
			defer wg.Done()
			task := Task[testInput]{
				Fn: func(input testInput) {
					input.Do(context.TODO())
				},
				Input: testInput{value: 1, counter: &counter},
			}
			pool.Submit(task)
		}(i)
	}

	wg.Wait()
	pool.Shutdown()

	if got := counter.Load(); got != tasks {
		t.Errorf("expected counter = %d, got %d", tasks, got)
	}
}

func TestPool_ZeroSize(t *testing.T) {
	pool := New[testInput](0) // should default to 1 worker
	defer pool.Shutdown()

	var counter atomic.Int32

	task := Task[testInput]{
		Fn: func(input testInput) {
			input.Do(context.TODO())
		},
		Input: testInput{value: 42, counter: &counter},
	}

	if !pool.Submit(task) {
		t.Error("failed to submit task")
	}

	pool.Shutdown()

	if got := counter.Load(); got != 42 {
		t.Errorf("expected counter = 42, got %d", got)
	}
}

func TestPool_Backpressure(t *testing.T) {
	pool := New[testInput](1)
	defer pool.Shutdown()

	var counter atomic.Int32
	blocker := make(chan struct{})

	// submit blocking task
	task1 := Task[testInput]{
		Fn: func(input testInput) {
			<-blocker // block here
			input.Do(context.TODO())
		},
		Input: testInput{value: 1, counter: &counter},
	}

	go pool.Submit(task1)
	time.Sleep(100 * time.Millisecond) // let worker pick up task1

	// try to submit second task should block since channel is unbuffered
	submitted := make(chan bool)
	task2 := Task[testInput]{
		Fn: func(input testInput) {
			input.Do(context.TODO())
		},
		Input: testInput{value: 2, counter: &counter},
	}

	go func() {
		submitted <- pool.Submit(task2)
	}()

	// verify Submit is blocked
	select {
	case <-submitted:
		t.Error("Submit should be blocked")
	case <-time.After(50 * time.Millisecond):
		// expected Submit is blocked
	}

	close(blocker) // unblock worker

	// now Submit should complete
	select {
	case result := <-submitted:
		if !result {
			t.Error("expected Submit to succeed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Submit should have completed")
	}

	pool.Shutdown()

	if got := counter.Load(); got != 3 {
		t.Errorf("expected counter = 3, got %d", got)
	}
}

func TestPool_WithBuffer(t *testing.T) {
	t.Parallel()

	pool := New(3, WithBuffer[testInput](10))
	defer pool.Shutdown()

	var counter atomic.Int32

	// submit 20 tasks
	for i := 1; i <= 20; i++ {
		task := Task[testInput]{
			Fn: func(input testInput) {
				input.Do(context.TODO())
			},
			Input: testInput{value: i, counter: &counter},
		}
		if !pool.Submit(task) {
			t.Error("failed to submit task")
		}
	}

	pool.Shutdown()

	// sum of 1..20 = 210
	if got := counter.Load(); got != 210 {
		t.Errorf("expected counter = 210, got %d", got)
	}
}

func TestPool_UnbufferedDefault(t *testing.T) {
	// without WithBuffer option, channel should be unbuffered
	pool := New[testInput](1)
	defer pool.Shutdown()

	counter := &atomic.Int32{}
	blocker := make(chan struct{})
	started := make(chan struct{})

	// submit blocking task
	go func() {
		task := Task[testInput]{
			Fn: func(input testInput) {
				close(started)
				<-blocker
				input.Do(context.TODO())
			},
			Input: testInput{value: 1, counter: counter},
		}
		pool.Submit(task)
	}()

	<-started

	// try to submit another should block since unbuffered
	submitted := make(chan bool)
	go func() {
		task := Task[testInput]{
			Fn: func(input testInput) {
				input.Do(context.TODO())
			},
			Input: testInput{value: 2, counter: counter},
		}
		pool.Submit(task)
		submitted <- true
	}()

	// verify submit is blocked
	select {
	case <-submitted:
		t.Error("submit should be blocked on unbuffered channel")
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	close(blocker)

	// wait for second submit to complete
	<-submitted

	pool.Shutdown()

	if got := counter.Load(); got != 3 {
		t.Errorf("expected counter = 3, got %d", got)
	}
}
