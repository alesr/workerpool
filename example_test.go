package workerpool_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alesr/workerpool"
)

// job is a Task that simulates processing item id and returns a score.
// It respects its context and stops waiting if the context is done.
type job struct {
	id      int
	latency time.Duration
}

func (j job) Do(ctx context.Context) (int, error) {
	select {
	case <-time.After(j.latency):
		return j.id * 10, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// blockingJob occupies a worker until release is closed. It signals via
// started when a worker has picked it up.
type blockingJob struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (b blockingJob) Do(ctx context.Context) (int, error) {
	close(b.started)
	<-b.release
	return 0, nil
}

func Example() {
	pool := workerpool.New[int](context.Background(), 4)

	// Do blocks until the task finishes, so fan out from goroutines
	// to keep all workers busy.
	var wg sync.WaitGroup
	results := make([]int, 8)
	for i := range results {
		wg.Go(func() {
			n, err := pool.Do(context.Background(), job{id: i, latency: time.Millisecond})
			if err != nil {
				return
			}
			results[i] = n
		})
	}
	wg.Wait()

	if err := pool.GracefulShutdown(); err != nil {
		fmt.Println("shutdown error:", err)
	}

	fmt.Println(results)

	// After shutdown, new submissions are rejected and never run.
	_, err := pool.Do(context.Background(), job{id: 99})
	fmt.Println("closed:", workerpool.IsPoolClosed(err))

	// Output:
	// [0 10 20 30 40 50 60 70]
	// closed: true
}

func ExamplePool_TryDo() {
	// One worker and no queue: a task can only be accepted while a
	// worker is free.
	pool := workerpool.New[int](context.Background(), 1, workerpool.WithBuffer(0))

	started := make(chan struct{})
	release := make(chan struct{})

	var wg sync.WaitGroup
	wg.Go(func() {
		pool.Do(context.Background(), blockingJob{started: started, release: release})
	})

	<-started // the worker is now busy

	// TryDo does not block: with all workers busy and no queue space,
	// it reports ErrQueueFull instead of waiting.
	_, err := pool.TryDo(context.Background(), job{id: 1})
	fmt.Println("queue full:", workerpool.IsQueueFull(err))

	close(release)
	wg.Wait()
	if err := pool.GracefulShutdown(); err != nil {
		fmt.Println("shutdown error:", err)
	}

	// Output:
	// queue full: true
}

func ExampleNew_unbounded() {
	// Unbounded queue: a burst of submissions is buffered in memory, so
	// Do never fails on capacity. Watch memory if producers outpace
	// workers for long enough.
	pool := workerpool.New[int](context.Background(), 2, workerpool.WithUnboundedQueue())

	var total atomic.Int32
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			n, err := pool.Do(context.Background(), job{id: i})
			if err == nil {
				total.Add(int32(n))
			}
		})
	}
	wg.Wait()

	if err := pool.GracefulShutdown(); err != nil {
		fmt.Println("shutdown error:", err)
	}

	fmt.Println("sum:", total.Load())

	// Output:
	// sum: 49500
}

func ExamplePool_Do_taskTimeout() {
	pool := workerpool.New[int](context.Background(), 1)

	// A task that outlives its deadline and keeps running in the
	// background — the pool does not kill it.
	release := make(chan struct{})
	task := blockingJob{started: make(chan struct{}), release: release}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := pool.Do(ctx, task)
	fmt.Println("timeout:", workerpool.IsTaskTimeout(err))

	close(release) // let the worker finish so shutdown does not hang
	if err := pool.GracefulShutdown(); err != nil {
		fmt.Println("shutdown error:", err)
	}

	// Output:
	// timeout: true
}
