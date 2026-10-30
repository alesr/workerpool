# workerpool

[![codecov](https://codecov.io/gh/alesr/workerpool/graph/badge.svg?token=4dxDuntYgf)](https://codecov.io/gh/alesr/workerpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/alesr/workerpool)](https://goreportcard.com/report/github.com/alesr/workerpool)
[![Go Reference](https://pkg.go.dev/badge/github.com/alesr/workerpool.git.svg)](https://pkg.go.dev/github.com/alesr/workerpool)

Generic, type-safe handy worker pool in Go.

It executes tasks using a fixed number of worker goroutines passed to the pool constructor.

## Queue and send semantics

The pool combines two independent axes:

| | Bounded queue | Unbounded queue |
|---|---|---|
| **`Do`** (blocking send) | backpressure — blocks when the queue is full | never blocks on capacity |
| **`TryDo`** (non-blocking send) | returns `ErrQueueFull` when full | may return `ErrQueueFull` if ingress buffer is full |

- **Bounded** (default): tasks flow through a buffered channel (default size = workers).
  Use `WithBuffer(N)` to set a custom queue size, or `WithBuffer(0)` for unbuffered
  direct handoff. Use this when you want backpressure and bounded memory usage.
- **Unbounded** (opt-in via `WithUnboundedQueue`): tasks flow through an ingress buffer
  (default size = workers × 50) into an unbounded internal queue. Use `WithIngressBuffer(N)`
  to configure the submission buffer. Note: `TryDo` may still return `ErrQueueFull` if the
  ingress buffer is full. Use this when you must minimize blocking the caller on submission,
  at the cost of potentially higher memory usage during backpressure.

`Do` and `TryDo` both wait for the task to finish and return its result — the
difference is only whether the *send* blocks on queue capacity.

## Shutdown

The pool supports both immediate cancellation via context and graceful shutdown.

- **Immediate cancellation** (cancel the constructor context) causes workers to exit after
  completing their current task. Queued tasks that have not yet started execution are
  dropped — their callers receive `ErrPoolClosed` instead of hanging. Tasks already running
  will complete and return their results to callers of `Do`/`TryDo`.
- **Graceful shutdown** (`GracefulShutdown`) rejects new tasks, drains all queued
  tasks, and waits for in-flight tasks to complete. If called after immediate cancellation,
  it still waits for in-flight tasks to finish.

**Important**: In both cases, in-flight tasks are allowed to complete and return their
results to waiting callers. The pool guarantees that any task whose `Do`/`TryDo` call
returns successfully (without error) has executed exactly once.

## Context Handling

The pool supports two independent contexts:

### Task Context
- Passed directly to your task's `Do` method via `pool.Do(ctx, task)`
- Controls the task's execution lifetime
- If cancelled or deadline exceeded, the task observes it and can return early
- Results in `ErrTaskCancelled` (explicit cancel) or `ErrTaskTimeout` (deadline exceeded)

### Pool Context
- Passed to the pool constructor: `workerpool.New(ctx, workers)`
- Controls the pool's lifetime (workers and broker goroutines)
- If cancelled, the pool stops accepting new tasks and workers exit after completing their
  current task. New submissions return `ErrPoolClosed`.

### Interaction
| Scenario | Task Context | Pool Context | Result |
|----------|--------------|--------------|--------|
| Task cancelled | ✅ Cancelled | Active | `ErrTaskCancelled` |
| Task timeout | ⏰ Deadline | Active | `ErrTaskTimeout` |
| Pool shutdown | Active | ✅ Cancelled | `ErrPoolClosed` (if queued) |
| Task + Pool cancelled | ✅ Cancelled | ✅ Cancelled | `ErrTaskCancelled` or `ErrTaskTimeout` (priority) |
| Pool context cancelled | Active | ✅ Cancelled | Workers exit after current task |

**Important**: The task's context is independent of the pool's context. A task will run to
completion (or until its own context is done) even if the pool context is cancelled, as long
as it was already submitted to a worker.

**Note on context priority**: When both task and pool contexts are cancelled, the task
context takes priority. This ensures deterministic error returns.

## Errors

The pool provides error sentinels and helper functions for error checking:

**Error Sentinels:**
- `ErrPoolClosed` — the pool shut down before the task ran.
- `ErrTaskCancelled` — the task's own context was explicitly cancelled.
- `ErrTaskTimeout` — the task's context deadline was exceeded.
- `ErrQueueFull` — `TryDo` on a full bounded queue or full ingress buffer.
- `ErrTaskPanic` — the task panicked during execution. The panic is recovered and returned as an error, and the worker continues processing other tasks.
- Any other error — the value returned by the task's `Do` method.

**Helper Functions:**
```go
workerpool.IsPoolClosed(err)    // Check for ErrPoolClosed
workerpool.IsQueueFull(err)     // Check for ErrQueueFull
workerpool.IsTaskPanic(err)     // Check for ErrTaskPanic
workerpool.IsTaskTimeout(err)   // Check for ErrTaskTimeout
workerpool.IsTaskCancelled(err) // Check for ErrTaskCancelled
```

These helpers use `errors.Is` for wrapped error checking.

## Usage Notes

**Worker Count**: The pool requires at least one worker. Calling `New` with zero or
negative worker counts will panic. This is a programming error and should be caught
during development.

**Memory Considerations**: The unbounded pool uses an internal queue that can grow
indiscriminately under sustained load. Monitor your application's memory usage when using
unbounded pools with high task volumes.

## Install

```bash
go get github.com/alesr/workerpool
```

## Usage

### Bounded pool (default)

```go
type bazooka struct {
	ammo      uint8
	targetID  string
	bodyCount *atomic.Int32
}

func (b *bazooka) Do(ctx context.Context) (int, error) {
	b.ammo--
	fmt.Fprintln(os.Stderr, "bazooking: "+b.targetID)
	b.bodyCount.Add(1)
	return int(b.ammo), nil
}

func main() {
	pool := workerpool.New[int](context.TODO(), 3)

	var bodyCount atomic.Int32

	bazookas := []bazooka{
		{ammo: 69, targetID: "foo-id", bodyCount: &bodyCount},
		{ammo: 42, targetID: "bar-id", bodyCount: &bodyCount},
		{ammo: 11, targetID: "qux-id", bodyCount: &bodyCount},
	}

	for i := range bazookas {
		if _, err := pool.Do(context.TODO(), &bazookas[i]); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}
	}

	if err := pool.GracefulShutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
	}

	fmt.Printf("Body count: %d\n", bodyCount.Load())
}
```

### Unbounded pool (opt-in)

Use `WithUnboundedQueue` to create a pool where `Do` never blocks on capacity:

```go
func main() {
	pool := workerpool.New[int](context.TODO(), 3, workerpool.WithUnboundedQueue())

	// ... same usage as the bounded pool ...
}
```

### Unbuffered pool (direct handoff)

Use `WithBuffer(0)` to create a pool with no queue. Tasks are handed off directly
to available workers. `Do` blocks until a worker is free, and `TryDo` returns
`ErrQueueFull` immediately if no worker is available:

```go
pool := workerpool.New[int](context.TODO(), 3, workerpool.WithBuffer(0))

// Do will block until a worker is free
result, err := pool.Do(context.TODO(), &bazooka{})

// TryDo returns ErrQueueFull if all workers are busy
_, err = pool.TryDo(context.TODO(), &bazooka{})
if errors.Is(err, workerpool.ErrQueueFull) {
	// all workers busy, no queue to buffer the task
}
```

### Non-blocking send (bounded)

Use `TryDo` to submit without blocking when the queue is full:

```go
pool := workerpool.New[int](context.TODO(), 3, workerpool.WithBuffer(10))

_, err := pool.TryDo(context.TODO(), &bazooka{})
if errors.Is(err, workerpool.ErrQueueFull) {
	// queue is full: handle overflow here instead of blocking
}
```

### Checking Pool State

Use `IsClosed()` to check if a pool has been shut down:

```go
if pool.IsClosed() {
	// Pool is shutting down or already closed
	// New tasks will be rejected with ErrPoolClosed
}

// Submit task only if pool is open
if !pool.IsClosed() {
	result, err := pool.Do(ctx, task)
	// ...
}
```
