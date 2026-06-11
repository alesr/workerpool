# workerpool

[![codecov](https://codecov.io/gh/alesr/workerpool/graph/badge.svg?token=4dxDuntYgf)](https://codecov.io/gh/alesr/workerpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/alesr/workerpool)](https://goreportcard.com/report/github.com/alesr/workerpool)
[![Go Reference](https://pkg.go.dev/badge/github.com/alesr/workerpool.git.svg)](https://pkg.go.dev/github.com/alesr/workerpool)

Generic, type-safe handy worker pool in Go.

It executes tasks using a fixed number of worker goroutines passed to the pool constructor.

Two implementations are available:

- **Bounded** (default): tasks flow through a buffered channel. The buffer size is configurable via `WithBuffer`. When the buffer is full, `Submit` blocks until capacity becomes available. Use this when you want backpressure and bounded memory usage.
- **Unbounded** (opt-in via `WithUnboundedQueue`): tasks are buffered in a dynamic slice. `Submit` never blocks on capacity. Use this when you must never block the caller, at the cost of unbounded memory growth during backpressure.

The pool supports both immediate cancellation via context and graceful shutdown.

Immediate cancellation stops workers and drops queued tasks that have not yet started execution, while graceful shutdown drains all queued tasks and waits for completion. In both cases, in-flight tasks are allowed to complete.

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

func (b *bazooka) Do(ctx context.Context) {
	b.ammo--
	fmt.Fprintln(os.Stderr, "bazooking: "+b.targetID)
	b.bodyCount.Add(1)
}

func main() {
	pool := workerpool.New[*bazooka](context.TODO(), 3)

	var bodyCount atomic.Int32

	bazookas := []bazooka{
		{ammo: 69, targetID: "foo-id", bodyCount: &bodyCount},
		{ammo: 42, targetID: "bar-id", bodyCount: &bodyCount},
		{ammo: 11, targetID: "qux-id", bodyCount: &bodyCount},
	}

	for i := range bazookas {
		_ = pool.Submit(context.TODO(), &bazookas[i])
	}

	_ = pool.GracefulShutdown()

	fmt.Printf("Body count: %d\n", bodyCount.Load())
}
```

### Unbounded pool (opt-in)

Use `WithUnboundedQueue` to create a pool where `Submit` never blocks:

```go
func main() {
	pool := workerpool.New(context.TODO(), 3, workerpool.WithUnboundedQueue[*bazooka]())

	// ... same usage as the bounded pool ...
}
```

When the backpressure is undesired, the unbounded variant
gives callers a guarantee that `Submit` will always return immediately.
