# workerpool

[![codecov](https://codecov.io/gh/alesr/workerpool/graph/badge.svg?token=4dxDuntYgf)](https://codecov.io/gh/alesr/workerpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/alesr/workerpool)](https://goreportcard.com/report/github.com/alesr/workerpool)
[![Go Reference](https://pkg.go.dev/badge/github.com/alesr/workerpool.git.svg)](https://pkg.go.dev/github.com/alesr/workerpool)

A generic, type-safe worker pool for Go. No dependencies. Requires Go 1.26+.

`New` starts a fixed number of worker goroutines. A task is anything implementing
`Task[R]` — a single `Do(context.Context) (R, error)` method. `Pool.Do` and
`Pool.TryDo` submit a task and return its result in one call, so results come
back to the caller directly instead of through a separate callback or channel.

## Install

```bash
go get github.com/alesr/workerpool
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/alesr/workerpool"
)

// resize is a Task that processes one image.
type resize struct{ id int }

func (r resize) Do(ctx context.Context) (int, error) {
	// do the work, watching ctx for cancellation
	return r.id * 2, nil
}

func main() {
	pool := workerpool.New[int](context.Background(), 4)

	// Do blocks until the task finishes, so call it from several
	// goroutines to keep all workers busy.
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := pool.Do(context.Background(), resize{id: i})
			if err != nil {
				fmt.Println("task", i, "failed:", err)
				return
			}
			fmt.Println("resized", i, "->", n)
		}()
	}
	wg.Wait()

	if err := pool.GracefulShutdown(); err != nil {
		fmt.Println("shutdown:", err)
	}
}
```

More runnable examples live in [example_test.go](example_test.go) and on
[pkg.go.dev](https://pkg.go.dev/github.com/alesr/workerpool#pkg-examples).

## Queue model and send semantics

Two independent choices:

| | Bounded (default) | Unbounded (`WithUnboundedQueue()`) |
|---|---|---|
| `Do` | blocks while the queue is full (backpressure) | blocks only while the ingress buffer is full |
| `TryDo` | returns `ErrQueueFull` when full | returns `ErrQueueFull` if the ingress buffer is full |

`Do` and `TryDo` both wait for the task and return its result — the only
difference is whether the send blocks on capacity.

**Bounded** — tasks sit in a buffered channel, capacity equal to the worker
count by default. `WithBuffer(n)` sets a custom capacity. `WithBuffer(0)` means
no queue at all (direct handoff): `Do` blocks until a worker is free, and
`TryDo` fails immediately when every worker is busy. Use this when you want
backpressure and a hard cap on queued work.

**Unbounded** — submissions land in an ingress buffer (default: workers x 50,
tune with `WithIngressBuffer(n)`), and a broker goroutine drains it into an
in-memory queue that feeds the workers. `Do` therefore almost never blocks on
capacity, at the cost of memory that grows without bound if producers outpace
workers for long enough. Use this when blocking the caller is worse than
growing memory.

## Contexts

There are two contexts, independent of each other:

- **Task context** — the `ctx` passed to `Do`/`TryDo`. It is handed to the task's
  `Do` and controls that one execution. If it is cancelled or hits its deadline
  while the call is in flight, the call returns `ErrTaskCancelled` or
  `ErrTaskTimeout`. The pool does not kill the running task — it keeps running
  in the background — so tasks should respect their context.
- **Pool context** — the `ctx` passed to `New`. It controls the pool's lifetime:
  workers and the broker goroutine. Cancelling it rejects new submissions with
  `ErrPoolClosed`, and workers exit after finishing their current task.

## Shutdown

- `GracefulShutdown()` rejects new tasks, drains everything still queued, and
  waits for in-flight tasks to finish. Call it once you are done submitting.
- Cancelling the pool context is the immediate variant: queued tasks never run
  (their callers get `ErrPoolClosed`), while in-flight tasks still run to
  completion and deliver their results to the waiting caller.
- `GracefulShutdown()` called after the pool context was cancelled still waits
  for in-flight tasks, but returns an error noting the pool was force-terminated.

The invariant that matters: **if `Do`/`TryDo` returns a nil error, the task
executed exactly once. If it returns `ErrPoolClosed` or `ErrQueueFull`, the task
never ran.**

## Errors

Sentinels, checkable with `errors.Is` or the helpers underneath:

| Sentinel | Meaning |
|---|---|
| `ErrPoolClosed` | the pool shut down before the task ran |
| `ErrQueueFull` | `TryDo` on a full queue or full ingress buffer |
| `ErrTaskCancelled` | task context cancelled while the call was in flight |
| `ErrTaskTimeout` | task context deadline exceeded while in flight |
| `ErrTaskPanic` | the task panicked — recovered, wrapped with the panic value, and the worker keeps processing other tasks |
| anything else | whatever the task's `Do` returned |

Helpers: `IsPoolClosed`, `IsQueueFull`, `IsTaskCancelled`, `IsTaskTimeout`,
`IsTaskPanic` — thin `errors.Is` wrappers, safe on wrapped errors.

## Options and misc

| Option | Effect |
|---|---|
| `WithBuffer(n)` | bounded queue capacity (`0` = direct handoff); default = worker count |
| `WithUnboundedQueue()` | in-memory unbounded queue plus a broker goroutine |
| `WithIngressBuffer(n)` | ingress capacity for the unbounded pool; default = workers x 50 |

- `New` panics if `workers <= 0` — that is a programming error, so it fails fast
  at startup instead of silently doing nothing.
- `IsClosed()` reports whether the pool has been shut down, gracefully or via
  context cancellation.

## Adapters

[`adapters/redisstreams`](adapters/redisstreams) is a separate Go module that
reads a Redis Stream consumer group and hands each message to a `Handler`.
Wire it to a pool by wrapping the message in a `Task` and calling `Do`, acking
on success. `Consume` invokes the handler synchronously, so a handler that
waits on `Do` applies backpressure to consumption:

```go
type streamTask struct{ msg redisstreams.Message }

func (t streamTask) Do(ctx context.Context) (any, error) {
	// process t.msg.Values ...
	return nil, t.msg.Ack() // ack only after the work succeeded
}

adapter.Consume(ctx, redisstreams.HandlerFunc(func(ctx context.Context, msg redisstreams.Message) error {
	_, err := pool.Do(ctx, streamTask{msg: msg})
	return err
}))
```

Two caveats: pending entries of a crashed consumer are not reclaimed
automatically (use `XAUTOCLAIM`/`XPENDING` externally), and only string field
values are surfaced in `Message.Values`.

```bash
go get github.com/alesr/workerpool/adapters/redisstreams
```

## Development

```sh
make fmt   # go fmt ./...
make vet   # go vet ./...
make test  # go test -short -race -count=1 -v ./...
make lint  # vet + staticcheck
```
