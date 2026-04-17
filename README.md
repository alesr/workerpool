# workerpool

[![codecov](https://codecov.io/gh/alesr/workerpool/graph/badge.svg?token=4dxDuntYgf)](https://codecov.io/gh/alesr/workerpool)
[![Go Report Card](https://goreportcard.com/badge/github.com/alesr/workerpool)](https://goreportcard.com/report/github.com/alesr/workerpool)
[![Go Reference](https://pkg.go.dev/badge/github.com/alesr/workerpool.git.svg)](https://pkg.go.dev/github.com/alesr/workerpool.git)

Generic, type-safe handy worker pool in Go.

## Install

```bash
go get github.com/alesr/workerpool
```

How:

```go
type bazooka struct {
	ammo      uint8
	targetID  string
	bodyCount *atomic.Int32
}

// Do simulate some bazooking, and
// implement the generic constraint
func (b bazooka) Do(ctx context.Context) {
	b.ammo--
	fmt.Fprintln(os.Stderr, "bazooking: "+b.targetID)
	b.bodyCount.Add(1)
}

...
pool := workerpool.New[bazooka](3)
defer pool.Shutdown()

var bodyCount atomic.Int32

bazookas := []bazooka{
	{ammo: 69, targetID: "foo-id", bodyCount: &bodyCount},
	{ammo: 42, targetID: "bar-id", bodyCount: &bodyCount},
	{ammo: 11, targetID: "qux-id", bodyCount: &bodyCount},
}

for _, bazz := range bazookas {
	task := workerpool.Task[bazooka]{
		Fn: func(input bazooka) {
			input.Do(context.TODO())
		},
		Input: bazz,
	}
	pool.Submit(task)
}
```
