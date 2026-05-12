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

// Do simulate some bazooking
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
		pool.Submit(context.TODO(), &bazookas[i])
	}
	
	_ = pool.GracefulShutdown()
	
	fmt.Printf("Body count: %d\n", bodyCount.Load())	
}
```
