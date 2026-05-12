package workerpool_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"

	"github.com/alesr/workerpool"
)

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

func Example() {
	// starts a pool with 3 workers
	// use the context to cancel the pool without waiting for buffered tasks to complete
	pool := workerpool.New[*bazooka](context.TODO(), 3)

	var bodyCount atomic.Int32

	bazookas := []bazooka{
		{ammo: 69, targetID: "foo-id", bodyCount: &bodyCount},
		{ammo: 42, targetID: "bar-id", bodyCount: &bodyCount},
		{ammo: 11, targetID: "qux-id", bodyCount: &bodyCount},
	}

	for _, bazz := range bazookas {
		pool.Submit(&bazz)
	}

	if err := pool.GracefulShutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
	}

	fmt.Printf("Body count: %d\n", bodyCount.Load())
	// Output:
	// Body count: 3
}
