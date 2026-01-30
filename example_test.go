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
func (b bazooka) Do(ctx context.Context) {
	b.ammo--
	fmt.Fprintln(os.Stderr, "bazooking: "+b.targetID)
	b.bodyCount.Add(1)
}

func Example() {
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

	fmt.Printf("Body count: %d\n", bodyCount.Load())
	// Output:
	// Body count: 3
}
