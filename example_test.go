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

func (b *bazooka) Do(ctx context.Context) (int, error) {
	b.ammo--
	fmt.Fprintln(os.Stderr, "bazooking: "+b.targetID)
	b.bodyCount.Add(1)
	return int(b.ammo), nil
}

func Example_bounded() {
	pool := workerpool.New[int](context.TODO(), 3)

	var bodyCount atomic.Int32

	bazookas := []bazooka{
		{ammo: 69, targetID: "foo-id", bodyCount: &bodyCount},
		{ammo: 42, targetID: "bar-id", bodyCount: &bodyCount},
		{ammo: 11, targetID: "qux-id", bodyCount: &bodyCount},
	}

	for i := range bazookas {
		pool.Do(context.TODO(), &bazookas[i])
	}

	if err := pool.GracefulShutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
	}

	fmt.Printf("Body count: %d\n", bodyCount.Load())
	// Output:
	// Body count: 3
}

func Example_unbounded() {
	pool := workerpool.New[int](context.TODO(), 3, workerpool.WithUnboundedQueue())

	var bodyCount atomic.Int32

	bazookas := []bazooka{
		{ammo: 69, targetID: "foo-id", bodyCount: &bodyCount},
		{ammo: 42, targetID: "bar-id", bodyCount: &bodyCount},
		{ammo: 11, targetID: "qux-id", bodyCount: &bodyCount},
	}

	for i := range bazookas {
		pool.Do(context.TODO(), &bazookas[i])
	}

	if err := pool.GracefulShutdown(); err != nil {
		fmt.Fprintf(os.Stderr, "shutdown error: %v\n", err)
	}

	fmt.Printf("Body count: %d\n", bodyCount.Load())
	// Output:
	// Body count: 3
}
