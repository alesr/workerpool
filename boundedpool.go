package workerpool

import "context"

// boundedPool implements a fixed-size channel-backed pool.
type boundedPool[T any] struct {
	poolCore[T]
	dispatch chan *entry[T]
}

func newBoundedPool[T any](ctx context.Context, workers, buffer int) *boundedPool[T] {
	poolCtx, cancel := context.WithCancel(ctx)

	p := &boundedPool[T]{
		dispatch: make(chan *entry[T], buffer),
	}
	p.ctx = poolCtx
	p.cancel = cancel
	p.stop = make(chan struct{})
	p.closeQueue = func() { close(p.dispatch) }

	p.wg.Add(workers)
	for range workers {
		go p.worker()
	}
	return p
}

func (p *boundedPool[T]) worker() {
	defer p.wg.Done()
	for {
		select {
		case <-p.ctx.Done():
			return
		case e, ok := <-p.dispatch:
			if !ok {
				return
			}
			e.started.Store(true)
			p.run(e)
		}
	}
}

func (p *boundedPool[T]) Do(ctx context.Context, task Task[T]) (T, error) {
	resultCh := make(chan result[T], 1)
	e := &entry[T]{ctx: ctx, task: task, result: resultCh}

	if err := p.submit(ctx, p.dispatch, e, false); err != nil {
		var zero T
		return zero, err
	}
	return p.waitResult(ctx, resultCh, &e.started)
}

func (p *boundedPool[T]) TryDo(ctx context.Context, task Task[T]) (T, error) {
	resultCh := make(chan result[T], 1)
	e := &entry[T]{ctx: ctx, task: task, result: resultCh}

	if err := p.submit(ctx, p.dispatch, e, true); err != nil {
		var zero T
		return zero, err
	}
	return p.waitResult(ctx, resultCh, &e.started)
}
