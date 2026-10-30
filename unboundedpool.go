package workerpool

import "context"

// unboundedPool implements a memory-backed pool with unbounded queuing.
// Do and TryDo write to a buffered ingestion channel; a broker goroutine drains
// it into a dynamic slice and feeds workers through dispatch.
type unboundedPool[T any] struct {
	poolCore[T]
	ingress  chan *entry[T]
	dispatch chan *entry[T]
}

func newUnboundedPool[T any](ctx context.Context, workers int, cfg config) *unboundedPool[T] {
	poolCtx, cancel := context.WithCancel(ctx)

	// default ingress buffer: 50 tasks per worker
	ingressSize := cfg.ingressBuffer
	if ingressSize == 0 {
		ingressSize = workers * 50
	}

	p := &unboundedPool[T]{
		ingress:  make(chan *entry[T], ingressSize),
		dispatch: make(chan *entry[T], workers),
	}
	p.ctx = poolCtx
	p.cancel = cancel
	p.stop = make(chan struct{})
	p.closeQueue = func() { close(p.ingress) }

	p.wg.Add(workers)
	for range workers {
		go p.worker()
	}

	p.wg.Add(1) // broker
	go p.broker()

	return p
}

func (p *unboundedPool[T]) worker() {
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

func (p *unboundedPool[T]) broker() {
	defer p.wg.Done()

	var queue []*entry[T]
	for {
		var (
			front      *entry[T]
			dispatchCh chan *entry[T]
		)
		if len(queue) > 0 {
			front = queue[0]
			dispatchCh = p.dispatch
		}

		select {
		case <-p.ctx.Done():
			return
		case e, ok := <-p.ingress:
			if !ok {
				// intake closed after the submitMu barrier, so no more sends
				// the channel yields any remaining buffered entries (ok=true)
				// before this !ok branch, so queue now holds everything.
				// drain all queued entries to dispatch. this must complete even
				// if context is cancelled, as workers are still running
				for _, e := range queue {
					p.dispatch <- e
				}
				close(p.dispatch)
				return
			}
			queue = append(queue, e)
		case dispatchCh <- front:
			queue[0] = nil // release reference before reslicing
			queue = queue[1:]
		}
	}
}

func (p *unboundedPool[T]) Do(ctx context.Context, task Task[T]) (T, error) {
	resultCh := make(chan result[T], 1)
	e := &entry[T]{ctx: ctx, task: task, result: resultCh}

	if err := p.submit(ctx, p.ingress, e, false); err != nil {
		var zero T
		return zero, err
	}
	return p.waitResult(ctx, resultCh, &e.started)
}

func (p *unboundedPool[T]) TryDo(ctx context.Context, task Task[T]) (T, error) {
	resultCh := make(chan result[T], 1)
	e := &entry[T]{ctx: ctx, task: task, result: resultCh}

	if err := p.submit(ctx, p.ingress, e, true); err != nil {
		var zero T
		return zero, err
	}
	return p.waitResult(ctx, resultCh, &e.started)
}
