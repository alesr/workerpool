package workerpool

import "context"

// unboundedPool implements a memory-backed pool with unbounded queuing.
// Submit writes to a non-blocking ingestion channel; a broker goroutine
// drains it into a dynamic slice and feeds tasks to workers
type unboundedPool[T Task] struct {
	poolCore[T]
	ingress chan entry[T]
}

func newUnboundedPool[T Task](ctx context.Context, workers int) *unboundedPool[T] {
	poolCtx, cancel := context.WithCancel(ctx)

	ingressBuf := max(workers, 1)

	p := &unboundedPool[T]{}

	p.ctx = poolCtx
	p.cancel = cancel
	p.stop = make(chan struct{})
	p.ingress = make(chan entry[T], ingressBuf)
	p.dispatch = make(chan entry[T], workers)

	p.wg.Add(workers)
	for range workers {
		go p.worker()
	}

	go p.broker()

	return p
}

func (p *unboundedPool[T]) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			p.ungracefulStop.Store(true)
			return
		case entry, ok := <-p.dispatch:
			if !ok {
				return
			}
			entry.job.Do(entry.ctx)
		}
	}
}

func (p *unboundedPool[T]) broker() {
	var queue []entry[T]

	for {
		var (
			front      entry[T]
			dispatchCh chan entry[T]
		)

		if len(queue) > 0 {
			front = queue[0]
			dispatchCh = p.dispatch
		}

		select {
		case <-p.ctx.Done():
			return
		case <-p.stop:
		drainLoop:
			for {
				select {
				case e := <-p.ingress:
					queue = append(queue, e)
				default:
					break drainLoop
				}
			}

			for _, e := range queue {
				select {
				case <-p.ctx.Done():
					return
				case p.dispatch <- e:
				}
			}
			close(p.dispatch)
			return
		case e := <-p.ingress:
			queue = append(queue, e)
		case dispatchCh <- front:
			queue = queue[1:]
		}
	}
}

func (p *unboundedPool[T]) Submit(ctx context.Context, task T) error {
	e := entry[T]{ctx: ctx, job: task}

	select {
	case <-p.stop:
		return ErrPoolClosed
	case <-p.ctx.Done():
		return ErrPoolClosed
	default:
	}

	select {
	case p.ingress <- e:
		return nil
	default:
	}

	select {
	case p.ingress <- e:
		return nil
	case <-ctx.Done():
		return ErrTaskCancelled
	case <-p.ctx.Done():
		return ErrPoolClosed
	case <-p.stop:
		return ErrPoolClosed
	}
}
