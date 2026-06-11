package workerpool

import "context"

// boundedPool implements a fixed-size channel-backed pool
type boundedPool[T Task] struct {
	poolCore[T]
}

func newBoundedPool[T Task](ctx context.Context, workers int, buffer int) *boundedPool[T] {
	poolCtx, cancel := context.WithCancel(ctx)

	p := &boundedPool[T]{}
	p.ctx = poolCtx
	p.cancel = cancel
	p.stop = make(chan struct{})
	p.dispatch = make(chan entry[T], buffer)

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
			p.ungracefulStop.Store(true)
			return
		case <-p.stop:
			for {
				select {
				case entry := <-p.dispatch:
					entry.job.Do(entry.ctx)
				default:
					return
				}
			}
		case entry := <-p.dispatch:
			entry.job.Do(entry.ctx)
		}
	}
}

func (p *boundedPool[T]) Submit(ctx context.Context, task T) error {
	select {
	case <-ctx.Done():
		return ErrTaskCancelled
	case <-p.ctx.Done():
		return ErrPoolClosed
	case <-p.stop:
		return ErrPoolClosed
	case p.dispatch <- entry[T]{ctx: ctx, job: task}:
		return nil
	}
}
