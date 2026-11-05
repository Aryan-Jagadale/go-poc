package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"
)

var (
	ErrInvalidConfig  = errors.New("workers and queue size must be positive")
	ErrQueueFull      = errors.New("queue is full")
	ErrShuttingDown   = errors.New("processor is shutting down")
	errNotImplemented = errors.New("not implemented")
)

// Job is one unit of work, such as settling a payment. Run must return
// promptly once ctx is cancelled.
type Job struct {
	ID  int
	Run func(ctx context.Context) error
}

// Processor runs jobs on a fixed pool of worker goroutines.
type Processor struct {
	jobs chan Job
	wg   sync.WaitGroup

	// ctx is passed to every job. cancel stops the jobs that are still
	// running when Shutdown's deadline passes.
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
}

// NewProcessor starts workers goroutines that read from a queue holding up
// to queueSize jobs.
func NewProcessor(workers, queueSize int) (*Processor, error) {
	// TODO: validate
	if workers <= 0 || queueSize <= 0 {
		return nil, ErrInvalidConfig
	}
	// TODO: build the processor, start the workers
	jobs := make(chan Job, queueSize)
	ctx, cancel := context.WithCancel(context.Background())
	p := &Processor{
		jobs:   jobs,
		ctx:    ctx,
		cancel: cancel,
	}

	for range workers {
		p.wg.Add(1)
		go p.worker()
	}

	return p, nil
}

// ---------------------------------------------------------------------------
// Stage 1: no locking at all. Write it, then watch -race catch it.
// ---------------------------------------------------------------------------

// worker runs jobs until the queue is closed and empty. Once p.ctx is
// cancelled, it skips the jobs left in the queue instead of starting them.
// A job's error is ignored here; a real system would log or retry it.
func (p *Processor) worker() {
	defer p.wg.Done()

	for job := range p.jobs {

		if p.ctx.Err() != nil {
			continue
		}
		job.Run(p.ctx)
	}
}

// ---------------------------------------------------------------------------
// Stage 1: the obvious version. Write it, then watch it panic and hang.
// ---------------------------------------------------------------------------

// SubmitNaive queues a job without blocking, or returns ErrQueueFull.
// It is deliberately broken.
func (p *Processor) SubmitNaive(job Job) error {
	// TODO: try to send; if the queue is full, return ErrQueueFull
	select {
	case p.jobs <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

// ShutdownNaive closes the queue and waits for the workers to finish.
// It is deliberately broken.
func (p *Processor) ShutdownNaive() {

	close(p.jobs)
	p.wg.Wait()

}

// ---------------------------------------------------------------------------
// Stage 2: the real thing.
// ---------------------------------------------------------------------------

// Submit queues a job without blocking. It returns ErrQueueFull if the queue
// is full and ErrShuttingDown once Shutdown has been called. It is safe to
// call from many goroutines, including while Shutdown is running.
func (p *Processor) Submit(job Job) error {

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrShuttingDown
	}

	select {
	case p.jobs <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

// Shutdown stops accepting jobs and waits for every queued and running job to
// finish, then returns nil.
//
// If ctx is done first, Shutdown cancels the running jobs, makes sure the
// queued ones are never started, still waits for the workers to exit, and
// returns ctx.Err().
//
// Calling Shutdown more than once must not panic; later calls return
// ErrShuttingDown.
func (p *Processor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.closed { // someone already called Shutdown
		p.mu.Unlock()
		return ErrShuttingDown
	}
	p.closed = true // flip the sign
	close(p.jobs)   // workers will finish the queue, then exit
	p.mu.Unlock()   // let go BEFORE waiting (the cycle lesson)

	done := make(chan struct{})
	go func() {
		p.wg.Wait() // the helper freezes here, not Shutdown
		close(done) // ring the bell when the workers are done
	}()

	select {
	case <-done: // workers finished first
		p.cancel()
		return nil
	case <-ctx.Done(): // deadline first
		p.cancel() // tell the jobs to stop
		<-done     // wait for the workers to actually exit
		return ctx.Err()
	}

}

func main() {
	p, err := NewProcessor(3, 10)
	if err != nil {
		fmt.Println("processor:", err)
		return
	}

	for i := 1; i <= 10; i++ {
		err := p.Submit(Job{ID: i, Run: func(ctx context.Context) error {
			select {
			case <-time.After(time.Second):
				fmt.Println("job", i, "done")
				return nil
			case <-ctx.Done():
				fmt.Println("job", i, "cancelled")
				return ctx.Err()
			}
		}})
		if err != nil {
			fmt.Println("submit", i, ":", err)
		}
	}

	stop, stopCancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stopCancel()
	fmt.Println("running; press Ctrl+C to shut down")
	<-stop.Done()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fmt.Println("shutting down, waiting up to 2s")
	fmt.Println("shutdown:", p.Shutdown(ctx))
}
