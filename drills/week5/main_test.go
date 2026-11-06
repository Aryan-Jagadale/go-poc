package main

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// checkNoLeak records the goroutine count and, at test cleanup, waits up to 1s
// for it to drop back to that baseline.
func checkNoLeak(t *testing.T) {
	t.Helper()
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(time.Second)
		for {
			after := runtime.NumGoroutine()
			if after <= before {
				return
			}
			if time.Now().After(deadline) {
				buf := make([]byte, 1<<16)
				n := runtime.Stack(buf, true)
				t.Fatalf("goroutine leak: before=%d after=%d\n%s", before, after, buf[:n])
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// runWithin runs f and fails the test with a goroutine dump if it does not
// finish within d. Used to turn a hang into a readable failure.
func runWithin(t *testing.T, d time.Duration, f func()) {
	t.Helper()
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		f()
	}()
	select {
	case <-finished:
	case <-time.After(d):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("did not finish within %v: HANG\n%s", d, buf[:n])
	}
}

// waitFor fails the test if ch is not closed within 1s.
func waitFor(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func newProcessor(t *testing.T, workers, queueSize int) *Processor {
	t.Helper()
	p, err := NewProcessor(workers, queueSize)
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	return p
}

// countJob returns a job that adds one to ran.
func countJob(ran *atomic.Int64) Job {
	return Job{Run: func(context.Context) error {
		ran.Add(1)
		return nil
	}}
}

// waitJob returns a job that closes started, then blocks until release is
// closed or ctx is cancelled. A nil release blocks until ctx is cancelled.
func waitJob(started chan<- struct{}, release <-chan struct{}) Job {
	return Job{Run: func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
}

func requireDemo(t *testing.T) {
	t.Helper()
	if os.Getenv("DRILL_DEMO") == "" {
		t.Skip("demo of a deliberately broken version; run with DRILL_DEMO=1")
	}
}

// ---------------------------------------------------------------------------
// Demos of the broken stage. Skipped unless DRILL_DEMO=1 is set.
// ---------------------------------------------------------------------------

func TestDemo_Stage1_SubmitAfterShutdown(t *testing.T) {
	requireDemo(t)
	p := newProcessor(t, 2, 10)
	p.ShutdownNaive()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("SubmitNaive panicked: %v", r)
		}
	}()
	p.SubmitNaive(Job{Run: func(context.Context) error { return nil }})
}

func TestDemo_Stage1_StuckJob(t *testing.T) {
	requireDemo(t)
	p := newProcessor(t, 1, 1)
	started := make(chan struct{})
	if err := p.SubmitNaive(waitJob(started, nil)); err != nil {
		t.Fatalf("SubmitNaive: %v", err)
	}
	waitFor(t, started, "the job to start")

	runWithin(t, 2*time.Second, p.ShutdownNaive)
}

// ---------------------------------------------------------------------------
// Stage 2: Submit and Shutdown
// ---------------------------------------------------------------------------

func TestNewProcessor_InvalidConfig(t *testing.T) {
	cases := []struct {
		name               string
		workers, queueSize int
	}{
		{"zero workers", 0, 10},
		{"negative workers", -1, 10},
		{"zero queue", 2, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewProcessor(tc.workers, tc.queueSize); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("err = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestProcessor_RunsAllJobs(t *testing.T) {
	checkNoLeak(t)
	p := newProcessor(t, 4, 100)

	var ran atomic.Int64
	for i := range 100 {
		if err := p.Submit(countJob(&ran)); err != nil {
			t.Fatalf("Submit #%d: %v", i, err)
		}
	}
	runWithin(t, 2*time.Second, func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	if got := ran.Load(); got != 100 {
		t.Fatalf("ran %d jobs, want 100", got)
	}
}

func TestSubmit_QueueFull(t *testing.T) {
	checkNoLeak(t)
	p := newProcessor(t, 1, 1)

	started, release := make(chan struct{}), make(chan struct{})
	if err := p.Submit(waitJob(started, release)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitFor(t, started, "the first job to start")

	var ran atomic.Int64
	if err := p.Submit(countJob(&ran)); err != nil {
		t.Fatalf("the queue has room for one job, but Submit returned %v", err)
	}
	if err := p.Submit(countJob(&ran)); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}

	close(release)
	runWithin(t, 2*time.Second, func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	if got := ran.Load(); got != 1 {
		t.Fatalf("ran %d queued jobs, want 1", got)
	}
}

func TestSubmit_AfterShutdown(t *testing.T) {
	checkNoLeak(t)
	p := newProcessor(t, 2, 10)
	runWithin(t, 2*time.Second, func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Submit after Shutdown panicked: %v", r)
		}
	}()
	var ran atomic.Int64
	if err := p.Submit(countJob(&ran)); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("err = %v, want ErrShuttingDown", err)
	}
}

func TestShutdown_Twice(t *testing.T) {
	checkNoLeak(t)
	p := newProcessor(t, 2, 10)

	runWithin(t, 2*time.Second, func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Errorf("first Shutdown: %v", err)
		}
	})
	runWithin(t, time.Second, func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("second Shutdown panicked: %v", r)
			}
		}()
		if err := p.Shutdown(context.Background()); !errors.Is(err, ErrShuttingDown) {
			t.Errorf("second Shutdown err = %v, want ErrShuttingDown", err)
		}
	})
}

func TestShutdown_WaitsForQueuedJobs(t *testing.T) {
	checkNoLeak(t)
	p := newProcessor(t, 1, 20)

	var ran atomic.Int64
	for i := range 20 {
		err := p.Submit(Job{Run: func(context.Context) error {
			time.Sleep(5 * time.Millisecond)
			ran.Add(1)
			return nil
		}})
		if err != nil {
			t.Fatalf("Submit #%d: %v", i, err)
		}
	}
	runWithin(t, 2*time.Second, func() {
		if err := p.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	if got := ran.Load(); got != 20 {
		t.Fatalf("Shutdown returned after %d of 20 jobs: it must wait for queued jobs too", got)
	}
}

func TestShutdown_DeadlineCancelsJobs(t *testing.T) {
	checkNoLeak(t)
	p := newProcessor(t, 1, 10)

	started := make(chan struct{})
	var sawCancel atomic.Bool
	err := p.Submit(Job{Run: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // cleanup, like rolling back a charge
		sawCancel.Store(true)
		return ctx.Err()
	}})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	waitFor(t, started, "the stuck job to start")

	var ran atomic.Int64
	for i := range 5 {
		if err := p.Submit(countJob(&ran)); err != nil {
			t.Fatalf("Submit #%d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	var shutdownErr error
	runWithin(t, 2*time.Second, func() { shutdownErr = p.Shutdown(ctx) })
	elapsed := time.Since(begin)

	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown err = %v, want context.DeadlineExceeded", shutdownErr)
	}
	if elapsed > time.Second {
		t.Fatalf("Shutdown took %v with a 100ms deadline", elapsed)
	}
	if !sawCancel.Load() {
		t.Fatal("Shutdown returned before the cancelled job finished cleaning up: it must wait for the workers after cancelling")
	}
	if got := ran.Load(); got != 0 {
		t.Fatalf("%d queued jobs started after the deadline, want 0", got)
	}
}

func TestShutdown_ConcurrentSubmitNeverPanicsOrDropsJobs(t *testing.T) {
	checkNoLeak(t)

	runWithin(t, 30*time.Second, func() {
		for iter := range 200 {
			p, err := NewProcessor(4, 8)
			if err != nil {
				t.Errorf("NewProcessor: %v", err)
				return
			}

			var accepted, ran atomic.Int64
			var wg sync.WaitGroup
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						err := p.Submit(countJob(&ran))
						switch {
						case err == nil:
							accepted.Add(1)
						case errors.Is(err, ErrQueueFull):
							runtime.Gosched()
						case errors.Is(err, ErrShuttingDown):
							return
						default:
							t.Errorf("unexpected Submit error: %v", err)
							return
						}
					}
				}()
			}

			time.Sleep(time.Millisecond)
			if err := p.Shutdown(context.Background()); err != nil {
				t.Errorf("iteration %d: Shutdown: %v", iter, err)
				return
			}
			wg.Wait()

			if a, r := accepted.Load(), ran.Load(); a != r {
				t.Errorf("iteration %d: %d jobs accepted but %d ran: Shutdown dropped accepted jobs", iter, a, r)
				return
			}
		}
	})
}
