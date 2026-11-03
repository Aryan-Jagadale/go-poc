package main

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strings"
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

// ---------------------------------------------------------------------------
// Warm-up
// ---------------------------------------------------------------------------

func expectedOddEven(n int) string {
	var b strings.Builder
	for i := 1; i <= n; i++ {
		if i%2 == 1 {
			fmt.Fprintf(&b, "odd: %d\n", i)
		} else {
			fmt.Fprintf(&b, "even: %d\n", i)
		}
	}
	return b.String()
}

func TestPrintOddEven_Sequence(t *testing.T) {
	for _, n := range []int{0, 1, 2, 7, 10, 101} {
		t.Run(fmt.Sprintf("n=%d", n), func(t *testing.T) {
			checkNoLeak(t)
			// Repeat to shake out scheduling-dependent ordering bugs.
			for range 50 {
				var buf bytes.Buffer
				if err := PrintOddEven(n, &buf); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got, want := buf.String(), expectedOddEven(n); got != want {
					t.Fatalf("wrong output\n got: %q\nwant: %q", got, want)
				}
			}
		})
	}
}

func TestPrintOddEven_Negative(t *testing.T) {
	checkNoLeak(t)
	var buf bytes.Buffer
	if err := PrintOddEven(-1, &buf); err == nil {
		t.Fatal("expected error for negative n")
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no output, got %q", buf.String())
	}
}

// ---------------------------------------------------------------------------
// Worker pool
// ---------------------------------------------------------------------------

func makeJobs(n int) []Job {
	jobs := make([]Job, n)
	for i := range jobs {
		jobs[i] = Job{ID: i, Payload: i}
	}
	return jobs
}

func TestRunPool_AllJobsExactlyOnce(t *testing.T) {
	checkNoLeak(t)
	jobs := makeJobs(100)

	var mu sync.Mutex
	seen := make(map[int]int)
	process := func(j Job) (int, error) {
		mu.Lock()
		seen[j.ID]++
		mu.Unlock()
		time.Sleep(time.Millisecond)
		return j.Payload * j.Payload, nil
	}

	results, err := RunPool(jobs, 5, process)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != len(jobs) {
		t.Fatalf("got %d results, want %d", len(results), len(jobs))
	}
	for i, r := range results {
		if r.JobID != jobs[i].ID {
			t.Fatalf("results[%d].JobID = %d, want %d", i, r.JobID, jobs[i].ID)
		}
		if want := jobs[i].Payload * jobs[i].Payload; r.Value != want {
			t.Fatalf("results[%d].Value = %d, want %d", i, r.Value, want)
		}
		if r.Err != nil {
			t.Fatalf("results[%d].Err = %v, want nil", i, r.Err)
		}
		if r.WorkerID < 0 || r.WorkerID >= 5 {
			t.Fatalf("results[%d].WorkerID = %d, want in [0,5)", i, r.WorkerID)
		}
	}
	for _, j := range jobs {
		if seen[j.ID] != 1 {
			t.Fatalf("job %d processed %d times, want 1", j.ID, seen[j.ID])
		}
	}
}

func TestRunPool_BoundedConcurrency(t *testing.T) {
	checkNoLeak(t)
	const workers = 5
	var inflight, maxInflight atomic.Int64
	workerIDs := sync.Map{}

	process := func(j Job) (int, error) {
		cur := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if cur <= m || maxInflight.CompareAndSwap(m, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inflight.Add(-1)
		return j.Payload, nil
	}

	results, err := RunPool(makeJobs(100), workers, process)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, r := range results {
		workerIDs.Store(r.WorkerID, true)
	}

	if m := maxInflight.Load(); m > workers {
		t.Fatalf("max in-flight = %d, exceeds %d workers", m, workers)
	} else if m < 2 {
		t.Fatalf("max in-flight = %d, jobs are not running concurrently", m)
	}
	distinct := 0
	workerIDs.Range(func(_, _ any) bool { distinct++; return true })
	if distinct < 2 {
		t.Fatalf("only %d distinct WorkerIDs seen, work is not spread across workers", distinct)
	}
}

func TestRunPool_JobErrorsAreCollected(t *testing.T) {
	checkNoLeak(t)
	errBad := errors.New("bad job")
	process := func(j Job) (int, error) {
		if j.ID%10 == 0 {
			return 0, fmt.Errorf("job %d: %w", j.ID, errBad)
		}
		return j.Payload + 1, nil
	}

	results, err := RunPool(makeJobs(100), 5, process)
	if err != nil {
		t.Fatalf("a failing job must not fail the whole pool, got %v", err)
	}
	if len(results) != 100 {
		t.Fatalf("got %d results, want 100", len(results))
	}
	for i, r := range results {
		if i%10 == 0 {
			if !errors.Is(r.Err, errBad) {
				t.Fatalf("results[%d].Err = %v, want wrapping errBad", i, r.Err)
			}
		} else if r.Err != nil || r.Value != i+1 {
			t.Fatalf("results[%d] = %+v, want Value=%d Err=nil", i, r, i+1)
		}
	}
}

func TestRunPool_PanicBecomesJobError(t *testing.T) {
	checkNoLeak(t)
	process := func(j Job) (int, error) {
		if j.ID == 50 {
			var m map[string]int
			m["boom"] = 1 // panics: assignment to entry in nil map
		}
		return j.Payload + 1, nil
	}

	results, err := RunPool(makeJobs(100), 5, process)
	if err != nil {
		t.Fatalf("a panicking job must not fail the whole pool, got %v", err)
	}
	if len(results) != 100 {
		t.Fatalf("got %d results, want 100", len(results))
	}
	for i, r := range results {
		if i == 50 {
			if r.Err == nil || !strings.Contains(r.Err.Error(), "panicked") {
				t.Fatalf("results[50].Err = %v, want a panic error", r.Err)
			}
			continue
		}
		if r.Err != nil || r.Value != i+1 {
			t.Fatalf("results[%d] = %+v, want Value=%d Err=nil", i, r, i+1)
		}
	}
}

func TestRunPool_DuplicateJobIDs(t *testing.T) {
	checkNoLeak(t)
	jobs := []Job{{ID: 7, Payload: 1}, {ID: 7, Payload: 2}, {ID: 7, Payload: 3}}
	results, err := RunPool(jobs, 2, func(j Job) (int, error) { return j.Payload * 10, nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	for i, r := range results {
		if r.Value != jobs[i].Payload*10 {
			t.Fatalf("results[%d].Value = %d, want %d", i, r.Value, jobs[i].Payload*10)
		}
	}
}

func TestRunPool_EdgeCases(t *testing.T) {
	ok := func(j Job) (int, error) { return j.Payload, nil }

	t.Run("zero workers", func(t *testing.T) {
		checkNoLeak(t)
		if _, err := RunPool(makeJobs(3), 0, ok); err == nil {
			t.Fatal("expected error for numWorkers=0")
		}
	})
	t.Run("negative workers", func(t *testing.T) {
		checkNoLeak(t)
		if _, err := RunPool(makeJobs(3), -2, ok); err == nil {
			t.Fatal("expected error for numWorkers<0")
		}
	})
	t.Run("nil process", func(t *testing.T) {
		checkNoLeak(t)
		if _, err := RunPool(makeJobs(3), 2, nil); err == nil {
			t.Fatal("expected error for nil process")
		}
	})
	t.Run("no jobs", func(t *testing.T) {
		checkNoLeak(t)
		results, err := RunPool(nil, 5, ok)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("got %d results, want 0", len(results))
		}
	})
	t.Run("more workers than jobs", func(t *testing.T) {
		checkNoLeak(t)
		results, err := RunPool(makeJobs(3), 50, ok)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 3 {
			t.Fatalf("got %d results, want 3", len(results))
		}
	})
}
