package main

import (
	"context"
	"errors"
	"fmt"
	"runtime"
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

var errBoom = errors.New("boom")

// ---------------------------------------------------------------------------
// Part A: FakeAPI
// ---------------------------------------------------------------------------

func TestFakeAPI_Success(t *testing.T) {
	checkNoLeak(t)
	start := time.Now()
	got, err := FakeAPI("users", 50*time.Millisecond, nil)(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "users: ok" {
		t.Fatalf("got %q, want %q", got, "users: ok")
	}
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Fatalf("returned after %v, before its 50ms latency", d)
	}
}

func TestFakeAPI_Failure(t *testing.T) {
	checkNoLeak(t)
	got, err := FakeAPI("kyc", 20*time.Millisecond, errBoom)(context.Background())
	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want errBoom", err)
	}
	if got != "" {
		t.Fatalf("got %q, want empty string on failure", got)
	}
}

func TestFakeAPI_StopsWhenCancelled(t *testing.T) {
	checkNoLeak(t)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)

	start := time.Now()
	_, err := FakeAPI("slow", 5*time.Second, nil)(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("took %v to notice cancellation, want ~30ms", d)
	}
}

func TestFakeAPI_AlreadyCancelled(t *testing.T) {
	checkNoLeak(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, err := FakeAPI("x", time.Second, nil)(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("took %v on an already-cancelled ctx, want immediate", d)
	}
}

// ---------------------------------------------------------------------------
// Part B: FetchAll
// ---------------------------------------------------------------------------

func TestFetchAll_AllSucceedInParallel(t *testing.T) {
	checkNoLeak(t)
	fetchers := []Fetcher{
		FakeAPI("a", 150*time.Millisecond, nil),
		FakeAPI("b", 250*time.Millisecond, nil),
		FakeAPI("c", 100*time.Millisecond, nil),
		FakeAPI("d", 200*time.Millisecond, nil),
		FakeAPI("e", 50*time.Millisecond, nil),
	}

	start := time.Now()
	results, err := FetchAll(context.Background(), fetchers, 2*time.Second)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"a: ok", "b: ok", "c: ok", "d: ok", "e: ok"}
	if len(results) != len(want) {
		t.Fatalf("got %d results, want %d", len(results), len(want))
	}
	for i := range want {
		if results[i] != want[i] {
			t.Fatalf("results[%d] = %q, want %q (results must be in input order)", i, results[i], want[i])
		}
	}
	// Sequential would be 750ms; parallel is bounded by the slowest (250ms).
	if elapsed > 600*time.Millisecond {
		t.Fatalf("took %v; calls are not running concurrently", elapsed)
	}
}

func TestFetchAll_OneFailsCancelsTheRest(t *testing.T) {
	checkNoLeak(t)
	var cancelled atomic.Int64
	slow := func(ctx context.Context) (string, error) {
		select {
		case <-time.After(3 * time.Second):
			return "slow: ok", nil
		case <-ctx.Done():
			cancelled.Add(1)
			return "", ctx.Err()
		}
	}
	fetchers := []Fetcher{slow, slow, FakeAPI("bad", 50*time.Millisecond, errBoom), slow, slow}

	start := time.Now()
	results, err := FetchAll(context.Background(), fetchers, 5*time.Second)
	elapsed := time.Since(start)

	if !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want the failing call's error (errBoom), not a sibling's cancellation", err)
	}
	if results != nil {
		t.Fatalf("results = %v, want nil on failure", results)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("took %v; siblings were not cancelled after the failure at 50ms", elapsed)
	}
	if n := cancelled.Load(); n != 4 {
		t.Fatalf("%d of 4 slow calls saw cancellation, want 4", n)
	}
}

func TestFetchAll_Timeout(t *testing.T) {
	checkNoLeak(t)
	fetchers := []Fetcher{
		FakeAPI("fast", 20*time.Millisecond, nil),
		FakeAPI("hangs", 10*time.Second, nil),
	}

	start := time.Now()
	results, err := FetchAll(context.Background(), fetchers, 200*time.Millisecond)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if results != nil {
		t.Fatalf("results = %v, want nil on timeout", results)
	}
	if elapsed < 200*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("returned after %v, want ~200ms", elapsed)
	}
}

func TestFetchAll_ParentCancelled(t *testing.T) {
	checkNoLeak(t)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	start := time.Now()
	_, err := FetchAll(ctx, []Fetcher{
		FakeAPI("a", 5*time.Second, nil),
		FakeAPI("b", 5*time.Second, nil),
	}, 10*time.Second)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Fatalf("took %v after parent cancel at 50ms", d)
	}
}

func TestFetchAll_EdgeCases(t *testing.T) {
	t.Run("no fetchers", func(t *testing.T) {
		checkNoLeak(t)
		results, err := FetchAll(context.Background(), nil, time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(results) != 0 {
			t.Fatalf("got %d results, want 0", len(results))
		}
	})
	t.Run("non-positive timeout", func(t *testing.T) {
		checkNoLeak(t)
		_, err := FetchAll(context.Background(), []Fetcher{FakeAPI("a", time.Millisecond, nil)}, 0)
		if err == nil {
			t.Fatal("expected error for timeout <= 0")
		}
	})
	t.Run("nil fetcher in list", func(t *testing.T) {
		checkNoLeak(t)
		_, err := FetchAll(context.Background(), []Fetcher{FakeAPI("a", time.Millisecond, nil), nil}, time.Second)
		if err == nil {
			t.Fatal("expected error for a nil fetcher")
		}
	})
	t.Run("many fetchers", func(t *testing.T) {
		checkNoLeak(t)
		fetchers := make([]Fetcher, 200)
		for i := range fetchers {
			fetchers[i] = FakeAPI(fmt.Sprintf("f%d", i), 20*time.Millisecond, nil)
		}
		results, err := FetchAll(context.Background(), fetchers, 2*time.Second)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for i, r := range results {
			if want := fmt.Sprintf("f%d: ok", i); r != want {
				t.Fatalf("results[%d] = %q, want %q", i, r, want)
			}
		}
	})
}
