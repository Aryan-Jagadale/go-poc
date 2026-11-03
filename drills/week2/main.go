package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
	"golang.org/x/sync/errgroup"
)

var errNotImplemented = errors.New("not implemented")

// Fetcher calls one downstream API. It must stop early and return ctx.Err()
// if ctx is cancelled before it finishes.
type Fetcher func(ctx context.Context) (string, error)

// ---------------------------------------------------------------------------
// Part A: a fake API that respects cancellation
// ---------------------------------------------------------------------------

// FakeAPI returns a Fetcher that simulates a network call taking latency.
// When the call completes: if failErr is non-nil it returns ("", failErr),
// otherwise it returns ("<name>: ok", nil).
// If ctx is cancelled first, it returns ("", ctx.Err()) immediately.
func FakeAPI(name string, latency time.Duration, failErr error) Fetcher {
	return func(ctx context.Context) (string, error) {

		// ctx, cancel := context.WithTimeout(ctx, latency+) 
		// defer cancel()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(latency):
			if failErr != nil {
				return "", failErr
			}
			return fmt.Sprintf("%s: ok", name), nil
		}
	}
}

// ---------------------------------------------------------------------------
// Part B: fetch everything in parallel
// ---------------------------------------------------------------------------

// FetchAll calls every fetcher concurrently and returns their results, with
// results[i] coming from fetchers[i].
//
//   - If any fetcher fails, the others are cancelled and FetchAll returns
//     that first error.
//   - The whole call is bounded by timeout.
//   - If the parent ctx is cancelled, FetchAll stops and returns its error.
func FetchAll(ctx context.Context, fetchers []Fetcher, timeout time.Duration) ([]string, error) {
	// TODO: validate input
	if len(fetchers) == 0 {
		return []string{}, nil
	}
	if timeout <= 0 {
		return nil, errors.New("timeout must be positive")
	}
	for _, fetcher := range fetchers {
		if fetcher == nil {
			return nil, errors.New("fetcher cannot be nil")
		}
	}
	// TODO: derive a context that enforces the timeout
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	results := make([]string, len(fetchers))

	for i, fetcher := range fetchers {
		if fetcher == nil {
			return nil, errors.New("fetcher cannot be nil")
		}

		g.Go(func() error {
			result, err := fetcher(ctx)
			if err != nil {
				return err
			}
			results[i] = result
			return nil
		})
	}

	if err := g.Wait(); err != nil {
			return nil, err
		}
	return results, nil

}

func main() {
	fetchers := []Fetcher{
		FakeAPI("users", 300*time.Millisecond, nil),
		FakeAPI("orders", 500*time.Millisecond, nil),
		FakeAPI("payments", 200*time.Millisecond, nil),
		FakeAPI("ledger", 400*time.Millisecond, nil),
		FakeAPI("kyc", 100*time.Millisecond, nil),
	}

	start := time.Now()
	results, err := FetchAll(context.Background(), fetchers, 2*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch:", err)
		os.Exit(1)
	}
	for _, r := range results {
		fmt.Println(r)
	}
	fmt.Printf("done in %v\n", time.Since(start))
}
