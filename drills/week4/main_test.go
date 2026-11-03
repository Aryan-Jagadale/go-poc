package main

import (
	"errors"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
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
// finish within d. Used to turn a deadlock into a readable failure.
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
		t.Fatalf("did not finish within %v: DEADLOCK\n%s", d, buf[:n])
	}
}

func newBank(t *testing.T, n int, each int64) *Bank {
	t.Helper()
	balances := make(map[int]int64, n)
	for id := 1; id <= n; id++ {
		balances[id] = each
	}
	b, err := NewBank(balances)
	if err != nil {
		t.Fatalf("NewBank: %v", err)
	}
	return b
}

func total(t *testing.T, b *Bank, n int) int64 {
	t.Helper()
	var sum int64
	for id := 1; id <= n; id++ {
		bal, err := b.Balance(id)
		if err != nil {
			t.Fatalf("Balance(%d): %v", id, err)
		}
		if bal < 0 {
			t.Fatalf("account %d has negative balance %d", id, bal)
		}
		sum += bal
	}
	return sum
}

func requireDemo(t *testing.T) {
	t.Helper()
	if os.Getenv("DRILL_DEMO") == "" {
		t.Skip("demo of a deliberately broken version; run with DRILL_DEMO=1")
	}
}

// ---------------------------------------------------------------------------
// Demos of the broken stages. Skipped unless DRILL_DEMO=1 is set.
// ---------------------------------------------------------------------------

func TestDemo_Stage1_Unsafe(t *testing.T) {
	requireDemo(t)
	const n = 5
	b := newBank(t, n, 1000)

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				from, to := rand.IntN(n)+1, rand.IntN(n)+1
				if from != to {
					b.TransferUnsafe(from, to, int64(rand.IntN(50)+1))
				}
			}
		}()
	}
	wg.Wait()

	if got := total(t, b, n); got != n*1000 {
		t.Fatalf("total money = %d, want %d: money was created or destroyed", got, n*1000)
	}
}

func TestDemo_Stage2_NaiveLock(t *testing.T) {
	requireDemo(t)
	b := newBank(t, 2, 1_000_000)

	runWithin(t, 3*time.Second, func() {
		var wg sync.WaitGroup
		for range 1000 {
			wg.Add(2)
			go func() { defer wg.Done(); b.TransferNaiveLock(1, 2, 1) }()
			go func() { defer wg.Done(); b.TransferNaiveLock(2, 1, 1) }()
		}
		wg.Wait()
	})
}

// ---------------------------------------------------------------------------
// Stage 3: Transfer
// ---------------------------------------------------------------------------

func TestBalance(t *testing.T) {
	b := newBank(t, 2, 500)
	if bal, err := b.Balance(1); err != nil || bal != 500 {
		t.Fatalf("Balance(1) = %d, %v; want 500, nil", bal, err)
	}
	if _, err := b.Balance(99); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("Balance(99) err = %v, want ErrAccountNotFound", err)
	}
}

func TestTransfer_MovesMoney(t *testing.T) {
	b := newBank(t, 2, 1000)
	if err := b.Transfer(1, 2, 300); err != nil {
		t.Fatalf("Transfer: %v", err)
	}
	a, _ := b.Balance(1)
	c, _ := b.Balance(2)
	if a != 700 || c != 1300 {
		t.Fatalf("balances = %d, %d; want 700, 1300", a, c)
	}
}

func TestTransfer_InsufficientFunds(t *testing.T) {
	b := newBank(t, 2, 100)
	if err := b.Transfer(1, 2, 101); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("err = %v, want ErrInsufficientFunds", err)
	}
	a, _ := b.Balance(1)
	c, _ := b.Balance(2)
	if a != 100 || c != 100 {
		t.Fatalf("a failed transfer changed balances to %d, %d; want 100, 100", a, c)
	}
	// Exactly the full balance is allowed.
	if err := b.Transfer(1, 2, 100); err != nil {
		t.Fatalf("transferring the exact balance: %v", err)
	}
}

func TestTransfer_InvalidInput(t *testing.T) {
	b := newBank(t, 2, 1000)
	cases := []struct {
		name     string
		from, to int
		amount   int64
		want     error
	}{
		{"zero amount", 1, 2, 0, ErrInvalidAmount},
		{"negative amount", 1, 2, -50, ErrInvalidAmount},
		{"unknown from", 99, 2, 10, ErrAccountNotFound},
		{"unknown to", 1, 99, 10, ErrAccountNotFound},
		{"same account", 1, 1, 10, ErrSameAccount},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runWithin(t, time.Second, func() {
				if err := b.Transfer(tc.from, tc.to, tc.amount); !errors.Is(err, tc.want) {
					t.Errorf("err = %v, want %v", err, tc.want)
				}
			})
		})
	}
	if got := total(t, b, 2); got != 2000 {
		t.Fatalf("invalid transfers changed the total to %d", got)
	}
}

func TestTransfer_OppositeDirectionsDoNotDeadlock(t *testing.T) {
	checkNoLeak(t)
	b := newBank(t, 2, 1_000_000)

	runWithin(t, 5*time.Second, func() {
		var wg sync.WaitGroup
		for range 2000 {
			wg.Add(2)
			go func() { defer wg.Done(); b.Transfer(1, 2, 1) }()
			go func() { defer wg.Done(); b.Transfer(2, 1, 1) }()
		}
		wg.Wait()
	})

	if got := total(t, b, 2); got != 2_000_000 {
		t.Fatalf("total = %d, want 2000000", got)
	}
}

func TestTransfer_ConcurrentMoneyIsConserved(t *testing.T) {
	checkNoLeak(t)
	const n = 10
	b := newBank(t, n, 1000)

	runWithin(t, 10*time.Second, func() {
		var wg sync.WaitGroup
		for range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 500 {
					from, to := rand.IntN(n)+1, rand.IntN(n)+1
					if from == to {
						continue
					}
					err := b.Transfer(from, to, int64(rand.IntN(200)+1))
					if err != nil && !errors.Is(err, ErrInsufficientFunds) {
						t.Errorf("unexpected error: %v", err)
					}
				}
			}()
		}
		wg.Wait()
	})

	if got := total(t, b, n); got != n*1000 {
		t.Fatalf("total = %d, want %d: money was created or destroyed", got, n*1000)
	}
}
