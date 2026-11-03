package main

import (
	"fmt"
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

// fakeClock is a Clock that only moves when the test says so.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func mustBucket(t *testing.T, rate float64, burst int, c *fakeClock) *TokenBucket {
	t.Helper()
	b, err := NewTokenBucket(rate, burst, c.Now)
	if err != nil {
		t.Fatalf("NewTokenBucket: %v", err)
	}
	return b
}

func countAllowed(b *TokenBucket, attempts int) int {
	n := 0
	for range attempts {
		if b.Allow() {
			n++
		}
	}
	return n
}

// ---------------------------------------------------------------------------
// Part A: token bucket
// ---------------------------------------------------------------------------

func TestTokenBucket_StartsFull(t *testing.T) {
	b := mustBucket(t, 10, 10, newFakeClock())
	if n := countAllowed(b, 15); n != 10 {
		t.Fatalf("allowed %d of 15 with the clock frozen, want burst=10", n)
	}
}

func TestTokenBucket_Refill(t *testing.T) {
	c := newFakeClock()
	b := mustBucket(t, 10, 10, c)
	countAllowed(b, 10) // drain

	c.Advance(100 * time.Millisecond) // 10/sec * 0.1s = 1 token
	if n := countAllowed(b, 5); n != 1 {
		t.Fatalf("after 100ms allowed %d, want 1", n)
	}

	c.Advance(500 * time.Millisecond) // 5 tokens
	if n := countAllowed(b, 10); n != 5 {
		t.Fatalf("after 500ms allowed %d, want 5", n)
	}
}

func TestTokenBucket_RefillIsCappedAtBurst(t *testing.T) {
	c := newFakeClock()
	b := mustBucket(t, 10, 10, c)
	countAllowed(b, 10)

	c.Advance(time.Hour) // would be 36,000 tokens without a cap
	if n := countAllowed(b, 100); n != 10 {
		t.Fatalf("after 1h idle allowed %d, want burst=10", n)
	}
}

func TestTokenBucket_FractionalTokensAccumulate(t *testing.T) {
	c := newFakeClock()
	b := mustBucket(t, 10, 10, c)
	countAllowed(b, 10)

	// Each 50ms step earns half a token. Checking in between must not throw
	// the half away.
	c.Advance(50 * time.Millisecond)
	if b.Allow() {
		t.Fatal("allowed after 50ms (half a token), want denied")
	}
	c.Advance(50 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("denied after 2 x 50ms (one full token), want allowed")
	}
}

func TestTokenBucket_ClockGoingBackwardsGrantsNothing(t *testing.T) {
	c := newFakeClock()
	b := mustBucket(t, 10, 10, c)
	countAllowed(b, 10)

	c.Advance(-time.Minute)
	if n := countAllowed(b, 5); n != 0 {
		t.Fatalf("allowed %d after clock moved backwards, want 0", n)
	}
}

func TestTokenBucket_ClockGoingBackwardsTakesNothing(t *testing.T) {
	c := newFakeClock()
	b := mustBucket(t, 10, 10, c) // full: 10 tokens

	c.Advance(-time.Minute)
	if n := countAllowed(b, 15); n != 10 {
		t.Fatalf("full bucket allowed %d after clock moved backwards, want 10 (a backwards clock must not remove tokens)", n)
	}
}

func TestTokenBucket_ConcurrentCallers(t *testing.T) {
	checkNoLeak(t)
	b := mustBucket(t, 10, 10, newFakeClock())

	var allowed atomic.Int64
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Allow() {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if n := allowed.Load(); n != 10 {
		t.Fatalf("200 concurrent callers: %d allowed, want exactly 10", n)
	}
}

func TestTokenBucket_InvalidArgs(t *testing.T) {
	c := newFakeClock()
	cases := []struct {
		name  string
		rate  float64
		burst int
		now   Clock
	}{
		{"zero rate", 0, 10, c.Now},
		{"negative rate", -1, 10, c.Now},
		{"zero burst", 10, 0, c.Now},
		{"nil clock", 10, 10, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewTokenBucket(tc.rate, tc.burst, tc.now); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Part B: per-user limiter
// ---------------------------------------------------------------------------

func mustLimiter(t *testing.T, c *fakeClock, idleTTL time.Duration) *PerUserLimiter {
	t.Helper()
	l, err := NewPerUserLimiter(10, 10, idleTTL, 10*time.Millisecond, c.Now)
	if err != nil {
		t.Fatalf("NewPerUserLimiter: %v", err)
	}
	t.Cleanup(l.Close)
	return l
}

func TestPerUser_UsersAreIndependent(t *testing.T) {
	checkNoLeak(t)
	l := mustLimiter(t, newFakeClock(), time.Hour)

	alice := 0
	for range 15 {
		if l.Allow("alice") {
			alice++
		}
	}
	bob := 0
	for range 15 {
		if l.Allow("bob") {
			bob++
		}
	}
	if alice != 10 || bob != 10 {
		t.Fatalf("alice=%d bob=%d, want 10 each (one user's traffic must not use another's tokens)", alice, bob)
	}
	if n := l.Len(); n != 2 {
		t.Fatalf("Len() = %d, want 2", n)
	}
}

func TestPerUser_ConcurrentUsers(t *testing.T) {
	checkNoLeak(t)
	l := mustLimiter(t, newFakeClock(), time.Hour)

	const users, callsPerUser = 20, 50
	var allowed [users]atomic.Int64
	var wg sync.WaitGroup
	for u := range users {
		for range callsPerUser {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if l.Allow(fmt.Sprintf("user-%d", u)) {
					allowed[u].Add(1)
				}
			}()
		}
	}
	wg.Wait()

	for u := range users {
		if n := allowed[u].Load(); n != 10 {
			t.Fatalf("user-%d: %d allowed, want exactly 10 (two buckets created for one user?)", u, n)
		}
	}
	if n := l.Len(); n != users {
		t.Fatalf("Len() = %d, want %d", n, users)
	}
}

func TestPerUser_IdleUsersAreCleanedUp(t *testing.T) {
	checkNoLeak(t)
	c := newFakeClock()
	l := mustLimiter(t, c, time.Minute)

	l.Allow("idle-bob")
	l.Allow("active-alice")

	c.Advance(2 * time.Minute)
	l.Allow("active-alice") // alice is seen again, bob is not

	deadline := time.Now().Add(time.Second)
	for l.Len() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("Len() = %d after bob was idle past the TTL, want 1", l.Len())
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A returning user starts with a fresh, full bucket.
	n := 0
	for range 15 {
		if l.Allow("idle-bob") {
			n++
		}
	}
	if n != 10 {
		t.Fatalf("returning user allowed %d, want a fresh burst of 10", n)
	}
}

func TestPerUser_CloseStopsCleanup(t *testing.T) {
	checkNoLeak(t)
	c := newFakeClock()
	l, err := NewPerUserLimiter(10, 10, time.Minute, 10*time.Millisecond, c.Now)
	if err != nil {
		t.Fatalf("NewPerUserLimiter: %v", err)
	}
	l.Allow("a")
	l.Close()
	l.Close() // must not panic
}

func TestPerUser_CloseDoesNotDeadlock(t *testing.T) {
	checkNoLeak(t)
	c := newFakeClock()
	for i := range 500 {
		// A very fast ticker keeps the cleanup goroutine busy, so Close often
		// lands while it is running or about to run.
		l, err := NewPerUserLimiter(10, 10, time.Minute, time.Microsecond, c.Now)
		if err != nil {
			t.Fatalf("NewPerUserLimiter: %v", err)
		}
		l.Allow("a")
		time.Sleep(time.Duration(i%5) * 10 * time.Microsecond)

		closed := make(chan struct{})
		go func() {
			l.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, true)
			t.Fatalf("iteration %d: Close() did not return within 2s (deadlock)\n%s", i, buf[:n])
		}
	}
}

func TestPerUser_InvalidArgs(t *testing.T) {
	checkNoLeak(t)
	c := newFakeClock()
	cases := []struct {
		name                  string
		rate                  float64
		burst                 int
		idleTTL, cleanupEvery time.Duration
		now                   Clock
	}{
		{"zero rate", 0, 10, time.Minute, time.Second, c.Now},
		{"zero burst", 10, 0, time.Minute, time.Second, c.Now},
		{"zero idleTTL", 10, 10, 0, time.Second, c.Now},
		{"zero cleanupEvery", 10, 10, time.Minute, 0, c.Now},
		{"nil clock", 10, 10, time.Minute, time.Second, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, err := NewPerUserLimiter(tc.rate, tc.burst, tc.idleTTL, tc.cleanupEvery, tc.now)
			if err == nil {
				l.Close()
				t.Fatal("expected an error")
			}
		})
	}
}
