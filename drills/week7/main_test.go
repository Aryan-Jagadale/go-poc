package main

import (
	"errors"
	"fmt"
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

// together starts n goroutines, releases them at the same moment, and waits
// for all of them to finish.
func together(n int, f func(i int)) {
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			f(i)
		}()
	}
	close(gate)
	wg.Wait()
}

// balanceOf is the fake database's balance for an account.
func balanceOf(account string) int64 {
	var s int64
	for _, r := range account {
		s = s*31 + int64(r)
	}
	return s % 1_000_000
}

// slowLoader returns a Loader that takes delay per call, plus a count of calls.
func slowLoader(delay time.Duration) (Loader, *atomic.Int64) {
	var calls atomic.Int64
	return func(account string) (int64, error) {
		calls.Add(1)
		time.Sleep(delay)
		return balanceOf(account), nil
	}, &calls
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

func TestDemo_Stage1_ThunderingHerd(t *testing.T) {
	requireDemo(t)
	load, calls := slowLoader(50 * time.Millisecond)
	c := NewCache(load)

	together(100, func(int) { c.GetNaive("ACC1") })

	if n := calls.Load(); n != 1 {
		t.Fatalf("100 callers asked for ACC1 at once and the database was hit %d times, want 1", n)
	}
}

func TestDemo_Stage1_LockedSerializes(t *testing.T) {
	requireDemo(t)
	load, _ := slowLoader(50 * time.Millisecond)
	c := NewCache(load)

	begin := time.Now()
	together(10, func(i int) { c.GetLocked(fmt.Sprintf("ACC%d", i)) })
	if d := time.Since(begin); d > 200*time.Millisecond {
		t.Fatalf("10 different accounts took %v, want about 50ms: each one waited for the others' loads", d)
	}
}

// ---------------------------------------------------------------------------
// Stage 2: Group.Do
// ---------------------------------------------------------------------------

func TestGroup_SingleCaller(t *testing.T) {
	checkNoLeak(t)
	var g Group
	v, err, shared := g.Do("ACC1", func() (int64, error) { return 42, nil })
	if v != 42 || err != nil || shared {
		t.Fatalf("Do = %d, %v, shared=%v; want 42, nil, false", v, err, shared)
	}
}

func TestGroup_SameKeyRunsOnce(t *testing.T) {
	checkNoLeak(t)
	var g Group
	var calls, wrong, shared atomic.Int64
	release := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()

	runWithin(t, 3*time.Second, func() {
		together(100, func(int) {
			v, err, sh := g.Do("ACC1", func() (int64, error) {
				calls.Add(1)
				<-release
				return 777, nil
			})
			if v != 777 || err != nil {
				wrong.Add(1)
			}
			if sh {
				shared.Add(1)
			}
		})
	})

	if n := calls.Load(); n != 1 {
		t.Fatalf("fn ran %d times for 100 callers of the same key, want 1", n)
	}
	if n := wrong.Load(); n != 0 {
		t.Fatalf("%d callers did not get 777, nil", n)
	}
	if n := shared.Load(); n != 99 {
		t.Fatalf("%d callers reported shared=true, want 99 (everyone but the one who ran fn)", n)
	}
}

func TestGroup_DifferentKeysRunInParallel(t *testing.T) {
	checkNoLeak(t)
	var g Group
	var calls atomic.Int64

	begin := time.Now()
	runWithin(t, 3*time.Second, func() {
		together(10, func(i int) {
			g.Do(fmt.Sprintf("ACC%d", i), func() (int64, error) {
				calls.Add(1)
				time.Sleep(50 * time.Millisecond)
				return 1, nil
			})
		})
	})

	if n := calls.Load(); n != 10 {
		t.Fatalf("fn ran %d times for 10 different keys, want 10", n)
	}
	if d := time.Since(begin); d > 200*time.Millisecond {
		t.Fatalf("10 different keys took %v, want about 50ms: different keys must not wait for each other", d)
	}
}

func TestGroup_ErrorIsShared(t *testing.T) {
	checkNoLeak(t)
	var g Group
	errDB := errors.New("database timeout")
	var calls, gotErr atomic.Int64
	release := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()

	runWithin(t, 3*time.Second, func() {
		together(50, func(int) {
			_, err, _ := g.Do("ACC1", func() (int64, error) {
				calls.Add(1)
				<-release
				return 0, errDB
			})
			if errors.Is(err, errDB) {
				gotErr.Add(1)
			}
		})
	})

	if n := calls.Load(); n != 1 {
		t.Fatalf("fn ran %d times, want 1", n)
	}
	if n := gotErr.Load(); n != 50 {
		t.Fatalf("%d of 50 callers got the error, want all 50", n)
	}
}

func TestGroup_ForgetsKeyAfterCall(t *testing.T) {
	checkNoLeak(t)
	var g Group
	var calls atomic.Int64
	fn := func() (int64, error) { calls.Add(1); return 5, nil }

	runWithin(t, time.Second, func() {
		g.Do("ACC1", fn)
		g.Do("ACC1", fn)
	})

	if n := calls.Load(); n != 2 {
		t.Fatalf("two Do calls one after another ran fn %d times, want 2: remove a finished call from the map", n)
	}
}

// ---------------------------------------------------------------------------
// Stage 3: Cache.Get
// ---------------------------------------------------------------------------

func TestCache_HitDoesNotLoad(t *testing.T) {
	checkNoLeak(t)
	load, calls := slowLoader(0)
	c := NewCache(load)

	for i := range 3 {
		v, err := c.Get("ACC1")
		if err != nil || v != balanceOf("ACC1") {
			t.Fatalf("Get #%d = %d, %v; want %d, nil", i+1, v, err, balanceOf("ACC1"))
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("3 Gets of ACC1 hit the database %d times, want 1", n)
	}
}

func TestCache_ConcurrentMissLoadsOnce(t *testing.T) {
	checkNoLeak(t)
	load, calls := slowLoader(100 * time.Millisecond)
	c := NewCache(load)
	var wrong atomic.Int64

	runWithin(t, 3*time.Second, func() {
		together(100, func(int) {
			if v, err := c.Get("ACC1"); err != nil || v != balanceOf("ACC1") {
				wrong.Add(1)
			}
		})
	})

	if n := calls.Load(); n != 1 {
		t.Fatalf("100 callers missed on ACC1 at once and the database was hit %d times, want 1", n)
	}
	if n := wrong.Load(); n != 0 {
		t.Fatalf("%d callers got a wrong balance or an error", n)
	}
}

func TestCache_DifferentAccountsInParallel(t *testing.T) {
	checkNoLeak(t)
	load, calls := slowLoader(50 * time.Millisecond)
	c := NewCache(load)

	begin := time.Now()
	runWithin(t, 3*time.Second, func() {
		together(10, func(i int) { c.Get(fmt.Sprintf("ACC%d", i)) })
	})

	if n := calls.Load(); n != 10 {
		t.Fatalf("10 different accounts caused %d loads, want 10", n)
	}
	if d := time.Since(begin); d > 200*time.Millisecond {
		t.Fatalf("10 different accounts took %v, want about 50ms", d)
	}
}

func TestCache_ErrorsAreNotCached(t *testing.T) {
	checkNoLeak(t)
	errDB := errors.New("database timeout")
	var calls atomic.Int64
	c := NewCache(func(account string) (int64, error) {
		if calls.Add(1) == 1 {
			return 0, errDB
		}
		return balanceOf(account), nil
	})

	if _, err := c.Get("ACC1"); !errors.Is(err, errDB) {
		t.Fatalf("first Get err = %v, want the database error", err)
	}
	if v, err := c.Get("ACC1"); err != nil || v != balanceOf("ACC1") {
		t.Fatalf("second Get = %d, %v; want a fresh load to succeed", v, err)
	}
	if v, err := c.Get("ACC1"); err != nil || v != balanceOf("ACC1") {
		t.Fatalf("third Get = %d, %v; want the cached balance", v, err)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("database hit %d times, want 2 (the failed load, then one that is cached)", n)
	}
}

func TestCache_Stress(t *testing.T) {
	checkNoLeak(t)
	load, _ := slowLoader(time.Millisecond)
	c := NewCache(load)
	var wrong atomic.Int64

	runWithin(t, 20*time.Second, func() {
		together(50, func(i int) {
			for j := range 200 {
				acc := fmt.Sprintf("ACC%d", (i+j)%20)
				if v, err := c.Get(acc); err != nil || v != balanceOf(acc) {
					wrong.Add(1)
				}
			}
		})
	})

	if n := wrong.Load(); n != 0 {
		t.Fatalf("%d of 10000 Gets returned a wrong balance or an error", n)
	}
}
