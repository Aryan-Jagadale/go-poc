package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
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

// drain reads ch until it is closed, failing the test if that takes over 1s.
func drain[T any](t *testing.T, ch <-chan T) []T {
	t.Helper()
	if ch == nil {
		t.Fatal("stage returned a nil channel")
	}
	var got []T
	timeout := time.After(time.Second)
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, v)
		case <-timeout:
			t.Fatalf("channel not closed after 1s (got %d values so far)", len(got))
		}
	}
}

// goodLines returns n valid lines, spread over three accounts, 100 paise each.
func goodLines(n int) []string {
	lines := make([]string, n)
	for i := range lines {
		lines[i] = fmt.Sprintf("ACC%d,100", i%3+1)
	}
	return lines
}

// setFraudCheck makes fraudCheck call f until the test ends.
func setFraudCheck(t *testing.T, f func(Txn) error) {
	t.Helper()
	fraudCheckFn.Store(&f)
	t.Cleanup(func() { fraudCheckFn.Store(nil) })
}

func requireDemo(t *testing.T) {
	t.Helper()
	if os.Getenv("DRILL_DEMO") == "" {
		t.Skip("demo of a deliberately broken version; run with DRILL_DEMO=1")
	}
}

// ---------------------------------------------------------------------------
// Demo of the broken stage. Skipped unless DRILL_DEMO=1 is set.
// ---------------------------------------------------------------------------

func TestDemo_Stage1_GeneratorLeaks(t *testing.T) {
	requireDemo(t)
	checkNoLeak(t)
	ctx, cancel := context.WithCancel(context.Background())

	out := GenerateNaive(ctx, goodLines(100))
	if out == nil {
		t.Fatal("GenerateNaive returned a nil channel")
	}
	<-out    // the consumer reads one line...
	cancel() // ...then gives up, like Run does on a bad line
}

// ---------------------------------------------------------------------------
// Stage 2: Generate and Parse
// ---------------------------------------------------------------------------

func TestGenerate_SendsAllInOrder(t *testing.T) {
	checkNoLeak(t)
	lines := []string{"a", "b", "c"}
	got := drain(t, Generate(context.Background(), lines))
	if !slices.Equal(got, lines) {
		t.Fatalf("got %q, want %q", got, lines)
	}
}

func TestGenerate_StopsOnCancel(t *testing.T) {
	checkNoLeak(t)
	ctx, cancel := context.WithCancel(context.Background())
	out := Generate(ctx, goodLines(1000))
	if out == nil {
		t.Fatal("Generate returned a nil channel")
	}
	<-out
	cancel()
	if got := drain(t, out); len(got) > 50 {
		t.Fatalf("Generate sent %d more lines after cancel, want it to stop", len(got))
	}
}

func TestParse_GoodAndBadLines(t *testing.T) {
	checkNoLeak(t)
	ctx := context.Background()
	got := drain(t, Parse(ctx, Generate(ctx, []string{"ACC1,250", "nonsense", "ACC2,abc", "ACC3,-5", "ACC2,99"})))

	if len(got) != 5 {
		t.Fatalf("got %d results, want one per line (5)", len(got))
	}
	if got[0].Err != nil || got[0].Txn != (Txn{"ACC1", 250}) {
		t.Errorf("line 1: got %+v, want ACC1 250", got[0])
	}
	for i := 1; i <= 3; i++ {
		if !errors.Is(got[i].Err, ErrBadLine) {
			t.Errorf("line %d: err = %v, want ErrBadLine", i+1, got[i].Err)
		}
	}
	if got[4].Err != nil || got[4].Txn != (Txn{"ACC2", 99}) {
		t.Errorf("line 5: got %+v, want ACC2 99", got[4])
	}
}

func TestParse_FraudCheckError(t *testing.T) {
	checkNoLeak(t)
	errFraud := errors.New("flagged by fraud check")
	setFraudCheck(t, func(tx Txn) error {
		if tx.Account == "ACC9" {
			return errFraud
		}
		return nil
	})

	ctx := context.Background()
	got := drain(t, Parse(ctx, Generate(ctx, []string{"ACC1,100", "ACC9,100"})))
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	if got[0].Err != nil {
		t.Errorf("ACC1: unexpected error %v", got[0].Err)
	}
	if !errors.Is(got[1].Err, errFraud) {
		t.Errorf("ACC9: err = %v, want the fraud check's error", got[1].Err)
	}
}

func TestParse_StopsOnCancel(t *testing.T) {
	t.Run("while waiting to send", func(t *testing.T) {
		checkNoLeak(t)
		ctx, cancel := context.WithCancel(context.Background())
		in := make(chan string, 1)
		in <- "ACC1,100"
		out := Parse(ctx, in) // nobody ever reads out
		if out == nil {
			t.Fatal("Parse returned a nil channel")
		}
		time.Sleep(20 * time.Millisecond)
		cancel()
	})
	t.Run("while waiting to receive", func(t *testing.T) {
		checkNoLeak(t)
		ctx, cancel := context.WithCancel(context.Background())
		in := make(chan string) // never sent to, never closed
		out := Parse(ctx, in)
		if out == nil {
			t.Fatal("Parse returned a nil channel")
		}
		cancel()
		drain(t, out)
	})
}

// ---------------------------------------------------------------------------
// Stage 3: Merge and Run
// ---------------------------------------------------------------------------

func feed(vals ...int64) <-chan Parsed {
	ch := make(chan Parsed, len(vals))
	for _, v := range vals {
		ch <- Parsed{Txn: Txn{Account: "A", Paise: v}}
	}
	close(ch)
	return ch
}

func TestMerge_ForwardsEverythingThenCloses(t *testing.T) {
	checkNoLeak(t)
	got := drain(t, Merge(context.Background(), feed(1, 2, 3), feed(), feed(4, 5), feed(6)))

	var paise []int64
	for _, p := range got {
		paise = append(paise, p.Txn.Paise)
	}
	slices.Sort(paise)
	if want := []int64{1, 2, 3, 4, 5, 6}; !slices.Equal(paise, want) {
		t.Fatalf("merged %v, want %v (in any order)", paise, want)
	}
}

func TestMerge_NoInputs(t *testing.T) {
	checkNoLeak(t)
	if got := drain(t, Merge(context.Background())); len(got) != 0 {
		t.Fatalf("got %d values from no inputs", len(got))
	}
}

func TestMerge_StopsOnCancel(t *testing.T) {
	checkNoLeak(t)
	ctx, cancel := context.WithCancel(context.Background())
	stuck := make(chan Parsed, 1) // has a value, never closed
	stuck <- Parsed{}
	out := Merge(ctx, stuck, feed(1, 2)) // nobody reads out
	if out == nil {
		t.Fatal("Merge returned a nil channel")
	}
	time.Sleep(20 * time.Millisecond)
	cancel()
}

func TestRun_InvalidWorkers(t *testing.T) {
	for _, w := range []int{0, -1} {
		if _, err := Run(goodLines(3), w); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Run(workers=%d) err = %v, want ErrInvalidConfig", w, err)
		}
	}
}

func TestRun_Totals(t *testing.T) {
	checkNoLeak(t)
	var totals map[string]int64
	var err error
	runWithin(t, 5*time.Second, func() { totals, err = Run(goodLines(3000), 4) })
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := map[string]int64{"ACC1": 100_000, "ACC2": 100_000, "ACC3": 100_000}
	for acc, w := range want {
		if totals[acc] != w {
			t.Errorf("%s = %d, want %d", acc, totals[acc], w)
		}
	}
	if len(totals) != len(want) {
		t.Errorf("got %d accounts, want %d: %v", len(totals), len(want), totals)
	}
}

func TestRun_StopsAtFirstError(t *testing.T) {
	checkNoLeak(t)
	lines := goodLines(10_000)
	lines[5] = "ACC1,oops"

	var err error
	runWithin(t, 5*time.Second, func() { _, err = Run(lines, 4) })
	if !errors.Is(err, ErrBadLine) {
		t.Fatalf("err = %v, want ErrBadLine", err)
	}
}

func TestRun_FanOutRunsInParallel(t *testing.T) {
	checkNoLeak(t)
	setFraudCheck(t, func(Txn) error { time.Sleep(10 * time.Millisecond); return nil })

	begin := time.Now()
	var err error
	runWithin(t, 5*time.Second, func() { _, err = Run(goodLines(40), 8) })
	elapsed := time.Since(begin)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// One worker would take 40 x 10ms = 400ms. Eight should take about 50ms.
	if elapsed > 250*time.Millisecond {
		t.Fatalf("40 checks of 10ms with 8 workers took %v: the Parse stages are not running in parallel", elapsed)
	}
}

func TestRun_EarlyExitStress(t *testing.T) {
	checkNoLeak(t)
	lines := goodLines(500)
	lines[250] = "broken"
	runWithin(t, 20*time.Second, func() {
		for i := range 300 {
			if _, err := Run(lines, 1+i%8); !errors.Is(err, ErrBadLine) {
				t.Errorf("run %d: err = %v, want ErrBadLine", i, err)
				return
			}
		}
	})
}
