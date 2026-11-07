package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrInvalidConfig  = errors.New("workers must be positive")
	ErrBadLine        = errors.New("bad line")
	errNotImplemented = errors.New("not implemented")
)

// Txn is one transaction from a settlement file. Amounts are in paise.
type Txn struct {
	Account string
	Paise   int64
}

// Parsed is what the parse stage sends for each line: a transaction, or the
// reason the line was rejected.
type Parsed struct {
	Txn Txn
	Err error
}

// fraudCheck stands in for a slow call to a fraud service. It is done for
// you; tests swap in their own version through fraudCheckFn.
func fraudCheck(t Txn) error {
	if f := fraudCheckFn.Load(); f != nil {
		return (*f)(t)
	}
	return nil
}

var fraudCheckFn atomic.Pointer[func(Txn) error]

// parseLine turns "ACC1,25050" into Txn{"ACC1", 25050}. It is done for you;
// this drill is about the channels around it.
func parseLine(line string) (Txn, error) {
	account, amount, ok := strings.Cut(line, ",")
	if !ok || account == "" {
		return Txn{}, fmt.Errorf("%q: %w", line, ErrBadLine)
	}
	paise, err := strconv.ParseInt(amount, 10, 64)
	if err != nil || paise <= 0 {
		return Txn{}, fmt.Errorf("%q: %w", line, ErrBadLine)
	}
	return Txn{Account: account, Paise: paise}, nil
}

// ---------------------------------------------------------------------------
// Stage 1: a generator that forgets about cancellation. Watch it leak.
// ---------------------------------------------------------------------------

// GenerateNaive sends each line on the returned channel, then closes it.
// It is deliberately broken.
func GenerateNaive(ctx context.Context, lines []string) <-chan string {
	// TODO: make the channel, start a goroutine that sends every line and
	// closes the channel, return the channel. Ignore ctx.

	ch := make(chan string)
	go func() {
		for _, line := range lines {
			ch <- line
		}
		close(ch)
	}()

	return ch
}

// ---------------------------------------------------------------------------
// Stage 2: stages that stop when ctx is cancelled.
// ---------------------------------------------------------------------------

// Generate sends each line on the returned channel, then closes it. If ctx
// is cancelled first, it stops sending and closes the channel early.
func Generate(ctx context.Context, lines []string) <-chan string {
	ch := make(chan string)
	go func() {
		defer close(ch)
		for _, line := range lines {
			select {
			case <-ctx.Done():
				return
			case ch <- line:

			}
		}
	}()
	return ch
}

// Parse reads lines from in until in is closed. For each line it sends one
// Parsed: the Txn if parseLine and fraudCheck both succeed, or the error.
// It closes its output when in is closed, or early if ctx is cancelled.
func Parse(ctx context.Context, in <-chan string) <-chan Parsed {
	out := make(chan Parsed)
	go func() {
		defer close(out)

		for {
			var result Parsed // an empty box for this line's answer

			// ── WAITING SPOT 1: receive ─────────────────────────
			select {
			case <-ctx.Done(): // told to stop while waiting for a line
				return
			case l, ok := <-in: // a line arrived (or in was closed)
				if !ok {
					return // in closed: Generate is finished
				}

				// ── WORK: can't get stuck ───────────────────────
				parser, err := parseLine(l)
				if err != nil {
					result = Parsed{Err: err} // bad format
				} else if err := fraudCheck(parser); err != nil {
					result = Parsed{Err: err} // flagged
				} else {
					result = Parsed{Txn: parser} // all good
				}
			}

			// ── WAITING SPOT 2: send ────────────────────────────
			select {
			case <-ctx.Done(): // told to stop while waiting for a reader
				return
			case out <- result: // someone read our result
			}
		} // back to the top for the next line

	}()
	return out
}

// ---------------------------------------------------------------------------
// Stage 3: fan-out, fan-in, and stopping early.
// ---------------------------------------------------------------------------

// Merge forwards every value from all of ins onto one channel. It closes that
// channel once every input is closed, or early if ctx is cancelled.
func Merge(ctx context.Context, ins ...<-chan Parsed) <-chan Parsed {
	out := make(chan Parsed)

	go func() {
		defer close(out)

		var wg sync.WaitGroup

		for _, in := range ins {
			wg.Add(1)

			go func(in <-chan Parsed) {
				defer wg.Done()

				for p := range in {
					select {
					case <-ctx.Done():
						return
					case out <- p:
					}
				}
			}(in)
		}

		wg.Wait() // wait for all the workers to finish

	}()
	return out
}

// Run parses lines using workers Parse stages in parallel and returns the
// total paise per account. It stops at the first error and returns it.
// Either way, no goroutine it started is left blocked after it returns.
func Run(lines []string, workers int) (map[string]int64, error) {
	// TODO: validate
	if workers <= 0 {
		return nil, ErrInvalidConfig
	}
	if len(lines) == 0 {
		return map[string]int64{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := Generate(ctx, lines)
	parsedChans := make([]<-chan Parsed, workers)
	for i := 0; i < workers; i++ {
		parsedChans[i] = Parse(ctx, ch)
	}

	merged := Merge(ctx, parsedChans...)

	totals := make(map[string]int64)

	if merged == nil {
		return nil, errors.New("merged channel is nil")
	}

	for p := range merged {
		if p.Err != nil {
			return nil, p.Err
		}
		totals[p.Txn.Account] += p.Txn.Paise
	}

	return totals, nil
}

func main() {
	lines := []string{
		"ACC1,25050",
		"ACC2,10000",
		"ACC1,4950",
		"ACC3,99900",
		"ACC2,500",
	}
	totals, err := Run(lines, 3)
	if err != nil {
		fmt.Println("run:", err)
		return
	}
	for _, acc := range []string{"ACC1", "ACC2", "ACC3"} {
		fmt.Printf("%s: ₹%d.%02d\n", acc, totals[acc]/100, totals[acc]%100)
	}
}
