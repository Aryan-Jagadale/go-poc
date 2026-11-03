package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

var errNotImplemented = errors.New("not implemented")

// ---------------------------------------------------------------------------
// Warm-up: odd/even
// ---------------------------------------------------------------------------

// PrintOddEven prints 1..n to w, one number per line, formatted exactly as
// "odd: <k>\n" or "even: <k>\n". Odd numbers must be written by one goroutine,
// even numbers by another, strictly alternating.
func PrintOddEven(n int, w io.Writer) error {
	if n < 0 {
		return fmt.Errorf("n must be >= 0, got %d", n)
	}
	if n == 0 {
		return nil // nothing to print, don't start any goroutines
	}

	oddTurn := make(chan struct{})  // "odd, it's your turn"
	evenTurn := make(chan struct{}) // "even, it's your turn"

	var wg sync.WaitGroup
	wg.Add(2)

	go func() { // odd goroutine
		defer wg.Done()
		for i := 1; i <= n; i += 2 {
			<-oddTurn                      // 1. wait for my turn
			fmt.Fprintf(w, "odd: %d\n", i) // 2. do my work
			if i+1 <= n {                  // 3. pass the baton, only if even has a number left
				evenTurn <- struct{}{}
			}
		}
	}()

	go func() { // even goroutine
		defer wg.Done()
		for i := 2; i <= n; i += 2 {
			<-evenTurn
			fmt.Fprintf(w, "even: %d\n", i)
			if i+1 <= n {
				oddTurn <- struct{}{}
			}
		}
	}()

	oddTurn <- struct{}{} // main hands odd the first baton
	wg.Wait()             // block until both goroutines have returned
	return nil
}

// ---------------------------------------------------------------------------
// Drill 1: worker pool
// ---------------------------------------------------------------------------

type Job struct {
	ID      int
	Payload int
}

type Result struct {
	JobID    int
	Value    int
	WorkerID int
	Err      error
}

// ProcessFunc does the work for a single job.
type ProcessFunc func(Job) (int, error)

// safeProcess runs process for a single job and converts a panic into an
// error, so one bad job can't crash the program or kill its worker.
func safeProcess(process ProcessFunc, job Job) (value int, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("job %d panicked: %v", job.ID, r)
		}
	}()
	return process(job)
}

// RunPool processes every job using exactly numWorkers worker goroutines and
// returns one Result per job. results[i] must correspond to jobs[i].
func RunPool(jobs []Job, numWorkers int, process ProcessFunc) ([]Result, error) {
	// TODO: validate inputs
	if numWorkers <= 0 {
		return nil, errors.New("numWorkers must be a positive integer")
	}
	if process == nil {
		return nil, errors.New("process must not be nil")
	}
	if len(jobs) == 0 {
		return []Result{}, nil
	}

	results := make([]Result, len(jobs))
	jobCh := make(chan int, numWorkers) // buffered: main can queue up to numWorkers indexes ahead

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for w := 0; w < numWorkers; w++ {
		go func(workerID int) {

			defer wg.Done()

			for jobIndex := range jobCh {
				
				job := jobs[jobIndex]
				value , err := safeProcess(process, job);
				if err != nil {
					results[jobIndex] = Result{
						JobID:    job.ID,
						Value:    0,
						WorkerID: workerID,
						Err:      err,
					}
					continue;
				}
				results[jobIndex] = Result{
					JobID:    job.ID,
					Value:    value,
					WorkerID: workerID,
					Err:      err,
				}

			}
		}(w)
	}

	for i := range jobs {
		jobCh <- i
	}

	close(jobCh)

	wg.Wait()

	return results, nil
}

func main() {
	if err := PrintOddEven(10, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "odd/even:", err)
	}

	jobs := make([]Job, 100)
	for i := range jobs {
		jobs[i] = Job{ID: i, Payload: i}
	}
	square := func(j Job) (int, error) {
		time.Sleep(10 * time.Millisecond)
		return j.Payload * j.Payload, nil
	}

	start := time.Now()
	results, err := RunPool(jobs, 5, square)
	if err != nil {
		fmt.Fprintln(os.Stderr, "pool:", err)
		os.Exit(1)
	}
	fmt.Printf("processed %d jobs in %v\n", len(results), time.Since(start))
}
