# Go concurrency drills

Eight hands-on drills for learning Go concurrency, aimed at backend interviews at Go-heavy fintech companies. Each drill starts with the obvious version, shows it breaking under a test (a race, a deadlock, a leak, a panic), and then builds the version that holds up.

Coming from Node.js/TypeScript, the biggest shift is that Go code really runs in parallel. Two goroutines can touch the same variable in the same nanosecond, on two CPU cores, and nothing stops them unless you do.

## Running the drills

Each drill is its own Go module in `drills/weekN/`.

```sh
cd drills/week5
go test -race -v                    # the real tests, with the race detector on
DRILL_DEMO=1 go test -run Demo -v   # watch the deliberately broken stage fail
go run .                            # run the example program
```

On Windows PowerShell, set the variable with `$env:DRILL_DEMO=1` first.

Always use `-race`. A concurrency bug can pass ten runs and crash on the eleventh. The race detector flags unsafe access every time, even on runs that happen to work.

## Progress

| Drill | Topic | Folder | Status |
|---|---|---|---|
| 1 | Odd/even ping-pong, worker pool, `recover` | [`week1`](drills/week1) | ✅ done |
| 2 | `context` timeouts and `errgroup` | [`week2`](drills/week2) | ✅ done |
| 3 | Token bucket and per-user rate limiter | [`week3`](drills/week3) | ✅ done |
| 4 | Bank transfer and lock ordering | [`week4`](drills/week4) | ✅ done |
| 5 | Graceful shutdown | [`week5`](drills/week5) | ✅ done |
| 6 | Pipelines: fan-out, fan-in, cancellation | [`week6`](drills/week6) | ✅ done |
| 7 | Singleflight cache | [`week7`](drills/week7) | ✅ done |
| 8 | Payment processor (capstone) | `week8` | ⏳ next |

## Channel cheat sheet

From [`drills/week1/read.md`](drills/week1/read.md). Most bugs in these drills trace back to one cell of this table.

| Operation | Unbuffered | Buffered | Closed | Nil |
|---|---|---|---|---|
| **send** | blocks until a receiver | blocks only when full | **panics** | blocks forever |
| **receive** | blocks until a sender | blocks only when empty | returns `zero, false` at once | blocks forever |
| **close** | ok | ok | **panics** | **panics** |

---

## Drill 1: odd/even and the worker pool

**Odd/even warm-up.** Two goroutines print 1 to n, strictly alternating. Each goroutine waits for its turn on an unbuffered channel, prints, then hands the turn to the other one, like passing a baton. Unbuffered channels make a send and a receive happen together, so they're a good fit for "you go, then me."

**Worker pool.** A fixed number of workers `range` over a channel of job indexes.

- Main sends every index, then calls `close(jobs)`. That close is what ends each worker's `range` loop.
- Each worker writes `results[i]` for its own index only. No two goroutines ever write the same slot, so no mutex is needed.
- `wg.Add` before starting a goroutine, `defer wg.Done()` inside it, and `wg.Wait()` before reading the results.

**`recover`.** A panic in one job would crash the whole program. `safeProcess` wraps each job in a deferred `recover()` and turns the panic into an error on that job's result. `recover` only works inside a deferred function, in the same goroutine that panicked.

> **Interview line:** "N workers range over a closed jobs channel, write results by index so there's no shared write, and a deferred recover turns one bad job's panic into an error instead of a crash."

## Drill 2: context and errgroup

Call several APIs in parallel, give up on all of them if one fails, and bound the whole thing with a timeout.

- `context.WithTimeout(ctx, d)` makes a context that cancels itself after `d`. Always `defer cancel()`.
- `errgroup.WithContext(ctx)` runs goroutines with `g.Go`. The first one to return an error cancels the shared context, so the others can stop early. `g.Wait()` returns that first error.
- Cancellation only works if the work **checks** the context: a fetcher must `select` on `ctx.Done()` and return `ctx.Err()`.

> **Interview line:** "Wrap the parent context with a timeout, run each call in an errgroup sharing a derived context, and have every call watch ctx.Done(). The first failure cancels the rest."

## Drill 3: rate limiting

**Token bucket.** Up to `burst` requests at once, refilling at `rate` tokens per second. The bucket doesn't need a timer: on each `Allow()` it works out how much time has passed, adds that many tokens (capped at `burst`), and takes one if it can. A mutex guards the token count.

**Testable time.** The bucket takes a `Clock func() time.Time` instead of calling `time.Now` directly. Production passes `time.Now`; tests pass a fake clock they move forward by hand, so tests are instant and exact.

**Per-user limiter.** A map from user ID to that user's bucket, behind a mutex. A background "janitor" goroutine uses a `time.Ticker` to delete users who've been idle too long, so the map doesn't grow forever.

**Stopping a background goroutine.** `Close()` closes a `stop` channel, then waits on a `done` channel that the janitor closes when it exits. `sync.Once` makes `Close()` safe to call twice.

**The lesson that stuck:** never hold a lock while waiting for another goroutine that needs that same lock. If `Close()` held the map's mutex while waiting for the janitor, and the janitor needed that mutex to finish its sweep, both would wait forever.

> **Interview line:** "Lazy refill from elapsed time, an injected clock for tests, a janitor goroutine for idle users, and a stop/done channel pair so Close can shut it down and wait for it."

## Drill 4: bank transfer and lock ordering

Move money between two accounts while thousands of transfers run at once.

**Stage 1, no locks: lost updates.** `balance -= amount` is really three steps: read, subtract, write. Two goroutines can both read 1000, both write 990, and one transfer disappears. In the demo, 5000 paise became 2576. `-race` catches it immediately.

**Stage 2, lock "from" then "to": deadlock.** A locks account 1 and waits for 2. B locks account 2 and waits for 1. That's a cycle, so neither can move, and every other transfer touching those accounts piles up behind them.

**Stage 3, the fix: always lock the lower account ID first.** Everyone takes locks in the same global order, so a cycle can't form, for any number of accounts.

Money is stored as `int64` paise, never as a float.

> **Interview line:** "A deadlock needs a cycle. If every goroutine takes locks in the same order, say by account ID, nobody can ever wait on someone further down the ladder. It's the same fix as `SELECT ... FOR UPDATE ... ORDER BY id` in Postgres."

## Drill 5: graceful shutdown

Stop a job processor during a deploy without crashing and without losing accepted work.

**Stage 1, naive: a panic and a hang.**
- `close(jobs)` followed by a late `Submit` gives `panic: send on closed channel`.
- `wg.Wait()` with a job that only stops when cancelled hangs forever, because nobody ever calls `cancel()`.

**Stage 2, the real thing.**
1. **A closed flag under a mutex.** Checking a flag and then sending is *check-then-act*: Shutdown can slip in between the two. The mutex makes "check, then send" one unit. Using a context instead of a bool doesn't fix it; the gap is still there.
2. **Never hold a lock while waiting.** Submit uses a non-blocking `select` with `default`, so it never waits while holding the lock. Shutdown unlocks **before** it waits, otherwise every late caller would wait seconds just to hear "no".
3. **`wg.Wait()` has no timeout.** A helper goroutine calls `wg.Wait()` and then closes a `done` channel, so Shutdown can `select` on `done` versus `ctx.Done()`. It's Go's version of `Promise.race`.
4. **Cancel, then still wait.** When the deadline passes, `p.cancel()` only *asks* jobs to stop. `<-done` waits until they actually have. Returning early lets `main` exit while a job is halfway through rolling back a charge.

> **Interview line:** "Flip a closed flag and close the queue under a short lock, then race a done channel against the deadline. If the deadline wins, cancel the jobs' context and still wait for the workers before returning ctx.Err()."

## Drill 6: pipelines

Parse a settlement file (`ACC1,25050` means account ACC1, ₹250.50) through stages connected by channels: `Generate → N × Parse → Merge → Run`.

**Stage 1, a generator that ignores ctx: a goroutine leak.** When the consumer stops early, the generator sits on `out <- line` forever. Nothing crashes and nothing logs; memory just grows.

**The rules:**
- **The stage that writes to a channel closes it**, with `defer close(out)` at the top so it runs on every way out.
- **Every place a goroutine can wait needs a `ctx.Done()` case.** In Parse that's two places: the receive and the send. A `select` only watches its cases while the goroutine is standing at that `select`. Cancelling only *signals*; it reaches nobody who isn't listening at that moment.
- **Fan-out:** several Parse workers read from the same channel, and each line goes to exactly one of them.
- **Fan-in:** Merge starts one forwarder goroutine per input. None of them may close `out`, because the others are still sending. Only a goroutine that waits for all of them (`wg.Wait()`) closes it, exactly once.
- **One result per line.** Decide what to send first, then send once. A bad line is still a line; stopping on the first error is Run's job.
- **`defer cancel()` in the consumer** is what frees every stage behind it when it returns early.

> **Interview line:** "Every stage owns and closes its output, and every blocking send and receive also selects on ctx.Done(). Otherwise the moment the reader leaves, everything upstream leaks."

## Drill 7: singleflight cache

1,000 people check the same balance the moment the cache restarts. Ask the database once, and never make ACC2 wait for ACC1.

**Stage 1, two obvious caches, each broken differently.**
- **Short lock, load outside it:** the *thundering herd*. All 100 callers check, all miss, all hit the database: 100 identical queries.
- **Lock held during the load:** one load per account, but every account waits in one queue. Ten 50ms loads took 505ms.

**Stage 2, `Group.Do`.** A small table of loads that are running right now, keyed by account.
- The first caller adds a `call`, unlocks, and runs the load **outside** the lock.
- Later callers find the call, unlock, and wait on its `done` channel. Closing that channel wakes every waiter at once; a plain send would wake only one.
- **Fill the box, then ring the bell.** Save the result *before* `close(done)`. Swap them and waiters silently read `0, nil`: a customer sees ₹0.00 and nothing errors.
- Remove the key when the call finishes. `Group` deduplicates loads; it doesn't remember results.

**Stage 3, `Cache.Get`.** Check the cache, and on a miss load through the group.
- Store the value **inside** the function passed to `Do`, so it's cached before `Do` removes the key. Stored after `Do` returns, there's a moment when the value is neither in flight nor cached, and a new request loads it again.
- Optionally re-check the cache at the start of that function (double-checked loading). On a load test this took extra database hits from about 50 to 0.
- Don't cache errors, or a one-second database blip becomes a permanent "balance unavailable".
- Every access to the cache map needs its lock. Two loads for different accounts writing at once gives `fatal error: concurrent map writes`, which can't be recovered.

> **Interview line:** "Keep a map of in-flight calls under a mutex. The first caller loads; the rest wait on a done channel that's closed after the result is saved. The cache remembers results, singleflight only deduplicates loads in progress."

---

## Go gotchas from these drills

| Gotcha | What happened | Fix |
|---|---|---|
| `:=` inside an `if` or `case` | made a **new** variable; the outer one stayed empty (`err`, `p`) | use `=` to assign to the existing variable |
| `break` inside a `select` | only left the `select`; the `for` kept spinning on a closed channel (464,623 empty values) | `for v := range ch`, or a labelled `break` |
| `range` wrapped in `for {}` | after the channel closed, the outer loop ran `range` again forever | `range` is already a loop; don't wrap it |
| leaving `return errNotImplemented` | the function did its work and still reported failure | check the last line of every stub you fill in |
| writing to a `nil` map | panics | `if m == nil { m = make(...) }` |
| `wg.Add` inside the goroutine | `Wait` can run before `Add` | call `wg.Add(1)` before `go` |
| `wg.Done()` after an early `return` | skipped, so `Wait` hangs | `defer wg.Done()` as the first line |
| `defer close(out)` in each fan-in worker | the first to finish closes it, the rest panic | one closer after `wg.Wait()` |
| `defer mu.Unlock()` in a function that then waits | every other caller waits too | unlock by hand before the slow part |
| "it passed when I ran it" | concurrency bugs depend on timing | run with `-race`, and stress-test |

## Rules worth remembering

1. **Name what every goroutine is blocked on.** When something hangs, list each goroutine, what it's waiting for, and who would have to act. The cycle or the missing signal is usually obvious once the list is written down.
2. **Check-then-act needs a lock around both steps.** A check is only good while nobody else can change what you checked.
3. **Never hold a lock while waiting on something slow, or on another goroutine.**
4. **Every blocking channel operation in long-lived code needs a way out:** `ctx.Done()`, a `done` channel, or a `default`.
5. **Whoever writes to a channel closes it, exactly once.**
6. **Closing a channel is a broadcast** and a publish: everything written before `close` is safe to read after `<-done`.
7. **Cancel, then wait.** Cancelling asks; waiting confirms.
8. **Money is `int64` in the smallest unit**, never a float.
9. **Prove bugs with tests** (2,000 runs, a stress loop, a goroutine leak check) instead of arguing about them.

## Visual maps

Step-through diagrams made while learning (private links, so they open only for the repo owner):

- Drill 5, graceful shutdown: https://claude.ai/artifact/GcUoEpB2QakrAwhZoMtKB8
- Drill 6, pipelines: https://claude.ai/artifact/RMJfxvLknuZbSmM91RqiDp
- Drill 7, singleflight cache: https://claude.ai/artifact/KgZQn13dgRfSdEJsurQ1cr
