package main

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var errNotImplemented = errors.New("not implemented")

// Loader fetches one account's balance, in paise, from the slow database.
type Loader func(account string) (int64, error)

// Cache keeps balances in memory so most requests never reach the database.
type Cache struct {
	mu    sync.Mutex
	data  map[string]int64 // account -> balance, filled as loads succeed
	load  Loader
	group Group // used by Get in Stage 3
}

// NewCache returns an empty cache that loads missing balances with load.
func NewCache(load Loader) *Cache {
	return &Cache{data: make(map[string]int64), load: load}
}

// ---------------------------------------------------------------------------
// Stage 1: the two obvious caches. Write both, then watch each one fail.
// ---------------------------------------------------------------------------

// GetNaive returns the cached balance, or loads and caches it on a miss. It
// holds the lock only to read and write the map. It is deliberately broken.
func (c *Cache) GetNaive(account string) (int64, error) {
	// TODO: lock, look up, unlock
	c.mu.Lock()
	price, ok := c.data[account]
	c.mu.Unlock()
	if ok {
		return price, nil
	}

	// TODO: on a miss: load (without the lock), then lock, store, unlock
	result, err := c.load(account)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.data[account] = result
	c.mu.Unlock()
	return result, nil
}

// GetLocked returns the cached balance, or loads and caches it on a miss. It
// holds the lock for the whole call, load included. It is deliberately broken.
func (c *Cache) GetLocked(account string) (int64, error) {
	// TODO: lock (and defer unlock), look up, load on a miss, store

	c.mu.Lock()
	defer c.mu.Unlock()

	price, ok := c.data[account]
	if ok {
		return price, nil
	}

	result, err := c.load(account)
	if err != nil {
		return 0, err
	}
	c.data[account] = result
	return result, nil
}

// ---------------------------------------------------------------------------
// Stage 2: singleflight.
// ---------------------------------------------------------------------------

// call is one load in progress. Callers check  want the same key wait on it.
type call struct {
	// TODO: a way for waiters to block until the load finishes, and fields
	// to hold its result

	result int64
	err    error
	done   chan struct{}
}

// Group runs at most one load per key at a time. A caller who asks for a key
// while its load is running waits for that load and gets its result. The zero
// value is ready to use. It is a small version of
// golang.org/x/sync/singleflight.
type Group struct {
	mu    sync.Mutex
	calls map[string]*call // loads running right now, by key; nil until first use
}

// Do runs fn for key and returns its result, unless a call for key is already
// running: then it waits for that call and returns the same result. shared is
// true when the result came from another caller's call. Once a call finishes,
// the next Do for that key runs fn again; Group does not cache.
func (g *Group) Do(key string, fn func() (int64, error)) (v int64, err error, shared bool) {
	// TODO: lock; if a call for key is running, unlock and wait for it

	g.mu.Lock()
	if g.calls == nil {
		g.calls = make(map[string]*call)
	}

	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		<-c.done // wait for the existing call to finish
		return c.result, c.err, true
	}

	// No call is running for this key; create a new one
	c := &call{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	// Run the function outside the lock
	c.result, c.err = fn()

	// Mark the call as done and remove it from the map
	close(c.done)

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()

	return c.result, c.err, false

}

// ---------------------------------------------------------------------------
// Stage 3: a cache that is both safe and fast.
// ---------------------------------------------------------------------------

// Get returns the cached balance, or loads it on a miss. However many callers
// miss on the same account at once, the database is asked only once, and
// callers asking for different accounts never wait for each other. A failed
// load is not cached, so the next Get tries again.
func (c *Cache) Get(account string) (int64, error) {
	// TODO: check the cache
	c.mu.Lock()
	price, ok := c.data[account]
	c.mu.Unlock()
	if ok {
		return price, nil
	}
	// TODO: on a miss, load through c.group; cache the value if it worked

	result, err, _ := c.group.Do(account, func() (int64, error) {
		p, err := c.load(account)
		if err == nil {
			c.mu.Lock()
			c.data[account] = p
			c.mu.Unlock()
		}
		return p, err
	})

	return result, err
}

func main() {
	var dbHits atomic.Int64
	c := NewCache(func(account string) (int64, error) {
		dbHits.Add(1)
		time.Sleep(100 * time.Millisecond) // a slow database query
		return 125_000, nil
	})

	var wg sync.WaitGroup
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Get("ACC1")
		}()
	}
	wg.Wait()
	fmt.Printf("1000 requests for ACC1, database hit %d time(s)\n", dbHits.Load())
}
